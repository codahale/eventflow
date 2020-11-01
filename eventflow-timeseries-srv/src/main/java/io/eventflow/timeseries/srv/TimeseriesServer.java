package io.eventflow.timeseries.srv;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.io.Resources;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opencensus.common.Duration;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TimeseriesServer {
  private final Server server;

  public TimeseriesServer(DatabaseClient spanner, int port) {
    this.server = ServerBuilder.forPort(port).addService(new TimeseriesImpl(spanner)).build();
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  public void start() throws IOException {
    server.start();
    System.out.println("server is running");
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println("*** shutting down gRPC server since JVM is shutting down");
                  try {
                    TimeseriesServer.this.stop();
                  } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                  }
                  System.err.println("*** server shut down");
                }));
  }

  public void stop() throws InterruptedException {
    if (server != null) {
      server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    var project = args[0];
    var instance = args[1];
    var database = args[2];
    var port = 8080;
    if (args.length > 3) {
      port = Integer.parseInt(args[3]);
    }

    // Register all gRPC views and enable stats.
    RpcViews.registerAllGrpcViews();

    // Trace every call.
    var traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
        traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());

    // Export stats to Stackdriver every 5s.
    StackdriverStatsExporter.createAndRegister(
        StackdriverStatsConfiguration.builder()
            .setProjectId(project)
            .setExportInterval(Duration.create(5, 0))
            .build());

    // Export traces to Stackdriver.
    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder()
            .setProjectId(project)
            .setFixedAttributes(
                Map.of("git-version", AttributeValue.stringAttributeValue(gitVersion())))
            .build());

    try (var spanner = SpannerOptions.newBuilder().build().getService()) {
      var client = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      var server = new TimeseriesServer(client, port);
      server.start();
      server.blockUntilShutdown();
    }
  }

  private static String gitVersion() {
    try {
      var url = Resources.getResource("git.properties");
      var props = new Properties();
      props.load(url.openStream());
      return props.getProperty("git.commit.id.abbrev");
    } catch (IOException e) {
      return "unknown";
    }
  }
}
