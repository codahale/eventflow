package io.eventflow.timeseries.srv;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.io.Resources;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TimeseriesServer {
  private final Server server;

  public TimeseriesServer(
      DatabaseClient spanner, int port, CaffeineSpec caffeineSpec, Duration maxCacheAge) {
    var impl = new TimeseriesImpl(spanner);
    this.server =
        ServerBuilder.forPort(port)
            .addService(
                new CachedTimeseriesImpl(impl, caffeineSpec, Clock.systemUTC(), maxCacheAge))
            .build();
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  public void start() throws IOException {
    server.start();
    System.out.println("server is running, " + gitVersion());
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

    var cacheSpec = "maximumSize=1000";
    if (args.length > 4) {
      cacheSpec = args[4];
    }

    var maxCacheAge = Duration.ofDays(7);
    if (args.length > 5) {
      maxCacheAge = Duration.parse(args[5]);
    }

    try (var spanner = SpannerOptions.newBuilder().build().getService()) {
      var client = spanner.getDatabaseClient(DatabaseId.of(project, instance, database));
      var server = new TimeseriesServer(client, port, CaffeineSpec.parse(cacheSpec), maxCacheAge);
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
