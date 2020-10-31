package io.eventflow.timeseries.srv;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TimeseriesServer {
  private final Server server;

  public TimeseriesServer(DatabaseClient spanner, int port) {
    this.server = ServerBuilder.forPort(port).addService(new TimeseriesImpl(spanner)).build();
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  public void start() throws IOException {
    server.start();
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
    // TODO read params from args
    var project = "your-project-id";
    var instanceId = "test-instance";
    var database = "timeseries";
    var port = 8080;

    try (var spanner = SpannerOptions.newBuilder().build().getService()) {
      var client = spanner.getDatabaseClient(DatabaseId.of(project, instanceId, database));
      var server = new TimeseriesServer(client, port);
      server.start();
      server.blockUntilShutdown();
    }
  }
}
