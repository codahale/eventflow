/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.eventflow.timeseries.srv;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.io.Resources;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;

public class TimeSeriesServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesServer.class);
  private final Server server;

  public TimeSeriesServer(DatabaseClient spanner, int port, URI redisUri, Duration minCacheAge) {
    var cache = new RedisCache(new JedisPool(redisUri));
    var impl = new TimeSeriesServiceImpl(spanner, cache, minCacheAge, Clock.systemUTC());
    this.server = ServerBuilder.forPort(port).addService(impl).build();
  }

  @SuppressWarnings("CatchAndPrintStackTrace")
  public void start() throws IOException {
    server.start();
    LOGGER.info("server started, version = {}", gitVersion());
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println("*** shutting down gRPC server since JVM is shutting down");
                  try {
                    TimeSeriesServer.this.stop();
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
    var instance = Objects.requireNonNull(System.getenv("SPANNER_INSTANCE"));
    var database = Objects.requireNonNull(System.getenv("SPANNER_DB"));
    var port = Integer.parseInt(getEnv("PORT", "8080"));
    var redisUri = getEnv("REDIS_URI", "redis://localhost:6379");
    var minCacheAge = Duration.parse(getEnv("MIN_CACHE_AGE", "PT168H"));

    setupTelemetry(port);

    try (var spanner = SpannerOptions.newBuilder().build().getService()) {
      var client =
          spanner.getDatabaseClient(
              DatabaseId.of(spanner.getOptions().getProjectId(), instance, database));
      var server = new TimeSeriesServer(client, port, URI.create(redisUri), minCacheAge);
      server.start();
      server.blockUntilShutdown();
    }
  }

  private static String getEnv(String name, String defaultValue) {
    var s = System.getenv(name);
    if (s != null) {
      return s;
    }
    return defaultValue;
  }

  private static void setupTelemetry(int port) throws IOException {
    var traceConfig = Tracing.getTraceConfig();
    traceConfig.updateActiveTraceParams(
        traceConfig.getActiveTraceParams().toBuilder().setSampler(Samplers.alwaysSample()).build());

    RpcViews.registerAllGrpcViews();
    TimeSeriesStats.registerViews();

    ZPageHandlers.startHttpServerAndRegisterAll(port + 1);
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
