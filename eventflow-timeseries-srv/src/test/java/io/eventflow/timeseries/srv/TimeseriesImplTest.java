package io.eventflow.timeseries.srv;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableMap;
import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.TimeseriesClient;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;

public class TimeseriesImplTest {
  private final DatabaseClient spanner = mock(DatabaseClient.class);
  @Rule public GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final String serverName = InProcessServerBuilder.generateName();
  private final TimeseriesClient client =
      new TimeseriesClient(
          TimeseriesGrpc.newBlockingStub(
              grpcCleanup.register(
                  InProcessChannelBuilder.forName(serverName).directExecutor().build())));

  public TimeseriesImplTest() throws IOException {
    grpcCleanup.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new TimeseriesImpl(spanner))
            .build()
            .start());
  }

  @Test
  public void getMinutelySum() {
    var tx = mock(ReadOnlyTransaction.class);
    when(tx.executeQuery(any()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("n0", Type.string()),
                    Type.StructField.of("n1", Type.float64())),
                List.of(
                    Struct.newBuilder()
                        .set("n0")
                        .to("2020-10-30T00:00:00")
                        .set("n1")
                        .to(123.4d)
                        .build(),
                    Struct.newBuilder()
                        .set("n0")
                        .to("2020-10-30T00:01:00")
                        .set("n1")
                        .to(56.789d)
                        .build())));
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofMaxStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.get(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            GetRequest.Granularity.GRAN_MINUTE,
            GetRequest.Aggregation.AGG_SUM);

    assertEquals(
        ImmutableMap.of(
            ZonedDateTime.of(2020, 10, 30, 0, 0, 0, 0, timeZone), 123.4d,
            ZonedDateTime.of(2020, 10, 30, 0, 1, 0, 0, timeZone), 56.789d),
        res);

    verify(tx)
        .executeQuery(
            Statement.newBuilder(
                    "SELECT FORMAT_TIMESTAMP(@fmt, interval_ts, @tz), SUM(value) AS value FROM intervals_minutely WHERE name = @name AND interval_ts BETWEEN @start AND @end GROUP BY 1 ORDER BY 1")
                .bind("name")
                .to("example")
                .bind("fmt")
                .to("%E4Y-%m-%dT%H:%M:00")
                .bind("tz")
                .to("America/Denver")
                .bind("start")
                .to(Timestamp.parseTimestamp("2020-10-29T00:00:00Z"))
                .bind("end")
                .to(Timestamp.parseTimestamp("2020-10-31T00:00:00Z"))
                .build());
  }

  @Test
  public void getHourlyAvg() {
    var tx = mock(ReadOnlyTransaction.class);
    when(tx.executeQuery(any()))
        .thenReturn(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("n0", Type.string()),
                    Type.StructField.of("n1", Type.float64())),
                List.of(
                    Struct.newBuilder()
                        .set("n0")
                        .to("2020-10-30T00:00:00")
                        .set("n1")
                        .to(123.4d)
                        .build(),
                    Struct.newBuilder()
                        .set("n0")
                        .to("2020-10-30T00:01:00")
                        .set("n1")
                        .to(56.789d)
                        .build())));
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofMaxStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.get(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            GetRequest.Granularity.GRAN_HOUR,
            GetRequest.Aggregation.AGG_AVG);

    assertEquals(
        ImmutableMap.of(
            ZonedDateTime.of(2020, 10, 30, 0, 0, 0, 0, timeZone), 123.4d,
            ZonedDateTime.of(2020, 10, 30, 0, 1, 0, 0, timeZone), 56.789d),
        res);

    verify(tx)
        .executeQuery(
            Statement.newBuilder(
                    "WITH intervals AS ( SELECT interval_ts, SUM(value) AS value FROM intervals_minutely WHERE name = @name AND interval_ts BETWEEN @start AND @end GROUP BY 1 ) SELECT FORMAT_TIMESTAMP(@fmt, interval_ts, @tz), AVG(value) FROM intervals GROUP BY 1 ORDER BY 1")
                .bind("name")
                .to("example")
                .bind("fmt")
                .to("%E4Y-%m-%dT%H:00:00")
                .bind("tz")
                .to("America/Denver")
                .bind("start")
                .to(Timestamp.parseTimestamp("2020-10-29T00:00:00Z"))
                .bind("end")
                .to(Timestamp.parseTimestamp("2020-10-31T00:00:00Z"))
                .build());
  }
}
