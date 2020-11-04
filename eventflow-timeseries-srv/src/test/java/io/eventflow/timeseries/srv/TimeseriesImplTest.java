/*
 * Copyright 2020 Coda Hale
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
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
import io.grpc.testing.GrpcServerRule;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class TimeseriesImplTest {
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Rule public GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Mock private DatabaseClient spanner;
  @Mock private ReadOnlyTransaction tx;
  private TimeseriesClient client;

  @Before
  public void setUp() {
    grpcServerRule.getServiceRegistry().addService(new TimeseriesImpl(spanner));
    this.client = new TimeseriesClient(TimeseriesGrpc.newBlockingStub(grpcServerRule.getChannel()));
  }

  @Test
  public void sumOfMinutelySums() {
    var rs =
        spy(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("n0", Type.timestamp()),
                    Type.StructField.of("n1", Type.float64())),
                List.of(
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:00:00Z"))
                        .set("n1")
                        .to(123.4d)
                        .build(),
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:01:00"))
                        .set("n1")
                        .to(56.789d)
                        .build())));

    when(tx.executeQuery(any())).thenReturn(rs);
    when(tx.getReadTimestamp()).thenReturn(Timestamp.ofTimeSecondsAndNanos(12345678, 0));
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

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    var inOrder = Mockito.inOrder(tx, rs);
    inOrder
        .verify(tx)
        .executeQuery(
            Statement.newBuilder(
                    "SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz), SUM(value) FROM intervals_minutes WHERE name = @name AND interval_ts BETWEEN @start AND @end GROUP BY 1 ORDER BY 1")
                .bind("name")
                .to("example")
                .bind("tz")
                .to("America/Denver")
                .bind("start")
                .to(Timestamp.parseTimestamp("2020-10-29T00:00:00Z"))
                .bind("end")
                .to(Timestamp.parseTimestamp("2020-10-31T00:00:00Z"))
                .build());
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }

  @Test
  public void averageOfHourlySums() {
    var rs =
        spy(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("n0", Type.timestamp()),
                    Type.StructField.of("n1", Type.float64())),
                List.of(
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:00:00Z"))
                        .set("n1")
                        .to(123.4d)
                        .build(),
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:01:00Z"))
                        .set("n1")
                        .to(56.789d)
                        .build())));
    when(tx.executeQuery(any())).thenReturn(rs);
    when(tx.getReadTimestamp()).thenReturn(Timestamp.ofTimeSecondsAndNanos(12345678, 0));
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

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    var inOrder = Mockito.inOrder(tx, rs);
    inOrder
        .verify(tx)
        .executeQuery(
            Statement.newBuilder(
                    "WITH intervals AS (SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, SUM(value) AS value FROM intervals_minutes WHERE name = @name AND interval_ts BETWEEN @start AND @end GROUP BY 1) SELECT TIMESTAMP_TRUNC(interval_ts, HOUR, @tz), AVG(value) FROM intervals GROUP BY 1 ORDER BY 1")
                .bind("name")
                .to("example")
                .bind("tz")
                .to("America/Denver")
                .bind("start")
                .to(Timestamp.parseTimestamp("2020-10-29T00:00:00Z"))
                .bind("end")
                .to(Timestamp.parseTimestamp("2020-10-31T00:00:00Z"))
                .build());
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }

  @Test
  public void maxOfMinutelyMins() {
    var rs =
        spy(
            ResultSets.forRows(
                Type.struct(
                    Type.StructField.of("n0", Type.timestamp()),
                    Type.StructField.of("n1", Type.float64())),
                List.of(
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:00:00Z"))
                        .set("n1")
                        .to(123.4d)
                        .build(),
                    Struct.newBuilder()
                        .set("n0")
                        .to(Timestamp.parseTimestamp("2020-10-30T00:01:00"))
                        .set("n1")
                        .to(56.789d)
                        .build())));

    when(tx.executeQuery(any())).thenReturn(rs);
    when(tx.getReadTimestamp()).thenReturn(Timestamp.ofTimeSecondsAndNanos(12345678, 0));
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofMaxStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.get(
            "example.min",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            GetRequest.Granularity.GRAN_MINUTE,
            GetRequest.Aggregation.AGG_MAX);

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    var inOrder = Mockito.inOrder(tx, rs);
    inOrder
        .verify(tx)
        .executeQuery(
            Statement.newBuilder(
                    "WITH intervals AS (SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, MIN(value) AS value FROM intervals_minutes WHERE name = @name AND interval_ts BETWEEN @start AND @end GROUP BY 1) SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz), MAX(value) FROM intervals GROUP BY 1 ORDER BY 1")
                .bind("name")
                .to("example.min")
                .bind("tz")
                .to("America/Denver")
                .bind("start")
                .to(Timestamp.parseTimestamp("2020-10-29T00:00:00Z"))
                .bind("end")
                .to(Timestamp.parseTimestamp("2020-10-31T00:00:00Z"))
                .build());
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }
}
