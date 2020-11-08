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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.AggregateFunction;
import io.eventflow.timeseries.api.GetIntervalValuesRequest;
import io.eventflow.timeseries.api.Granularity;
import io.eventflow.timeseries.api.IntervalValues;
import io.eventflow.timeseries.api.TimeSeriesClient;
import io.eventflow.timeseries.api.TimeSeriesServiceGrpc;
import io.grpc.testing.GrpcServerRule;
import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
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

public class TimeSeriesServiceImplTest {
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Rule public GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Mock private DatabaseClient spanner;
  @Mock private ReadOnlyTransaction tx;
  @Mock private Clock clock;
  @Mock private RedisCache cache;
  private TimeSeriesClient client;

  @Before
  public void setUp() {
    grpcServerRule
        .getServiceRegistry()
        .addService(new TimeSeriesServiceImpl(spanner, cache, Duration.ofDays(1), clock));
    this.client =
        new TimeSeriesClient(TimeSeriesServiceGrpc.newBlockingStub(grpcServerRule.getChannel()));
  }

  @Test
  public void cacheMiss() throws ParseException {
    var timeZone = ZoneId.of("America/Denver");

    when(clock.millis()).thenReturn(Instant.parse("2020-12-31T20:00:00Z").toEpochMilli());
    when(cache.getIfPresent(any())).thenReturn(null);
    var key =
        GetIntervalValuesRequest.newBuilder()
            .setName("example")
            .setStart(Timestamps.parse("2020-10-29T00:00:00Z"))
            .setEnd(Timestamps.parse("2020-10-31T00:00:00Z"))
            .setTimeZone(timeZone.getId())
            .setGranularity(Granularity.GRAN_MINUTE)
            .setAggregateFunction(AggregateFunction.AGG_SUM)
            .build()
            .toByteArray();

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
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofExactStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var res =
        client.getIntervalValues(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            Granularity.GRAN_MINUTE,
            AggregateFunction.AGG_SUM);

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    var inOrder = Mockito.inOrder(tx, rs, cache);
    inOrder.verify(cache).getIfPresent(key);
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
    inOrder.verify(rs, times(3)).next();
    inOrder.verify(tx).getReadTimestamp();
    inOrder
        .verify(cache)
        .put(
            key,
            IntervalValues.newBuilder()
                .addTimestamps(1604016000)
                .addTimestamps(1604016060)
                .addValues(123.4)
                .addValues(56.789)
                .setReadTimestamp(
                    com.google.protobuf.Timestamp.newBuilder().setSeconds(12345678).build())
                .build());
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }

  @Test
  public void cacheHit() throws ParseException {
    var timeZone = ZoneId.of("America/Denver");

    when(clock.millis()).thenReturn(Instant.parse("2020-12-31T20:00:00Z").toEpochMilli());
    when(cache.getIfPresent(any()))
        .thenReturn(
            IntervalValues.newBuilder()
                .addTimestamps(1604016000)
                .addTimestamps(1604016060)
                .addValues(123.4)
                .addValues(56.789)
                .setReadTimestamp(
                    com.google.protobuf.Timestamp.newBuilder().setSeconds(12345678).build())
                .build());

    var res =
        client.getIntervalValues(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            Granularity.GRAN_MINUTE,
            AggregateFunction.AGG_SUM);

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    var key =
        GetIntervalValuesRequest.newBuilder()
            .setName("example")
            .setStart(Timestamps.parse("2020-10-29T00:00:00Z"))
            .setEnd(Timestamps.parse("2020-10-31T00:00:00Z"))
            .setTimeZone(timeZone.getId())
            .setGranularity(Granularity.GRAN_MINUTE)
            .setAggregateFunction(AggregateFunction.AGG_SUM)
            .build()
            .toByteArray();

    verify(cache).getIfPresent(key);
    verifyNoInteractions(spanner);
  }

  @Test
  public void sumOfMinutelySums() {
    when(clock.millis()).thenReturn(Instant.parse("2020-10-31T20:00:00Z").toEpochMilli());

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
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofExactStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.getIntervalValues(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            Granularity.GRAN_MINUTE,
            AggregateFunction.AGG_SUM);

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    verifyNoInteractions(cache);
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
    inOrder.verify(rs, times(3)).next();
    inOrder.verify(tx).getReadTimestamp();
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }

  @Test
  public void averageOfHourlySums() {
    when(clock.millis()).thenReturn(Instant.parse("2020-10-31T20:00:00Z").toEpochMilli());

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
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofExactStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.getIntervalValues(
            "example",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            Granularity.GRAN_HOUR,
            AggregateFunction.AGG_AVG);

    assertThat(res)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(2020, 10, 29, 18, 0, 0, 0, timeZone), 123.4d,
                ZonedDateTime.of(2020, 10, 29, 18, 1, 0, 0, timeZone), 56.789d));

    verifyNoInteractions(cache);
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
    inOrder.verify(rs, times(3)).next();
    inOrder.verify(tx).getReadTimestamp();
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
    when(spanner.singleUseReadOnlyTransaction(TimestampBound.ofExactStaleness(1, TimeUnit.MINUTES)))
        .thenReturn(tx);

    var timeZone = ZoneId.of("America/Denver");
    var res =
        client.getIntervalValues(
            "example.min",
            Instant.parse("2020-10-29T00:00:00Z"),
            Instant.parse("2020-10-31T00:00:00Z"),
            timeZone,
            Granularity.GRAN_MINUTE,
            AggregateFunction.AGG_MAX);

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
    inOrder.verify(rs, times(3)).next();
    inOrder.verify(tx).getReadTimestamp();
    inOrder.verify(rs).close();
    inOrder.verify(tx).close();
  }
}
