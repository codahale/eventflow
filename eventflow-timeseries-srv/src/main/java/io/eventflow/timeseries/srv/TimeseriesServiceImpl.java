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

import static io.opencensus.trace.AttributeValue.booleanAttributeValue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.AggregateFunction;
import io.eventflow.timeseries.api.GetIntervalValuesRequest;
import io.eventflow.timeseries.api.Granularity;
import io.eventflow.timeseries.api.IntervalValues;
import io.eventflow.timeseries.api.TimeseriesServiceGrpc;
import io.grpc.stub.StreamObserver;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class TimeseriesServiceImpl extends TimeseriesServiceGrpc.TimeseriesServiceImplBase {
  private static final Tracer tracer = Tracing.getTracer();

  private final DatabaseClient spanner;
  private final RedisCache cache;
  private final long minCacheAgeMs;
  private final Clock clock;

  public TimeseriesServiceImpl(
      DatabaseClient spanner, RedisCache cache, Duration minCacheAge, Clock clock) {
    this.spanner = spanner;
    this.cache = cache;
    this.minCacheAgeMs = minCacheAge.toMillis();
    this.clock = clock;
  }

  @Override
  public void getIntervalValues(
      GetIntervalValuesRequest request, StreamObserver<IntervalValues> responseObserver) {
    var key = request.toByteArray();
    var cacheable = isCacheable(request);
    tracer.getCurrentSpan().putAttribute("cacheable", booleanAttributeValue(cacheable));

    // If the request is cacheable (i.e., it's for intervals which are sufficiently in the past),
    // check the cache for the response.
    if (cacheable) {
      var resp = cache.getIfPresent(key);
      // If we find a cached response, serve it.
      if (resp != null) {
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
        return;
      }
    }

    // Generate the Spanner query to pull timestamps and values for the time series.
    var statement =
        query(request)
            .bind("name")
            .to(request.getName())
            .bind("tz")
            .to(request.getTimeZone())
            .bind("start")
            .to(Timestamp.fromProto(request.getStart()))
            .bind("end")
            .to(Timestamp.fromProto(request.getEnd()))
            .build();

    // Use a single-use, read-only transaction with an exact staleness bound. This allows results to
    // be served from replicas without having to negotiate between them, which reduces latency and
    // contention at the end of the time series.
    try (var tx =
            spanner.singleUseReadOnlyTransaction(
                TimestampBound.ofExactStaleness(1, TimeUnit.MINUTES));
        var results = tx.executeQuery(statement)) {

      // Convert the query results to a list of interval values.
      var builder = IntervalValues.newBuilder();
      while (results.next()) {
        builder.addTimestamps(results.getTimestamp(0).getSeconds());
        builder.addValues(results.getDouble(1));
      }
      // Include the read timestamp from Spanner.
      builder.setReadTimestamp(tx.getReadTimestamp().toProto());
      var values = builder.build();

      // Return the response.
      responseObserver.onNext(values);
      responseObserver.onCompleted();

      // If the request was cacheable, but wasn't in the cache, put it in the cache.
      if (cacheable) {
        cache.put(key, values);
      }
    }
  }

  private Statement.Builder query(GetIntervalValuesRequest request) {
    // Detect the interval aggregate function based on the name. This function is used to aggregate
    // the multiple possible rows for each minutely interval into a single value.
    var intervalAggFunc = intervalAggFunc(request.getName());

    // Determine the query aggregate function. This function is used to aggregate minutely interval
    // values into less granular interval values. If none is specified, the interval aggregate
    // function is used.
    var queryAggFunc = request.getAggregateFunction();
    if (queryAggFunc == AggregateFunction.AGG_NONE) {
      queryAggFunc = intervalAggFunc;
    }

    // If the interval aggregate function is the same as the query aggregate function, we can
    // aggregate the interval values directly, because the aggregate functions are commutative: the
    // sum of sums of values is the sum of the values, etc.
    if (intervalAggFunc.equals(queryAggFunc)) {
      return Statement.newBuilder("SELECT TIMESTAMP_TRUNC(interval_ts, ")
          .append(granPart(request.getGranularity()))
          .append(", @tz), ")
          .append(aggFunc(intervalAggFunc))
          .append(" FROM intervals_minutes")
          .append(" WHERE name = @name")
          .append(" AND interval_ts BETWEEN @start AND @end")
          .append(" GROUP BY 1 ORDER BY 1");
    }

    // If the interval aggregate function and the query aggregate function are different, then we
    // need to materialize the actual interval values in a common table expression, because e.g. the
    // average of values isn't the same as the average of the sum of values.
    return Statement.newBuilder("WITH intervals AS (")
        .append("SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, ")
        .append(aggFunc(intervalAggFunc))
        .append(" AS value")
        .append(" FROM intervals_minutes")
        .append(" WHERE name = @name")
        .append(" AND interval_ts BETWEEN @start AND @end")
        .append(" GROUP BY 1")
        .append(")")
        .append(" SELECT TIMESTAMP_TRUNC(interval_ts, ")
        .append(granPart(request.getGranularity()))
        .append(", @tz), ")
        .append(aggFunc(queryAggFunc))
        .append(" FROM intervals GROUP BY 1 ORDER BY 1");
  }

  // Detect the interval aggregation function based on the metric name.
  private AggregateFunction intervalAggFunc(String name) {
    if (name.endsWith(".min")) {
      return AggregateFunction.AGG_MIN;
    } else if (name.endsWith(".max")) {
      return AggregateFunction.AGG_MAX;
    }
    return AggregateFunction.AGG_SUM;
  }

  private String granPart(Granularity granularity) {
    switch (granularity) {
      case GRAN_MINUTE:
        return "MINUTE";
      case GRAN_HOUR:
        return "HOUR";
      case GRAN_DAY:
        return "DAY";
      case GRAN_WEEK:
        return "WEEK";
      case GRAN_ISOWEEK:
        return "ISOWEEK";
      case GRAN_MONTH:
        return "MONTH";
      case GRAN_QUARTER:
        return "QUARTER";
      case GRAN_YEAR:
        return "YEAR";
      case GRAN_ISOYEAR:
        return "ISOYEAR";
      default:
        throw new IllegalArgumentException("unrecognized granularity");
    }
  }

  private String aggFunc(AggregateFunction aggregateFunction) {
    switch (aggregateFunction) {
      case AGG_SUM:
        return "SUM(value)";
      case AGG_MAX:
        return "MAX(value)";
      case AGG_MIN:
        return "MIN(value)";
      case AGG_AVG:
        return "AVG(value)";
      default:
        throw new IllegalArgumentException("unrecognized aggregate function");
    }
  }

  private boolean isCacheable(GetIntervalValuesRequest request) {
    return Timestamps.toMillis(request.getEnd()) < clock.millis() - minCacheAgeMs;
  }
}
