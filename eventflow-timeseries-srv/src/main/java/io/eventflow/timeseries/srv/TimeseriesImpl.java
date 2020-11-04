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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.GetResponse;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.TimeUnit;

public class TimeseriesImpl extends TimeseriesGrpc.TimeseriesImplBase {
  private static final TimestampBound MAX_STALENESS =
      TimestampBound.ofMaxStaleness(1, TimeUnit.MINUTES);

  private final DatabaseClient spanner;

  public TimeseriesImpl(DatabaseClient spanner) {
    this.spanner = spanner;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    var statement =
        Statement.newBuilder("WITH intervals AS (")
            .append("SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, ")
            .append(intervalAggFunc(request.getName()))
            .append(" AS value")
            .append(" FROM intervals_minutes")
            .append(" WHERE name = @name")
            .append(" AND interval_ts BETWEEN @start AND @end")
            .append(" GROUP BY 1")
            .append(")")
            .append(" SELECT TIMESTAMP_TRUNC(interval_ts, ")
            .append(granPart(request.getGranularity()))
            .append(", @tz), ")
            .append(aggFunc(request.getAggregation()))
            .append(" FROM intervals GROUP BY 1 ORDER BY 1")
            .bind("name")
            .to(request.getName())
            .bind("tz")
            .to(request.getTimeZone())
            .bind("start")
            .to(Timestamp.fromProto(request.getStart()))
            .bind("end")
            .to(Timestamp.fromProto(request.getEnd()))
            .build();

    try (var tx = spanner.singleUseReadOnlyTransaction(MAX_STALENESS);
        var results = tx.executeQuery(statement)) {
      var resp = GetResponse.newBuilder().setReadTimestamp(tx.getReadTimestamp().toProto());
      while (results.next()) {
        resp.addTimestamps(results.getTimestamp(0).getSeconds());
        resp.addValues(results.getDouble(1));
      }
      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    }
  }

  // Detect the interval aggregation function based on the metric name.
  private String intervalAggFunc(String name) {
    if (name.endsWith(".min")) {
      return "MIN(value)";
    } else if (name.endsWith(".max")) {
      return "MIN(value)";
    }
    return "SUM(value)";
  }

  private String granPart(GetRequest.Granularity granularity) {
    switch (granularity) {
      case GRAN_UNKNOWN:
        throw new IllegalArgumentException("missing granularity");
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

  private String aggFunc(GetRequest.Aggregation aggregation) {
    switch (aggregation) {
      case AGG_UNKNOWN:
        throw new IllegalArgumentException("missing aggregation function");
      case AGG_SUM:
        return "SUM(value)";
      case AGG_MAX:
        return "MAX(value)";
      case AGG_MIN:
        return "MIN(value)";
      case AGG_AVG:
        return "AVG(value)";
      default:
        throw new IllegalArgumentException("unrecognized aggregation function");
    }
  }
}
