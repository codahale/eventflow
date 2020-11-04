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
import com.google.protobuf.util.Timestamps;
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
        query(
                request.getName(),
                granPart(request.getGranularity()),
                aggFunc(request.getAggregation()))
            .bind("name")
            .to(request.getName())
            .bind("tz")
            .to(request.getTimeZone())
            .bind("start")
            .to(Timestamp.ofTimeMicroseconds(Timestamps.toMicros(request.getStart())))
            .bind("end")
            .to(Timestamp.ofTimeMicroseconds(Timestamps.toMicros(request.getEnd())))
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

  private Statement.Builder query(String name, String granPart, String aggFunc) {

    // Detect the interval aggregation type based on name.
    var intervalAggFunc = "SUM(value)";
    if (name.endsWith(".min")) {
      intervalAggFunc = "MIN(value)";
    } else if (name.endsWith(".max")) {
      intervalAggFunc = "MIN(value)";
    }

    return Statement.newBuilder("WITH intervals AS (")
        .append("SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, ")
        .append(intervalAggFunc)
        .append(" AS value")
        .append(" FROM intervals_minutes")
        .append(" WHERE name = @name")
        .append(" AND interval_ts BETWEEN @start AND @end")
        .append(" GROUP BY 1")
        .append(")")
        .append(" SELECT TIMESTAMP_TRUNC(interval_ts, ")
        .append(granPart)
        .append(", @tz), ")
        .append(aggFunc)
        .append(" FROM intervals GROUP BY 1 ORDER BY 1");
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
