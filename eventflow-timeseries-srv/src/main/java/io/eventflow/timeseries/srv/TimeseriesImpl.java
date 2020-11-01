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
  private final DatabaseClient spanner;

  public TimeseriesImpl(DatabaseClient spanner) {
    this.spanner = spanner;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    var statement =
        Statement.newBuilder(query(request.getGranularity(), request.getAggregation()))
            .bind("name")
            .to(request.getName())
            .bind("tz")
            .to(request.getTimeZone())
            .bind("start")
            .to(Timestamp.ofTimeMicroseconds(Timestamps.toMicros(request.getStart())))
            .bind("end")
            .to(Timestamp.ofTimeMicroseconds(Timestamps.toMicros(request.getEnd())))
            .build();

    try (var results =
        spanner
            .singleUseReadOnlyTransaction(TimestampBound.ofMaxStaleness(1, TimeUnit.MINUTES))
            .executeQuery(statement)) {
      var resp = GetResponse.newBuilder();
      while (results.next()) {
        resp.addTimestamps(results.getTimestamp(0).getSeconds());
        resp.addValues(results.getDouble(1));
      }
      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    }
  }

  /*
   CREATE TABLE intervals_minutes (
     name STRING(1000) NOT NULL,
     interval_ts TIMESTAMP NOT NULL,
     insert_id INT64 NOT NULL,
     value FLOAT64 NOT NULL,
     insert_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
   ) PRIMARY KEY (name, interval_ts, insert_id)
  */

  private String query(GetRequest.Granularity granularity, GetRequest.Aggregation aggregation) {
    // Because there may be multiple value rows per interval, aggregation functions other than SUM
    // use a common table expression to materialize the actual minutely intervals in order to be
    // correct. For example, if one interval has two rows of 10 and 20, and another interval has one
    // row of 21, the MAX of those two intervals should be 30, not 21. But because SUM is what's
    // needed to aggregate the value rows, we can special-case SUM queries to avoid the overhead of
    // the CTE.

    if (aggregation == GetRequest.Aggregation.AGG_SUM) {
      return "SELECT TIMESTAMP_TRUNC(interval_ts, "
          + granPart(granularity)
          + ", @tz), SUM(value) AS value"
          + " FROM intervals_minutes"
          + " WHERE name = @name"
          + " AND interval_ts BETWEEN @start AND @end"
          + " GROUP BY 1"
          + " ORDER BY 1";
    }

    return "WITH intervals AS ("
        + " SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts, SUM(value) AS value"
        + " FROM intervals_minutes"
        + " WHERE name = @name"
        + " AND interval_ts BETWEEN @start AND @end"
        + " GROUP BY 1"
        + " )"
        + " SELECT TIMESTAMP_TRUNC(interval_ts, "
        + granPart(granularity)
        + ", @tz), "
        + aggFunc(aggregation)
        + "(value) FROM intervals GROUP BY 1 ORDER BY 1";
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
        return "SUM";
      case AGG_MAX:
        return "MAX";
      case AGG_MIN:
        return "MIN";
      case AGG_AVG:
        return "AVG";
      default:
        throw new IllegalArgumentException("unrecognized aggregation function");
    }
  }
}
