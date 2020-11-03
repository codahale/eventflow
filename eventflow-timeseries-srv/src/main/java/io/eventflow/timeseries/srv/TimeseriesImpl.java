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
        query(request.getGranularity(), request.getAggregation())
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
      var resp = GetResponse.newBuilder();
      while (results.next()) {
        resp.addTimestamps(results.getTimestamp(0).getSeconds());
        resp.addValues(results.getDouble(1));
      }
      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    }
  }

  private Statement.Builder query(
      GetRequest.Granularity granularity, GetRequest.Aggregation aggregation) {

    // Because there may be multiple value rows per interval, aggregation functions other than SUM
    // use a common table expression to materialize the actual minutely intervals in order to be
    // correct. For example, if one interval has two rows of 10 and 20, and another interval has one
    // row of 21, the MAX of those two intervals should be 30, not 21. But because SUM is what's
    // needed to aggregate the value rows, we can special-case SUM queries to avoid the overhead of
    // the CTE.

    if (aggregation == GetRequest.Aggregation.AGG_SUM) {
      return Statement.newBuilder("SELECT TIMESTAMP_TRUNC(interval_ts, ")
          .append(granPart(granularity))
          .append(", @tz), SUM(value) AS value")
          .append(" FROM intervals_minutes")
          .append(" WHERE name = @name")
          .append(" AND interval_ts BETWEEN @start AND @end")
          .append(" GROUP BY 1")
          .append(" ORDER BY 1");
    }

    return Statement.newBuilder("WITH intervals AS (")
        .append("SELECT TIMESTAMP_TRUNC(interval_ts, MINUTE, @tz) AS interval_ts,")
        .append(" SUM(value) AS value")
        .append(" FROM intervals_minutes")
        .append(" WHERE name = @name")
        .append(" AND interval_ts BETWEEN @start AND @end")
        .append(" GROUP BY 1")
        .append(")")
        .append(" SELECT TIMESTAMP_TRUNC(interval_ts, ")
        .append(granPart(granularity))
        .append(", @tz), ")
        .append(aggFunc(aggregation))
        .append("(value) FROM intervals GROUP BY 1 ORDER BY 1");
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
