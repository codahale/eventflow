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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

public class TimeseriesImpl extends TimeseriesGrpc.TimeseriesImplBase {
  private final DatabaseClient spanner;

  public TimeseriesImpl(DatabaseClient spanner) {
    this.spanner = spanner;
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    var zoneId = ZoneId.of(request.getTimeZone());
    var statement =
        Statement.newBuilder(query(request.getAggregation()))
            .bind("name")
            .to(request.getName())
            .bind("fmt")
            .to(granFmt(request.getGranularity()))
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
        resp.addTimestamps(
            DateTimeFormatter.ISO_LOCAL_DATE_TIME
                .parse(results.getString(0), LocalDateTime::from)
                .atZone(zoneId)
                .toEpochSecond());
        resp.addValues(results.getDouble(1));
      }
      responseObserver.onNext(resp.build());
      responseObserver.onCompleted();
    }
  }

  /*
   CREATE TABLE intervals_minutely (
     name STRING(1000) NOT NULL,
     interval_ts TIMESTAMP NOT NULL,
     insert_id INT64 NOT NULL,
     value FLOAT64 NOT NULL,
     insert_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
   ) PRIMARY KEY (name, interval_ts, insert_id)
  */

  private String query(GetRequest.Aggregation aggregation) {
    if (aggregation == GetRequest.Aggregation.AGG_SUM) {
      return "SELECT FORMAT_TIMESTAMP(@fmt, interval_ts, @tz), SUM(value) AS value"
          + " FROM intervals_minutely "
          + "WHERE name = @name "
          + "AND interval_ts BETWEEN @start AND @end "
          + "GROUP BY 1 "
          + "ORDER BY 1";
    }

    return "WITH intervals AS ("
        + " SELECT interval_ts, SUM(value) AS value"
        + " FROM intervals_minutely"
        + " WHERE name = @name"
        + " AND interval_ts BETWEEN @start AND @end"
        + " GROUP BY 1"
        + " )"
        + " SELECT FORMAT_TIMESTAMP(@fmt, interval_ts, @tz), "
        + aggFunc(aggregation)
        + "(value) FROM intervals GROUP BY 1 ORDER BY 1";
  }

  private String granFmt(GetRequest.Granularity granularity) {
    switch (granularity) {
      case GRAN_UNKNOWN:
        throw new IllegalArgumentException("missing granularity");
      case GRAN_MINUTE:
        return "%E4Y-%m-%dT%H:%M:00";
      case GRAN_HOUR:
        return "%E4Y-%m-%dT%H:00:00";
      case GRAN_DAY:
        return "%E4Y-%m-%dT00:00:00";
      case GRAN_MONTH:
        return "%E4Y-%m-01T00:00:00";
      case GRAN_YEAR:
        return "%E4Y-01-01T00:00:00";
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
