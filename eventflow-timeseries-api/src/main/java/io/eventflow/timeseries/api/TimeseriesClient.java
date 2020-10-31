package io.eventflow.timeseries.api;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.GetRequest.Aggregation;
import io.eventflow.timeseries.api.GetRequest.Granularity;
import io.eventflow.timeseries.api.TimeseriesGrpc.TimeseriesBlockingStub;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeseriesClient {
  private final TimeseriesBlockingStub stub;

  public TimeseriesClient(TimeseriesBlockingStub stub) {
    this.stub = stub;
  }

  public ImmutableMap<ZonedDateTime, Double> get(
      String name,
      Instant start,
      Instant end,
      ZoneId timeZone,
      Granularity granularity,
      Aggregation aggregation) {
    var req =
        GetRequest.newBuilder()
            .setName(name)
            .setStart(Timestamps.fromSeconds(start.getEpochSecond()))
            .setEnd(Timestamps.fromSeconds(end.getEpochSecond()))
            .setTimeZone(timeZone.getId())
            .setGranularity(granularity)
            .setAggregation(aggregation)
            .build();
    var resp = stub.get(req);
    var results =
        ImmutableMap.<ZonedDateTime, Double>builderWithExpectedSize(resp.getTimestampsCount());
    for (int i = 0; i < resp.getTimestampsCount(); i++) {
      results.put(Instant.ofEpochSecond(resp.getTimestamps(i)).atZone(timeZone), resp.getValues(i));
    }
    return results.build();
  }
}
