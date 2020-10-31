package io.eventflow.timeseries.api;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class TimeseriesClient {
  private final TimeseriesGrpc.TimeseriesBlockingStub stub;

  public TimeseriesClient(TimeseriesGrpc.TimeseriesBlockingStub stub) {
    this.stub = stub;
  }

  public ImmutableMap<ZonedDateTime, Long> get(
      String name, Instant start, Instant end, ZoneId timeZone) {
    var req =
        GetRequest.newBuilder()
            .setName(name)
            .setStart(Timestamps.fromSeconds(start.getEpochSecond()))
            .setEnd(Timestamps.fromSeconds(end.getEpochSecond()))
            .setTimeZone(timeZone.getId())
            .setGranularity(GetRequest.Granularity.GRAN_MINUTE)
            .setAggregation(GetRequest.Aggregation.AGG_SUM)
            .build();
    var resp = stub.get(req);
    var results =
        ImmutableMap.<ZonedDateTime, Long>builderWithExpectedSize(resp.getTimestampsCount());
    for (int i = 0; i < resp.getTimestampsCount(); i++) {
      results.put(Instant.ofEpochSecond(resp.getTimestamps(i)).atZone(timeZone), resp.getValues(i));
    }
    return results.build();
  }
}
