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
