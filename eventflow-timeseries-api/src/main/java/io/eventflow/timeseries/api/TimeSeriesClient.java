/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.TimeSeriesServiceGrpc.TimeSeriesServiceBlockingStub;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/** A high-level client for the time series API. */
public class TimeSeriesClient {
  private final TimeSeriesServiceBlockingStub stub;

  public TimeSeriesClient(TimeSeriesServiceBlockingStub stub) {
    this.stub = stub;
  }

  /** Returns a map of interval timestamps to interval values for the given time series. */
  public ImmutableSortedMap<ZonedDateTime, Double> getIntervalValues(
      String name,
      Instant start,
      Instant end,
      ZoneId timeZone,
      Granularity granularity,
      AggregateFunction aggregateFunction) {
    Preconditions.checkArgument(start.isBefore(end), "start must be before end");

    var req =
        GetIntervalValuesRequest.newBuilder()
            .setName(name)
            .setStart(Timestamps.fromSeconds(start.getEpochSecond()))
            .setEnd(Timestamps.fromSeconds(end.getEpochSecond()))
            .setTimeZone(timeZone.getId())
            .setGranularity(granularity)
            .setAggregateFunction(aggregateFunction)
            .build();

    var resp = stub.getIntervalValues(req);
    var results = ImmutableSortedMap.<ZonedDateTime, Double>naturalOrder();
    for (var i = 0; i < resp.getTimestampsCount(); i++) {
      var ts = Instant.ofEpochSecond(resp.getTimestamps(i));
      results.put(ts.atZone(timeZone), resp.getValues(i));
    }
    return results.build();
  }

  /** Returns a list of time series which match the given name prefix. */
  public TimeSeriesList listTimeSeries(String namePrefix) {
    var req = ListTimeSeriesRequest.newBuilder().setNamePrefix(namePrefix).build();
    return stub.listTimeSeries(req);
  }
}
