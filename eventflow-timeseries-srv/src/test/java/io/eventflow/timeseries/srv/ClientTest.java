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

import io.eventflow.timeseries.api.AggregateFunction;
import io.eventflow.timeseries.api.Granularity;
import io.eventflow.timeseries.api.TimeseriesClient;
import io.eventflow.timeseries.api.TimeseriesServiceGrpc;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.time.ZoneId;

public class ClientTest {
  public static void main(String[] args) {
    var channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
    var stub = TimeseriesServiceGrpc.newBlockingStub(channel);

    var client = new TimeseriesClient(stub);

    var start = Instant.parse("2020-10-29T00:00:00Z");
    var end = Instant.parse("2020-11-01T00:00:00Z");

    System.out.println(
        "daily avg: "
            + client.getIntervalValues(
                "example",
                start,
                end,
                ZoneId.systemDefault(),
                Granularity.GRAN_DAY,
                AggregateFunction.AGG_AVG));

    System.out.println(
        "hourly sums: "
            + client.getIntervalValues(
                "example",
                start,
                end,
                ZoneId.systemDefault(),
                Granularity.GRAN_HOUR,
                AggregateFunction.AGG_SUM));

    System.out.println(
        "yearly avg: "
            + client.getIntervalValues(
                "example",
                start,
                end,
                ZoneId.systemDefault(),
                Granularity.GRAN_YEAR,
                AggregateFunction.AGG_AVG));

    System.out.println(
        "minutely sums: "
            + client.getIntervalValues(
                "example",
                start,
                end,
                ZoneId.systemDefault(),
                Granularity.GRAN_MINUTE,
                AggregateFunction.AGG_SUM));

    System.out.println(
        "ancient request: "
            + client.getIntervalValues(
                "example",
                Instant.parse("2010-10-29T00:00:00Z"),
                Instant.parse("2010-10-29T00:00:00Z"),
                ZoneId.systemDefault(),
                Granularity.GRAN_MINUTE,
                AggregateFunction.AGG_SUM));
  }
}
