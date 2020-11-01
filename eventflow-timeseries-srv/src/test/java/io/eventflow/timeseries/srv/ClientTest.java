package io.eventflow.timeseries.srv;

import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.TimeseriesClient;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.time.ZoneId;

public class ClientTest {
  public static void main(String[] args) {
    var channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
    var stub = TimeseriesGrpc.newBlockingStub(channel);

    var client = new TimeseriesClient(stub);

    var start = Instant.parse("2020-10-29T00:00:00Z");
    var end = Instant.parse("2020-11-01T00:00:00Z");

    System.out.println(
        "daily avg: "
            + client.get(
                "example",
                start,
                end,
                ZoneId.systemDefault(),
                GetRequest.Granularity.GRAN_DAY,
                GetRequest.Aggregation.AGG_AVG));

    System.out.println(
        "hourly sums: "
            + client.get(
            "example",
            start,
            end,
            ZoneId.systemDefault(),
            GetRequest.Granularity.GRAN_HOUR,
            GetRequest.Aggregation.AGG_SUM));

    System.out.println(
        "minutely sums: "
            + client.get(
            "example",
            start,
            end,
            ZoneId.systemDefault(),
            GetRequest.Granularity.GRAN_HOUR,
            GetRequest.Aggregation.AGG_SUM));
  }
}
