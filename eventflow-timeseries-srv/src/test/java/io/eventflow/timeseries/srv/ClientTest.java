package io.eventflow.timeseries.srv;

import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.TimeseriesClient;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.ManagedChannelBuilder;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;

public class ClientTest {
  public static void main(String[] args) {
    var channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
    var stub = TimeseriesGrpc.newBlockingStub(channel);

    var client = new TimeseriesClient(stub);

    var start = Instant.now().minus(2, ChronoUnit.DAYS);
    var end = start.plus(1, ChronoUnit.DAYS);

    var resp =
        client.get(
            "example",
            start,
            end,
            ZoneId.systemDefault(),
            GetRequest.Granularity.GRAN_HOUR,
            GetRequest.Aggregation.AGG_AVG);

    System.out.println("resp: " + resp);
  }
}
