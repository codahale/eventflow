package io.eventflow.timeseries.api;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.Timestamps;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class TimeseriesClientTest {
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Rule public GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Mock public TimeseriesGrpc.TimeseriesImplBase service;
  private TimeseriesClient client;

  @Before
  public void setUp() {
    grpcServerRule.getServiceRegistry().addService(service);
    this.client = new TimeseriesClient(TimeseriesGrpc.newBlockingStub(grpcServerRule.getChannel()));
  }

  @Test
  public void get() {
    var timeZone = ZoneId.of("America/Denver");

    doAnswer(
            invocation -> {
              StreamObserver<GetResponse> observer = invocation.getArgument(1);
              observer.onNext(
                  GetResponse.newBuilder()
                      .addTimestamps(123456789)
                      .addValues(22.3)
                      .addTimestamps(1234567891)
                      .addValues(45.6)
                      .build());
              observer.onCompleted();
              return null;
            })
        .when(service)
        .get(any(), any());

    var results =
        client.get(
            "example.count",
            Instant.ofEpochSecond(1234),
            Instant.ofEpochSecond(4567),
            timeZone,
            GetRequest.Granularity.GRAN_MINUTE,
            GetRequest.Aggregation.AGG_AVG);

    assertEquals(
        ImmutableMap.of(
            ZonedDateTime.of(1973, 11, 29, 14, 33, 9, 0, timeZone),
            22.3,
            ZonedDateTime.of(2009, 2, 13, 16, 31, 31, 0, timeZone),
            45.6),
        results);

    verify(service)
        .get(
            eq(
                GetRequest.newBuilder()
                    .setName("example.count")
                    .setStart(Timestamps.fromSeconds(1234))
                    .setEnd(Timestamps.fromSeconds(4567))
                    .setTimeZone(timeZone.getId())
                    .setGranularity(GetRequest.Granularity.GRAN_MINUTE)
                    .setAggregation(GetRequest.Aggregation.AGG_AVG)
                    .build()),
            any());
  }
}
