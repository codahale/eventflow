package io.eventflow.timeseries.srv;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.GetResponse;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class CachedTimeseriesImplTest {
  private static final Clock CLOCK = Clock.fixed(Instant.ofEpochSecond(10000), ZoneOffset.UTC);
  private static final GetResponse RESP =
      GetResponse.newBuilder().addTimestamps(10).addValues(10).build();

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Rule public GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Mock public RedisCache cache;
  @Spy public final FakeBase base = new FakeBase();
  private TimeseriesGrpc.TimeseriesBlockingStub client;

  @Before
  public void setUp() {
    grpcServerRule
        .getServiceRegistry()
        .addService(new CachedTimeseriesImpl(base, cache, CLOCK, Duration.ofSeconds(10)));
    this.client = TimeseriesGrpc.newBlockingStub(grpcServerRule.getChannel());
  }

  @Test
  public void getUncacheableRequest() {
    var req =
        GetRequest.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(9_000))
            .setEnd(Timestamp.newBuilder().setSeconds(10_000))
            .build();

    assertEquals(RESP, client.get(req));
    assertEquals(RESP, client.get(req));

    verify(base, times(2)).get(eq(req), any());
    verifyNoInteractions(cache);
  }

  @Test
  public void getCacheableRequestEmptyCache() {
    var req =
        GetRequest.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(1_000))
            .setEnd(Timestamp.newBuilder().setSeconds(2_000))
            .build();

    when(cache.getIfPresent(req.toByteArray())).thenReturn(null, RESP);

    assertEquals(RESP, client.get(req));
    assertEquals(RESP, client.get(req));

    verify(base, times(1)).get(eq(req), any());
    verify(cache).put(req.toByteArray(), RESP);
  }

  @Test
  public void getCacheableRequestFullCache() {
    var req =
        GetRequest.newBuilder()
            .setStart(Timestamp.newBuilder().setSeconds(1_000))
            .setEnd(Timestamp.newBuilder().setSeconds(2_000))
            .build();

    when(cache.getIfPresent(req.toByteArray())).thenReturn(RESP);

    assertEquals(RESP, client.get(req));
    assertEquals(RESP, client.get(req));

    verify(base, never()).get(eq(req), any());
    verify(cache, never()).put(any(), any());
  }

  private static class FakeBase extends TimeseriesGrpc.TimeseriesImplBase {
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
      responseObserver.onNext(RESP);
      responseObserver.onCompleted();
    }
  }
}
