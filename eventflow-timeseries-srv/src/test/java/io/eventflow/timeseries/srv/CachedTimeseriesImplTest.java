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

import static org.assertj.core.api.Assertions.assertThat;
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

    assertThat(client.get(req)).isEqualTo(RESP);
    assertThat(client.get(req)).isEqualTo(RESP);

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

    assertThat(client.get(req)).isEqualTo(RESP);
    assertThat(client.get(req)).isEqualTo(RESP);

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

    assertThat(client.get(req)).isEqualTo(RESP);
    assertThat(client.get(req)).isEqualTo(RESP);

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
