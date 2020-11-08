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

import static org.assertj.core.api.Assertions.assertThat;
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

public class TimeSeriesClientTest {
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Rule public GrpcServerRule grpcServerRule = new GrpcServerRule();
  @Mock public TimeSeriesServiceGrpc.TimeSeriesServiceImplBase service;
  private TimeSeriesClient client;

  @Before
  public void setUp() {
    grpcServerRule.getServiceRegistry().addService(service);
    this.client =
        new TimeSeriesClient(TimeSeriesServiceGrpc.newBlockingStub(grpcServerRule.getChannel()));
  }

  @Test
  public void get() {
    var timeZone = ZoneId.of("America/Denver");

    doAnswer(
            invocation -> {
              StreamObserver<IntervalValues> observer = invocation.getArgument(1);
              observer.onNext(
                  IntervalValues.newBuilder()
                      .addTimestamps(123456789)
                      .addValues(22.3)
                      .addTimestamps(1234567891)
                      .addValues(45.6)
                      .build());
              observer.onCompleted();
              return null;
            })
        .when(service)
        .getIntervalValues(any(), any());

    var results =
        client.getIntervalValues(
            "example.count",
            Instant.ofEpochSecond(1234),
            Instant.ofEpochSecond(4567),
            timeZone,
            Granularity.GRAN_MINUTE,
            AggregateFunction.AGG_AVG);

    assertThat(results)
        .isEqualTo(
            ImmutableMap.of(
                ZonedDateTime.of(1973, 11, 29, 14, 33, 9, 0, timeZone),
                22.3,
                ZonedDateTime.of(2009, 2, 13, 16, 31, 31, 0, timeZone),
                45.6));

    verify(service)
        .getIntervalValues(
            eq(
                GetIntervalValuesRequest.newBuilder()
                    .setName("example.count")
                    .setStart(Timestamps.fromSeconds(1234))
                    .setEnd(Timestamps.fromSeconds(4567))
                    .setTimeZone(timeZone.getId())
                    .setGranularity(Granularity.GRAN_MINUTE)
                    .setAggregateFunction(AggregateFunction.AGG_AVG)
                    .build()),
            any());
  }
}
