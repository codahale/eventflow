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

import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.GetIntervalValuesRequest;
import io.eventflow.timeseries.api.IntervalValues;
import io.eventflow.timeseries.api.TimeseriesServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;

public class CachedTimeseriesServiceImpl extends TimeseriesServiceGrpc.TimeseriesServiceImplBase {
  private final TimeseriesServiceGrpc.TimeseriesServiceImplBase base;
  private final RedisCache cache;
  private final Clock clock;
  private final long minCacheThreshold;

  public CachedTimeseriesServiceImpl(
      TimeseriesServiceGrpc.TimeseriesServiceImplBase base,
      RedisCache cache,
      Clock clock,
      Duration minCacheThreshold) {
    this.base = base;
    this.cache = cache;
    this.clock = clock;
    this.minCacheThreshold = minCacheThreshold.toMillis();
  }

  @Override
  public void getIntervalValues(
      GetIntervalValuesRequest request, StreamObserver<IntervalValues> responseObserver) {
    if (isCacheable(request)) {
      var key = request.toByteArray();
      var resp = cache.getIfPresent(key);
      if (resp != null) {
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
      } else {
        base.getIntervalValues(
            request,
            new StreamObserver<>() {
              @Override
              public void onNext(IntervalValues value) {
                cache.put(key, value);
                responseObserver.onNext(value);
              }

              @Override
              public void onError(Throwable t) {
                responseObserver.onError(t);
              }

              @Override
              public void onCompleted() {
                responseObserver.onCompleted();
              }
            });
      }
    } else {
      base.getIntervalValues(request, responseObserver);
    }
  }

  private boolean isCacheable(GetIntervalValuesRequest request) {
    return Timestamps.toMillis(request.getEnd()) < clock.millis() - minCacheThreshold;
  }
}
