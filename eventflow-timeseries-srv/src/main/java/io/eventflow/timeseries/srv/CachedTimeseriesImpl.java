package io.eventflow.timeseries.srv;

import com.google.protobuf.util.Timestamps;
import io.eventflow.timeseries.api.GetRequest;
import io.eventflow.timeseries.api.GetResponse;
import io.eventflow.timeseries.api.TimeseriesGrpc;
import io.grpc.stub.StreamObserver;
import java.time.Clock;
import java.time.Duration;

public class CachedTimeseriesImpl extends TimeseriesGrpc.TimeseriesImplBase {
  private final TimeseriesGrpc.TimeseriesImplBase base;
  private final RedisCache cache;
  private final Clock clock;
  private final long minCacheThreshold;

  public CachedTimeseriesImpl(
      TimeseriesGrpc.TimeseriesImplBase base,
      RedisCache cache,
      Clock clock,
      Duration minCacheThreshold) {
    this.base = base;
    this.cache = cache;
    this.clock = clock;
    this.minCacheThreshold = minCacheThreshold.toMillis();
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    if (isCacheable(request)) {
      var key = request.toByteArray();
      var resp = cache.getIfPresent(key);
      if (resp != null) {
        responseObserver.onNext(resp);
        responseObserver.onCompleted();
      } else {
        base.get(
            request,
            new StreamObserver<>() {
              @Override
              public void onNext(GetResponse value) {
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
      base.get(request, responseObserver);
    }
  }

  private boolean isCacheable(GetRequest request) {
    return Timestamps.toMillis(request.getEnd()) < clock.millis() - minCacheThreshold;
  }
}
