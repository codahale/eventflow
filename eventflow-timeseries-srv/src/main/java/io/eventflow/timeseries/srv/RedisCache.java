package io.eventflow.timeseries.srv;

import com.google.protobuf.InvalidProtocolBufferException;
import io.eventflow.timeseries.api.GetResponse;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.params.SetParams;

public class RedisCache {
  private static final int TTL = (int) TimeUnit.DAYS.toSeconds(1);
  private final JedisPool pool;

  public RedisCache(JedisPool pool) {
    this.pool = pool;
  }

  @Nullable
  public GetResponse getIfPresent(byte[] key) {
    try (var jedis = pool.getResource()) {
      var data = jedis.get(key);
      if (data != null) {
        return GetResponse.parseFrom(data);
      }
    } catch (InvalidProtocolBufferException e) {
      // If we can't parse it, pretend it doesn't exist.
    }
    return null;
  }

  public void put(byte[] key, GetResponse response) {
    try (var jedis = pool.getResource()) {
      jedis.set(key, response.toByteArray(), SetParams.setParams().ex(TTL));
    }
  }
}
