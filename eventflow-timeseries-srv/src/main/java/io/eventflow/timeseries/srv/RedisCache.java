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
