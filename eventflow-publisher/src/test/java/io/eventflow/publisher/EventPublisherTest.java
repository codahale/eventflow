/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
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
package io.eventflow.publisher;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class EventPublisherTest {
  private final Instant instant = Instant.ofEpochSecond(12345678);
  private final Clock clock = Clock.fixed(instant, ZoneOffset.UTC);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Mock private Publisher pubsub;
  @Mock private Supplier<String> idGenerator;

  @Test
  public void publishEvent() throws ExecutionException, InterruptedException {
    var publisher = new EventPublisher(clock, "publisher", pubsub, idGenerator);

    when(idGenerator.get()).thenReturn("000102030405060708090A0B0C0D0E0F");
    when(pubsub.publish(any())).thenReturn(ApiFutures.immediateFuture("id"));

    var event = Event.newBuilder().setType("click").build();
    assertThat(publisher.publish(event).get()).isEqualTo("id");

    verify(pubsub)
        .publish(
            argThat(
                msg -> {
                  assertThat(msg.getAttributesMap())
                      .containsExactly(
                          "event.id", "000102030405060708090A0B0C0D0E0F",
                          "event.timestamp", "1970-05-23T21:21:18Z");
                  try {
                    assertThat(Event.parseFrom(msg.getData()))
                        .isEqualTo(
                            Event.newBuilder(event)
                                .setId("000102030405060708090A0B0C0D0E0F")
                                .setSource("publisher")
                                .setTimestamp(Timestamps.fromSeconds(12345678))
                                .build());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                  return true;
                }));
  }
}
