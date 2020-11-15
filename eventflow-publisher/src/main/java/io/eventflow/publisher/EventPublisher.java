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

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.PubsubMessage;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.time.Clock;
import java.util.UUID;

/** Publishes events asynchronously to a Pub/Sub topic for ingestion. */
public class EventPublisher {

  /** A thread-safe generator of unique IDs. */
  @FunctionalInterface
  public interface IdGenerator {
    String generate();
  }

  private static final IdGenerator DEFAULT_GENERATOR = () -> UUID.randomUUID().toString();

  private final String source;
  private final Publisher publisher;
  @VisibleForTesting final IdGenerator idGenerator;
  @VisibleForTesting final Clock clock;

  /** Create a new {@link EventPublisher} using the system clock and random UUIDs as event IDs. */
  public EventPublisher(Publisher publisher, String source) {
    this(publisher, source, Clock.systemUTC(), DEFAULT_GENERATOR);
  }

  /** Create a new {@link EventPublisher} with the given parameters. */
  public EventPublisher(Publisher publisher, String source, Clock clock, IdGenerator idGenerator) {
    this.clock = clock;
    this.source = source;
    this.publisher = publisher;
    this.idGenerator = idGenerator;
  }

  /** Publishes the given Event, setting a timestamp and source. */
  public ApiFuture<String> publish(Event.Builder event) {
    var id = idGenerator.generate();
    var timestamp = Timestamps.fromMillis(clock.millis());

    var data = event.setId(id).setTimestamp(timestamp).setSource(source).build().toByteString();
    var msg =
        PubsubMessage.newBuilder()
            .setData(data)
            .putAttributes(Constants.ID_ATTRIBUTE, id)
            .putAttributes(Constants.TIMESTAMP_ATTRIBUTE, Timestamps.toString(timestamp))
            .build();

    return publisher.publish(msg);
  }

  /** Publishes the given Event, setting a timestamp and source. */
  public ApiFuture<String> publish(Event event) {
    return publish(Event.newBuilder(event));
  }
}
