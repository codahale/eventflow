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
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.PubsubMessage;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.io.IOException;
import java.time.Clock;
import java.util.UUID;
import java.util.function.Supplier;

public class EventPublisher {
  private final Clock clock;
  private final String source;
  private final Publisher publisher;
  private final Supplier<String> idGenerator;

  public EventPublisher(String source, String topicName) throws IOException {
    this(
        Clock.systemUTC(),
        source,
        Publisher.newBuilder(topicName).build(),
        () -> UUID.randomUUID().toString());
  }

  public EventPublisher(
      Clock clock, String source, Publisher publisher, Supplier<String> idGenerator) {
    this.clock = clock;
    this.source = source;
    this.publisher = publisher;
    this.idGenerator = idGenerator;
  }

  public ApiFuture<String> publish(Event.Builder event) {
    var id = idGenerator.get();
    event.setId(id).setTimestamp(Timestamps.fromMillis(clock.millis())).setSource(source);

    return publisher.publish(
        PubsubMessage.newBuilder()
            .setData(event.build().toByteString())
            .putAttributes(Constants.ID_ATTRIBUTE, id)
            .putAttributes(Constants.TIMESTAMP_ATTRIBUTE, Timestamps.toString(event.getTimestamp()))
            .build());
  }

  public ApiFuture<String> publish(Event event) {
    return publish(Event.newBuilder(event));
  }
}
