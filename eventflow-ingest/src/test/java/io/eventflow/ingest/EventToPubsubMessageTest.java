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
package io.eventflow.ingest;

import static io.eventflow.testing.beam.PCollectionAssert.assertThat;
import static io.eventflow.testing.beam.PCollectionAssert.givenAll;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import io.eventflow.testing.ProtobufAssert;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class EventToPubsubMessageTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            PubsubMessage.class, PubsubMessageWithAttributesAndMessageIdCoder.of());
  }

  @Test
  public void eventWithCustomer() {
    var event =
        Event.newBuilder()
            .setId("mcduck")
            .setSource("mobile")
            .setType("click")
            .setTimestamp(Timestamps.fromSeconds(200))
            .setCustomer(StringValue.of("scrooge"))
            .build();
    var messages = pipeline.apply(Create.of(event)).apply(ParDo.of(new EventToPubsubMessage()));

    var attributes = new HashMap<String, String>();
    attributes.put("event.customer", "scrooge");
    attributes.put("event.id", "mcduck");
    attributes.put("event.source", "mobile");
    attributes.put("event.timestamp", "1970-01-01T00:03:20Z");
    attributes.put("event.type", "click");

    assertThat(messages)
        .satisfies(
            givenAll(
                msgs ->
                    assertThat(msgs)
                        .hasSize(1)
                        .allSatisfy(
                            msg -> {
                              assertThat(msg.getAttributeMap()).isEqualTo(attributes);
                              assertThat(ProtobufAssert.assertThat(Event::parseFrom))
                                  .canParse(msg.getPayload())
                                  .isEqualTo(event);
                            })));

    pipeline.run();
  }

  @Test
  public void eventWithoutCustomer() {
    var event =
        Event.newBuilder()
            .setId("mcduck")
            .setSource("mobile")
            .setType("click")
            .setTimestamp(Timestamps.fromSeconds(200))
            .build();
    var messages = pipeline.apply(Create.of(event)).apply(ParDo.of(new EventToPubsubMessage()));

    var attributes = new HashMap<String, String>();
    attributes.put("event.id", "mcduck");
    attributes.put("event.source", "mobile");
    attributes.put("event.timestamp", "1970-01-01T00:03:20Z");
    attributes.put("event.type", "click");

    assertThat(messages)
        .satisfies(
            givenAll(
                msgs ->
                    assertThat(msgs)
                        .hasSize(1)
                        .allSatisfy(
                            msg -> {
                              assertThat(msg.getAttributeMap()).isEqualTo(attributes);
                              assertThat(ProtobufAssert.assertThat(Event::parseFrom))
                                  .canParse(msg.getPayload())
                                  .isEqualTo(event);
                            })));

    pipeline.run();
  }
}
