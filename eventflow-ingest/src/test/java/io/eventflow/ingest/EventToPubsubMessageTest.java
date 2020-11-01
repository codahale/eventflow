package io.eventflow.ingest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.testing.PAssert;
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

    PAssert.that(messages)
        // PubsubMessage, delightfully, doesn't have an equals impl
        .satisfies(
            msgs -> {
              for (PubsubMessage msg : msgs) {
                assertEquals(attributes, msg.getAttributeMap());
                assertArrayEquals(event.toByteArray(), msg.getPayload());
              }
              return null;
            });

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

    PAssert.that(messages)
        // PubsubMessage, delightfully, doesn't have an equals impl
        .satisfies(
            msgs -> {
              for (PubsubMessage msg : msgs) {
                assertEquals(attributes, msg.getAttributeMap());
                assertArrayEquals(event.toByteArray(), msg.getPayload());
              }
              return null;
            });

    pipeline.run();
  }
}
