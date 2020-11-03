package io.eventflow.ingest;

import static io.eventflow.testing.beam.PCollectionAssert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.eventflow.common.AttributeValues;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import io.eventflow.ingest.pb.InvalidMessage;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class MessageParserTest {

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  private final Instant instant = Instant.ofEpochSecond(12345678);

  @Before
  public void setUp() {
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            PubsubMessage.class, PubsubMessageWithAttributesAndMessageIdCoder.of());
    pipeline.getCoderRegistry().registerCoderForClass(Event.class, ProtoCoder.of(Event.class));
  }

  @Test
  public void invalidProtobuf() {
    var message =
        new PubsubMessage("weird".getBytes(StandardCharsets.UTF_8), ImmutableMap.of(), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(ByteString.copyFromUtf8("weird"))
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("invalid protobuf")
                .build());

    pipeline.run();
  }

  @Test
  public void blankId() {
    var event = Event.newBuilder().build();
    var message = new PubsubMessage(event.toByteArray(), ImmutableMap.of(), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("blank event id")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void idAttributeMismatch() {
    var event = Event.newBuilder().setId("id").build();
    var message = new PubsubMessage(event.toByteArray(), ImmutableMap.of(), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("event id/attribute mismatch")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void blankEventType() {
    var event = Event.newBuilder().setId("id").build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("blank event type")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void blankEventSource() {
    var event = Event.newBuilder().setId("id").setType("click").build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("blank event source")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void blankTimestamp() {
    var event = Event.newBuilder().setId("id").setType("click").setSource("mobile").build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("invalid event timestamp")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void invalidTimestamp() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(Long.MAX_VALUE))
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("invalid event timestamp")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void noAttributes() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("no event attributes")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void blankAttributeKey() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .putAttributes("", AttributeValues.boolValue(false))
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("blank attribute key")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void blankAttributeValue() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .putAttributes("blank", AttributeValue.newBuilder().build())
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("blank value for attribute blank")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void invalidTimestampAttribute() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .putAttributes(
                "ts",
                AttributeValue.newBuilder()
                    .setTimestampValue(Timestamp.newBuilder().setSeconds(Long.MIN_VALUE))
                    .build())
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).empty();
    assertThat(results.get(MessageParser.INVALID))
        .containsInAnyOrder(
            InvalidMessage.newBuilder()
                .setMessageId("12345")
                .setMessageData(event.toByteString())
                .putMessageAttributes("event.id", "id")
                .setReceivedAt(Timestamp.newBuilder().setSeconds(12345678))
                .setError("invalid timestamp value for attribute ts")
                .setEvent(event)
                .build());

    pipeline.run();
  }

  @Test
  public void validEvent() {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .putAttributes("bool", AttributeValues.boolValue(true))
            .build();
    var message =
        new PubsubMessage(event.toByteArray(), ImmutableMap.of("event.id", "id"), "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).containsInAnyOrder(event);
    assertThat(results.get(MessageParser.INVALID)).empty();

    pipeline.run();
  }

  @Test
  public void jsonEvent() throws InvalidProtocolBufferException {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("click")
            .setSource("mobile")
            .setTimestamp(Timestamp.newBuilder().setSeconds(1234567))
            .putAttributes("bool", AttributeValues.boolValue(true))
            .build();
    var message =
        new PubsubMessage(
            JsonFormat.printer().print(event).getBytes(StandardCharsets.UTF_8),
            ImmutableMap.of("event.id", "id"),
            "12345");
    var results =
        pipeline
            .apply(Create.of(message))
            .apply(
                ParDo.of(new MessageParser(Clock.fixed(instant, ZoneOffset.UTC)))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    assertThat(results.get(MessageParser.VALID)).containsInAnyOrder(event);
    assertThat(results.get(MessageParser.INVALID)).empty();

    pipeline.run();
  }
}
