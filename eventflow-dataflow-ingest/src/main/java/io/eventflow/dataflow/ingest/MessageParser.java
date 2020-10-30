package io.eventflow.dataflow.ingest;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import io.eventflow.ingest.pb.InvalidMessage;
import java.time.Clock;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

public class MessageParser extends DoFn<PubsubMessage, Event> {
  private static final long serialVersionUID = -3444251307350466926L;

  static final TupleTag<Event> VALID =
      new TupleTag<>() {
        private static final long serialVersionUID = 6993653980433915514L;
      };
  static final TupleTag<InvalidMessage> INVALID =
      new TupleTag<>() {
        private static final long serialVersionUID = -8091864608888047813L;
      };

  private final Clock clock;

  public MessageParser(Clock clock) {
    this.clock = clock;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    var instant = clock.instant();
    var message = c.element();
    try {
      var event = tryParse(message.getPayload());
      try {
        validateEvent(event, message);
        c.output(VALID, event);
      } catch (IllegalArgumentException e) {
        c.output(
            INVALID,
            InvalidMessage.newBuilder()
                .setMessageId(message.getMessageId())
                .putAllMessageAttributes(message.getAttributeMap())
                .setMessageData(ByteString.copyFrom(message.getPayload()))
                .setReceivedAt(
                    com.google.protobuf.Timestamp.newBuilder()
                        .setSeconds(instant.getEpochSecond())
                        .setNanos(instant.getNano()))
                .setError(e.getMessage())
                .setEvent(event)
                .build());
      }
    } catch (InvalidProtocolBufferException e) {
      c.output(
          INVALID,
          InvalidMessage.newBuilder()
              .setMessageId(message.getMessageId())
              .putAllMessageAttributes(message.getAttributeMap())
              .setMessageData(ByteString.copyFrom(message.getPayload()))
              .setReceivedAt(
                  com.google.protobuf.Timestamp.newBuilder()
                      .setSeconds(instant.getEpochSecond())
                      .setNanos(instant.getNano()))
              .setError("invalid protobuf")
              .build());
    }
  }

  private void validateEvent(Event event, PubsubMessage message) {
    if (event.getId().isBlank()) {
      throw new IllegalArgumentException("blank event id");
    }

    var idAttribute = message.getAttribute(Constants.ID_ATTRIBUTE);
    if (!event.getId().equals(idAttribute)) {
      throw new IllegalArgumentException("event id/attribute mismatch");
    }

    if (event.getType().isBlank()) {
      throw new IllegalArgumentException("blank event type");
    }

    if (event.getSource().isBlank()) {
      throw new IllegalArgumentException("blank event source");
    }

    if (!event.hasTimestamp() || !Timestamps.isValid(event.getTimestamp())) {
      throw new IllegalArgumentException("invalid event timestamp");
    }

    if (event.getAttributesCount() == 0) {
      throw new IllegalArgumentException("no event attributes");
    }

    for (Map.Entry<String, AttributeValue> attribute : event.getAttributesMap().entrySet()) {
      var key = attribute.getKey();
      var value = attribute.getValue();

      if (key.isBlank()) {
        throw new IllegalArgumentException("blank attribute key");
      }

      if (value.getValueCase() == AttributeValue.ValueCase.VALUE_NOT_SET) {
        throw new IllegalArgumentException("blank value for attribute " + key);
      }

      if (value.getValueCase() == AttributeValue.ValueCase.TIMESTAMP_VALUE
          && !Timestamps.isValid(value.getTimestampValue())) {
        throw new IllegalArgumentException("invalid timestamp value for attribute " + key);
      }
    }
  }

  private Event tryParse(byte[] payload) throws InvalidProtocolBufferException {
    try {
      // First, we try parsing the payload as a binary protobuf.
      return Event.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      // If this fails, we try parsing the payload as JSON.
      var builder = Event.newBuilder();
      JsonFormat.parser().merge(new String(payload, Charsets.UTF_8), builder);
      return builder.build();
    }
  }
}
