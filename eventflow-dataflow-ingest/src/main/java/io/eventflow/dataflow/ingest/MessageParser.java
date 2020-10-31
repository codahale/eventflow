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
import javax.annotation.Nullable;
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
    var event = parse(message.getPayload());
    if (event == null) {
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
      return;
    }

    var error = validateEvent(event, message);
    if (error != null) {
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
              .setError(error)
              .setEvent(event)
              .build());
      return;
    }

    c.output(VALID, event);
  }

  @Nullable
  private String validateEvent(Event event, PubsubMessage message) {
    if (event.getId().isBlank()) {
      return "blank event id";
    }

    var idAttribute = message.getAttribute(Constants.ID_ATTRIBUTE);
    if (!event.getId().equals(idAttribute)) {
      return "event id/attribute mismatch";
    }

    if (event.getType().isBlank()) {
      return "blank event type";
    }

    if (event.getSource().isBlank()) {
      return "blank event source";
    }

    if (!event.hasTimestamp() || !Timestamps.isValid(event.getTimestamp())) {
      return "invalid event timestamp";
    }

    if (event.getAttributesCount() == 0) {
      return "no event attributes";
    }

    for (Map.Entry<String, AttributeValue> attribute : event.getAttributesMap().entrySet()) {
      var key = attribute.getKey();
      var value = attribute.getValue();

      if (key.isBlank()) {
        return "blank attribute key";
      }

      if (value.getValueCase() == AttributeValue.ValueCase.VALUE_NOT_SET) {
        return "blank value for attribute " + key;
      }

      if (value.getValueCase() == AttributeValue.ValueCase.TIMESTAMP_VALUE
          && !Timestamps.isValid(value.getTimestampValue())) {
        return "invalid timestamp value for attribute " + key;
      }
    }

    return null;
  }

  @Nullable
  private Event parse(byte[] payload) {
    try {
      // First, we try parsing the payload as a binary protobuf.
      return Event.parseFrom(payload);
    } catch (InvalidProtocolBufferException e) {
      try {
        // If this fails, we try parsing the payload as JSON.
        var builder = Event.newBuilder();
        JsonFormat.parser().merge(new String(payload, Charsets.UTF_8), builder);
        return builder.build();
      } catch (InvalidProtocolBufferException e2) {
        // If we can't parse it as either protobuf or JSON, then oh well.
        return null;
      }
    }
  }
}
