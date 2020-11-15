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
package io.eventflow.ingest;

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
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Parses incoming Pub/Sub messages. If they parse as Events, outputs the parsed Event object with
 * the VALID tag. Otherwise, outputs an InvalidMessage object with the INVALID tag.
 */
public class MessageParser extends DoFn<PubsubMessage, Event> {
  private static final long serialVersionUID = -3444251307350466926L;

  /** Used to tag valid Event objects. */
  static final TupleTag<Event> VALID =
      new TupleTag<>() {
        private static final long serialVersionUID = 6993653980433915514L;
      };

  /** Used to tag InvalidMessage objects. */
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
    var message = c.element();

    // First, we try parsing the message's data as an Event, either in protobuf or JSON.
    var event = parse(message.getPayload());
    if (event == null) {
      /// If that's unsuccessful, we output an InvalidMessage and bail.
      c.output(INVALID, invalidMessage(message).setError("invalid protobuf").build());
      return;
    }

    // Second, we validate the event.
    var error = validateEvent(event, message);
    if (error != null) {
      // If there is a problem with the event, we output an InvalidMessage and bail.
      c.output(INVALID, invalidMessage(message).setError(error).setEvent(event).build());
      return;
    }

    // Third, we output the valid event.
    c.output(VALID, event);
  }

  private InvalidMessage.Builder invalidMessage(PubsubMessage message) {
    var instant = clock.instant();
    return InvalidMessage.newBuilder()
        .setMessageId(message.getMessageId())
        .putAllMessageAttributes(message.getAttributeMap())
        .setMessageData(ByteString.copyFrom(message.getPayload()))
        .setReceivedAt(
            com.google.protobuf.Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano()));
  }

  @Nullable
  private String validateEvent(Event event, PubsubMessage message) {
    // All events must have an ID. Otherwise, downstream systems can't disambiguate between
    // duplicate events and events with common properties.
    if (event.getId().isBlank()) {
      return "blank event id";
    }

    // All events must be published in Pub/Sub messages with the event ID in the ID attribute.
    // Otherwise, Dataflow can't deduplicate the messages it receives by the events they contain.
    var idAttribute = message.getAttribute(Constants.ID_ATTRIBUTE);
    if (!event.getId().equals(idAttribute)) {
      return "event id/attribute mismatch";
    }

    // All events must have an event type. The event type disambiguates attribute semantics.
    if (event.getType().isBlank()) {
      return "blank event type";
    }

    // All events must have an event source.
    if (event.getSource().isBlank()) {
      return "blank event source";
    }

    // All events must have a vaid timestamp.
    if (!event.hasTimestamp() || !Timestamps.isValid(event.getTimestamp())) {
      return "invalid event timestamp";
    }

    // All events must have at least one attribute.
    if (event.getAttributesCount() == 0) {
      return "no event attributes";
    }

    for (var attribute : event.getAttributesMap().entrySet()) {
      var key = attribute.getKey();
      var value = attribute.getValue();

      // All event attributes must have keys.
      if (key.isBlank()) {
        return "blank attribute key";
      }

      // All event attributes must have values.
      if (value.getValueCase() == AttributeValue.ValueCase.VALUE_NOT_SET) {
        return "blank value for attribute " + key;
      }

      // All timestamp attribute values must be valid.
      if (value.getValueCase() == AttributeValue.ValueCase.TIMESTAMP_VALUE
          && !Timestamps.isValid(value.getTimestampValue())) {
        return "invalid timestamp value for attribute " + key;
      }
    }

    return null;
  }

  @Nullable
  private Event parse(byte[] payload) {
    // First, we try parsing the payload as a binary protobuf.
    try {
      return Event.parseFrom(payload);
    } catch (InvalidProtocolBufferException ignored) {
      // It's not a binary protobuf, so continue.
    }

    // If this fails, we try parsing the payload as JSON.
    try {
      var builder = Event.newBuilder();
      JsonFormat.parser().merge(new String(payload, Charsets.UTF_8), builder);
      return builder.build();
    } catch (InvalidProtocolBufferException ignored) {
      // It's not a JSON protobuf, so continue.
    }

    // If we can't parse it as either protobuf or JSON, then oh well.
    return null;
  }
}
