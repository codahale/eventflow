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

import com.google.protobuf.TextFormat;
import com.google.protobuf.util.Timestamps;
import io.eventflow.ingest.pb.InvalidMessage;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Converts InvalidMessage objects to Avro records capable of being read by BigQuery */
public class InvalidMessageToAvro
    implements SerializableFunction<AvroWriteRequest<InvalidMessage>, GenericRecord> {

  private static final long serialVersionUID = 9081039968919527265L;

  @Override
  public GenericRecord apply(AvroWriteRequest<InvalidMessage> input) {
    var schema = Objects.requireNonNull(input).getSchema();
    var attrSchema = schema.getField("message_attributes").schema().getElementType();

    var message = Objects.requireNonNull(input.getElement());
    var row = new GenericRecordBuilder(schema);

    row.set("message_id", message.getMessageId());
    row.set(
        "message_attributes",
        message.getMessageAttributesMap().entrySet().stream()
            .map(
                entry ->
                    new GenericRecordBuilder(attrSchema)
                        .set("key", entry.getKey())
                        .set("value", entry.getValue())
                        .build())
            .collect(Collectors.toList()));
    row.set("message_data", message.getMessageData().asReadOnlyByteBuffer());
    row.set("received_at", Timestamps.toMicros(message.getReceivedAt()));
    row.set("error", message.getError());

    if (message.hasEvent()) {
      // As cool as it'd be to store bad events as JSON, the JsonFormat class formats timestamps as
      // ISO8601 timestamps. That's great for APIs, but throws an exception if the timestamp isn't
      // valid, and invalid timestamps is a reason why events might end up here.
      row.set("event", TextFormat.shortDebugString(message.getEvent()));
    } else {
      row.set("event", null);
    }

    return row.build();
  }
}
