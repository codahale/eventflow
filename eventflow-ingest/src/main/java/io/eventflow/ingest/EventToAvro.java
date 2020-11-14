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

import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Converts Event objects to Avro records capable of being read by BigQuery */
public class EventToAvro implements SerializableFunction<AvroWriteRequest<Event>, GenericRecord> {
  private static final long serialVersionUID = 8195502090379626481L;

  @Override
  public GenericRecord apply(AvroWriteRequest<Event> input) {
    var schema = input.getSchema();
    var attrSchema = schema.getField("attributes").schema().getElementType();

    var event = input.getElement();
    var row = new GenericRecordBuilder(schema);

    // Write common fields.
    row.set("id", event.getId());
    row.set("type", event.getType());
    row.set("source", event.getSource());
    row.set("timestamp", Timestamps.toMicros(event.getTimestamp()));

    // Always write null, since Beam doesn't include default values for nullable fields.
    row.set("customer", event.hasCustomer() ? event.getCustomer().getValue() : null);

    // Convert attributes to their Avro equivalents.
    row.set(
        "attributes",
        event.getAttributesMap().entrySet().stream()
            .map(attr -> attributeValue(attrSchema, attr.getKey(), attr.getValue()))
            .collect(Collectors.toList()));

    return row.build();
  }

  private GenericData.Record attributeValue(Schema schema, String key, AttributeValue value) {
    var attribute = new GenericRecordBuilder(schema);

    // Initialize all nullable fields to null.
    for (var field : schema.getFields()) {
      if (field.schema().getType() == Schema.Type.UNION) {
        attribute.set(field, null);
      }
    }

    // Write the attribute key.
    attribute.set("key", key);

    // Write the attribute value.
    switch (value.getValueCase()) {
      case BOOL_VALUE:
        attribute.set("bool_value", value.getBoolValue());
        break;
      case INT_VALUE:
        attribute.set("int_value", value.getIntValue());
        break;
      case FLOAT_VALUE:
        attribute.set("float_value", value.getFloatValue());
        break;
      case STRING_VALUE:
        attribute.set("string_value", value.getStringValue());
        break;
      case BYTES_VALUE:
        attribute.set("bytes_value", value.getBytesValue().asReadOnlyByteBuffer());
        break;
      case TIMESTAMP_VALUE:
        attribute.set("timestamp_value", Timestamps.toMicros(value.getTimestampValue()));
        break;
      case DURATION_VALUE:
        attribute.set("duration_value", Durations.toMicros(value.getDurationValue()));
        break;
      case VALUE_NOT_SET:
        throw new IllegalStateException("missing attribute value");
    }

    return attribute.build();
  }
}
