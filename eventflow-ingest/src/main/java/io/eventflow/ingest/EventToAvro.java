package io.eventflow.ingest;

import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import java.util.ArrayList;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class EventToAvro implements SerializableFunction<AvroWriteRequest<Event>, GenericRecord> {
  private static final long serialVersionUID = 8195502090379626481L;

  @Override
  public GenericRecord apply(AvroWriteRequest<Event> input) {
    var event = input.getElement();
    var row = new GenericRecordBuilder(input.getSchema());
    row.set("id", event.getId());
    row.set("type", event.getType());
    row.set("source", event.getSource());
    row.set("timestamp", Timestamps.toMicros(event.getTimestamp()));

    if (event.hasCustomer()) {
      row.set("customer", event.getCustomer().getValue());
    } else {
      row.set("customer", null);
    }

    var attributes = new ArrayList<GenericRecord>(event.getAttributesCount());
    var attributeSchema = input.getSchema().getField("attributes").schema().getElementType();

    for (Map.Entry<String, AttributeValue> entry : event.getAttributesMap().entrySet()) {
      var attribute = new GenericRecordBuilder(attributeSchema);

      // Initialize all nullable fields to null.
      for (Schema.Field field : attributeSchema.getFields()) {
        if (field.schema().getType() == Schema.Type.UNION) {
          attribute.set(field, null);
        }
      }

      attribute.set("key", entry.getKey());
      var value = entry.getValue();
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
      attributes.add(attribute.build());
    }
    row.set("attributes", attributes);

    return row.build();
  }
}
