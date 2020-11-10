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

import static com.google.common.truth.Truth.assertThat;
import static io.eventflow.testing.beam.SchemaSubject.assertThat;

import com.google.protobuf.StringValue;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.AttributeValues;
import io.eventflow.common.pb.Event;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.junit.Before;
import org.junit.Test;

public class EventToAvroTest {
  private final EventToAvro f = new EventToAvro();
  private Schema schema;

  @Before
  public void setUp() throws Exception {
    this.schema = Schemas.tableSchemaToAvroSchema(Schemas.loadTableSchema("events"));
  }

  @Test
  public void eventWithCustomer() throws IOException {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("type")
            .setSource("source")
            .setCustomer(StringValue.of("customer"))
            .setTimestamp(Timestamps.fromSeconds(123456))
            .putAttributes("bool", AttributeValues.boolValue(true))
            .putAttributes("int", AttributeValues.intValue(200))
            .putAttributes("float", AttributeValues.floatValue(200.2))
            .putAttributes("string", AttributeValues.stringValue("ok"))
            .putAttributes("bytes", AttributeValues.bytesValue("yes"))
            .putAttributes(
                "timestamp", AttributeValues.timestampValue(Instant.ofEpochSecond(12345)))
            .putAttributes("duration", AttributeValues.durationValue(Duration.ofMinutes(20)))
            .build();

    var row = f.apply(new AvroWriteRequest<>(event, schema));
    assertThat(row.toString())
        .isEqualTo(
            "{\"id\": \"id\", \"type\": \"type\", \"source\": \"source\", \"customer\": \"customer\", \"timestamp\": 123456000000, \"attributes\": [{\"key\": \"bool\", \"bool_value\": true, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"int\", \"bool_value\": null, \"int_value\": 200, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"float\", \"bool_value\": null, \"int_value\": null, \"float_value\": 200.2, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"string\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": \"ok\", \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"bytes\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": {\"bytes\": \"yes\"}, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"timestamp\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": 12345000000, \"duration_value\": null}, {\"key\": \"duration\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": 1200000000}]}");
    assertThat(schema).canReadAndWrite(row);
  }

  @Test
  public void eventWithoutCustomer() throws IOException {
    var event =
        Event.newBuilder()
            .setId("id")
            .setType("type")
            .setSource("source")
            .setTimestamp(Timestamps.fromSeconds(123456))
            .putAttributes("bool", AttributeValues.boolValue(true))
            .putAttributes("int", AttributeValues.intValue(200))
            .putAttributes("float", AttributeValues.floatValue(200.2))
            .putAttributes("string", AttributeValues.stringValue("ok"))
            .putAttributes("bytes", AttributeValues.bytesValue("yes"))
            .putAttributes(
                "timestamp", AttributeValues.timestampValue(Instant.ofEpochSecond(12345)))
            .putAttributes("duration", AttributeValues.durationValue(Duration.ofMinutes(20)))
            .build();

    var row = f.apply(new AvroWriteRequest<>(event, schema));
    assertThat(row.toString())
        .isEqualTo(
            "{\"id\": \"id\", \"type\": \"type\", \"source\": \"source\", \"customer\": null, \"timestamp\": 123456000000, \"attributes\": [{\"key\": \"bool\", \"bool_value\": true, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"int\", \"bool_value\": null, \"int_value\": 200, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"float\", \"bool_value\": null, \"int_value\": null, \"float_value\": 200.2, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"string\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": \"ok\", \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"bytes\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": {\"bytes\": \"yes\"}, \"timestamp_value\": null, \"duration_value\": null}, {\"key\": \"timestamp\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": 12345000000, \"duration_value\": null}, {\"key\": \"duration\", \"bool_value\": null, \"int_value\": null, \"float_value\": null, \"string_value\": null, \"bytes_value\": null, \"timestamp_value\": null, \"duration_value\": 1200000000}]}");
    assertThat(schema).canReadAndWrite(row);
  }
}
