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

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import io.eventflow.ingest.pb.InvalidMessage;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.junit.Before;
import org.junit.Test;

public class InvalidMessageToAvroTest {
  private final InvalidMessageToAvro f = new InvalidMessageToAvro();
  private Schema schema;

  @Before
  public void setUp() throws Exception {
    this.schema = Schemas.tableSchemaToAvroSchema(Schemas.loadTableSchema("invalid_messages"));
  }

  @Test
  public void invalidMessageWithoutEventOrAttributes() throws IOException {
    var message =
        InvalidMessage.newBuilder()
            .setMessageId("12345")
            .setMessageData(ByteString.copyFromUtf8("ok"))
            .setReceivedAt(Timestamps.fromSeconds(123456))
            .setError("bad vibes")
            .build();

    var row = f.apply(new AvroWriteRequest<>(message, schema));
    assertThat(row.toString())
        .isEqualTo(
            "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": null}");
    assertThat(schema).canReadAndWrite(row);
  }

  @Test
  public void invalidMessageWithoutEvent() throws IOException {
    var message =
        InvalidMessage.newBuilder()
            .setMessageId("12345")
            .putMessageAttributes("event.id", "test")
            .setMessageData(ByteString.copyFromUtf8("ok"))
            .setReceivedAt(Timestamps.fromSeconds(123456))
            .setError("bad vibes")
            .build();

    var row = f.apply(new AvroWriteRequest<>(message, schema));
    assertThat(row.toString())
        .isEqualTo(
            "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [{\"key\": \"event.id\", \"value\": \"test\"}], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": null}");
    assertThat(schema).canReadAndWrite(row);
  }

  @Test
  public void invalidMessage() throws IOException {
    var message =
        InvalidMessage.newBuilder()
            .setMessageId("12345")
            .putMessageAttributes("event.id", "test")
            .setMessageData(ByteString.copyFromUtf8("ok"))
            .setReceivedAt(Timestamps.fromSeconds(123456))
            .setError("bad vibes")
            .setEvent(Event.newBuilder().setId("test").build())
            .build();

    var row = f.apply(new AvroWriteRequest<>(message, schema));
    assertThat(row.toString())
        .isEqualTo(
            "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [{\"key\": \"event.id\", \"value\": \"test\"}], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": \"id: \\\"test\\\"\"}");
    assertThat(schema).canReadAndWrite(row);
  }
}
