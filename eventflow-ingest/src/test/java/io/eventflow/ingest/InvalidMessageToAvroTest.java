package io.eventflow.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import io.eventflow.ingest.pb.InvalidMessage;
import io.eventflow.testing.beam.SchemaAssert;
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
  public void invalidMessageWithoutEventOrAttributes() {
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
    SchemaAssert.assertThat(schema).canReadAndWrite(row);
  }

  @Test
  public void invalidMessageWithoutEvent() {
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
    SchemaAssert.assertThat(schema).canReadAndWrite(row);
  }

  @Test
  public void invalidMessage() {
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
    SchemaAssert.assertThat(schema).canReadAndWrite(row);
  }
}
