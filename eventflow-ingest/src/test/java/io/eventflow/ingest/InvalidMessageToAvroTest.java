package io.eventflow.ingest;

import static io.eventflow.ingest.EventToAvroTest.assertWritable;
import static org.junit.Assert.assertEquals;

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
    assertEquals(
        "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": null}",
        row.toString());
    assertWritable(schema, row);
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
    assertEquals(
        "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [{\"key\": \"event.id\", \"value\": \"test\"}], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": null}",
        row.toString());
    assertWritable(schema, row);
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
    assertEquals(
        "{\"message_id\": \"12345\", \"message_data\": {\"bytes\": \"ok\"}, \"message_attributes\": [{\"key\": \"event.id\", \"value\": \"test\"}], \"received_at\": 123456000000, \"error\": \"bad vibes\", \"event\": \"id: \\\"test\\\"\"}",
        row.toString());
    assertWritable(schema, row);
  }
}
