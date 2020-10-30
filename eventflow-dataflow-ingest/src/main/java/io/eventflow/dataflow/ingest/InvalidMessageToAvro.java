package io.eventflow.dataflow.ingest;

import io.eventflow.ingest.pb.InvalidMessage;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class InvalidMessageToAvro
    implements SerializableFunction<AvroWriteRequest<InvalidMessage>, GenericRecord> {

  private static final long serialVersionUID = 9081039968919527265L;

  @Override
  public GenericRecord apply(AvroWriteRequest<InvalidMessage> input) {
    // TODO map invalid messages to Avro
    return null;
  }
}
