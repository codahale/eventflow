package io.eventflow.dataflow.ingest;

import io.eventflow.common.pb.Event;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class EventToAvro implements SerializableFunction<AvroWriteRequest<Event>, GenericRecord> {
  private static final long serialVersionUID = 8195502090379626481L;

  @Override
  public GenericRecord apply(AvroWriteRequest<Event> input) {
    // TODO map events to Avro
    return null;
  }
}
