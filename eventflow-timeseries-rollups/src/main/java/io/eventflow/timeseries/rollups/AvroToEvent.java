package io.eventflow.timeseries.rollups;

import io.eventflow.common.pb.Event;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

public class AvroToEvent implements SerializableFunction<SchemaAndRecord, Event> {
  private static final long serialVersionUID = -8477003233796891745L;

  @Override
  public Event apply(SchemaAndRecord input) {
    // TODO
    return Event.newBuilder().build();
  }
}
