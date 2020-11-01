package io.eventflow.timeseries.rollups;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MapToBatchMutation extends DoFn<KV<KV<String, Long>, Double>, Mutation> {
  private static final long serialVersionUID = 5720912088985693896L;

  private final String table;

  public MapToBatchMutation(String table) {
    this.table = table;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    c.output(
        Mutation.newInsertOrUpdateBuilder(table)
            .set("name")
            .to(c.element().getKey().getKey())
            .set("interval_ts")
            .to(com.google.cloud.Timestamp.ofTimeMicroseconds(c.element().getKey().getValue()))
            .set("value")
            .to(c.element().getValue())
            .set("update_ts")
            .to(Value.COMMIT_TIMESTAMP)
            .build());
  }
}
