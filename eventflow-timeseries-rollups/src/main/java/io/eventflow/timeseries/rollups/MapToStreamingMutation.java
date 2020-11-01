package io.eventflow.timeseries.rollups;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.security.SecureRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MapToStreamingMutation extends DoFn<KV<KV<String, Long>, Double>, Mutation> {
  private static final long serialVersionUID = -7963039695463881759L;

  private final SecureRandom random;

  public MapToStreamingMutation(SecureRandom random) {
    this.random = random;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    c.output(
        Mutation.newInsertBuilder("intervals_minutes")
            .set("name")
            .to(c.element().getKey().getKey())
            .set("interval_ts")
            .to(com.google.cloud.Timestamp.ofTimeMicroseconds(c.element().getKey().getValue()))
            .set("insert_id")
            .to(random.nextLong())
            .set("value")
            .to(c.element().getValue())
            .set("insert_ts")
            .to(Value.COMMIT_TIMESTAMP)
            .build());
  }
}
