package io.eventflow.timeseries.rollups;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.StringJoiner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class MapToGroupKeysAndValues extends DoFn<Event, KV<KV<String, Long>, Double>> {
  private static final long serialVersionUID = -3444438370108834605L;

  private final ImmutableMultimap<String, String> customAggregates;
  private final ChronoUnit chronoUnit;

  public MapToGroupKeysAndValues(String s, ChronoUnit chronoUnit) {
    var builder = ImmutableMultimap.<String, String>builder();
    for (String custom : Splitter.on(',').split(s)) {
      var parts = Splitter.on('=').limit(2).splitToList(custom);
      builder.put(parts.get(0), parts.get(1));
    }
    this.customAggregates = builder.build();
    this.chronoUnit = chronoUnit;
  }

  public MapToGroupKeysAndValues(
      ImmutableMultimap<String, String> customAggregates, ChronoUnit chronoUnit) {
    this.customAggregates = customAggregates;
    this.chronoUnit = chronoUnit;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    var event = c.element();
    var ts = truncate(event.getTimestamp());

    // Record the count.
    c.output(KV.of(KV.of(name(event, "count"), ts), 1.0));

    // Record all custom aggregates.
    for (String attribute : customAggregates.get(event.getType())) {
      var value = event.getAttributesMap().get(attribute);
      if (value != null) {
        switch (value.getValueCase()) {
          case BOOL_VALUE:
          case STRING_VALUE:
          case BYTES_VALUE:
          case TIMESTAMP_VALUE:
          case VALUE_NOT_SET:
            break;
          case INT_VALUE:
            c.output(KV.of(KV.of(name(event, attribute), ts), (double) value.getIntValue()));
            break;
          case FLOAT_VALUE:
            c.output(KV.of(KV.of(name(event, attribute), ts), value.getFloatValue()));
            break;
          case DURATION_VALUE:
            c.output(
                KV.of(
                    KV.of(name(event, attribute), ts),
                    (double) Durations.toMicros(value.getDurationValue())));
            break;
        }
      }
    }
  }

  private String name(Event event, String name) {
    var j = new StringJoiner(".");
    j.add(event.getType());

    if (event.hasCustomer()) {
      j.add(event.getCustomer().getValue());
    }

    j.add(name);
    return j.toString();
  }

  private long truncate(com.google.protobuf.Timestamp timestamp) {
    return Instant.ofEpochMilli(Timestamps.toMillis(timestamp))
            .truncatedTo(chronoUnit)
            .toEpochMilli()
        * 1000;
  }
}
