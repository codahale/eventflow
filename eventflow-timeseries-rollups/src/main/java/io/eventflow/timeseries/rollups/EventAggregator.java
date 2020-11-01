package io.eventflow.timeseries.rollups;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.StringJoiner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class EventAggregator extends PTransform<PCollection<Event>, PCollection<Mutation>> {
  private static final long serialVersionUID = -4941981619223641177L;

  // TODO figure out how to support more than just sums
  // Internally, this would be parsing custom rollups as event.type=max(attr_name) into an
  // ImmutableTable<String, String, TupleTag>, doing per-function aggregations during the window,
  // recombining for the write to Spanner. This would support sums, maxes, and mins. Averages can
  // be supported as-is by dividing sums by counts. Support for percentiles would require storing
  // buckets and then doing some serious backflips in SQL.
  private final ImmutableMultimap<String, String> customRollups;

  public EventAggregator(ImmutableMultimap<String, String> customRollups) {
    this.customRollups = customRollups;
  }

  @Override
  public PCollection<Mutation> expand(PCollection<Event> input) {
    return input
        .apply("Map To Group Key And Values", ParDo.of(new MapToGroupKeysAndValues(customRollups)))
        .apply("Window By Minute", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("Group And Sum", Sum.doublesPerKey())
        .apply("Map To Mutation", ParDo.of(new MapToStreamingMutation()));
  }

  public static ImmutableMultimap<String, String> parseCustomRollups(String s) {
    var builder = ImmutableMultimap.<String, String>builder();
    for (String custom : Splitter.on(',').split(s)) {
      var parts = Splitter.on('=').limit(2).splitToList(custom);
      builder.put(parts.get(0), parts.get(1));
    }
    return builder.build();
  }

  private static class MapToGroupKeysAndValues extends DoFn<Event, KV<KV<String, Long>, Double>> {
    private static final long serialVersionUID = -3444438370108834605L;

    private final ImmutableMultimap<String, String> customAggregates;

    public MapToGroupKeysAndValues(ImmutableMultimap<String, String> customAggregates) {
      this.customAggregates = customAggregates;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      var event = c.element();
      var ts = truncate(event.getTimestamp());

      // Record the count.
      c.output(KV.of(KV.of(metricName(event, "count"), ts), 1.0));

      // Record all custom aggregates.
      for (String name : customAggregates.get(event.getType())) {
        var value = event.getAttributesMap().get(name);
        if (value != null) {
          var key = KV.of(metricName(event, name), ts);
          switch (value.getValueCase()) {
            case BOOL_VALUE:
            case STRING_VALUE:
            case BYTES_VALUE:
            case TIMESTAMP_VALUE:
            case VALUE_NOT_SET:
              break;
            case INT_VALUE:
              c.output(KV.of(key, (double) value.getIntValue()));
              break;
            case FLOAT_VALUE:
              c.output(KV.of(key, value.getFloatValue()));
              break;
            case DURATION_VALUE:
              c.output(KV.of(key, (double) Durations.toMicros(value.getDurationValue())));
              break;
          }
        }
      }
    }

    private String metricName(Event event, String name) {
      var j = new StringJoiner(".").add(event.getType());
      if (event.hasCustomer()) {
        j.add(event.getCustomer().getValue());
      }
      return j.add(name).toString();
    }

    private long truncate(com.google.protobuf.Timestamp timestamp) {
      return Instant.ofEpochMilli(Timestamps.toMillis(timestamp))
          .truncatedTo(ChronoUnit.MINUTES)
          .getEpochSecond();
    }
  }

  private static class MapToStreamingMutation extends DoFn<KV<KV<String, Long>, Double>, Mutation> {
    private static final long serialVersionUID = -7963039695463881759L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      var ts = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(c.element().getKey().getValue(), 0);
      c.output(
          Mutation.newInsertBuilder("intervals_minutes")
              .set("name")
              .to(c.element().getKey().getKey())
              .set("interval_ts")
              .to(ts)
              .set("insert_id")
              .to(c.timestamp().getMillis())
              .set("value")
              .to(c.element().getValue())
              .set("insert_ts")
              .to(Value.COMMIT_TIMESTAMP)
              .build());
    }
  }
}
