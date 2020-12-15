/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
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
package io.eventflow.timeseries.rollups;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTagList;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A Beam transform which windows incoming events into minutely batches, maps batched events into
 * rollup group keys and values, aggregates them by rollup types, and maps those aggregates to
 * Spanner inserts.
 */
public class EventAggregator
    extends PTransform<@NonNull PCollection<Event>, @NonNull PCollection<Mutation>> {
  private static final long serialVersionUID = -4941981619223641177L;

  private final RollupSpec rollupSpec;

  private final SecureRandom random;

  public EventAggregator(RollupSpec rollupSpec, SecureRandom random) {
    this.rollupSpec = rollupSpec;
    this.random = random;
  }

  @Override
  public @NonNull PCollection<Mutation> expand(PCollection<Event> input) {
    // Window events into minutely batches.
    var batched =
        input.apply("Widow By Minute", Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    // Map batched events to rollup group keys and values.
    var groupKeysAndValues =
        batched.apply(
            "Map To Group Key And Values",
            ParDo.of(new MapToGroupKeysAndValues(rollupSpec))
                .withOutputTags(
                    RollupSpec.SUM, TupleTagList.of(List.of(RollupSpec.MIN, RollupSpec.MAX))));

    // Aggregate rollups by group key and rollup type.
    var aggregates =
        PCollectionList.of(
            List.of(
                groupKeysAndValues.get(RollupSpec.SUM).apply("Sum Values", Sum.doublesPerKey()),
                groupKeysAndValues.get(RollupSpec.MIN).apply("Min Values", Min.doublesPerKey()),
                groupKeysAndValues.get(RollupSpec.MAX).apply("Max Values", Max.doublesPerKey())));

    // Combine aggregates and map to Spanner inserts.
    return aggregates
        .apply("Flatten", Flatten.pCollections())
        .apply("Map To Mutation", ParDo.of(new MapToStreamingMutation(random)));
  }

  /**
   * Maps events to rollup group keys and values. The group keys are composed of the time series
   * name (e.g. event_type.attr.sum) and a 64-bit floating point value. Counts of all event types
   * are recorded in addition to any specified rollups.
   */
  private static class MapToGroupKeysAndValues extends DoFn<Event, KV<KV<String, Long>, Double>> {
    private static final long serialVersionUID = -3444438370108834605L;

    private final RollupSpec rollupSpec;

    public MapToGroupKeysAndValues(RollupSpec rollupSpec) {
      this.rollupSpec = rollupSpec;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      var event = Objects.requireNonNull(c.element());

      // Truncate the event timestamp to the minute.
      var ts =
          Instant.ofEpochMilli(Timestamps.toMillis(event.getTimestamp()))
              .truncatedTo(ChronoUnit.MINUTES)
              .getEpochSecond();

      // Record the count.
      c.output(RollupSpec.SUM, KV.of(KV.of(metricName(event, "count"), ts), 1.0));

      // Record all other rollups.
      for (var rollup : rollupSpec.rollups(event.getType())) {
        var name = rollup.getKey();
        var value = event.getAttributesMap().get(name);
        if (value != null) {
          var key = KV.of(metricName(event, name, rollup.getValue().getId()), ts);
          var output = extractValue(value);
          if (output != null) {
            c.output(rollup.getValue(), KV.of(key, output));
          }
        }
      }
    }

    @Nullable
    private Double extractValue(AttributeValue value) {
      switch (value.getValueCase()) {
        case INT_VALUE:
          return (double) value.getIntValue();
        case FLOAT_VALUE:
          return value.getFloatValue();
        case DURATION_VALUE:
          return (double) Durations.toMicros(value.getDurationValue());
        default:
          return null;
      }
    }

    private String metricName(Event event, String... names) {
      var j = new StringJoiner(".").add(event.getType());
      if (event.hasCustomer()) {
        j.add(event.getCustomer().getValue());
      }

      for (var name : names) {
        j.add(name);
      }
      return j.toString();
    }
  }

  /**
   * Maps rollup aggregates to Spanner insert mutations, generating random insert IDs to allow for
   * multiple rows per interval timestamp.
   */
  private static class MapToStreamingMutation extends DoFn<KV<KV<String, Long>, Double>, Mutation> {
    private static final long serialVersionUID = -7963039695463881759L;

    private final SecureRandom random;

    private MapToStreamingMutation(SecureRandom random) {
      this.random = random;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      var element = Objects.requireNonNull(c.element());
      var key = Objects.requireNonNull(element.getKey());
      var value = Objects.requireNonNull(element.getValue());
      var keyKey = Objects.requireNonNull(key.getKey());
      var keyValue = Objects.requireNonNull(key.getValue());
      var ts = com.google.cloud.Timestamp.ofTimeSecondsAndNanos(keyValue, 0);
      c.output(
          Mutation.newInsertBuilder("intervals_minutes")
              .set("name")
              .to(keyKey)
              .set("interval_ts")
              .to(ts)
              .set("insert_id")
              .to(random.nextLong())
              .set("value")
              .to(value)
              .set("insert_ts")
              .to(Value.COMMIT_TIMESTAMP)
              .build());
    }
  }
}
