/*
 * Copyright 2020 Coda Hale
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
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableTable;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.AttributeValue;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
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
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

public class EventAggregator extends PTransform<PCollection<Event>, PCollection<Mutation>> {
  private static final long serialVersionUID = -4941981619223641177L;

  private static final TupleTag<KV<KV<String, Long>, Double>> SUM =
      new TupleTag<>("sum") {
        private static final long serialVersionUID = 365666602980929507L;
      };

  private static final TupleTag<KV<KV<String, Long>, Double>> MIN =
      new TupleTag<>("min") {
        private static final long serialVersionUID = 365666602980929507L;
      };

  private static final TupleTag<KV<KV<String, Long>, Double>> MAX =
      new TupleTag<>("max") {
        private static final long serialVersionUID = 365666602980929507L;
      };

  private final ImmutableTable<String, String, TupleTag<KV<KV<String, Long>, Double>>> rollups;

  private final SecureRandom random;

  public EventAggregator(
      ImmutableTable<String, String, TupleTag<KV<KV<String, Long>, Double>>> rollups,
      SecureRandom random) {
    this.rollups = rollups;
    this.random = random;
  }

  @Override
  public PCollection<Mutation> expand(PCollection<Event> input) {
    var values =
        input
            .apply("Widow By Minute", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
            .apply(
                "Map To Group Key And Values",
                ParDo.of(new MapToGroupKeysAndValues(rollups))
                    .withOutputTags(SUM, TupleTagList.of(List.of(MIN, MAX))));

    return PCollectionList.of(
            List.of(
                values.get(SUM).apply("Sum Values", Sum.doublesPerKey()),
                values.get(MIN).apply("Min Values", Min.doublesPerKey()),
                values.get(MAX).apply("Max Values", Max.doublesPerKey())))
        .apply("Flatten", Flatten.pCollections())
        .apply("Map To Mutation", ParDo.of(new MapToStreamingMutation(random)));
  }

  private static final Pattern ROLLUP = Pattern.compile("^(sum|max|min)\\((.+)\\)$");

  public static ImmutableTable<String, String, TupleTag<KV<KV<String, Long>, Double>>> parseRollups(
      String s) {
    var builder = ImmutableTable.<String, String, TupleTag<KV<KV<String, Long>, Double>>>builder();
    for (String custom : Splitter.on(',').split(s)) {
      var parts = Splitter.on('=').limit(2).splitToList(custom);
      var matcher = ROLLUP.matcher(parts.get(1));
      if (!matcher.matches()) {
        throw new IllegalArgumentException("bad rollup: " + parts.get(1));
      }

      if (matcher.group(1).equals(MIN.getId())) {
        builder.put(parts.get(0), matcher.group(2), MIN);
      } else if (matcher.group(1).equals(MAX.getId())) {
        builder.put(parts.get(0), matcher.group(2), MAX);
      } else {
        builder.put(parts.get(0), matcher.group(2), SUM);
      }
    }
    return builder.build();
  }

  private static class MapToGroupKeysAndValues extends DoFn<Event, KV<KV<String, Long>, Double>> {
    private static final long serialVersionUID = -3444438370108834605L;

    private final ImmutableTable<String, String, TupleTag<KV<KV<String, Long>, Double>>> rollups;

    public MapToGroupKeysAndValues(
        ImmutableTable<String, String, TupleTag<KV<KV<String, Long>, Double>>> rollups) {
      this.rollups = rollups;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      var event = c.element();
      var ts = truncate(event.getTimestamp());

      // Record the count.
      c.output(SUM, KV.of(KV.of(metricName(event, "count"), ts), 1.0));

      // Record all other rollups.
      for (var rollup : rollups.row(event.getType()).entrySet()) {
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

      for (String name : names) {
        j.add(name);
      }
      return j.toString();
    }

    private long truncate(com.google.protobuf.Timestamp timestamp) {
      return Instant.ofEpochMilli(Timestamps.toMillis(timestamp))
          .truncatedTo(ChronoUnit.MINUTES)
          .getEpochSecond();
    }
  }

  private static class MapToStreamingMutation extends DoFn<KV<KV<String, Long>, Double>, Mutation> {
    private static final long serialVersionUID = -7963039695463881759L;

    private final SecureRandom random;

    private MapToStreamingMutation(SecureRandom random) {
      this.random = random;
    }

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
              .to(random.nextLong())
              .set("value")
              .to(c.element().getValue())
              .set("insert_ts")
              .to(Value.COMMIT_TIMESTAMP)
              .build());
    }
  }
}
