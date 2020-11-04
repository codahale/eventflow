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

import static io.eventflow.testing.beam.PCollectionAssert.assertThat;

import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.AttributeValues;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Duration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class EventAggregatorTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private final ImmutableMultimap<String, String> customRollups =
      EventAggregator.parseCustomRollups("two=int,two=float,three=duration");
  private final EventAggregator eventAggregator =
      new EventAggregator(customRollups, new FakeRandom());

  @Test
  public void endToEnd() throws ParseException {
    var results =
        pipeline
            .apply(
                Create.of(
                    Event.newBuilder()
                        .setType("one")
                        .setTimestamp(Timestamps.parse("2020-11-01T12:23:34Z"))
                        .build(),
                    Event.newBuilder()
                        .setType("one")
                        .setTimestamp(Timestamps.parse("2020-11-01T12:23:36Z"))
                        .build(),
                    Event.newBuilder()
                        .setType("two")
                        .setTimestamp(Timestamps.parse("2020-11-01T12:24:02Z"))
                        .putAttributes("int", AttributeValues.intValue(200))
                        .putAttributes("float", AttributeValues.floatValue(404))
                        .build(),
                    Event.newBuilder()
                        .setType("three")
                        .setTimestamp(Timestamps.parse("2020-11-01T12:24:59Z"))
                        .putAttributes("int", AttributeValues.intValue(200))
                        .putAttributes("float", AttributeValues.floatValue(404))
                        .putAttributes(
                            "duration", AttributeValues.durationValue(Duration.ofMinutes(22)))
                        .build()))
            .apply(WithTimestamps.of(input -> Instant.ofEpochSecond(2234)))
            .apply(eventAggregator)
            .apply(ToString.elements());

    assertThat(results)
        .containsInAnyOrder(
            "insert(intervals_minutes{name=three.duration,interval_ts=2020-11-01T12:24:00Z,insert_id=12345,value=1.32E9,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.int,interval_ts=2020-11-01T12:24:00Z,insert_id=12345,value=200.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.float,interval_ts=2020-11-01T12:24:00Z,insert_id=12345,value=404.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=three.count,interval_ts=2020-11-01T12:24:00Z,insert_id=12345,value=1.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=one.count,interval_ts=2020-11-01T12:23:00Z,insert_id=12345,value=2.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.count,interval_ts=2020-11-01T12:24:00Z,insert_id=12345,value=1.0,insert_ts=spanner.commit_timestamp()})");

    pipeline.run();
  }

  private static class FakeRandom extends SecureRandom {
    private static final long serialVersionUID = 1510630497569983702L;

    @Override
    public long nextLong() {
      return 12345;
    }
  }
}
