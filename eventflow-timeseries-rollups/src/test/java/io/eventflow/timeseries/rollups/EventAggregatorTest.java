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
import static io.eventflow.testing.beam.PCollectionAssert.givenAll;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.AttributeValues;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Duration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class EventAggregatorTest {
  @Rule public final TestPipeline pipeline = TestPipeline.create();

  private final RollupSpec rollups =
      RollupSpec.parse("two:max(int),two:min(float),three:sum(duration)");
  private final EventAggregator eventAggregator = new EventAggregator(rollups, new FakeRandom());

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
                        .setType("two")
                        .setTimestamp(Timestamps.parse("2020-11-01T12:24:03Z"))
                        .putAttributes("int", AttributeValues.intValue(100))
                        .putAttributes("float", AttributeValues.floatValue(300))
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
            .apply(eventAggregator);

    assertThat(results)
        .satisfies(
            givenAll(
                inserts ->
                    assertThat(inserts)
                        .containsExactlyInAnyOrder(
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("one.count")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:23:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(2.0)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build(),
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("two.count")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:24:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(2.0)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build(),
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("two.float.min")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:24:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(300.0)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build(),
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("two.int.max")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:24:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(200.0)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build(),
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("three.count")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:24:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(1.0)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build(),
                            Mutation.newInsertBuilder("intervals_minutes")
                                .set("name")
                                .to("three.duration.sum")
                                .set("interval_ts")
                                .to(Timestamp.parseTimestamp("2020-11-01T12:24:00Z"))
                                .set("insert_id")
                                .to(12345L)
                                .set("value")
                                .to(1.32e9)
                                .set("insert_ts")
                                .to(Value.COMMIT_TIMESTAMP)
                                .build())));
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
