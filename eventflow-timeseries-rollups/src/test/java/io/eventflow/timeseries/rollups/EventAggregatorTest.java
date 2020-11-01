package io.eventflow.timeseries.rollups;

import com.google.common.collect.ImmutableMultimap;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.AttributeValues;
import io.eventflow.common.pb.Event;
import java.text.ParseException;
import java.time.Duration;
import org.apache.beam.sdk.testing.PAssert;
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
  private final EventAggregator eventAggregator = new EventAggregator(customRollups);

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

    PAssert.that(results)
        .containsInAnyOrder(
            "insert(intervals_minutes{name=three.duration,interval_ts=2020-11-01T12:24:00Z,insert_id=2279999,value=1.32E9,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.int,interval_ts=2020-11-01T12:24:00Z,insert_id=2279999,value=200.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.float,interval_ts=2020-11-01T12:24:00Z,insert_id=2279999,value=404.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=three.count,interval_ts=2020-11-01T12:24:00Z,insert_id=2279999,value=1.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=one.count,interval_ts=2020-11-01T12:23:00Z,insert_id=2279999,value=2.0,insert_ts=spanner.commit_timestamp()})",
            "insert(intervals_minutes{name=two.count,interval_ts=2020-11-01T12:24:00Z,insert_id=2279999,value=1.0,insert_ts=spanner.commit_timestamp()})");

    pipeline.run();
  }
}
