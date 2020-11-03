package io.eventflow.publisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import io.eventflow.testing.ProtobufAssert;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

public class EventPublisherTest {
  private final Instant instant = Instant.ofEpochSecond(12345678);
  private final Clock clock = Clock.fixed(instant, ZoneOffset.UTC);

  @Rule public MockitoRule rule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);
  @Mock private Publisher pubsub;
  @Mock private Supplier<String> idGenerator;

  @Test
  public void publishEvent() {
    var publisher = new EventPublisher(clock, "publisher", pubsub, idGenerator);

    when(idGenerator.get()).thenReturn("000102030405060708090A0B0C0D0E0F");
    when(pubsub.publish(any())).thenReturn(ApiFutures.immediateFuture("id"));

    var event = Event.newBuilder().setType("click").build();
    assertThat(publisher.publish(event)).succeedsWithin(Duration.ofMillis(100)).isEqualTo("id");

    verify(pubsub)
        .publish(
            argThat(
                msg -> {
                  assertThat(msg.getAttributesMap())
                      .containsOnly(
                          Map.entry("event.id", "000102030405060708090A0B0C0D0E0F"),
                          Map.entry("event.timestamp", "1970-05-23T21:21:18Z"));
                  ProtobufAssert.assertThat(Event::parseFrom)
                      .canParse(msg.getData().toByteArray())
                      .isEqualTo(
                          Event.newBuilder(event)
                              .setId("000102030405060708090A0B0C0D0E0F")
                              .setSource("publisher")
                              .setTimestamp(Timestamps.fromSeconds(12345678))
                              .build());
                  return true;
                }));
  }
}
