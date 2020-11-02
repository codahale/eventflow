package io.eventflow.publisher;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import io.eventflow.common.pb.Event;
import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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
  public void publishEvent() throws ExecutionException, InterruptedException {
    var publisher = new EventPublisher(clock, "publisher", pubsub, idGenerator);

    when(idGenerator.get()).thenReturn("000102030405060708090A0B0C0D0E0F");
    when(pubsub.publish(any())).thenReturn(ApiFutures.immediateFuture("id"));

    var event = Event.newBuilder().setType("click").build();
    assertEquals("id", publisher.publish(event).get());

    verify(pubsub)
        .publish(
            argThat(
                msg -> {
                  assertEquals(
                      Map.of(
                          "event.id",
                          "000102030405060708090A0B0C0D0E0F",
                          "event.timestamp",
                          "1970-05-23T21:21:18Z"),
                      msg.getAttributesMap());
                  try {
                    assertEquals(
                        Event.newBuilder(event)
                            .setId("000102030405060708090A0B0C0D0E0F")
                            .setSource("publisher")
                            .setTimestamp(Timestamps.parse("1970-05-23T21:21:18Z"))
                            .build(),
                        Event.parseFrom(msg.getData()));
                  } catch (ParseException | InvalidProtocolBufferException e) {
                    throw new AssertionError(e);
                  }
                  return true;
                }));
  }
}
