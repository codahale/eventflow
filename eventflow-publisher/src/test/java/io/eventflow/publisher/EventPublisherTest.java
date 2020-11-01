package io.eventflow.publisher;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import io.eventflow.common.pb.Event;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
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
    var f = publisher.publish(event);

    assertEquals("id", f.get());

    verify(pubsub)
        .publish(
            PubsubMessage.newBuilder()
                .setData(
                    ByteString.copyFrom(
                        new byte[] {
                          10, 32, 48, 48, 48, 49, 48, 50, 48, 51, 48, 52, 48, 53, 48, 54, 48, 55,
                          48, 56, 48, 57, 48, 65, 48, 66, 48, 67, 48, 68, 48, 69, 48, 70, 18, 5, 99,
                          108, 105, 99, 107, 26, 9, 112, 117, 98, 108, 105, 115, 104, 101, 114, 42,
                          5, 8, -50, -62, -15, 5
                        }))
                .putAttributes("event.id", "000102030405060708090A0B0C0D0E0F")
                .putAttributes("event.timestamp", "1970-05-23T21:21:18Z")
                .build());
  }
}
