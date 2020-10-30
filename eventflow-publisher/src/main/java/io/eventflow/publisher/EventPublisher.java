package io.eventflow.publisher;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.PubsubMessage;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.time.Clock;

public class EventPublisher {
  private final Clock clock;
  private final String source;
  private final Publisher publisher;
  private final SecureRandom random;

  public EventPublisher(Clock clock, String source, Publisher publisher, SecureRandom random) {
    this.clock = clock;
    this.source = source;
    this.publisher = publisher;
    this.random = random;
  }

  public ApiFuture<String> publish(Event.Builder event) {
    var id = generateId();
    event.setId(id).setTimestamp(Timestamps.fromMillis(clock.millis())).setSource(source);

    return publisher.publish(
        PubsubMessage.newBuilder()
            .setData(event.build().toByteString())
            .putAttributes(Constants.ID_ATTRIBUTE, id)
            .putAttributes(Constants.TIMESTAMP_ATTRIBUTE, Timestamps.toString(event.getTimestamp()))
            .build());
  }

  public ApiFuture<String> publish(Event event) {
    return publish(Event.newBuilder(event));
  }

  private String generateId() {
    var buf = new byte[16];
    random.nextBytes(buf);
    return BaseEncoding.base16().encode(buf);
  }
}
