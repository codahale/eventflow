package io.eventflow.dataflow.ingest;

import com.google.protobuf.util.Timestamps;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.util.HashMap;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class EventToPubsubMessage extends DoFn<Event, PubsubMessage> {
  private static final long serialVersionUID = 6311079205948964090L;

  @ProcessElement
  public void processElement(ProcessContext c) {
    var event = c.element();

    var attributes = new HashMap<String, String>();
    attributes.put(Constants.ID_ATTRIBUTE, event.getId());
    attributes.put("event.type", event.getType());
    attributes.put("event.source", event.getSource());
    attributes.put("event.timestamp", Timestamps.toString(event.getTimestamp()));
    if (event.hasCustomer()) {
      attributes.put("event.customer", event.getCustomer().getValue());
    }

    c.output(new PubsubMessage(event.toByteArray(), attributes));
  }
}
