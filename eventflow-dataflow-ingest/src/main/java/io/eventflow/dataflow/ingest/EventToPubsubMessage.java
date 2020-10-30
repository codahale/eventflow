package io.eventflow.dataflow.ingest;

import com.google.common.collect.ImmutableMap;
import io.eventflow.common.pb.Event;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

public class EventToPubsubMessage extends DoFn<Event, PubsubMessage> {
  private static final long serialVersionUID = 6311079205948964090L;

  @ProcessElement
  public void processElement(ProcessContext c) {
    var event = c.element();
    var attributes = ImmutableMap.<String, String>builder();
    attributes
        .put(IngestPipeline.ID_ATTRIBUTE, event.getId())
        .put("event.type", event.getType())
        .put("event.source", event.getSource());

    if (event.hasCustomer()) {
      attributes.put("event.customer", event.getCustomer().getValue());
    }

    c.output(new PubsubMessage(event.toByteArray(), attributes.build()));
  }
}
