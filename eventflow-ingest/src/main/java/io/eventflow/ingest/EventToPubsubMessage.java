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
package io.eventflow.ingest;

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
    attributes.put(Constants.TYPE_ATTRIBUTE, event.getType());
    attributes.put(Constants.SOURCE_ATTRIBUTE, event.getSource());
    attributes.put(Constants.TIMESTAMP_ATTRIBUTE, Timestamps.toString(event.getTimestamp()));
    if (event.hasCustomer()) {
      attributes.put(Constants.CUSTOMER_ATTRIBUTE, event.getCustomer().getValue());
    }

    c.output(new PubsubMessage(event.toByteArray(), attributes, ""));
  }
}
