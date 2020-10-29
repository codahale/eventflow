package io.eventflow.dataflow.ingest;

import com.google.pubsub.v1.ProjectSubscriptionName;
import io.eventflow.common.pb.Event;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

public class IngestPipeline {
  public static void main(String[] args) {
    var opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    var pipeline = Pipeline.create(opts);
    pipeline.getCoderRegistry().registerCoderForClass(Event.class, ProtoCoder.of(Event.class));

    var messages =
        pipeline.apply(
            "Read From Pub/Sub",
            PubsubIO.readMessagesWithAttributes()
                .fromSubscription(parseSubscription(opts.getProject(), opts.getSubscription()))
                .withIdAttribute("id"));

    // TODO validate
    // TODO write events to BQ via Avro
    // TODO write failured to BQ via Avro

    pipeline.run();
  }

  private static String parseSubscription(String project, String s) {
    if (ProjectSubscriptionName.isParsableFrom(s)) {
      return s;
    }
    return ProjectSubscriptionName.format(project, s);
  }

  public interface Options extends DataflowPipelineOptions {
    @Description("The name of the Pub/Sub subscription to read events from.")
    @Default.String("events-ingest")
    @Validation.Required
    String getSubscription();

    void setSubscription(String s);
  }
}
