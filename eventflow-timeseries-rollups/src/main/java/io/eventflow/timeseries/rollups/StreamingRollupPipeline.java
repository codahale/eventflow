package io.eventflow.timeseries.rollups;

import com.google.pubsub.v1.ProjectSubscriptionName;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;

public class StreamingRollupPipeline {
  public static void main(String[] args) {
    var opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(opts);

    var coders = pipeline.getCoderRegistry();
    coders.registerCoderForClass(Event.class, ProtoCoder.of(Event.class));

    var customRollups = EventAggregator.parseCustomRollups(opts.getCustomRollups());
    pipeline
        .apply(
            "Read Events",
            PubsubIO.readProtos(Event.class)
                .fromSubscription(parseSubscription(opts.getProject(), opts.getSubscription()))
                .withIdAttribute(Constants.ID_ATTRIBUTE))
        .apply("Aggregate Events", new EventAggregator(customRollups, new SecureRandom()))
        .apply(
            "Write To Spanner",
            SpannerIO.write()
                .withProjectId(opts.getProject())
                .withInstanceId(opts.getInstanceId())
                .withDatabaseId(opts.getDatabaseId()));
  }

  private static String parseSubscription(String project, String s) {
    if (ProjectSubscriptionName.isParsableFrom(s)) {
      return s;
    }
    return ProjectSubscriptionName.format(project, s);
  }

  @SuppressWarnings("unused")
  public interface Options extends DataflowPipelineOptions {
    @Description("The name of the Pub/Sub subscription to read events from.")
    @Default.String("events-rollups")
    @Validation.Required
    String getSubscription();

    void setSubscription(String s);

    @Description("The Spanner instance to write to.")
    @Validation.Required
    String getInstanceId();

    void setInstanceId(String s);

    @Description("The Spanner database to write to.")
    @Validation.Required
    String getDatabaseId();

    void setDatabaseId(String s);

    @Description("Comma-delimited, equal-separated event types and attribute names to be summed.")
    @Default.String("")
    String getCustomRollups();

    void setCustomRollups();
  }
}
