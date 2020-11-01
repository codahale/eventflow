package io.eventflow.timeseries.rollups;

import com.google.pubsub.v1.ProjectSubscriptionName;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.time.temporal.ChronoUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class StreamingRollupPipeline {
  public static void main(String[] args) {
    var opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(opts);

    var coders = pipeline.getCoderRegistry();
    coders.registerCoderForClass(Event.class, ProtoCoder.of(Event.class));

    pipeline
        .apply(
            "Read Events",
            PubsubIO.readProtos(Event.class)
                .fromSubscription(parseSubscription(opts.getProject(), opts.getSubscription()))
                .withIdAttribute(Constants.ID_ATTRIBUTE))
        .apply(
            "Map To Group Key And Values",
            ParDo.of(new MapToGroupKeysAndValues(opts.getCustomRollups(), ChronoUnit.MINUTES)))
        .apply("Window By Minute", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("Group And Sum", Sum.doublesPerKey())
        .apply("Map To Mutation", ParDo.of(new MapToStreamingMutation(new SecureRandom())))
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
  public interface Options extends CommonOptions {
    @Description("The name of the Pub/Sub subscription to read events from.")
    @Default.String("events-rollups")
    @Validation.Required
    String getSubscription();

    void setSubscription(String s);
  }
}
