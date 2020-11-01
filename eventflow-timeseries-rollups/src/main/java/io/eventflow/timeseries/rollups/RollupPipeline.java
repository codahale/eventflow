package io.eventflow.timeseries.rollups;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.ProjectSubscriptionName;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;

public class RollupPipeline {
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
            "Map To Name And Interval Timestamp",
            ParDo.of(
                new DoFn<Event, KV<String, Long>>() {
                  private static final long serialVersionUID = 224378893979284759L;

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    var event = c.element();
                    c.output(KV.of(name(event), truncate(event.getTimestamp())));
                  }

                  private String name(Event event) {
                    if (event.hasCustomer()) {
                      return event.getType() + "." + event.getCustomer().getValue();
                    }
                    return event.getType();
                  }

                  private long truncate(com.google.protobuf.Timestamp timestamp) {
                    return Instant.ofEpochMilli(Timestamps.toMillis(timestamp))
                            .truncatedTo(ChronoUnit.MINUTES)
                            .toEpochMilli()
                        * 1000;
                  }
                }))
        .apply("Window By Minute", Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("Group And Count", Count.perElement())
        .apply(
            "Map To Mutation",
            ParDo.of(
                new DoFn<KV<KV<String, Long>, Long>, Mutation>() {
                  private static final long serialVersionUID = 8102646569420002766L;

                  private final SecureRandom random = new SecureRandom();

                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    var name = c.element().getKey().getKey();
                    var ts =
                        com.google.cloud.Timestamp.ofTimeMicroseconds(
                            c.element().getKey().getValue());
                    var count = c.element().getValue();

                    c.output(
                        Mutation.newInsertBuilder("intervals_minutely")
                            .set("name")
                            .to(name)
                            .set("interval_ts")
                            .to(ts)
                            .set("insert_id")
                            .to(random.nextLong())
                            .set("value")
                            .to(count.doubleValue())
                            .set("insert_ts")
                            .to(Value.COMMIT_TIMESTAMP)
                            .build());
                  }
                }))
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
  }
}
