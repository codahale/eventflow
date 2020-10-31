package io.eventflow.ingest;

import com.google.api.services.bigquery.model.Clustering;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import io.eventflow.common.Constants;
import io.eventflow.common.pb.Event;
import io.eventflow.ingest.pb.InvalidMessage;
import java.io.IOException;
import java.time.Clock;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;

public class IngestPipeline {

  public static void main(String[] args) throws IOException {
    var opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(opts);

    var coders = pipeline.getCoderRegistry();
    coders.registerCoderForClass(Event.class, ProtoCoder.of(Event.class));
    coders.registerCoderForClass(InvalidMessage.class, ProtoCoder.of(InvalidMessage.class));

    var validated =
        pipeline
            // Read incoming messages from Pub/Sub, preserving their attributes and treating the
            // `event.id` attribute as the messages' unique identifiers. If publishers set that
            // attribute to the same value as the event's id field, Dataflow guarantees exactly-once
            // processing semantics.
            .apply(
                "Read From Pub/Sub",
                PubsubIO.readMessagesWithAttributes()
                    .fromSubscription(parseSubscription(opts.getProject(), opts.getSubscription()))
                    .withIdAttribute(Constants.ID_ATTRIBUTE))

            // Parse the message data as binary or JSON-encoded Event protobufs and validate their
            // fields. Valid events are output with the VALID tag, invalid messages are encoded as
            // such and output with the INVALID tag.
            .apply(
                "Parse And Validate",
                ParDo.of(new MessageParser(Clock.systemUTC()))
                    .withOutputTags(MessageParser.VALID, TupleTagList.of(MessageParser.INVALID)));

    var validEvents = validated.get(MessageParser.VALID);

    // Batch write valid events to a common table in BigQuery.
    validEvents.apply(
        "Write Events To BigQuery",
        bigQuerySink(
                BigQueryHelpers.parseTableSpec(opts.getEventsTable()),
                Schemas.loadTableSchema("events"),
                new EventToAvro())
            .withTableDescription("Events which have been ingested and validated.")

            // Partition the events by timestamp, retaining no more than two years of data and
            // requiring a timestamp range in all queries to avoid expensive mistakes.
            .withTimePartitioning(
                new TimePartitioning()
                    .setType("DAY")
                    .setField("timestamp")
                    .setExpirationMs(2 * 365 * 24 * 60 * 60 * 1000L)
                    .setRequirePartitionFilter(true))

            // Cluster each day's events by event type, source, and customer.
            .withClustering(
                new Clustering().setFields(ImmutableList.of("type", "source", "customer"))));

    // Re-publish valid events to a Pub/Sub topic for streaming consumers. This includes event
    // fields (e.g., id, type, source, customer) as message attributes, allowing for filtered
    // subscriptions.
    validEvents
        .apply("Convert To Pub/Sub Messages", ParDo.of(new EventToPubsubMessage()))
        .apply(
            "Publish To Pub/Sub",
            PubsubIO.writeMessages().to(parseTopic(opts.getProject(), opts.getTopic())));

    // Batch write invalid messages to a common table in BigQuery.
    var invalidMessages = validated.get(MessageParser.INVALID);
    invalidMessages.apply(
        "Write Invalid Messages To BigQuery",
        bigQuerySink(
                BigQueryHelpers.parseTableSpec(opts.getInvalidMessagesTable()),
                Schemas.loadTableSchema("invalid_messages"),
                new InvalidMessageToAvro())
            .withTableDescription(
                "Messages received by the ingest job which were not valid events.")

            // Partition the invalid messages by when they were received, retaining no more than two
            // months of data and requiring a timestamp range in all queries to avoid expensive
            // mistakes.
            .withTimePartitioning(
                new TimePartitioning()
                    .setType("DAY")
                    .setField("received_at")
                    .setExpirationMs(2 * 30 * 24 * 60 * 60 * 1000L)
                    .setRequirePartitionFilter(true)));

    pipeline.run();
  }

  private static <T> BigQueryIO.Write<T> bigQuerySink(
      TableReference table,
      TableSchema schema,
      SerializableFunction<AvroWriteRequest<T>, GenericRecord> toAvro) {
    return BigQueryIO.<T>write()
        // Write rows to the given table using the given schema.
        .to(table)
        .withSchema(schema)

        // Write everything to Avro files on GCS. It's dramatically faster than JSON.
        .withAvroFormatFunction(toAvro)

        // Load everything into BigQuery from GCS instead of streaming it. For a small latency
        // penalty, this saves huge amounts of money and offers much higher throughput.
        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)

        // Run load jobs every 90 seconds. This stays under the 1500 load jobs per table per day
        // quota, which is a hard quota, but keeps the batches small, which in turn reduces the size
        // of worker instances you need to run.
        .withTriggeringFrequency(Duration.standardSeconds(90))

        // Split data across 100 GCS objects. This allows for high throughput at high concurrency.
        .withNumFileShards(100)

        // If the table doesn't exist, create it.
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

        // Always append to the table if it exists.
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)

        // Allow adding fields and making required fields nullable via load jobs. This lets you
        // manage the table schemas entirely via Dataflow with minimal pipeline downtime.
        .withSchemaUpdateOptions(
            ImmutableSet.of(
                BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION));
  }

  private static String parseSubscription(String project, String s) {
    if (ProjectSubscriptionName.isParsableFrom(s)) {
      return s;
    }
    return ProjectSubscriptionName.format(project, s);
  }

  private static String parseTopic(String project, String s) {
    if (ProjectTopicName.isParsableFrom(s)) {
      return s;
    }
    return ProjectTopicName.format(project, s);
  }

  @SuppressWarnings("unused")
  public interface Options extends DataflowPipelineOptions {
    @Description("The name of the Pub/Sub subscription to read events from.")
    @Default.String("events-ingest")
    @Validation.Required
    String getSubscription();

    void setSubscription(String s);

    @Description("The name of the Pub/Sub topic to publish validated events to.")
    @Default.String("events")
    @Validation.Required
    String getTopic();

    void setTopic(String s);

    @Description("The name of the BigQuery table of valid events.")
    @Default.String("eventflow.events")
    @Validation.Required
    String getEventsTable();

    void setEventsTable(String s);

    @Description("The name of the BigQuery table of invalid messages.")
    @Default.String("eventflow.invalid_messages")
    @Validation.Required
    String getInvalidMessagesTable();

    void setInvalidMessagesTable(String s);
  }
}
