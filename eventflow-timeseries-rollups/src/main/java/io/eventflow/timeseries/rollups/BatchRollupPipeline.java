package io.eventflow.timeseries.rollups;

import io.eventflow.common.pb.Event;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;

public class BatchRollupPipeline {
  public static void main(String[] args) {
    var opts = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    var pipeline = Pipeline.create(opts);

    var coders = pipeline.getCoderRegistry();
    coders.registerCoderForClass(Event.class, ProtoCoder.of(Event.class));

    var chronoUnit = ChronoUnit.valueOf(opts.getChronoUnit().toUpperCase(Locale.ENGLISH));
    var tableName =
        opts.getTableNameBase() + "_" + chronoUnit.toString().toLowerCase(Locale.ENGLISH);
    pipeline
        .apply(
            "Read Events",
            BigQueryIO.read(new AvroToEvent())
                .usingStandardSql()
                .fromQuery(opts.getQuery())
                .usingStandardSql()
                .withoutResultFlattening())
        .apply(
            "Map To Group Key And Values",
            ParDo.of(new MapToGroupKeysAndValues(opts.getCustomRollups(), chronoUnit)))
        .apply("Group And Sum", Sum.doublesPerKey())
        .apply("Map To Mutation", ParDo.of(new MapToBatchMutation(tableName)))
        .apply(
            "Write To Spanner",
            SpannerIO.write()
                .withProjectId(opts.getProject())
                .withInstanceId(opts.getInstanceId())
                .withDatabaseId(opts.getDatabaseId()));
  }

  @SuppressWarnings("unused")
  public interface Options extends CommonOptions {

    String getChronoUnit();

    void setChronoUnit(String s);

    String getTableNameBase();

    void setTableNameBase(String s);

    String getQuery();

    void setQuery(String s);
  }
}
