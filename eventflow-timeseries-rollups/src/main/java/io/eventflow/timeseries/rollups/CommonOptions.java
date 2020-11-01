package io.eventflow.timeseries.rollups;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface CommonOptions extends DataflowPipelineOptions {

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
