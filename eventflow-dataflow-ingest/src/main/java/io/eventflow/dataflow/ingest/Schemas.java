package io.eventflow.dataflow.ingest;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

public class Schemas {
  private Schemas() {
    // singleton
  }

  public static TableSchema loadTableSchema(String table) throws IOException {
    var resource = Resources.getResource(Schemas.class, String.format("/schemas/%s.json", table));
    var json = Resources.toString(resource, Charsets.UTF_8);
    return BigQueryHelpers.fromJsonString(json, TableSchema.class);
  }

  public static Schema tableSchemaToAvroSchema(TableSchema tableSchema)
      throws ReflectiveOperationException {
    var klass = Class.forName("org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtils");
    var method = klass.getDeclaredMethod("toGenericAvroSchema", String.class, List.class);
    method.setAccessible(true);

    return (Schema) method.invoke(null, "root", tableSchema.getFields());
  }
}
