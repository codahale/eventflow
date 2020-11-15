/*
 * Copyright © 2020 Coda Hale (coda.hale@gmail.com)
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

import com.google.api.services.bigquery.model.TableSchema;
import java.util.List;
import org.apache.avro.Schema;

class BeamAvroSchemas {
  private BeamAvroSchemas() {
    // singleton
  }

  /**
   * Uses Beam to convert the given BigQuery table schema into an Avro schema.
   *
   * <p>When Beam writes data to GCS to be loaded into BigQuery, it generates a temporary Avro
   * schema based on the schema of the destination table. This uses reflection to call that
   * package-private method so that we can test the compatibility of our generated records with the
   * Avro schema Beam will be using to write them.
   */
  public static Schema tableSchemaToAvroSchema(TableSchema tableSchema)
      throws ReflectiveOperationException {
    var klass = Class.forName("org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtils");
    var method = klass.getDeclaredMethod("toGenericAvroSchema", String.class, List.class);
    method.setAccessible(true);

    return (Schema) method.invoke(null, "root", tableSchema.getFields());
  }
}
