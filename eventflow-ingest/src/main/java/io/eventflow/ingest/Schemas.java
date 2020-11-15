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
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

/** Helper methods for managing BigQuery schemas. */
public class Schemas {
  private Schemas() {
    // singleton
  }

  /** Loads the table schema for the given table. */
  public static TableSchema loadTableSchema(String table) throws IOException {
    var resource = Resources.getResource(Schemas.class, String.format("/schemas/%s.json", table));
    var json = Resources.toString(resource, Charsets.UTF_8);
    return BigQueryHelpers.fromJsonString(json, TableSchema.class);
  }
}
