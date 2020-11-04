/*
 * Copyright 2020 Coda Hale
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
package io.eventflow.testing.beam;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

public class SchemaAssert extends AbstractAssert<SchemaAssert, Schema> {

  public static SchemaAssert assertThat(Schema schema) {
    return new SchemaAssert(schema);
  }

  private SchemaAssert(Schema schema) {
    super(schema, SchemaAssert.class);
  }

  public void canReadAndWrite(GenericRecord record) {
    var out = new ByteArrayOutputStream();
    try {
      var dWriter = new GenericDatumWriter<GenericRecord>(actual);
      try (var fWriter = new DataFileWriter<>(dWriter)) {
        fWriter.create(actual, out);
        fWriter.append(record);
      }

    } catch (IOException e) {
      Assertions.fail("error writing record", e);
    }

    try {
      var dReader = new GenericDatumReader<GenericRecord>(actual);
      var in = new SeekableByteArrayInput(out.toByteArray());
      try (var fReader = new DataFileReader<>(in, dReader)) {
        fReader.forEachRemaining(r -> Assertions.assertThat(r).isEqualTo(record));
      }
    } catch (IOException e) {
      Assertions.fail("error reading record", e);
    }
  }
}
