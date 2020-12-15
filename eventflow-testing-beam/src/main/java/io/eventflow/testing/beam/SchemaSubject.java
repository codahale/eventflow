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
package io.eventflow.testing.beam;

import static com.google.common.truth.Truth.assertAbout;

import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SchemaSubject extends Subject {
  public static Subject.Factory<SchemaSubject, Schema> schemas() {
    return SchemaSubject::new;
  }

  public static SchemaSubject assertThat(Schema actual) {
    return assertAbout(schemas()).that(actual);
  }

  @Nullable private final Schema actual;

  private SchemaSubject(FailureMetadata metadata, @Nullable Schema actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public void canReadAndWrite(GenericRecord record) throws IOException {
    var out = new ByteArrayOutputStream();
    var dWriter = new GenericDatumWriter<GenericRecord>(actual);
    try (var fWriter = new DataFileWriter<>(dWriter)) {
      fWriter.create(actual, out);
      fWriter.append(record);
    }

    var dReader = new GenericDatumReader<GenericRecord>(actual);
    var in = new SeekableByteArrayInput(out.toByteArray());
    try (var fReader = new DataFileReader<>(in, dReader)) {
      fReader.forEachRemaining(
          r -> check("deserialize(schema.serialize(record))").that(r).isEqualTo(record));
    }
  }
}
