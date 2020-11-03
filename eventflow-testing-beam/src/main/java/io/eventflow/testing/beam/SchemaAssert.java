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
