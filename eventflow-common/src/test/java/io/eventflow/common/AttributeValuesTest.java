package io.eventflow.common;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Charsets;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.eventflow.common.pb.AttributeValue;
import java.time.Duration;
import java.time.Instant;
import org.junit.Test;

public class AttributeValuesTest {

  @Test
  public void boolValue() {
    assertEquals("{\"bool_value\":true}", format(AttributeValues.boolValue(true)));
  }

  @Test
  public void intValue() {
    assertEquals("{\"int_value\":\"100\"}", format(AttributeValues.intValue(100)));
  }

  @Test
  public void floatValue() {
    assertEquals("{\"float_value\":100.1}", format(AttributeValues.floatValue(100.1)));
  }

  @Test
  public void stringValue() {
    assertEquals("{\"string_value\":\"help\"}", format(AttributeValues.stringValue("help")));
  }

  @Test
  public void bytesValue() {
    assertEquals("{\"bytes_value\":\"aGVscA==\"}", format(AttributeValues.bytesValue("help")));
    assertEquals(
        "{\"bytes_value\":\"aGVscA==\"}",
        format(AttributeValues.bytesValue("help".getBytes(Charsets.UTF_8))));
    assertEquals(
        "{\"bytes_value\":\"aGVscA==\"}",
        format(AttributeValues.bytesValue(ByteString.copyFromUtf8("help"))));
  }

  @Test
  public void timestampValue() {
    var i = Instant.ofEpochSecond(1603937188, 230929000000000L);
    assertEquals(
        "{\"timestamp_value\":\"2020-10-31T18:15:17Z\"}",
        format(AttributeValues.timestampValue(i)));
  }

  @Test
  public void durationValue() {
    var d = Duration.ofMinutes(20);
    assertEquals("{\"duration_value\":\"1200s\"}", format(AttributeValues.durationValue(d)));
  }

  private static String format(AttributeValue v) {
    try {
      return JsonFormat.printer()
          .preservingProtoFieldNames()
          .omittingInsignificantWhitespace()
          .print(v);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
