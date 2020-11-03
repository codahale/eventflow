package io.eventflow.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.eventflow.common.pb.AttributeValue;
import java.time.Duration;
import java.time.Instant;
import org.junit.Test;

public class AttributeValuesTest {

  @Test
  public void boolValue() {
    assertThat(AttributeValues.boolValue(true))
        .isEqualTo(AttributeValue.newBuilder().setBoolValue(true).build());
  }

  @Test
  public void intValue() {
    assertThat(AttributeValues.intValue(100))
        .isEqualTo(AttributeValue.newBuilder().setIntValue(100).build());
  }

  @Test
  public void floatValue() {
    assertThat(AttributeValues.floatValue(100.1))
        .isEqualTo(AttributeValue.newBuilder().setFloatValue(100.1).build());
  }

  @Test
  public void stringValue() {
    assertThat(AttributeValues.stringValue("help"))
        .isEqualTo(AttributeValue.newBuilder().setStringValue("help").build());
  }

  @Test
  public void bytesValue() {
    var bytes = ByteString.copyFromUtf8("help");

    assertThat(AttributeValues.bytesValue("help"))
        .isEqualTo(AttributeValue.newBuilder().setBytesValue(bytes).build());
    assertThat(AttributeValues.bytesValue(bytes.toByteArray()))
        .isEqualTo(AttributeValue.newBuilder().setBytesValue(bytes).build());
    assertThat(AttributeValues.bytesValue(bytes))
        .isEqualTo(AttributeValue.newBuilder().setBytesValue(bytes).build());
  }

  @Test
  public void timestampValue() {
    var i = Instant.ofEpochMilli(1603937188123L);

    assertThat(AttributeValues.timestampValue(i))
        .isEqualTo(
            AttributeValue.newBuilder()
                .setTimestampValue(
                    Timestamp.newBuilder().setSeconds(1603937188).setNanos(123000000).build())
                .build());
  }

  @Test
  public void durationValue() {
    assertThat(AttributeValues.durationValue(Duration.ofMillis(20_000_123)))
        .isEqualTo(
            AttributeValue.newBuilder()
                .setDurationValue(
                    com.google.protobuf.Duration.newBuilder()
                        .setSeconds(20000)
                        .setNanos(123000000)
                        .build())
                .build());
  }
}
