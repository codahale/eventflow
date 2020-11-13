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
package io.eventflow.common;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import io.eventflow.common.pb.AttributeValue;
import java.time.Duration;
import java.time.Instant;

public class AttributeValues {
  private AttributeValues() {
    // singleton
  }

  public static AttributeValue boolValue(boolean b) {
    return AttributeValue.newBuilder().setBoolValue(b).build();
  }

  public static AttributeValue intValue(long n) {
    return AttributeValue.newBuilder().setIntValue(n).build();
  }

  public static AttributeValue floatValue(double n) {
    return AttributeValue.newBuilder().setFloatValue(n).build();
  }

  public static AttributeValue stringValue(String s) {
    return AttributeValue.newBuilder().setStringValue(s).build();
  }

  public static AttributeValue bytesValue(byte[] b) {
    return bytesValue(ByteString.copyFrom(b));
  }

  public static AttributeValue bytesValue(String b) {
    return bytesValue(ByteString.copyFromUtf8(b));
  }

  public static AttributeValue bytesValue(ByteString b) {
    return AttributeValue.newBuilder().setBytesValue(b).build();
  }

  public static AttributeValue timestampValue(Instant i) {
    return AttributeValue.newBuilder()
        .setTimestampValue(
            Timestamp.newBuilder().setSeconds(i.getEpochSecond()).setNanos(i.getNano()).build())
        .build();
  }

  public static AttributeValue durationValue(Duration d) {
    return AttributeValue.newBuilder().setDurationValue(Durations.fromNanos(d.toNanos())).build();
  }
}
