package io.eventflow.testing;

import static org.assertj.core.api.Assertions.fail;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.assertj.core.api.AssertDelegateTarget;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectAssert;

public class ProtobufAssert<T extends Message> implements AssertDelegateTarget {

  public interface Parser<T> {

    T parse(byte[] data) throws InvalidProtocolBufferException;
  }

  public static <T extends Message> ProtobufAssert<T> assertThat(Parser<T> parser) {
    return new ProtobufAssert<>(parser);
  }

  private final Parser<T> parser;

  private ProtobufAssert(Parser<T> parser) {
    this.parser = parser;
  }

  public ObjectAssert<T> canParse(byte[] data) {
    try {
      return Assertions.assertThat(parser.parse(data));
    } catch (InvalidProtocolBufferException e) {
      return fail("unable to parse data", e);
    }
  }
}
