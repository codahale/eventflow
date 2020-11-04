package io.eventflow.testing.beam;

import java.io.Serializable;
import java.util.function.Consumer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class PCollectionAssert {

  private PCollectionAssert() {
    // singleton
  }

  public static <T> PAssert.IterableAssert<T> assertThat(PCollection<T> actual) {
    return PAssert.that(actual);
  }

  public static <T> SerializableFunction<Iterable<T>, Void> forEach(SerializableConsumer<T> f) {
    return new SerializableFunction<>() {
      private static final long serialVersionUID = -2910645777299451294L;

      @Override
      public Void apply(Iterable<T> input) {
        for (T t : input) {
          f.accept(t);
        }
        return null;
      }
    };
  }

  public interface SerializableConsumer<T> extends Consumer<T>, Serializable {}
}
