package io.eventflow.testing.beam;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;

public class PCollectionAssert {

  private PCollectionAssert() {
    // singleton
  }

  public static <T> PAssert.IterableAssert<T> assertThat(PCollection<T> actual) {
    return PAssert.that(actual);
  }
}
