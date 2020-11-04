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
