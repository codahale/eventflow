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
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class PCollectionSubject<T> extends Subject {
  public static <T> Subject.Factory<PCollectionSubject<T>, PCollection<T>> pCollections() {
    return PCollectionSubject::new;
  }

  public static <T> PCollectionSubject<T> assertThat(PCollection<T> actual) {
    return assertAbout(PCollectionSubject.<T>pCollections()).that(actual);
  }

  private final PCollection<T> actual;

  private PCollectionSubject(FailureMetadata metadata, PCollection<T> actual) {
    super(metadata, actual);
    this.actual = actual;
  }

  public void isEmpty() {
    PAssert.that(actual).empty();
  }

  @SafeVarargs
  public final void containsExactlyInAnyOrder(T... elements) {
    PAssert.that(actual).containsInAnyOrder(elements);
  }

  public void allSatisfy(SerializableConsumer<T> consumer) {
    PAssert.that(actual).satisfies(new SerConFunc<>(consumer));
  }

  public interface SerializableConsumer<T> extends Serializable {
    void accept(T t) throws Exception;
  }

  private static class SerConFunc<T> implements SerializableFunction<Iterable<T>, Void> {
    private static final long serialVersionUID = -3724259793467901505L;

    private final SerializableConsumer<T> consumer;

    SerConFunc(SerializableConsumer<T> consumer) {
      this.consumer = consumer;
    }

    @Override
    public Void apply(Iterable<T> input) {
      for (T t : Objects.requireNonNull(input)) {
        try {
          consumer.accept(t);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return null;
    }
  }
}
