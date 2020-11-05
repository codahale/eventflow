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
import org.apache.beam.sdk.transforms.SerializableFunction;

public class PCollectionAssert {
  private PCollectionAssert() {}

  public static <T> SerializableFunction<Iterable<T>, Void> givenAll(
      SerializableConsumer<Iterable<T>> f) {
    return new SerializableFunction<>() {
      private static final long serialVersionUID = -2910645777299451294L;

      @Override
      public Void apply(Iterable<T> input) {
        try {
          f.accept(input);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };
  }

  public interface SerializableConsumer<T> extends Serializable {
    void accept(T t) throws Exception;
  }
}
