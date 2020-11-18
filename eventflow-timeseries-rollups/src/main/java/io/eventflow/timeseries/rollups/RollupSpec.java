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
package io.eventflow.timeseries.rollups;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

/** A specification of custom rollup types. */
public class RollupSpec implements Serializable {

  private static final long serialVersionUID = 7801555686791523874L;

  static final TupleTag<KV<KV<String, Long>, Double>> SUM =
      new TupleTag<>("sum") {
        private static final long serialVersionUID = 365666602980929507L;
      };
  static final TupleTag<KV<KV<String, Long>, Double>> MIN =
      new TupleTag<>("min") {
        private static final long serialVersionUID = 365666602980929507L;
      };
  static final TupleTag<KV<KV<String, Long>, Double>> MAX =
      new TupleTag<>("max") {
        private static final long serialVersionUID = 365666602980929507L;
      };

  /**
   * Parses a rollup spec.
   *
   * <p>Rollup specs are comma-delimited triples separated by colons:
   *
   * <p>{@code event_type:func:attr1,event_type:func2:attr2}
   *
   * <p>The supported aggregate functions are {@code min}, {@code sum}, and {@code max}.
   */
  public static RollupSpec parse(String spec) {
    var builder =
        ImmutableMultimap.<String, KV<String, TupleTag<KV<KV<String, Long>, Double>>>>builder();
    for (var rollup : Splitter.on(',').split(spec)) {
      var parts = Splitter.on(':').limit(3).splitToList(rollup);
      Preconditions.checkArgument(parts.size() == 3, "invalid rollup: %s", rollup);
      var eventType = parts.get(0);
      var aggregateFunction = parts.get(1);
      var attributeName = parts.get(2);
      if (aggregateFunction.equals(RollupSpec.MIN.getId())) {
        builder.put(eventType, KV.of(attributeName, RollupSpec.MIN));
      } else if (aggregateFunction.equals(RollupSpec.MAX.getId())) {
        builder.put(eventType, KV.of(attributeName, RollupSpec.MAX));
      } else if (aggregateFunction.equals(RollupSpec.SUM.getId())) {
        builder.put(eventType, KV.of(attributeName, RollupSpec.SUM));
      } else {
        throw new IllegalArgumentException("invalid rollup: " + rollup);
      }
    }
    return new RollupSpec(builder.build());
  }

  private final ImmutableMultimap<String, KV<String, TupleTag<KV<KV<String, Long>, Double>>>>
      rollups;

  private RollupSpec(
      ImmutableMultimap<String, KV<String, TupleTag<KV<KV<String, Long>, Double>>>> rollups) {
    this.rollups = rollups;
  }

  ImmutableCollection<KV<String, TupleTag<KV<KV<String, Long>, Double>>>> rollups(
      String eventType) {
    return rollups.get(eventType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RollupSpec)) {
      return false;
    }
    RollupSpec that = (RollupSpec) o;
    return rollups.equals(that.rollups);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rollups);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", RollupSpec.class.getSimpleName() + "[", "]")
        .add("rollups=" + rollups)
        .toString();
  }
}
