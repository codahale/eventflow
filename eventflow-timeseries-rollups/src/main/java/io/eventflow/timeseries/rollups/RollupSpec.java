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
package io.eventflow.timeseries.rollups;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMultimap;
import java.io.Serializable;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

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

  private static final Pattern ROLLUP = Pattern.compile("^(sum|max|min)\\((.+)\\)$");

  public static RollupSpec parse(String spec) {
    var builder =
        ImmutableMultimap.<String, KV<String, TupleTag<KV<KV<String, Long>, Double>>>>builder();
    for (String custom : Splitter.on(',').split(spec)) {
      var parts = Splitter.on(':').limit(2).splitToList(custom);
      var matcher = ROLLUP.matcher(parts.get(1));
      if (!matcher.matches()) {
        throw new IllegalArgumentException("bad rollup: " + parts.get(1));
      }

      if (matcher.group(1).equals(RollupSpec.MIN.getId())) {
        builder.put(parts.get(0), KV.of(matcher.group(2), RollupSpec.MIN));
      } else if (matcher.group(1).equals(RollupSpec.MAX.getId())) {
        builder.put(parts.get(0), KV.of(matcher.group(2), RollupSpec.MAX));
      } else {
        builder.put(parts.get(0), KV.of(matcher.group(2), RollupSpec.SUM));
      }
    }

    return new RollupSpec(builder.build());
  }

  @Nonnull
  private final ImmutableMultimap<String, KV<String, TupleTag<KV<KV<String, Long>, Double>>>>
      rollups;

  private RollupSpec(
      @Nonnull
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
