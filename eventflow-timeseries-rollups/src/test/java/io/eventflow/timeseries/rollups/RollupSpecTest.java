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

import static com.google.common.truth.Truth.assertThat;

import org.apache.beam.sdk.values.KV;
import org.junit.Test;

public class RollupSpecTest {
  private final RollupSpec spec =
      RollupSpec.parse("event_type:sum(attr),event_type:min(attr),event_type2:max(attr2)");

  @Test
  public void rollupsByEventType() {
    assertThat(spec.rollups("event_type"))
        .containsExactly(KV.of("attr", RollupSpec.SUM), KV.of("attr", RollupSpec.MIN));
    assertThat(spec.rollups("event_type2")).containsExactly(KV.of("attr2", RollupSpec.MAX));
  }
}
