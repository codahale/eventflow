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
package io.eventflow.timeseries.srv;

import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Stats;
import io.opencensus.stats.View;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagValue;
import java.util.List;

public class TimeSeriesStats {

  static final Measure.MeasureLong CACHE_HIT =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_hits", "Number of cache hits", "1");
  static final Measure.MeasureLong CACHE_MISS =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_miss", "Number of cache misses", "1");
  static final Measure.MeasureLong CACHE_WRITE =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_writes", "Number of cache writes", "1");
  static final Measure.MeasureLong GET_INTERVALS_REQUESTS =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/get_intervals_requests",
          "Number of requests for getIntervals",
          "1");
  static final TagKey CACHEABILITY = TagKey.create("cacheability");
  static final TagValue CACHEABLE = TagValue.create("cacheable");
  static final TagValue UNCACHEABLE = TagValue.create("uncacheable");

  private static final List<View> VIEWS =
      List.of(
          View.create(
              View.Name.create("io.eventflow/timeseries/srv/cache_misses"),
              "Number of cache misses",
              CACHE_MISS,
              Aggregation.Count.create(),
              List.of()),
          View.create(
              View.Name.create("io.eventflow/timeseries/srv/cache_hits"),
              "Number of cache hits",
              CACHE_HIT,
              Aggregation.Count.create(),
              List.of()),
          View.create(
              View.Name.create("io.eventflow/timeseries/srv/cache_writes"),
              "Number of cache writes",
              CACHE_WRITE,
              Aggregation.Count.create(),
              List.of()),
          View.create(
              View.Name.create("io.eventflow/timeseries/srv/get_intervals_requests"),
              "Number of requests for getIntervals",
              GET_INTERVALS_REQUESTS,
              Aggregation.Count.create(),
              List.of(CACHEABILITY)));

  private TimeSeriesStats() {
    // singleton
  }

  public static void registerViews() {
    VIEWS.forEach(Stats.getViewManager()::registerView);
  }
}
