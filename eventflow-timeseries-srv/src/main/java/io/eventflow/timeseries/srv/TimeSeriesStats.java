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
package io.eventflow.timeseries.srv;

import io.opencensus.stats.Aggregation;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Stats;
import io.opencensus.stats.StatsRecorder;
import io.opencensus.stats.View;
import io.opencensus.tags.TagKey;
import io.opencensus.tags.TagMetadata;
import io.opencensus.tags.TagValue;
import io.opencensus.tags.Tagger;
import io.opencensus.tags.Tags;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.List;

public class TimeSeriesStats {
  private static final StatsRecorder stats = Stats.getStatsRecorder();
  private static final Tracer tracer = Tracing.getTracer();
  private static final Tagger tagger = Tags.getTagger();

  private static final Measure.MeasureLong CACHE_HIT =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_hits", "Number of cache hits", "1");
  private static final Measure.MeasureLong CACHE_MISS =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_miss", "Number of cache misses", "1");
  private static final Measure.MeasureLong CACHE_WRITE =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/cache_writes", "Number of cache writes", "1");
  private static final Measure.MeasureLong GET_INTERVALS_REQUESTS =
      Measure.MeasureLong.create(
          "io.eventflow/timeseries/srv/get_intervals_requests",
          "Number of requests for getIntervals",
          "1");
  private static final TagKey CACHEABILITY = TagKey.create("cacheability");
  private static final TagValue CACHEABLE = TagValue.create("cacheable");
  private static final TagValue UNCACHEABLE = TagValue.create("uncacheable");

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

  static void registerViews() {
    VIEWS.forEach(Stats.getViewManager()::registerView);
  }

  static void recordGetIntervalValuesRequest(boolean cacheable) {
    stats
        .newMeasureMap()
        .put(TimeSeriesStats.GET_INTERVALS_REQUESTS, 1)
        .record(
            tagger
                .currentBuilder()
                .put(
                    TimeSeriesStats.CACHEABILITY,
                    cacheable ? TimeSeriesStats.CACHEABLE : TimeSeriesStats.UNCACHEABLE,
                    TagMetadata.create(TagMetadata.TagTtl.UNLIMITED_PROPAGATION))
                .build());
  }

  static void recordCacheMiss() {
    stats.newMeasureMap().put(CACHE_MISS, 1).record();
  }

  static void recordCacheHit() {
    stats.newMeasureMap().put(CACHE_HIT, 1).record();
  }

  static void recordCacheWrite() {
    stats.newMeasureMap().put(CACHE_WRITE, 1).record();
  }

  static Tracer tracer() {
    return tracer;
  }
}
