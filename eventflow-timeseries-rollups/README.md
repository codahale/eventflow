# eventflow-timeseries-rollups

eventflow-timeseries-rollups contains a streaming Dataflow job which pulls validated events from a
Pub/Sub subscription, aggregates them on a minutely basis, and writes the resulting time series
intervals to Spanner. It supports counts of all events, plus custom rollup types to sum/min/max
attribute values. In each case, the type of rollup is added to the event type and attribute name to
form a globally unique time series name. For example, rolling up the `high_score` attribute values
by `max` of the `game_finished` event type will produce a time series named
`game_finished.high_score.max`. Rollups are specified via a comma-separated list of mappings:

```
event_type:func(attribute_name)
```

Where `func` can be one of three options: `MIN`, `MAX`, and `SUM`.

It expects there to be a table in the Spanner database with the following name and schema:

```bigquery
CREATE TABLE intervals_minutes (
  name STRING(1000) NOT NULL,
  interval_ts TIMESTAMP NOT NULL,
  insert_id INT64 NOT NULL,
  value FLOAT64 NOT NULL,
  insert_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (name, interval_ts, insert_id)
```

The `insert_id` column is populated with a random number, allowing for multiple value rows per
interval timestamp. This allows for the streaming rollup job to handle late data gracefully by
simply inserting another row. For example, imagine it receives four events, all with timestamps in
the same minutely interval. It sums their counts to 4, and inserts a row with the time series name,
the interval timestamp, the randomly-generated `insert_id` of 26, and a value of 4. An hour later,
the job receives a delayed batch of three events, all with timestamps in the minutely interval of
the original four. Rather than look up the existing row and increment its count, it inserts a second
row for the interval, this time with a randomly-generated `insert_id` of 59 and a value of 3. Later,
when eventflow-timeseries-srv queries Spanner to compute the actual intervals, it groups them by
name and interval timestamp and aggregates the values via `SUM`. This will return the true count of
7.

This design allows eventflow-timeseries-rollups to batch write groups of mutations instead of
running individual DML queries, which is a much higher-throughput way of loading data into Spanner.
