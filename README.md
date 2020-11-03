# eventflow

> Thoughts arrive like butterflies…

A high-throughput, low-latency, GCP-native, analytical event pipeline. Events are encoded as
Protocol Buffer messages, published to a Pub/Sub topic, ingested by a Dataflow job, written to
BigQuery, re-published to Pub/Sub, and aggregated in realtime by another Dataflow job which writes
time series intervals to Spanner. Those intervals are served back up by a gRPC service which
supports time zones, multiple levels of granularity, and different aggregation functions.

It's designed to scale up to millions of events a second and require minimal operational resources.

## TODO

* [ ] Patiently wait for Dataflow to support Java 11
