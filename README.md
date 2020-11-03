# eventflow

> Thoughts arrive like butterflies…

A high-throughput, low-latency, GCP-native, analytical event pipeline. Events are encoded as
Protocol Buffer messages, published to a Pub/Sub topic, ingested by a Dataflow job, written to
BigQuery, re-published to Pub/Sub, and aggregated in realtime by another Dataflow job which writes
time series intervals to Spanner. Those intervals are served back up by a gRPC service which
supports time zones, multiple levels of granularity, and different aggregation functions.

It's designed to scale up to millions of events a second and require minimal operational resources.

## Modules

### eventflow-common

eventflow-common contains the Protocol Buffer definitions of events, their derived classes, and some
helper classes.

### eventflow-ingest

eventflow-ingest contains a streaming Dataflow job which pulls messages from a Pub/Sub subscription,
parses them as binary or JSON events, validates them, re-publishes them to a Pub/Sub topic for
validated events, and batch writes them to BigQuery.

### eventflow-publisher

eventflow-publisher provides a helper class for constructing events and publishing them to the
Pub/Sub topic for eventflow-ingest to process.

## eventflow-timeseries-api

eventflow-timeseries-api contains the gRPC definition of a time series service, its derived stubs,
and a helper client providing high-level type mapping. It supports time series interval granularites
of minute, hour, day, month, and year, as well as multiple aggregation functions. It supports all
time zones, and aggregates intervals in a given time location.

## eventflow-timeseries-rollups

eventflow-timeseries-rollups contains a streaming Dataflow job which pulls validated events from a
Pub/Sub subscription, aggregates them on a minutely basis, and writes the resulting time series
intervals to Spanner. It supports counts of all events, plus custom rollup types to sum attribute
values.

## eventflow-timeseries-srv

eventflow-timeseries-srv is a container-ready implementation of the gRPC service defined in
eventflow-timeseries-api. It caches older time series data in Redis.

## TODO

* [ ] Patiently wait for Dataflow to support Java 11
