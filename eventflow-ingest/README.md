# eventflow-ingest

eventflow-ingest contains a streaming Dataflow job which pulls messages from a Pub/Sub subscription,
parses them as binary or JSON events, validates them, re-publishes them to a Pub/Sub topic for
validated events, and batch writes them to BigQuery.

Data is written to BigQuery every 90 seconds, which is quickly enough to reduce the per-worker
requirements and allow for faster job updates, but still enough time that the job will stay under
BigQuery's hard quota of 1500 load jobs per table per day.

Using load jobs for writing to BigQuery has a number of advantages over using the streaming API:
radically lower operating costs, higher throughput, and less sensitive to temporary outages. That
said, Beam doesn't surface any errors with load jobs, so the production environment for
eventflow-ingest should add monitoring of load job statuses.

