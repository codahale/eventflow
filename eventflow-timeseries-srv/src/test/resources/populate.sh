#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

gcloud spanner instances create test-instance --configuration=emulator \
  --config=emulator-config --description="Test Instance" --nodes=1

gcloud spanner databases create timeseries --instance=test-instance --configuration=emulator

gcloud spanner databases ddl update timeseries --configuration=emulator --instance=test-instance \
  --ddl 'CREATE TABLE intervals_minutes (name STRING(1000) NOT NULL, interval_ts TIMESTAMP NOT NULL, insert_id INT64 NOT NULL, value FLOAT64 NOT NULL, insert_ts TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),) PRIMARY KEY (name, interval_ts, insert_id);'

gcloud spanner databases execute-sql timeseries --configuration=emulator --instance=test-instance \
  --sql 'INSERT INTO intervals_minutes (name, interval_ts, insert_id, value, insert_ts) VALUES ("example", "2020-10-30T19:40:22Z", 0, 100, PENDING_COMMIT_TIMESTAMP())'
gcloud spanner databases execute-sql timeseries --configuration=emulator --instance=test-instance \
  --sql 'INSERT INTO intervals_minutes (name, interval_ts, insert_id, value, insert_ts) VALUES ("example", "2020-10-30T19:55:22Z", 0, 200, PENDING_COMMIT_TIMESTAMP())'
