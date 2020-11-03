# eventflow-timeseries-api

eventflow-timeseries-api contains the gRPC definition of a time series service, its derived stubs,
and a helper client providing high-level type mapping. It supports time series interval
granularities of minute, hour, day, week, month, year, and others, as well as multiple aggregation
functions. It supports all time zones, and aggregates intervals in a given time location.
