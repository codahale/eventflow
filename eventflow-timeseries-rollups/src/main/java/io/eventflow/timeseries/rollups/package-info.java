/**
 * A streaming Dataflow job which batches events, creates minutely intervals of event attributes,
 * and writes them to Spanner.
 */
package io.eventflow.timeseries.rollups;
