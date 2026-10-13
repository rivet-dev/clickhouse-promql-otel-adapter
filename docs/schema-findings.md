# Default Collector schema: observed fidelity limits

Tested with Collector Contrib 0.162.0 and ClickHouse 26.9.18.11. Evidence is in
`tests/snapshots/otlp-dataset.json`, `collector-schema.sql`, and
`collector-rows.json`, generated through the real Collector (not copied DDL).

## Subsecond sample timestamps are lost

The `subsecond` fixture submits values 1 and 2 at 1700000000.100 and
1700000000.900 seconds. Both stored rows have `TimeUnix = 2023-11-14 22:13:20`.
The default `DateTime` columns only retain whole seconds, including start and
exemplar timestamps. An adapter cannot reconstruct the missing fraction, and
the rows now conflict at one timestamp.

Recommendation: use `DateTime64(9)` for metric, start and exemplar timestamps;
the unmodified Collector was verified to preserve `.100000000` and `.900000000` with precreated tables (`collector-rows-precise.json`).
Prometheus ultimately uses milliseconds, but retaining the original nanoseconds
makes the storage format faithful to OTLP. Test schema changes separately from
the default-schema compatibility path.

## Exponential histogram zero threshold is missing

The fixture sends `zeroThreshold = 0.01`. The default exponential histogram
table has `ZeroCount` but no zero-threshold column. The ingested rows therefore
cannot distinguish a custom zero bucket from the default zero-width bucket.

Recommendation: add a zero-threshold column and corresponding exporter support.
Changing DDL alone cannot make the exporter insert a field it does not emit.
Native histogram results must not claim exact fidelity when this value is lost.

## Optional histogram fields lose presence

`Sum`, `Min`, and `Max` are non-nullable floats. The upstream exporter must be
checked for its treatment of absent values: a stored zero cannot distinguish
"not provided" from a recorded zero without an explicit presence field. Exact
reconstruction requires nullable values or presence flags plus exporter support.

## Schema changes require new tables

ClickHouse rejected an in-place `MODIFY COLUMN TimeUnix DateTime64(9)` because `TimeUnix` participates in the sorting/partition keys. Create replacement tables with the desired types and migrate deliberately; the test variant creates fresh isolated tables. Existing rows cannot recover discarded fractions.

The ClickHouse differential report is in [storage-report.md](storage-report.md). The precision variant there additionally supplies ZeroThreshold directly; the unmodified exporter still needs a code change to populate that field.

## Additional validation still required

- Attribute type preservation (Map(String,String) stores textual values).
- Integer precision beyond the exact range of Float64.
- Cumulative/delta conversion and reset/start-time boundaries.
- Prometheus name/unit and job/instance round-trip behavior.
- Deduplication, stale markers and no-recorded-value flags.
- Predicate performance on resource labels versus default ORDER BY.

These are limitations to measure, not permission to silently reinterpret data.
