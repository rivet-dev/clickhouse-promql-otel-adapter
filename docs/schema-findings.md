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

`hist_absent` omits sum/min/max; `hist_zero` explicitly supplies zero for each.
The real collector stores `Sum = Min = Max = 0` for both. The exporter directly
reads these fields without recording their presence. Exact reconstruction
requires nullable values or presence flags plus exporter support.

## Integer and attribute representation

The gauge fixture sends integer `9007199254740993`, which is stored as
`9007199254740992` in Float64. Prometheus also represents float samples with
Float64, so the adapter cannot provide integer precision beyond that model.
Preserve a separate integer column/type flag if ClickHouse users need the
original OTLP integer.

The typed-attribute fixture stores bool `true`, integer `42`, double `1.25`,
array `[1,"two"]`, and map `{"key":"value"}` as strings. This supports Prometheus
label values but loses the original OTLP attribute types. Preserve typed
attributes separately if consumers need round-trip OTLP fidelity.

## Histogram exemplar bucket identity

OTLP histogram exemplars retain value, labels, IDs and timestamp, but do not
retain which classic Prometheus bucket originally carried the exemplar. The
adapter assigns it to the smallest bound containing its value, as Prometheus
exporters do. Arbitrary original bucket assignment cannot be reconstructed.


## Schema changes require new tables

ClickHouse rejected an in-place `MODIFY COLUMN TimeUnix DateTime64(9)` because `TimeUnix` participates in the sorting/partition keys. Create replacement tables with the desired types and migrate deliberately; the test variant creates fresh isolated tables. Existing rows cannot recover discarded fractions.

The ClickHouse differential report is in [storage-report.md](storage-report.md). The precision variant there additionally supplies ZeroThreshold directly; the unmodified exporter still needs a code change to populate that field.

## Prometheus metadata loses original type and unit

The receiver records the original Prometheus metric type in OTLP metric metadata
(`receiver/prometheusreceiver/internal/metricfamily.go`), but the ClickHouse
exporter omits `Metric.Metadata`. Untyped samples therefore become indistinguishable
from gauges. The receiver also translates unit names into UCUM (for example,
`seconds` becomes `s`); the adapter exposes the stored unit. HELP and conflicting
stored definitions are preserved and tested, but the original TYPE/unit cannot
always be reconstructed.

Recommendation: preserve the original metric metadata map, including original
Prometheus type, name and unit when available. This also makes suffix trimming
and translator upgrades auditable. The default receiver scope is recognized;
SDK metrics use the tested OTel naming translator. The live Grafana test verifies
the default Prometheus receiver with suffix trimming disabled.

## Temporality, duplicates and query cost

Cumulative sums/histograms, reset/start boundaries, stale flags, resource/scope
labels and exact retry duplicates are covered by the Collector snapshots and
real ClickHouse tests. Conflicting duplicate values or start timestamps return
an error. Configured HA replica labels use a tested Thanos penalty iterator.
Delta data is rejected with an actionable conversion message; deploy the
Collector's delta-to-cumulative processing before this exporter when needed.
A stateless query adapter cannot reliably reconstruct cumulative history from
arbitrarily truncated delta data.

Queries filter metric names and timestamps in ClickHouse, then apply complete
Prometheus label matching in Rust. Discovery uses distinct label projections,
not all historical samples. **This is a bounded correctness implementation,
not a large-cardinality performance certification.** The default ORDER BY and
map-valued labels are unlikely to suffice for every production workload.
Benchmark actual cardinality, range lengths and concurrent dashboards before
migration; consider materialized normalized series IDs, a label index, and
separate sample/series tables while retaining the raw OTLP tables. No production
scale benchmark or distributed query planner is claimed.

## Recommended priorities

1. Precreate replacement tables with `DateTime64(9)` before ingesting data that
   needs subsecond accuracy. Existing lost fractions cannot be restored.
2. Extend exporter and schema together for `ZeroThreshold` and optional-field
   presence. DDL changes alone cannot repair fields the exporter never writes.
3. Preserve original metric metadata/type/unit. Keep typed OTLP attributes and
   integer values separately only if their non-Prometheus fidelity is required.
4. Add series/label indexing based on measured workloads. Keep the adapter's
   default-schema path as a compatibility test while evaluating optimized layouts.

Prometheus native custom buckets, fractional/negative histogram counts, and
explicit native counter-reset hints are not all representable in OTLP. These
require an explicit richer input/storage format, rather than guessing lost data.
See [storage-report.md](storage-report.md) for exclusions counted separately.
