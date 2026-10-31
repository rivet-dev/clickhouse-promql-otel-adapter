# High-cardinality queries

The adapter filters time, metric names and safe label predicates in ClickHouse,
then evaluates PromQL over the selected samples in Rust. Storage cardinality
and query cardinality are different: millions of stored series can serve a
small selective query efficiently, while a query selecting every series over
a long window still needs all those raw samples.

## Query execution

- Exact label equality/inequality and literal regex alternatives (such as
  `region=~"east|west"`) are pushed down using bound SQL parameters.
  Missing labels, OTel normalization, resource-derived `job`/`instance`, and
  selector OR branches preserve Rust matcher semantics.
- General regular expressions and generated/scope labels remain Rust filters.
  A branch that cannot safely be filtered admits all potentially matching rows.
- Sample transport groups identical labels/metadata and sends their sample tuples
  together, for all five metric types. The raw row limit applies **before** SQL
  grouping, with one extra row to detect overflow. Every timestamp/value remains
  intact; this is lossless transport compression, not a statistical rollup.
- Sample reads omit exemplar payloads and other unused columns. Row and byte
  budgets span the metric tables, and expanded histogram/sample counts have a
  separate budget before evaluation.
- Label matching is cached across timestamps within an evaluation. Sample
  results are never cached. The separate shared metric dictionary cache expires
  after 60 seconds by default; new metric names may appear after that delay.
- Evaluation checks sample budgets before copying windows/buckets. Admission,
  pending requests, discovery scoring and serialization are bounded. A timed-out
  blocking task retains its execution permit until it actually exits.
- Float points occupy 40 bytes instead of 144 bytes on x86_64 by boxing native
  histograms only when present. Query responses serialize directly from results,
  releasing input data first, with a separate output-byte budget.
- Each transport group prepares its derived output labels once, then appends
  points to indexed buffers. Duplicate detection uses full-label hashing instead
  of repeatedly comparing long shared label prefixes; result order is unchanged.

## Review findings

Three independent reviews covered storage translation, evaluator correctness,
and HTTP resource control, followed by cross-review and a live FDB dashboard audit.

| Priority | Finding | Resolution |
|---|---|---|
| High | OR selectors could wrongly prune metric/bucket candidates | Preserve every OR branch when removing derived matchers; real storage regressions |
| High | Selective queries fetched whole metrics before label filtering | Safe bound SQL predicates, including missing/empty labels and literal regex sets |
| High | Repeated labels exceeded 64 MiB for normal one-hour dashboards | Lossless grouped sample transport for all metric types |
| High | Large selector/histogram expansion allocated before checking budgets | Pre-allocation limits, cumulative storage budgets, compact float points |
| High | Unbounded waiters and client timeout excluded admission | Bounded queue, shared deadline, cancellation while retaining execution permits |
| High | Large query output duplicated the result into a full JSON tree | Direct bounded serialization after releasing input/caches |
| Medium | Repeated selector scans, unused exemplar transfer, full search sorting | Per-evaluation matching cache, narrower projection, bounded top-k search |
| Medium | SQL parameter backslashes and helper timeout compatibility | Explicit escaped parameter encoding and dedicated regression tests |

## Reproduce measurements

The benchmark uses only the local test ClickHouse on `127.0.0.1:18123`. It creates
and removes an isolated database with the stock Collector schema. No cloud
credentials or production data are used.

```sh
just benchmark-cardinality --series 100000 --samples 30 \
  --output /tmp/promql-100k.json --require-success
just benchmark-cardinality --series 1000000 --samples 12 \
  --output /tmp/promql-1m.json --require-success
cargo test --release --locked --test review_correctness \
  high_cardinality_selector_cache_benchmark -- --ignored --nocapture
```

The dataset has a single gauge with unique `series_id` and 1,000 tenant values,
sampled every 15 seconds. Queries select one tenant, enumerate its label values,
and run eight concurrent queries. `--require-success` checks selected result
counts and sums. The unfiltered query is deliberately allowed to fail its budget;
an error is not reported as a fast successful query. This is a synthetic local
benchmark, not a production capacity guarantee or a p95 measurement.

## Review measurements — 2026-10-14

Local ClickHouse 26.9.18.11, release build, stock schema. Each number below is
one measured run, not a percentile. The 100k baseline used the preceding
metadata-cache build; the deployed build SHA256 is
`c9a2be9ebac671b39b09fd97451631af9a0c994abbc46fecc426c03e5f5e15a7`.

| Workload | Before, 100k series / 3M rows | Fixed, 100k / 3M | Fixed, 1M / 12M |
|---|---:|---:|---:|
| Selective instant, cold dictionary | byte-limit error | 115 ms | 378 ms |
| Selective instant, warm dictionary | byte-limit error | 57 ms | 310 ms |
| Selective literal-regex instant | not measured | 58 ms | 305 ms |
| Selective range | byte-limit error | 65 ms | 319 ms |
| Label values | 286 ms | 58 ms | 308 ms |
| Eight concurrent selective queries, total | all failed | 486 ms | 2.25 s |
| Adapter peak RSS, whole workload | 219 MiB | 147 MiB | 178 MiB |

Successful selective results and sums were validated. The unfiltered aggregate
failed the original byte limit; the fixed build reached the raw-row limit instead. Peak RSS includes that rejected
query; it is not an isolated per-query allocation measurement. The separate
50k-series / 60-step evaluator benchmark returned identical results in 215 ms
with repeated matching versus 103 ms with cached matching.

A representative classic-histogram evaluator benchmark covers 544 hosts × 19
buckets × 400 samples (4.13 million points), 11 labels per series, and a one-hour
range at 15-second resolution. Replacing duplicate detection's ordered set with
a hash set reduced execution from 9.312 seconds to 6.136 seconds (34%). All output
labels, values and 241 timestamps were asserted. Reproduce with:

```sh
cargo test --release --locked --test review_correctness \
  classic_histogram -- --ignored --nocapture
```

## Production measurements — 2026-10-31 (0.0.4)

A replay of 640 real dashboard and alert queries (one-hour range, 15-second
step, one tenant, sequential) against a multi-tenant ClickHouse holding about
300 million rows per metric table, from the adapter on a 2-vCPU VM. Every
result was compared by digest with the previous release: all identical.

| Workload | 0.0.3 | 0.0.4 |
|---|---:|---:|
| Metric name lookup after the 60 s cache expiry | 38–57 s per table, blocking | background scan of the last minutes |
| Queries landing on that expiry | 53–89 s | under 20 ms |
| All 640 queries, name cache pinned warm on 0.0.3 | 193 s | 51 s |
| p99 / max, same | 1.6 s / 93 s | 0.6 s / 1.5 s |
| Largest classic-histogram panel | 5.0 s | 1.5 s |
| 28-selector `label_replace` panel | 2.3 s | 0.5 s |

What changed:

- Metric names are scanned only for the queried time range, extended
  backwards on demand, refreshed forward in the background and warmed at
  startup. A full-table `DISTINCT` read every stored row.
- Metric tables are queried concurrently.
- Range queries reuse work across steps: shared label sets with cached hashes,
  per-series window cursors, group keys, selector matches and `label_replace`
  results. Label maps were previously copied for every series at every step.
- Histogram rows send bucket bounds once per group, omit bucket arrays when only
  `_count`/`_sum` are selected, and receive 64-bit integers unquoted.
- `--label-column` lets a selective tenant filter use a sort-key column.

## Operational limits

The final integrated suite passed **88 Rust tests**, with the two manual timing
benchmarks excluded from normal CI, plus **21,442 pinned Prometheus evaluator
cases**, the pinned HTTP contract, and both real-ClickHouse storage corpora.
Clippy with warnings denied and formatting checks passed.

A live staging FDB dashboard audit exercised all 54 queries across 47 panels at
one hour / 15-second resolution, two concurrent requests: every query returned
data without errors. Median latency across these different queries was 303 ms;
the three largest histogram queries took 8.5, 13.0 and 15.5 seconds. This is not
a per-query latency distribution or production capacity claim. Sampled adapter
peak memory was 488 MiB on the shared 2-vCPU/8-GiB Grafana VM. These heavy queries
previously failed the byte limit; there is still substantial CPU work for large
classic histograms.
The subsequent full browser check also passed all 47 panel requests, with the
three heaviest concurrent requests taking 16.0–21.6 seconds.

Use exact tenant/project/region filters and bounded time windows in dashboards.
See [API limits](api-contract.md) for CLI controls. Tune concurrency to CPU and
memory on the adapter and ClickHouse together; adding adapter replicas also
adds ClickHouse query load. The adapter is stateless and can have multiple
replicas, but there is no distributed PromQL execution or cross-replica cache.

Remaining scale limits:

- Broad aggregations still fetch raw samples; there is no SQL aggregation
  pushdown, recording-rule engine, or streaming window evaluator. An aggregate
  producing one result can still exceed input budgets. Use a dedicated rollup
  pipeline for fleet-wide, long-range queries until those features exist.
- General regex-only selectors can scan/fetch many rows. Native label map
  filtering does not create a per-label index or change the stock table order.
- Metadata/search still enumerate distinct series, and dictionary refresh still
  scans metric metadata. High churn in metric *names* differs from the benchmark's
  high cardinality in label values.
- Cancellation is cooperative. Parsing, a single-series sort and native histogram bucket preparation
  can finish their current CPU operation after a timeout. Sample slots and raw
  bytes are bounds, not an exact heap limit; use a process memory limit as well.
- Duplicate ingestion, Collector schema precision and delta temporality retain
  the constraints described in [schema findings](schema-findings.md).
