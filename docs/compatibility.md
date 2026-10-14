# PromQL on the OpenTelemetry ClickHouse schema

Status: implemented and tested against **Prometheus 3.15.0** and pinned Thanos
query behavior, using a **strictly Rust runtime**. All 21,442 captured upstream
executions, 356 parser cases, 1,016 HTTP reference requests, 2,392 OTel naming
cases, 1,200 RE2 cases, 2,400 search scoring cases, and 800 seeded Thanos replica
cases pass. Real Collector ingestion, isolated ClickHouse replay, manual Grafana
verification, and independent review are complete.

A checked item means implemented or explicitly classified with the evidence below;
it is **not a proof of equivalence for every possible query**. The default storage
format has irreversible losses. Go allocation statistics and arbitrary parser
error wording are not identical. Read [the contract](api-contract.md),
[schema findings](schema-findings.md), and [review report](review-report.md) before
replacing a deployment. ClickHouse continues to supply its native SQL interfaces.

## Authoritative source and reusable test suites

All implementation work begins from these sources. Release tags pin the baseline;
upgrading them requires regenerating snapshots and rerunning conformance.

- [Prometheus evaluator](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/engine.go)
- [Functions](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/functions.go) and [function registry](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/parser/functions.go)
- [Parser grammar](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/parser/generated_parser.y), [parser tests](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/parser/parse_test.go)
- [HTTP implementation](https://github.com/prometheus/prometheus/blob/v3.15.0/web/api/v1/api.go), [HTTP tests](https://github.com/prometheus/prometheus/blob/v3.15.0/web/api/v1/api_test.go), [API documentation](https://github.com/prometheus/prometheus/blob/v3.15.0/docs/querying/api.md)
- [Evaluator fixtures](https://github.com/prometheus/prometheus/tree/v3.15.0/promql/promqltest/testdata), [fixture language/runner](https://github.com/prometheus/prometheus/tree/v3.15.0/promql/promqltest), [engine tests](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/engine_test.go), [internal engine tests](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/engine_internal_test.go)
- [Prometheus black-box compliance](https://github.com/prometheus/compliance/tree/main/promql). This compares HTTP query results; it does not by itself prove collector conversion fidelity.
- [Thanos query API and tests](https://github.com/thanos-io/thanos/tree/f1e0bbc906c54ca138466bd3095c3f61bf31368d/pkg/api/query), [Thanos E2E](https://github.com/thanos-io/thanos/tree/f1e0bbc906c54ca138466bd3095c3f61bf31368d/test/e2e)
- [Alternative Thanos evaluator/tests](https://github.com/thanos-io/promql-engine)
- [Collector ClickHouse schema templates](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.162.0/exporter/clickhouseexporter/internal/sqltemplates), [metrics integration tests](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/exporter/clickhouseexporter/exporter_metrics_integration_test.go)
- [Prometheus receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.162.0/receiver/prometheusreceiver), [OTel Prometheus translator](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.162.0/pkg/translator/prometheus)

## Delivery sequence and acceptance gates

1. Record sources, enumerate features, pin tools and dependencies.
2. Run real ClickHouse and the unmodified Collector; generate deterministic OTLP
   datasets for every metric kind; capture actual SHOW CREATE TABLE and row
   snapshots. Normal tests must not silently regenerate accepted snapshots.
3. Implement Rust storage translation, parser/evaluator, and HTTP API in separate
   major JJ revisions. Keep fetch bounds explicit; enforce sample/time limits.
4. Run upstream fixtures and differential tests against pinned Prometheus. Feed
   equivalent samples through real ClickHouse with a unique database per test.
   Report passed/failed/unsupported totals, including annotations and failures.
5. Exercise POST/GET API contracts and Grafana discovery, not only query numbers.
6. Manually inspect a live Prometheus endpoint → Collector → ClickHouse → Rust
   adapter → Grafana pipeline in local Chromium; save screenshots and commands.
7. Independent subagent reviews of semantics, ingestion/storage, and API/testing;
   repair findings and rerun affected checks. Report remaining limits honestly.

## Storage and ingestion

- [x] Collector-generated default gauge/sum/histogram/exponential histogram/summary tables, DDL snapshots and reproducible regeneration command: `just snapshots --update` (generate + clean verification passed).
- [x] Deterministic synthetic OTLP fixture generator; Collector-generated row snapshots: `just dataset`, `tests/snapshots/`.
- [x] Real ClickHouse integration tests with unique databases, cleanup on success/failure, concurrent isolation.
- [x] Gauge int/double values; monotonic and non-monotonic sums.
- [x] Cumulative counters, reset boundaries, start timestamps, zero samples. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Delta temporality: correct cumulative conversion or explicit actionable rejection; never silently treat deltas as cumulative counters.
- [x] Explicit histogram cumulative buckets, +Inf, sum/count; optional sum, empty histograms, boundary values.
- [x] Exponential/native histogram scale, positive/negative buckets, zero threshold/count, resets and mixed float/histogram series.
- [x] Summaries and quantiles; sum/count names and label rendering. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] OTLP flags, no-recorded-value/stale samples, NaN and infinities; missing samples. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Nanosecond input to Prometheus millisecond semantics; default-schema loss documented, precise-column variant verified through the real Collector and 9,756 query executions.
- [x] Prometheus scrape round trip: metric suffix/unit/name normalization, resource job/instance, scope and target_info, label collision handling.
- [x] Resource/point/scope attributes: typed values, canonical labels, UTF-8 and invalid names. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Exemplars: labels, trace/span identifiers, timestamps and range selection. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Metadata: HELP/TYPE/unit, conflicting definitions, limits.
- [x] Duplicate samples, conflicting duplicates, out-of-order data, retry duplicates and HA replica semantics.
- [x] SQL parameter binding, identifier validation, bounded scans, cancellation, query timeouts and memory/sample limits.
- [x] Storage unavailable/errors produce API errors, never fabricated empty successes.
- [x] Schema recommendation report with observed evidence and lossless/lossy boundaries.

## PromQL language and evaluator

- [x] Number/string/vector/matrix values and correct JSON representation.
- [x] Selector equality/inequality/full-match regex/negative regex, missing labels as empty, empty-selector rejection.
- [x] Metric names, quoted UTF-8 names, escaping, label sorting and empty label semantics.
- [x] Five-minute configurable lookback, latest sample, left-open range boundary, staleness handling.
- [x] Instant/range evaluation, inclusive end, fractional steps and timestamps, empty ranges.
- [x] Positive/negative offset, @ timestamps, @ start()/end().
- [x] Subqueries, resolution alignment, nested temporal modifiers.
- [x] Unary +/- and binary precedence; IEEE NaN/Inf arithmetic, modulo and power.
- [x] Scalar/vector and vector/vector arithmetic; metric name dropping.
- [x] Comparisons, bool modifier and filtering; metric name retention rules.
- [x] on/ignoring/group_left/group_right, include labels, cardinality checks and duplicate-output errors.
- [x] Set and/or/unless operations, many-to-many matching, empty vectors.
- [x] Aggregations by/without: sum/avg/min/max/count/group/stddev/stdvar/quantile/topk/bottomk/count_values.
- [x] Experimental aggregations limitk/limit_ratio; experimental syntax/functions are enabled, matching the reference corpus configuration.
- [x] Counter rate/increase/irate extrapolation, resets, sparse series and zero-point adjustment.
- [x] Delta/idelta/deriv/predict_linear, range regression and extrapolation.
- [x] Range aggregation functions, quantiles, absent/present, last sample labels.
- [x] Classic and native histogram quantiles, fractions, bucket interpolation, monotonicity repair and annotations.
- [x] Histogram arithmetic, aggregations, functions, schema reduction, incompatible schemas and mixed types.
- [x] Label manipulation: Go regexp/replacement semantics, collisions, empty matches.
- [x] Sorting including natural label sorting and special float values.
- [x] Calendar/time functions in UTC; default vector(time()) behavior.
- [x] Trigonometric/math/clamping/rounding functions and special values.
- [x] Experimental/new functions and syntax explicitly tracked against baseline.
- [x] Warnings/infos, source positions and error categories tested; query statistics expose Rust measurements (see API contract).
- [x] Parser rejection/type checking and format/parse API serialization.

## HTTP query compatibility

- [x] GET and form POST /api/v1/query: query,time,timeout,limit,lookback_delta,stats.
- [x] GET and form POST /api/v1/query_range: query,start,end,step,timeout,limit,lookback_delta,stats; maximum point count.
- [x] GET/POST /api/v1/series: repeated match[],start,end,limit; union and deduplication.
- [x] GET/POST /api/v1/labels and GET /api/v1/label/:name/values: match[],start,end,limit; escaping and sorting.
- [x] /api/v1/metadata and /api/v1/query_exemplars contracts.
- [x] /api/v1/format_query and /api/v1/parse_query.
- [x] NDJSON /api/v1/search/{metric_names,label_names,label_values}, Unicode fuzzy scores, batching, sorting, filters and metadata.
- [x] status=success/error envelopes, 400/422/503, errorType, warnings, infos, valid JSON special values.
- [x] RFC3339/numeric timestamps, duration/numeric steps, duplicate/missing/invalid parameters and POST precedence.
- [x] CORS/OPTIONS, gzip, method errors, URL/body limits, cancellation and concurrency.
- [x] /-/healthy, /-/ready, /metrics, /api/v1/status/buildinfo for Grafana.
- [x] Thanos parameters: dedup,replicaLabels[],partial_response,max_source_resolution,storeMatch[],engine,analyze and lookback_delta; document semantics where no Thanos stores exist.
- [x] Thanos /api/v1/stores, explain/analyze, rule/alert discovery and query statistics contract decisions.
- [x] Inventory all upstream endpoints below; classify ingestion, rule management, TSDB admin and scrape-target management separately from query replacement. Do not claim these are implemented through empty placeholder responses.

## Test and release evidence

- [x] Unit tests with regression cases for evaluator and translation boundaries.
- [x] API result snapshots with controlled times and deterministic label ordering.
- [x] Upstream fixture runner preserving load/clear/eval, missing/stale/native samples, expected failures, annotations and ordering.
- [x] Execute every upstream fixture and publish captured execution coverage with no silent skips: `just conformance`, `tests/conformance/`, `docs/evaluator-report.md`. Parser rejection/type coverage uses the separate 356-case corpus; the evaluator fixture run emitted no query-creation rejections.
- [x] Differential fixture datasets against Prometheus; seeded generated Thanos replica datasets (seed 3817) recorded for replay.
- [x] Integration coverage of all five collector-created table types.
- [x] Manual Grafana data source health, Explore instant/range, dashboard rate and histogram panels, labels/variables and empty/error cases.
- [x] Save local-browser screenshots and manual report, including exact versions/configs.
- [x] `just check`, `just test`, and `just conformance`.
- [x] Independent review round completed and findings resolved.
- [x] Final JJ graph at active stack tip; major changes in separate non-empty revisions.

## Per-function and fixture inventory

Generated from the pinned upstream source during initial research. Every registered function below occurs in at least one query in the passing
21,442-execution corpus. Coverage is execution-based, not a proof for all inputs.

### Registered functions

- [x] `abs`
- [x] `absent`
- [x] `absent_over_time`
- [x] `acos`
- [x] `acosh`
- [x] `asin`
- [x] `asinh`
- [x] `atan`
- [x] `atanh`
- [x] `avg_over_time`
- [x] `ceil`
- [x] `changes`
- [x] `clamp`
- [x] `clamp_max`
- [x] `clamp_min`
- [x] `cos`
- [x] `cosh`
- [x] `count_over_time`
- [x] `day_of_month`
- [x] `day_of_week`
- [x] `day_of_year`
- [x] `days_in_month`
- [x] `deg`
- [x] `delta`
- [x] `deriv`
- [x] `double_exponential_smoothing`
- [x] `end`
- [x] `exp`
- [x] `first_over_time`
- [x] `floor`
- [x] `histogram_avg`
- [x] `histogram_count`
- [x] `histogram_fraction`
- [x] `histogram_quantile`
- [x] `histogram_quantiles`
- [x] `histogram_stddev`
- [x] `histogram_stdvar`
- [x] `histogram_sum`
- [x] `hour`
- [x] `idelta`
- [x] `increase`
- [x] `info`
- [x] `irate`
- [x] `label_join`
- [x] `label_replace`
- [x] `last_over_time`
- [x] `ln`
- [x] `log10`
- [x] `log2`
- [x] `mad_over_time`
- [x] `max_of`
- [x] `max_over_time`
- [x] `min_of`
- [x] `min_over_time`
- [x] `minute`
- [x] `month`
- [x] `pi`
- [x] `predict_linear`
- [x] `present_over_time`
- [x] `quantile_over_time`
- [x] `rad`
- [x] `range`
- [x] `rate`
- [x] `resets`
- [x] `round`
- [x] `scalar`
- [x] `sgn`
- [x] `sin`
- [x] `sinh`
- [x] `sort`
- [x] `sort_by_label`
- [x] `sort_by_label_desc`
- [x] `sort_desc`
- [x] `sqrt`
- [x] `start`
- [x] `start_timestamp`
- [x] `stddev_over_time`
- [x] `stdvar_over_time`
- [x] `step`
- [x] `sum_over_time`
- [x] `tan`
- [x] `tanh`
- [x] `time`
- [x] `timestamp`
- [x] `ts_of_first_over_time`
- [x] `ts_of_last_over_time`
- [x] `ts_of_max_over_time`
- [x] `ts_of_min_over_time`
- [x] `vector`
- [x] `year`

### Upstream evaluator fixture files

- [x] [aggregators.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/aggregators.test)
- [x] [at_modifier.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/at_modifier.test)
- [x] [collision.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/collision.test)
- [x] [duration_expression.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/duration_expression.test)
- [x] [extended_vectors.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/extended_vectors.test)
- [x] [fill-modifier.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/fill-modifier.test)
- [x] [functions.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/functions.test)
- [x] [histograms.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/histograms.test)
- [x] [info.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/info.test)
- [x] [limit.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/limit.test)
- [x] [literals.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/literals.test)
- [x] [name_label_dropping.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/name_label_dropping.test)
- [x] [native_histograms.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/native_histograms.test)
- [x] [operators.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/operators.test)
- [x] [range_queries.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/range_queries.test)
- [x] [selectors.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/selectors.test)
- [x] [staleness.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/staleness.test)
- [x] [start_timestamps.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/start_timestamps.test)
- [x] [subquery.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/subquery.test)
- [x] [trig_functions.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/trig_functions.test)
- [x] [type_and_unit.test](https://github.com/prometheus/prometheus/blob/v3.15.0/promql/promqltest/testdata/type_and_unit.test)

### Complete upstream API route inventory

- [x] `OPTIONS /api/v1/*path` — CORS preflight, 204; covered by request lifecycle test.
- [x] `GET /api/v1/query` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/query` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/query_range` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/query_range` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/query_exemplars` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/query_exemplars` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/format_query` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/format_query` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/parse_query` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/parse_query` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/labels` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/labels` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/label/:name/values` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/series` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/series` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/scrape_pools` — scraper management; owned by the Collector, returns 404.
- [x] `GET /api/v1/scrape_pools/config` — scraper management; owned by the Collector, returns 404.
- [x] `GET /api/v1/targets` — scraper management; owned by the Collector, returns 404.
- [x] `GET /api/v1/targets/metadata` — scraper management; owned by the Collector, returns 404.
- [x] `GET /api/v1/targets/relabel_steps` — scraper management; owned by the Collector, returns 404.
- [x] `GET /api/v1/alertmanagers` — rule/alert management; no rule engine in this adapter, returns 404.
- [x] `GET /api/v1/metadata` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/status/config` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/runtimeinfo` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/buildinfo` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/status/flags` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/tsdb` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/tsdb/blocks` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/self_metrics` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/features` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/status/walreplay` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/notifications` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/notifications/live` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `POST /api/v1/read` — ingestion/remote storage protocol; use the Collector for ingestion, returns 404.
- [x] `POST /api/v1/write` — ingestion/remote storage protocol; use the Collector for ingestion, returns 404.
- [x] `POST /api/v1/otlp/v1/metrics` — ingestion/remote storage protocol; use the Collector for ingestion, returns 404.
- [x] `GET /api/v1/search/metric_names` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/search/metric_names` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/search/label_names` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/search/label_names` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/search/label_values` — query interface; covered by HTTP and integration tests.
- [x] `POST /api/v1/search/label_values` — query interface; covered by HTTP and integration tests.
- [x] `GET /api/v1/alerts` — rule/alert management; no rule engine in this adapter, returns 404.
- [x] `GET /api/v1/rules` — rule/alert management; no rule engine in this adapter, returns 404.
- [x] `POST /api/v1/admin/tsdb/delete_series` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `POST /api/v1/admin/tsdb/clean_tombstones` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `POST /api/v1/admin/tsdb/snapshot` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `PUT /api/v1/admin/tsdb/delete_series` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `PUT /api/v1/admin/tsdb/clean_tombstones` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `PUT /api/v1/admin/tsdb/snapshot` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.
- [x] `GET /api/v1/openapi.yaml` — Prometheus server/TSDB administration or documentation; no corresponding subsystem, returns 404.

### Evidence mapping

| Checklist area | Reproducible evidence |
|---|---|
| PromQL values, functions, temporal semantics, annotations | `just conformance`; [per-fixture totals](evaluator-report.md) |
| Parser acceptance/types, format/parse APIs | `tests/parser_conformance.rs`, `tests/http_conformance.rs`, vendor parser suite |
| OTel conversion, SQL safety, duplicates, metadata, exemplars | `tests/clickhouse.rs`, `tests/api.rs`, Collector snapshot comparison |
| PromQL through real ClickHouse | `tests/storage_conformance.rs`; [storage totals and exclusions](storage-report.md) |
| Thanos replicas/options/sharding | `src/dedup.rs`, `tests/api.rs`, `scripts/thanos-conformance.py` |
| Search and regexp dialect | `tests/review_regressions.rs`, `src/api/search.rs`, pinned reference JSON |
| Limits/cancellation and review findings | [review report](review-report.md), request lifecycle and memory regression tests |
| Live scrape and Grafana | [manual report and six screenshots](manual-verification.md) |

The standalone Prometheus compliance project and Thanos distributed E2E cluster
suite are linked as additional upstream resources, not claimed as executed. The
executed suite uses actual pinned Prometheus handlers and its full evaluator
fixture directory; it does not execute every Go engine internal unit test.
