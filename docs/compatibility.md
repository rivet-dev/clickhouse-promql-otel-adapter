# PromQL on the OpenTelemetry ClickHouse schema

Status: all **21,442 captured upstream executions pass** against the strictly Rust evaluator. Real ClickHouse integration tests and collector snapshots also pass. 356 parser acceptance/type cases and 588 HTTP reference cases pass. Broader HTTP/Thanos coverage, manual Grafana validation, and independent reviews remain in progress. An unchecked item
is not a compatibility claim. Target: Prometheus 3.15.0 query semantics and the
Thanos HTTP query surface, backed by Collector Contrib 0.162.0's default schema.
ClickHouse's native SQL interfaces remain supplied by ClickHouse itself.

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

1. Record sources, enumerate features, pin tools and dependencies (this revision).
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

- [x] Collector-generated default gauge/sum/histogram/exponential histogram/summary tables, DDL snapshots and reproducible regeneration command: `scripts/generate-snapshots.py` (generate + clean verification passed).
- [x] Deterministic synthetic OTLP fixture generator; Collector-generated row snapshots: `scripts/datasets.py`, `tests/snapshots/`.
- [x] Real ClickHouse integration tests with unique databases, cleanup on success/failure, concurrent isolation.
- [x] Gauge int/double values; monotonic and non-monotonic sums.
- [x] Cumulative counters, reset boundaries, start timestamps, zero samples. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Delta temporality: correct cumulative conversion or explicit actionable rejection; never silently treat deltas as cumulative counters.
- [ ] Explicit histogram cumulative buckets, +Inf, sum/count; optional sum, empty histograms, boundary values.
- [ ] Exponential/native histogram scale, positive/negative buckets, zero threshold/count, resets and mixed float/histogram series.
- [x] Summaries and quantiles; sum/count names and label rendering. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] OTLP flags, no-recorded-value/stale samples, NaN and infinities; missing samples. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Nanosecond input to Prometheus millisecond semantics; default-schema loss documented, precise-column variant verified through the real Collector and 9,756 query executions.
- [ ] Prometheus scrape round trip: metric suffix/unit/name normalization, resource job/instance, scope and target_info, label collision handling.
- [x] Resource/point/scope attributes: typed values, canonical labels, UTF-8 and invalid names. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [x] Exemplars: labels, trace/span identifiers, timestamps and range selection. See collector snapshots and `tests/clickhouse.rs`; format losses are documented.
- [ ] Metadata: HELP/TYPE/unit, conflicting definitions, limits.
- [ ] Duplicate samples, conflicting duplicates, out-of-order data, retry duplicates and HA replica semantics.
- [ ] SQL parameter binding, identifier validation, bounded scans, cancellation, query timeouts and memory/sample limits.
- [ ] Storage unavailable/errors produce API errors, never fabricated empty successes.
- [ ] Schema recommendation report with observed evidence and lossless/lossy boundaries.

## PromQL language and evaluator

- [ ] Number/string/vector/matrix values and correct JSON representation.
- [ ] Selector equality/inequality/full-match regex/negative regex, missing labels as empty, empty-selector rejection.
- [ ] Metric names, quoted UTF-8 names, escaping, label sorting and empty label semantics.
- [ ] Five-minute configurable lookback, latest sample, left-open range boundary, staleness handling.
- [ ] Instant/range evaluation, inclusive end, fractional steps and timestamps, empty ranges.
- [ ] Positive/negative offset, @ timestamps, @ start()/end().
- [ ] Subqueries, resolution alignment, nested temporal modifiers.
- [ ] Unary +/- and binary precedence; IEEE NaN/Inf arithmetic, modulo and power.
- [ ] Scalar/vector and vector/vector arithmetic; metric name dropping.
- [ ] Comparisons, bool modifier and filtering; metric name retention rules.
- [ ] on/ignoring/group_left/group_right, include labels, cardinality checks and duplicate-output errors.
- [ ] Set and/or/unless operations, many-to-many matching, empty vectors.
- [ ] Aggregations by/without: sum/avg/min/max/count/group/stddev/stdvar/quantile/topk/bottomk/count_values.
- [ ] Experimental aggregations limitk/limit_ratio gated consistently.
- [ ] Counter rate/increase/irate extrapolation, resets, sparse series and zero-point adjustment.
- [ ] Delta/idelta/deriv/predict_linear, range regression and extrapolation.
- [ ] Range aggregation functions, quantiles, absent/present, last sample labels.
- [ ] Classic and native histogram quantiles, fractions, bucket interpolation, monotonicity repair and annotations.
- [ ] Histogram arithmetic, aggregations, functions, schema reduction, incompatible schemas and mixed types.
- [ ] Label manipulation: Go regexp/replacement semantics, collisions, empty matches.
- [ ] Sorting including natural label sorting and special float values.
- [ ] Calendar/time functions in UTC; default vector(time()) behavior.
- [ ] Trigonometric/math/clamping/rounding functions and special values.
- [ ] Experimental/new functions and syntax explicitly tracked against baseline.
- [ ] Warnings/infos, source positions, error categories and query statistics.
- [ ] Parser rejection/type checking and format/parse API serialization.

## HTTP query compatibility

- [ ] GET and form POST /api/v1/query: query,time,timeout,limit,lookback_delta,stats.
- [ ] GET and form POST /api/v1/query_range: query,start,end,step,timeout,limit,lookback_delta,stats; maximum point count.
- [ ] GET/POST /api/v1/series: repeated match[],start,end,limit; union and deduplication.
- [ ] GET/POST /api/v1/labels and GET /api/v1/label/:name/values: match[],start,end,limit; escaping and sorting.
- [ ] /api/v1/metadata and /api/v1/query_exemplars contracts.
- [ ] /api/v1/format_query and /api/v1/parse_query.
- [ ] status=success/error envelopes, 400/422/503, errorType, warnings, infos, valid JSON special values.
- [ ] RFC3339/numeric timestamps, duration/numeric steps, duplicate/missing/invalid parameters and POST precedence.
- [ ] CORS/OPTIONS, gzip, method errors, URL/body limits, cancellation and concurrency.
- [ ] /-/healthy, /-/ready, /metrics, /api/v1/status/buildinfo for Grafana.
- [ ] Thanos parameters: dedup,replicaLabels[],partial_response,max_source_resolution,storeMatch[],engine,analyze and lookback_delta; document semantics where no Thanos stores exist.
- [ ] Thanos /api/v1/stores, explain/analyze, rule/alert discovery and query statistics contract decisions.
- [ ] Inventory all upstream endpoints below; classify ingestion, rule management, TSDB admin and scrape-target management separately from query replacement. Do not claim these are implemented through empty placeholder responses.

## Test and release evidence

- [x] Unit tests with regression cases for evaluator and translation boundaries.
- [x] API result snapshots with controlled times and deterministic label ordering.
- [ ] Upstream fixture runner preserving load/clear/eval, missing/stale/native samples, expected failures, annotations and ordering.
- [x] Execute every upstream fixture and publish captured execution coverage with no silent skips: `scripts/conformance.py`, `tests/conformance/`, `docs/evaluator-report.md`. Query-creation rejections remain a separate pending check.
- [ ] Differential generated datasets against Prometheus, including randomized seeds recorded for replay.
- [x] Integration coverage of all five collector-created table types.
- [ ] Manual Grafana data source health, Explore instant/range, dashboard rate and histogram panels, labels/variables and empty/error cases.
- [ ] Save local-browser screenshots and manual report, including exact versions/configs.
- [ ] cargo fmt, clippy, unit/integration/conformance checks.
- [ ] Independent review round completed and findings resolved.
- [ ] Final JJ graph at active stack tip; major changes in separate non-empty revisions.

## Per-function and fixture inventory

Generated from the pinned upstream source during initial research. Every entry
needs test evidence before being marked complete; naming a function is not proof
of compatibility.

### Registered functions

- [ ] `abs`
- [ ] `absent`
- [ ] `absent_over_time`
- [ ] `acos`
- [ ] `acosh`
- [ ] `asin`
- [ ] `asinh`
- [ ] `atan`
- [ ] `atanh`
- [ ] `avg_over_time`
- [ ] `ceil`
- [ ] `changes`
- [ ] `clamp`
- [ ] `clamp_max`
- [ ] `clamp_min`
- [ ] `cos`
- [ ] `cosh`
- [ ] `count_over_time`
- [ ] `day_of_month`
- [ ] `day_of_week`
- [ ] `day_of_year`
- [ ] `days_in_month`
- [ ] `deg`
- [ ] `delta`
- [ ] `deriv`
- [ ] `double_exponential_smoothing`
- [ ] `end`
- [ ] `exp`
- [ ] `first_over_time`
- [ ] `floor`
- [ ] `histogram_avg`
- [ ] `histogram_count`
- [ ] `histogram_fraction`
- [ ] `histogram_quantile`
- [ ] `histogram_quantiles`
- [ ] `histogram_stddev`
- [ ] `histogram_stdvar`
- [ ] `histogram_sum`
- [ ] `hour`
- [ ] `idelta`
- [ ] `increase`
- [ ] `info`
- [ ] `irate`
- [ ] `label_join`
- [ ] `label_replace`
- [ ] `last_over_time`
- [ ] `ln`
- [ ] `log10`
- [ ] `log2`
- [ ] `mad_over_time`
- [ ] `max_of`
- [ ] `max_over_time`
- [ ] `min_of`
- [ ] `min_over_time`
- [ ] `minute`
- [ ] `month`
- [ ] `pi`
- [ ] `predict_linear`
- [ ] `present_over_time`
- [ ] `quantile_over_time`
- [ ] `rad`
- [ ] `range`
- [ ] `rate`
- [ ] `resets`
- [ ] `round`
- [ ] `scalar`
- [ ] `sgn`
- [ ] `sin`
- [ ] `sinh`
- [ ] `sort`
- [ ] `sort_by_label`
- [ ] `sort_by_label_desc`
- [ ] `sort_desc`
- [ ] `sqrt`
- [ ] `start`
- [ ] `start_timestamp`
- [ ] `stddev_over_time`
- [ ] `stdvar_over_time`
- [ ] `step`
- [ ] `sum_over_time`
- [ ] `tan`
- [ ] `tanh`
- [ ] `time`
- [ ] `timestamp`
- [ ] `ts_of_first_over_time`
- [ ] `ts_of_last_over_time`
- [ ] `ts_of_max_over_time`
- [ ] `ts_of_min_over_time`
- [ ] `vector`
- [ ] `year`

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

- [ ] `OPTIONS /api/v1/*path` — classify and test.
- [ ] `GET /api/v1/query` — classify and test.
- [ ] `POST /api/v1/query` — classify and test.
- [ ] `GET /api/v1/query_range` — classify and test.
- [ ] `POST /api/v1/query_range` — classify and test.
- [ ] `GET /api/v1/query_exemplars` — classify and test.
- [ ] `POST /api/v1/query_exemplars` — classify and test.
- [ ] `GET /api/v1/format_query` — classify and test.
- [ ] `POST /api/v1/format_query` — classify and test.
- [ ] `GET /api/v1/parse_query` — classify and test.
- [ ] `POST /api/v1/parse_query` — classify and test.
- [ ] `GET /api/v1/labels` — classify and test.
- [ ] `POST /api/v1/labels` — classify and test.
- [ ] `GET /api/v1/label/:name/values` — classify and test.
- [ ] `GET /api/v1/series` — classify and test.
- [ ] `POST /api/v1/series` — classify and test.
- [ ] `GET /api/v1/scrape_pools` — classify and test.
- [ ] `GET /api/v1/scrape_pools/config` — classify and test.
- [ ] `GET /api/v1/targets` — classify and test.
- [ ] `GET /api/v1/targets/metadata` — classify and test.
- [ ] `GET /api/v1/targets/relabel_steps` — classify and test.
- [ ] `GET /api/v1/alertmanagers` — classify and test.
- [ ] `GET /api/v1/metadata` — classify and test.
- [ ] `GET /api/v1/status/config` — classify and test.
- [ ] `GET /api/v1/status/runtimeinfo` — classify and test.
- [ ] `GET /api/v1/status/buildinfo` — classify and test.
- [ ] `GET /api/v1/status/flags` — classify and test.
- [ ] `GET /api/v1/status/tsdb` — classify and test.
- [ ] `GET /api/v1/status/tsdb/blocks` — classify and test.
- [ ] `GET /api/v1/status/self_metrics` — classify and test.
- [ ] `GET /api/v1/features` — classify and test.
- [ ] `GET /api/v1/status/walreplay` — classify and test.
- [ ] `GET /api/v1/notifications` — classify and test.
- [ ] `GET /api/v1/notifications/live` — classify and test.
- [ ] `POST /api/v1/read` — classify and test.
- [ ] `POST /api/v1/write` — classify and test.
- [ ] `POST /api/v1/otlp/v1/metrics` — classify and test.
- [ ] `GET /api/v1/search/metric_names` — classify and test.
- [ ] `POST /api/v1/search/metric_names` — classify and test.
- [ ] `GET /api/v1/search/label_names` — classify and test.
- [ ] `POST /api/v1/search/label_names` — classify and test.
- [ ] `GET /api/v1/search/label_values` — classify and test.
- [ ] `POST /api/v1/search/label_values` — classify and test.
- [ ] `GET /api/v1/alerts` — classify and test.
- [ ] `GET /api/v1/rules` — classify and test.
- [ ] `POST /api/v1/admin/tsdb/delete_series` — classify and test.
- [ ] `POST /api/v1/admin/tsdb/clean_tombstones` — classify and test.
- [ ] `POST /api/v1/admin/tsdb/snapshot` — classify and test.
- [ ] `PUT /api/v1/admin/tsdb/delete_series` — classify and test.
- [ ] `PUT /api/v1/admin/tsdb/clean_tombstones` — classify and test.
- [ ] `PUT /api/v1/admin/tsdb/snapshot` — classify and test.
- [ ] `GET /api/v1/openapi.yaml` — classify and test.
