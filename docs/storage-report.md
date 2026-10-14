# ClickHouse differential results

Pinned Prometheus reference results are replayed after inserting each dataset into its own real ClickHouse database. Every query uses the Rust planner, ClickHouse SQL, OTel row conversion, and Rust evaluator. Matching storage reads are cached within one dataset; values, labels, execution errors, warnings and infos are compared by the same reference comparator as the in-memory suite.

| Schema | Datasets represented | Executions passed | Conversion exclusions | Failures |
| --- | ---: | ---: | ---: | ---: |
| Default collector schema | 149 | 8,986 | 12,456 | 0 |
| DateTime64(9) + ZeroThreshold variant | 157 | 9,756 | 11,686 | 0 |

The default path tests 149 datasets, including float and exponential-histogram samples. The precision variant is an explicitly modified schema, not an assertion that the unmodified exporter writes ZeroThreshold. Both paths report every excluded dataset and reason in `.state/storage-conformance*.json`. Exclusions are never counted as passes.

Remaining exclusions include Prometheus native custom buckets, fractional/negative histogram counts, and explicit per-sample counter-reset hints that OTLP does not encode. Such PromQL inputs remain covered by the in-memory evaluator corpus.

Rows use the standard Prometheus receiver scope. The default receiver preserves original metric names (`trim_metric_suffixes: false`), so the adapter preserves those names and labels as well. SDK-originated OTLP metrics use naming conversion.

Run `just test-storage`. Each dataset database is dropped on success or panic.
