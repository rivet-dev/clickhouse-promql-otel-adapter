# Query API contract

The runtime is Rust throughout: HTTP, parser, evaluator, histogram math, replica
deduplication and ClickHouse access. Go is required only to regenerate reference
fixtures. Baseline semantics are pinned in `upstream-pins.json`.

## Supported surface

Prometheus instant/range queries, series, label names/values, metadata, exemplars,
format/parse, and NDJSON search are implemented. GET and form POST use the upstream
parameter precedence. Query values, special floats, native histograms, annotations,
error envelopes, limits and discovery are covered by the HTTP reference tests.
The buildinfo, health, readiness and adapter build metric routes support Grafana.
Readiness performs a real ClickHouse request.

Experimental PromQL functions, duration expressions, extended range selectors and
fill modifiers are enabled to match the reference fixture configuration. The
adapter does not reproduce Prometheus's full server feature-flag registry.
Parser acceptance/types are compared independently; arbitrary parser error text
and source positions beyond the reference cases may differ.

## Thanos behavior

| Parameter or route | Behavior |
|---|---|
| `dedup`, `replicaLabels[]` | Thanos penalty iterator; configured replica labels via repeated `--replica-label`. `dedup=false` retains replicas. |
| `partial_response` | Validated; there is one backend. Its failure remains an error, not an empty partial result. |
| `max_source_resolution` | Validated, including `auto`; all data is raw resolution, which satisfies any nonnegative maximum. |
| `storeMatch[]` | Matches the single backend's label set (`App.store_labels`, empty by default). |
| `shard_info` | Thanos label hashing partitions series; tested with complementary shards. |
| `engine` | Accepts `prometheus` and `thanos`; both execute the Rust evaluator. |
| `analyze` | Reports the upstream non-analyzable/fallback warning; no distributed optimizer tree is invented. |
| `query_explain`, `query_range_explain` | Return the upstream fallback bad-data response for a non-explainable query. |
| Omitted range `step` | Prometheus mode requires it. Start with `--thanos-default-step 60` for Thanos's `max(range_seconds/250, default_seconds)` behavior. |
| `/stores` | 404: no Thanos StoreAPI topology. |
| `/rules`, `/alerts` | 404: no rule engine. Use Grafana-managed alerts or a separate evaluator; rule management is not implemented. |

The [complete inventory](compatibility.md#complete-upstream-api-route-inventory)
classifies scrape management, remote read/write, OTLP ingestion and TSDB admin.
Those are not query adapter services and return 404. Ingestion remains the
Collector's responsibility. Thanos distributed execution, downsampling, StoreAPI
gRPC and object-store management are not implemented.

## Statistics and resource controls

`stats=true`/`all` provides the Prometheus response shape with Rust measurements:

- `samplesRead` and `totalQueryableSamples` count samples selected into evaluation
  windows/vectors. Repeated window reads count again; subquery-generated samples
  are not storage reads. These counters are not ClickHouse physical rows scanned.
- `peakSamples` estimates peak retained Rust sample slots, including output and
  invariant caches. A native histogram costs one slot plus bucket/custom-bound
  slots. It is not a byte allocation profile or the Go evaluator's memory count.
- Preparation, blocking-worker queue, evaluation and total execution durations
  are measured. Admission/body time is bounded separately. `resultSortTime` is
  zero because sorting is included in evaluation, not timed as a separate phase.

Default limits: 16 admitted requests, 128 pending requests, 120-second request/query/ClickHouse timeout,
1 MiB form body, 65,536 query bytes, 2,048 parser tokens, 128 evaluation depth,
5 million retained sample slots, 11,000 intervals per range series, 1 million
fetched rows and 64 MiB of sample/discovery/exemplar responses across tables per request.
Metric dictionaries have a separate 64 MiB response limit per table and a shared
60-second cache in the CLI (library caching is opt-in). The 5-million-sample
budget also bounds storage expansion before evaluation, including classic
histogram buckets, native buckets, discovery series and exemplars.
Search caps terms at 1,024 bytes
and scoring work at 100 million estimated character operations. These limits
are explicit protections, not identical Prometheus or Thanos deployment defaults.

Configure limits with `--max-concurrency`, `--max-pending-requests`,
`--request-timeout-seconds`, `--max-rows`, `--max-bytes`, `--max-samples`, and
`--max-response-bytes` (64 MiB of uncompressed success output by default).
Oversized output returns `422 execution` without sending a partial success.
An exhausted pending queue returns `503 unavailable`. Request parameters are
limited to 1,024 pairs and 1 MiB per body/query string.

Timeout covers body reading and admission; the client query timeout includes
time waiting for an execution slot. Blocking evaluation/scoring/serialization owns its
permit until completion. Encoding checks cancellation and byte limits on each
write. Cancellation is cooperative: parsing, a single series sort, and native
histogram bucket preparation can finish their current CPU operation after the deadline.
Dropped ClickHouse reads
request server cancellation. Backend request failures return `500 internal`;
conversion/evaluation failures return `422 execution`; parameter errors return
`400 bad_data`, and timeouts return `503 timeout`. Warning/info output uses the
upstream ten-annotation cap with an omission count.

The service has no built-in authentication or TLS termination. Bind/access it
according to the deployment's existing network and reverse-proxy controls.

See [high-cardinality performance](performance.md) for measured workloads and
the limits of raw-sample evaluation.
