# clickhouse-promql-otel-adapter

A Prometheus-compatible query API over the metrics tables the OpenTelemetry
Collector writes to ClickHouse. Point Grafana's standard Prometheus data source
at it and query your OTel metrics with PromQL.

## Why

If your metrics already land in ClickHouse through the Collector's ClickHouse
exporter, you shouldn't need a second metrics store (Prometheus, Thanos, Mimir)
just to get PromQL. This adapter reads the existing tables as they are. You don't
change the schema, add a second ingestion path or make a copy of the data.

### Why PromQL instead of raw ClickHouse SQL?

- **Every tool already speaks it.** Grafana dashboards, Explore, alerting and
  community dashboards all expect a Prometheus data source.
- **Counters are hard in SQL.** `rate()`, `increase()` and
  `histogram_quantile()` need counter-reset handling, extrapolation and lookback.
  In SQL that means a page of window functions per panel, and each copy can get
  it subtly wrong.
- **Queries stay short.** `sum by (service) (rate(http_requests_total[5m]))` is
  one line. The same query in SQL against the OTel schema joins maps, attributes
  and temporality by hand.
- **The queries are portable.** The same PromQL runs against Prometheus or Thanos
  if you ever move.

## Architecture

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./.github/media/architecture-dark.svg">
    <img src="./.github/media/architecture-light.svg" alt="Grafana queries the stateless Rust adapter over the Prometheus HTTP API. The adapter pushes series and time-range selection to ClickHouse as SQL against the five default otel_metrics tables, which the OpenTelemetry Collector's ClickHouse exporter writes." width="440">
  </picture>
</p>

- The **Collector** owns ingestion and creates the five default
  `otel_metrics_*` tables (gauge, sum, histogram, exponential histogram,
  summary). The adapter never writes to them.
- The **adapter** is a single Rust binary. It parses PromQL, pushes series and
  time-range selection down to ClickHouse as SQL, and runs evaluation in its own
  engine (rates, aggregations, histograms, Thanos-style replica dedup).
  OTel metric and attribute names are converted to Prometheus names the same way
  `prometheus/otlptranslator` converts them.
- It's **stateless**: no cache and no local storage. You can run as many replicas
  as you like.

It is tested against the upstream Prometheus PromQL test corpus (21k+ executions)
and 1k+ HTTP reference cases. See [compatibility](docs/compatibility.md). The
default exporter schema loses subsecond timestamps and some histogram and
metadata fields, so results aren't lossless in every case. See
[schema findings](docs/schema-findings.md).

## Quickstart

You need a ClickHouse database that the Collector's ClickHouse exporter already
writes to.

**Docker:**

```sh
docker run --rm -p 9091:9091 \
  -e CLICKHOUSE_URL=http://clickhouse:8123 \
  -e CLICKHOUSE_DATABASE=otel \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=... \
  ghcr.io/rivet-dev/clickhouse-promql-otel-adapter:latest
```

**From source:**

```sh
cargo build --locked --release
CLICKHOUSE_URL=http://127.0.0.1:8123 CLICKHOUSE_PASSWORD=... \
  ./target/release/clickhouse-promql --host 0.0.0.0
```

Then in Grafana, add a **Prometheus** data source with the URL
`http://<adapter-host>:9091`.

To see the whole pipeline running locally (metric generator, Collector,
ClickHouse, adapter and Grafana), follow the [demo](demo/README.md).

## Reference

### Configuration

| Flag | Env | Default | Description |
|---|---|---|---|
| `--host` | | `127.0.0.1` | Bind address (the container image uses `0.0.0.0`) |
| `--port` | | `9091` | HTTP port |
| `--clickhouse-url` | `CLICKHOUSE_URL` | `http://127.0.0.1:18123` | ClickHouse HTTP endpoint |
| `--database` | `CLICKHOUSE_DATABASE` | `otel` | Database containing the `otel_metrics_*` tables |
| `--username` | `CLICKHOUSE_USER` | `default` | ClickHouse user |
| `--password` | `CLICKHOUSE_PASSWORD` | empty | ClickHouse password |
| `--replica-label <name>` | | none | Label that identifies HA replicas for Thanos dedup. Repeat the flag to set several. |
| `--thanos-default-step <secs>` | | off | Accept range queries without `step`, using Thanos defaults (usually `60`). By default `step` is required, as in Prometheus. |
| | `RUST_LOG` | | Log filter, for example `info` or `clickhouse_promql=debug` |

### HTTP endpoints

- `/api/v1/*`: Prometheus query API (`query`, `query_range`, `series`, `labels`,
  `label/<name>/values`, `metadata`, `query_exemplars`, `format_query`,
  `parse_query`, `status/buildinfo`), plus Thanos query parameters (`dedup`,
  `replicaLabels[]`, `partial_response`, `max_source_resolution`, `shard_info`
  and others).
- `/-/healthy` checks liveness. `/-/ready` checks readiness and makes a real
  ClickHouse request.
- `/metrics` exposes adapter build info.

Not implemented: rules and alerts (use Grafana-managed alerting), remote
read/write, ingestion, TSDB admin and the Thanos StoreAPI. These routes return 404.

The adapter has **no built-in auth or TLS**. Put it behind your own network
controls or a reverse proxy.

Full details, including default resource limits and error codes, are in the
[API contract](docs/api-contract.md).

### Development

```sh
docker compose up -d --wait        # test ClickHouse on 127.0.0.1:18123
cargo test --locked
python3 scripts/conformance.py     # Prometheus corpus
```

Snapshot and reference regeneration (requires Go) is described in the
[evaluator](docs/evaluator-report.md) and [storage](docs/storage-report.md)
reports. Pushing a `vX.Y.Z` tag that matches `Cargo.toml` publishes Linux
amd64/arm64 binaries (glibc 2.39+) and a multi-arch image to GHCR.

## Maintenance

We built this for internal use at [Rivet](https://rivet.dev). We appreciate PRs,
but we probably won't merge them. If you maintain a serious fork, open an issue
and we'll list it here so others who find this project can use it.

**Forks:** none yet.

## License

[Apache 2.0](LICENSE). See [NOTICE](NOTICE) for third-party attributions
(Prometheus, Thanos, Greptime `promql-parser`, otlptranslator).
