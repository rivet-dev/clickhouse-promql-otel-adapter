# ClickHouse PromQL adapter

A **strictly Rust** PromQL evaluator and Prometheus/Thanos query HTTP adapter over
the unmodified OpenTelemetry Collector ClickHouse metrics tables. Point Grafana's
standard Prometheus data source at this server. Ingestion remains with the
Collector; no Go code runs in the adapter.

The pinned Prometheus corpus passes **21,442 executions**, with **1,016 HTTP
reference cases** and real ClickHouse integration tests. The
[compatibility checklist](docs/compatibility.md) links upstream sources and test
suites. Read [the API contract](docs/api-contract.md) and
[schema findings](docs/schema-findings.md): the default schema loses subsecond
precision and some histogram/metadata fields, so this is not a claim of lossless
replacement for every Thanos deployment.

## Run

Requires Rust and an existing ClickHouse database created by the Collector's
ClickHouse exporter (all five metric tables). Use the same database/user as the
exporter:

```sh
cargo build --locked --release
export CLICKHOUSE_URL=http://127.0.0.1:8123
export CLICKHOUSE_DATABASE=otel
export CLICKHOUSE_USER=default
# Set CLICKHOUSE_PASSWORD through your environment/secret manager.
./target/release/clickhouse-promql --host 0.0.0.0 --port 9091
```

For Thanos HA replicas add `--replica-label replica` (repeat for multiple labels).
Add `--thanos-default-step 60` to allow omitted range steps using Thanos defaults.
Without that option, range queries require `step`, as in Prometheus.

[Run the complete local demo](demo/README.md): an OpenMetrics generator, real
Collector, ClickHouse, Rust adapter and Grafana. The
[manual verification report](docs/manual-verification.md) includes six actual
browser screenshots of data source health, Explore, dashboards and error cases.

## Test and regenerate snapshots

Requires Docker Compose and Python 3. Start the dedicated test ClickHouse:

```sh
docker compose up -d --wait
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --locked
python3 scripts/conformance.py
cargo test --manifest-path vendor/promql-parser/Cargo.toml --all-features
python3 scripts/generate-snapshots.py
python3 scripts/generate-snapshots.py --precision-schema
```

Every ClickHouse integration test creates a unique database and cleans it on
success or panic. The tests require a real server; they do not silently skip it.
Compose exposes loopback ports 18123/19000 with local test credentials. Its volume
persists until `docker compose down -v`.

The snapshot script runs Collector Contrib 0.162.0, lets the actual exporter
create all five default tables, sends deterministic OTLP/HTTP data, and snapshots
SHOW CREATE TABLE plus ingested rows. It cleans its database and Collector
container. The precision variant precreates `DateTime64(9)` columns before using
the same unmodified exporter. Normal checks compare; updates are explicit:

```sh
python3 scripts/datasets.py > /tmp/metrics.json
python3 scripts/generate-snapshots.py --update
python3 scripts/generate-snapshots.py --precision-schema --update
python3 scripts/conformance.py --regenerate
python3 scripts/thanos-conformance.py --regenerate
```

Reference regeneration additionally requires Go and downloads pinned upstream
sources. Prometheus's actual evaluator assertions and HTTP handlers produce the
accepted corpus. Thanos reference generation uses seeded replica data and real
XOR iterators. Go is test tooling only. See [evaluator results](docs/evaluator-report.md),
[storage results and exclusions](docs/storage-report.md), and
[independent review corrections](docs/review-report.md).
