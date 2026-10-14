# Live manual verification

Run commands from the repository root. This demo uses local test credentials,
an anonymous Grafana viewer, and a dedicated `promql_demo` ClickHouse database.

```sh
docker compose up -d --wait
python3 demo/metrics.py --host 0.0.0.0
```

In another terminal, start the adapter:

```sh
cargo build --release
CLICKHOUSE_PASSWORD=local-test-only target/release/clickhouse-promql \
  --host 0.0.0.0 --database promql_demo
```

Then start the actual Prometheus receiver and Grafana:

```sh
docker compose -f compose.yaml -f demo/compose.yaml up -d --wait collector-demo grafana-demo
```

Open port 13000 on your machine's Tailscale IP. Dashboard UID:
`otel-clickhouse-live`. Grafana admin login is `admin` / `local-demo-only`;
anonymous viewing works without login. The Collector scrapes port 18080 every
five seconds, creates the unmodified default schema, and exports to ClickHouse.
Grafana queries the Rust adapter on port 9091 using its standard Prometheus data
source. Wait at least one minute for a complete rate window.

Stop the two foreground processes with Ctrl-C, then stop only the demo containers:

```sh
docker compose -f compose.yaml -f demo/compose.yaml stop collector-demo grafana-demo
```

The database remains available for inspection. Drop `promql_demo` explicitly
when its retained test data is no longer needed. Collector restarts reuse it;
restart the generator with a fresh database when a clean counter history matters.

This is a manual browser check, separate from the automated test suite. See
[the observed report](../docs/manual-verification.md) and saved screenshots.
