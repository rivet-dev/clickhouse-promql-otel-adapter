# Live manual verification

Run commands from the repository root. This demo uses local test credentials,
an anonymous Grafana viewer, and a dedicated `promql_demo` ClickHouse database.

```sh
just demo-metrics
```

In another terminal, start the adapter:

```sh
just demo-adapter
```

Then start the actual Prometheus receiver and Grafana:

```sh
just demo-up
```

Open port 13000 on your machine's Tailscale IP. Dashboard UID:
`otel-clickhouse-live`. Grafana admin login is `admin` / `local-demo-only`;
anonymous viewing works without login. The Collector scrapes port 18080 every
five seconds, creates the unmodified default schema, and exports to ClickHouse.
Grafana queries the Rust adapter on port 9091 using its standard Prometheus data
source. Wait at least one minute for a complete rate window.

Stop the two foreground processes with Ctrl-C, then stop only the demo containers:

```sh
just demo-down
```

The database remains available for inspection. Drop `promql_demo` explicitly
when its retained test data is no longer needed. Collector restarts reuse it;
restart the generator with a fresh database when a clean counter history matters.

This is a manual browser check, separate from the automated test suite. See
[the observed report](../docs/manual-verification.md) and saved screenshots.
