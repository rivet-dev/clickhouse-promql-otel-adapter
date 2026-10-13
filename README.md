# ClickHouse PromQL adapter

A Rust Prometheus/Thanos query API over the OpenTelemetry Collector's ClickHouse
metrics tables. **Under development; full compatibility is not yet implemented.**
See [the compatibility checklist](docs/compatibility.md) for the acceptance gates
and authoritative upstream sources.

## Reproduce collector schema and data snapshots

Requires Docker Compose and Python 3 (standard library only).

```sh
docker compose up -d --wait
python3 scripts/generate-snapshots.py          # compare with committed snapshots
python3 scripts/generate-snapshots.py --update # explicitly replace snapshots
python3 scripts/datasets.py > /tmp/metrics.json
```

The snapshot script starts the real Collector Contrib 0.162.0, lets its ClickHouse
exporter create all five default metric tables, and sends deterministic OTLP/HTTP
metrics. Each run creates and cleans up its own database and collector container.
Snapshots include actual ClickHouse DDL, ingested rows, and the input payload.
ClickHouse 26.9.18.11 listens on loopback ports 18123/19000 using local test
credentials. The compose volume persists until `docker compose down -v`.

[Schema findings](docs/schema-findings.md) explain fidelity issues in the default
format. No schema changes are required to run the baseline snapshot test.

Run the Rust and real ClickHouse tests with `cargo test`. Replay the committed
Prometheus evaluator, parser, and HTTP reference corpora with
`python3 scripts/conformance.py`; `--regenerate` explicitly replaces them using
the pinned upstream Go test tools. Go is never used by the adapter.

`python3 scripts/generate-snapshots.py --precision-schema` checks a separately
snapshotted `DateTime64(9)` schema using the unmodified Collector. This confirms
that precreated precise columns preserve subsecond input. See the
[ClickHouse differential report](docs/storage-report.md) for representable
upstream datasets and explicit format exclusions.
