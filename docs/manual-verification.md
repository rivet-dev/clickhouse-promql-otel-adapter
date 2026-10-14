# Manual Grafana and Collector verification

Observed in local Chromium on 2026-10-14 UTC. This was an interactive browser
inspection, not a mocked Grafana test. The real chain was:

`demo/metrics.py` OpenMetrics HTTP endpoint → Collector Contrib 0.162.0 Prometheus
receiver → ClickHouse 26.9.18.11 default exporter tables → Rust adapter → Grafana
12.2.0 standard Prometheus data source using POST.

The Collector created all five tables in the dedicated `promql_demo` database.
No handwritten DDL or direct sample insertion was used for this manual pipeline.
[Configs and reproduction commands](../demo/README.md) are committed.

| Browser check | Observed result | Screenshot |
|---|---|---|
| Data source Test button | “Successfully queried the Prometheus API.” | [Health](screenshots/grafana-datasource-health.png) |
| Four-panel dashboard | Live counter rates, two changing gauges, classic histogram p90, and three summary quantiles rendered | [Dashboard](screenshots/grafana-live-dashboard.png) |
| Explore, query type Both | Range graph plus instant results: 200 rate 12/s; 500 approximately 0.3/s | [Explore](screenshots/grafana-explore.png) |
| Metrics browser | Metric names, metric-specific labels, status values 200 and 500 populated | [Discovery](screenshots/grafana-label-discovery.png) |
| Unknown metric | “No data,” without a query error | [Empty](screenshots/grafana-empty-query.png) |
| Invalid `rate(demo_requests_total)` | Visible `bad_data` error explaining that rate requires a range vector | [Error](screenshots/grafana-invalid-query.png) |

The initial rate ramp is expected during the first incomplete one-minute window;
the successful request rate settles at the generator's 12/s. Histogram p90 is
approximately 0.75 s; summary quantiles are 512, 2048, and 4096 bytes. All panels
use actual scraped samples and API requests. The default schema retains only
whole seconds; this demo deliberately uses five-second scrapes.

Current workspace URL: <http://100.94.102.49:13000/d/otel-clickhouse-live>.
Screenshots remain available after the processes stop. They verify Grafana
interoperability for these workflows, not exhaustive PromQL compatibility.

The dashboard was refreshed and captured again after rebuilding the reviewed
Rust adapter at 01:01 UTC. Live API rates remained 12/s and approximately 0.291/s;
all four panels rendered without errors.
