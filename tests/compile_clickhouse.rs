//! Compiled leaves run against the real test ClickHouse.
mod common;

use std::collections::BTreeMap;

use clickhouse_promql::compile::{MAX_SERIES_PER_LEAF, Plan, SERIES_LIMIT_ERROR, compile};
use clickhouse_promql::storage::Store;
use serde_json::Value as Json;

const STEP: u32 = 60;
const FROM: u32 = 1_700_000_040;
const TO: u32 = 1_700_000_300;

fn plan(expr: &str) -> Plan {
    compile(expr, &BTreeMap::new()).unwrap()
}

async fn add_project_column(store: &Store) {
    for table in [
        "otel_metrics_gauge",
        "otel_metrics_sum",
        "otel_metrics_histogram",
    ] {
        store
            .query(
                &format!(
                    "ALTER TABLE {}.{table} ADD COLUMN ProjectId String FIRST",
                    store.database
                ),
                &[],
            )
            .await
            .unwrap();
    }
}

async fn insert(store: &Store, table: &str, columns: &str, rows: &[String]) {
    store
        .query(
            &format!(
                "INSERT INTO {}.{table} ({columns}) VALUES {}",
                store.database,
                rows.join(", ")
            ),
            &[],
        )
        .await
        .unwrap();
}

async fn run(store: &Store, sql: &str, project: &str) -> anyhow::Result<Vec<Json>> {
    let body = store
        .query(
            &format!("{sql}\nFORMAT JSONEachRow"),
            &[
                ("database", store.database.clone()),
                ("param_project", project.to_owned()),
                ("param_from", FROM.to_string()),
                ("param_to", TO.to_string()),
                ("param_query_from", (FROM - 300).to_string()),
                ("param_step", STEP.to_string()),
            ],
        )
        .await?;
    Ok(body
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect())
}

fn counter_row(project: &str, pod: &str, seconds: u32, value: f64) -> String {
    format!(
        "('{project}', map('service.name', 'engine', 'service.instance.id', '{pod}'), 'a_total', map('pod', '{pod}', 'rivet_project', '{project}'), toDateTime({seconds}), {value}, true, 2)"
    )
}

#[tokio::test]
async fn compiled_leaves_read_one_project() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let mut rows = Vec::new();
        for (offset, value) in [(0, 0.0), (60, 60.0), (120, 120.0), (180, 30.0), (240, 90.0)] {
            rows.push(counter_row("a", "p1", base + offset, value));
            rows.push(counter_row("b", "p9", base + offset, value * 100.0));
        }
        insert(
            &store,
            "otel_metrics_sum",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, IsMonotonic, AggregationTemporality",
            &rows,
        )
        .await;
        insert(
            &store,
            "otel_metrics_histogram",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds",
            &[
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({base}), 2, 1, [1, 1, 0], [0.1, 1])"),
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({}), 6, 3, [2, 3, 1], [0.1, 1])", base + 60),
                format!("('b', map('service.instance.id', 'p9'), 'h', map('pod', 'p9'), toDateTime({}), 99, 9, [90, 9, 0], [0.1, 1])", base + 60),
            ],
        )
        .await;
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value",
            &[
                format!("('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1', 'role', 'engine'), toDateTime({}), 5)", base + 70),
                format!("('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1', 'role', 'engine'), toDateTime({}), 7)", base + 100),
                format!("('a', map('service.instance.id', 'p2'), 'g', map('pod', 'p2', 'role', 'runner'), toDateTime({}), 1)", base + 100),
                format!("('b', map('service.instance.id', 'p9'), 'g', map('pod', 'p9', 'role', 'engine'), toDateTime({}), 9)", base + 100),
            ],
        )
        .await;

        // A counter's growth per step, per second, with the reset at 180s
        // counting the whole post-reset value.
        let rate = plan(r#"rate(a_total{rivet_project="$project", pod=~"$host"}[5m])"#);
        let rows = run(&store, &rate.leaves[0].sql, "a").await.unwrap();
        let values = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                (u64::from(FROM), 1.0),
                (u64::from(FROM + 60), 1.0),
                (u64::from(FROM + 120), 0.5),
                (u64::from(FROM + 180), 1.0),
            ]
        );
        assert_eq!(rows[0]["labels"]["pod"], "p1");
        assert_eq!(rows[0]["labels"]["job"], "engine");

        for expr in [
            "rate(a_total[1m])",
            "increase(a_total[1m])",
            "rate(h_bucket[1m])",
            "rate(h_count[1m])",
            "rate(h_sum[1m])",
            "g",
            r#"g{role=~"engine|runtime"}"#,
            "max_over_time(g[1m])",
            "a_total",
            "max_over_time(a_total[1m])",
            "changes(g[1m])",
            "deriv(g[1m])",
        ] {
            let leaf = &match expr {
                "rate(h_bucket[1m])" => plan(&format!("histogram_quantile(0.5, {expr})")),
                expr => plan(expr),
            }
            .leaves[0];
            for row in run(&store, &leaf.sql, "a").await.unwrap() {
                assert_ne!(row["labels"]["pod"], "p9", "{expr}: {row}");
            }
            assert!(
                run(&store, &leaf.sql, "c").await.unwrap().is_empty(),
                "{expr}"
            );
        }

        let buckets = plan("histogram_quantile(0.5, rate(h_bucket[1m]))");
        let rows = run(&store, &buckets.leaves[0].sql, "a").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["counts"], serde_json::json!([1, 2, 1]));
        assert_eq!(rows[0]["bounds"], serde_json::json!([0.1, 1]));

        let regex = plan(r#"g{role=~"engine|runtime"}"#);
        let rows = run(&store, &regex.leaves[0].sql, "a").await.unwrap();
        assert!(rows.iter().all(|r| r["labels"]["role"] == "engine"));
        assert!(!rows.is_empty());

        // A counter read without a counter function reads its samples.
        let raw = plan("count(a_total)");
        let rows = run(&store, &raw.leaves[0].sql, "a").await.unwrap();
        let values = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                (u64::from(FROM), 60.0),
                (u64::from(FROM + 60), 120.0),
                (u64::from(FROM + 120), 30.0),
                (u64::from(FROM + 180), 90.0),
            ]
        );
    })
    .await;
}

#[tokio::test]
async fn leaves_skip_staleness_markers() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let counter = |seconds: u32, value: f64, flags: u32| {
            format!(
                "('a', map('service.instance.id', 'p1'), 'a_total', map('pod', 'p1'), toDateTime({seconds}), {value}, {flags}, true, 2)"
            )
        };
        insert(
            &store,
            "otel_metrics_sum",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, Flags, IsMonotonic, AggregationTemporality",
            &[
                counter(base, 1000.0, 0),
                counter(base + 60, 1060.0, 0),
                counter(base + 90, 0.0, 1),
                counter(base + 150, 1090.0, 0),
            ],
        )
        .await;
        let gauge = |seconds: u32, value: f64, flags: u32| {
            format!(
                "('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1'), toDateTime({seconds}), {value}, {flags})"
            )
        };
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, Flags",
            &[
                gauge(FROM + 10, 7.0, 0),
                gauge(FROM + 50, 0.0, 1),
                gauge(FROM + 70, 0.0, 1),
            ],
        )
        .await;

        let increase = plan("increase(a_total[1m])");
        let rows = run(&store, &increase.leaves[0].sql, "a").await.unwrap();
        let values = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![(u64::from(FROM), 60.0), (u64::from(FROM + 60), 30.0)]
        );

        for expr in ["g", "min_over_time(g[1m])", "avg_over_time(g[1m])"] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            let values = rows
                .iter()
                .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(values, vec![(u64::from(FROM), 7.0)], "{expr}");
        }
    })
    .await;
}

#[tokio::test]
async fn counters_returning_after_the_lookback_start_without_growth() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 290;
        insert(
            &store,
            "otel_metrics_sum",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, IsMonotonic, AggregationTemporality",
            &[
                counter_row("a", "p1", base, 17000.0),
                counter_row("a", "p1", FROM + 130, 2.0),
                counter_row("a", "p1", FROM + 190, 5.0),
            ],
        )
        .await;
        insert(
            &store,
            "otel_metrics_histogram",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds",
            &[
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({base}), 90, 9, [80, 10, 0], [0.1, 1])"),
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({}), 2, 1, [1, 1, 0], [0.1, 1])", FROM + 130),
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({}), 5, 2, [3, 2, 0], [0.1, 1])", FROM + 190),
            ],
        )
        .await;

        let increase = plan("increase(a_total[1m])");
        let rows = run(&store, &increase.leaves[0].sql, "a").await.unwrap();
        let values = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![(u64::from(FROM + 120), 0.0), (u64::from(FROM + 180), 3.0)]
        );

        let buckets = plan("histogram_quantile(0.5, rate(h_bucket[1m]))");
        let rows = run(&store, &buckets.leaves[0].sql, "a").await.unwrap();
        let counts = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["counts"].clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            counts,
            vec![
                (u64::from(FROM + 120), serde_json::json!([0, 0, 0])),
                (u64::from(FROM + 180), serde_json::json!([2, 1, 0])),
            ]
        );
    })
    .await;
}

#[tokio::test]
async fn leaves_fail_past_the_series_limit() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let rows = (0..=MAX_SERIES_PER_LEAF)
            .map(|i| {
                format!(
                    "('a', map('service.instance.id', 'p'), 'g', map('n', '{i}'), toDateTime({FROM}), 1)"
                )
            })
            .collect::<Vec<_>>();
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value",
            &rows,
        )
        .await;
        let error = run(&store, &plan("g").leaves[0].sql, "a")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(SERIES_LIMIT_ERROR), "{error}");
    })
    .await;
}
