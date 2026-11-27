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
    run_window(store, sql, project, FROM, TO).await
}

async fn run_window(
    store: &Store,
    sql: &str,
    project: &str,
    from: u32,
    to: u32,
) -> anyhow::Result<Vec<Json>> {
    let body = store
        .query(
            &format!("{sql}\nFORMAT JSONEachRow"),
            &[
                ("database", store.database.clone()),
                ("param_project", project.to_owned()),
                ("param_from", from.to_string()),
                ("param_to", to.to_string()),
                ("param_query_from", (from - STEP - 300).to_string()),
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
        let counted = plan("count(a_total)");
        let rows = run(&store, &counted.leaves[0].sql, "a").await.unwrap();
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|r| r["value"] == 1.0), "{rows:?}");
        let raw = plan("a_total");
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
                format!("('a', map('service.instance.id', 'p1'), 'h2', map('pod', 'p1'), toDateTime({}), 0, 1)", FROM + 70),
                format!("('a', map('service.instance.id', 'p1'), 'h2', map('pod', 'p1'), toDateTime({}), 3, 0)", FROM + 100),
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
            vec![(u64::from(FROM), 60.0), (u64::from(FROM + 120), 30.0)]
        );

        // A step ending in a marker has no sample, as Prometheus drops the
        // series at the marker; a sample after a marker shows again.
        for (expr, expected) in [
            ("g", vec![]),
            ("max(g)", vec![]),
            ("h2", vec![(u64::from(FROM + 120), 3.0)]),
        ] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            assert_eq!(gauge_values(&rows), expected, "{expr}");
        }

        for expr in ["min_over_time(g[1m])", "avg_over_time(g[1m])"] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            let values = rows
                .iter()
                .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(values, vec![(u64::from(FROM + 60), 7.0)], "{expr}");
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
            vec![(u64::from(FROM + 180), 0.0), (u64::from(FROM + 240), 3.0)]
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
                (u64::from(FROM + 180), serde_json::json!([0, 0, 0])),
                (u64::from(FROM + 240), serde_json::json!([2, 1, 0])),
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

fn values_by(rows: &[Json], label: &str) -> Vec<(String, u64, f64)> {
    rows.iter()
        .map(|r| {
            (
                r["labels"][label].as_str().unwrap_or_default().to_owned(),
                r["t"].as_u64().unwrap(),
                r["value"].as_f64().unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn grouped_histograms_count_groups_toward_the_series_limit() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let phases = ["load", "init", "ready"];
        let mut rows = Vec::new();
        for i in 0..=MAX_SERIES_PER_LEAF {
            let phase = phases[i as usize % 3];
            let series = format!("map('pod', 'p{i}', 'phase', '{phase}')");
            rows.push(format!("('a', map('service.instance.id', 'p{i}'), 'h', {series}, toDateTime({base}), 2, 1, [1, 1, 0], [0.1, 1])"));
            rows.push(format!("('a', map('service.instance.id', 'p{i}'), 'h', {series}, toDateTime({}), 6, 3, [2, 3, 1], [0.1, 1])", base + 60));
        }
        // A series with other bounds cannot be summed into the group.
        rows.push(format!("('a', map('service.instance.id', 'odd'), 'h', map('pod', 'odd', 'phase', 'load'), toDateTime({base}), 0, 0, [0, 0], [5])"));
        rows.push(format!("('a', map('service.instance.id', 'odd'), 'h', map('pod', 'odd', 'phase', 'load'), toDateTime({}), 9, 9, [9, 0], [5])", base + 60));
        insert(
            &store,
            "otel_metrics_histogram",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds",
            &rows,
        )
        .await;

        let raw = plan("histogram_quantile(0.5, rate(h_bucket[1m]))");
        let error = run(&store, &raw.leaves[0].sql, "a")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains(SERIES_LIMIT_ERROR), "{error}");

        let grouped = plan("histogram_quantile(0.5, sum by (le, phase) (rate(h_bucket[1m])))");
        assert_eq!(grouped.leaves.len(), 1);
        assert_eq!(
            serde_json::to_value(&grouped.tree).unwrap(),
            serde_json::json!({ "op": "histogram_quantile", "q": 0.5, "arg": { "op": "leaf", "index": 0 } })
        );
        let rows = run(&store, &grouped.leaves[0].sql, "a").await.unwrap();
        let groups = rows
            .iter()
            .map(|r| {
                (
                    r["labels"].clone(),
                    r["t"].as_u64().unwrap(),
                    r["counts"].clone(),
                    r["bounds"].clone(),
                )
            })
            .collect::<Vec<_>>();
        let expected = |phase: &str, pods: u64| {
            (
                serde_json::json!({ "phase": phase }),
                u64::from(FROM),
                serde_json::json!([pods, 2 * pods, pods]),
                serde_json::json!([0.1, 1]),
            )
        };
        let mut groups = groups;
        groups.sort_by_key(|g| g.0.to_string());
        assert_eq!(
            groups,
            vec![expected("init", 334), expected("load", 334), expected("ready", 333)]
        );
    })
    .await;
}

#[tokio::test]
async fn aggregations_over_a_selector_run_in_the_leaf() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let gauge = |pod: &str, role: &str, value: f64| {
            format!(
                "('a', map('service.name', 'engine', 'service.instance.id', '{pod}'), 'g', map('pod', '{pod}', 'role', '{role}'), toDateTime({FROM}), {value})"
            )
        };
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value",
            &[
                gauge("p1", "engine", 5.0),
                gauge("p2", "engine", 7.0),
                gauge("p3", "runner", 1.0),
                gauge("p4", "", 4.0),
            ],
        )
        .await;
        let mut counters = Vec::new();
        for (pod, per_minute) in [("p1", 60.0), ("p2", 120.0), ("p3", 30.0)] {
            for offset in [0, 60] {
                counters.push(format!(
                    "('a', map('service.name', 'engine', 'service.instance.id', '{pod}'), 'a_total', map('pod', '{pod}', 'role', '{}'), toDateTime({}), {}, true, 2)",
                    if pod == "p3" { "runner" } else { "engine" },
                    base + offset,
                    per_minute * f64::from(offset / 60),
                ));
            }
        }
        insert(
            &store,
            "otel_metrics_sum",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, IsMonotonic, AggregationTemporality",
            &counters,
        )
        .await;

        let t = u64::from(FROM);
        for (expr, label, expected) in [
            ("count by (role) (g)", "role", vec![("", t, 1.0), ("engine", t, 2.0), ("runner", t, 1.0)]),
            ("avg by (role) (g)", "role", vec![("", t, 4.0), ("engine", t, 6.0), ("runner", t, 1.0)]),
            ("min by (role) (g)", "role", vec![("", t, 4.0), ("engine", t, 5.0), ("runner", t, 1.0)]),
            ("sum(g)", "role", vec![("", t, 17.0)]),
            ("max by (job) ((g))", "job", vec![("engine", t, 7.0)]),
            ("sum by (role) (rate(a_total[1m]))", "role", vec![("engine", t, 3.0), ("runner", t, 0.5)]),
            ("count(rate(a_total[1m]))", "role", vec![("", t, 3.0)]),
        ] {
            let compiled = plan(expr);
            assert_eq!(compiled.leaves.len(), 1, "{expr}");
            assert_eq!(
                serde_json::to_value(&compiled.tree).unwrap(),
                serde_json::json!({ "op": "leaf", "index": 0 }),
                "{expr}"
            );
            let rows = run(&store, &compiled.leaves[0].sql, "a").await.unwrap();
            let mut values = values_by(&rows, label);
            values.sort_by(|a, b| a.0.cmp(&b.0));
            let expected = expected
                .into_iter()
                .map(|(l, t, v)| (l.to_owned(), t, v))
                .collect::<Vec<_>>();
            assert_eq!(values, expected, "{expr}");
            for row in &rows {
                let names = row["labels"].as_object().unwrap().keys().cloned().collect::<Vec<_>>();
                assert!(names.iter().all(|n| n == label), "{expr}: {row}");
            }
        }
    })
    .await;
}

#[tokio::test]
async fn growth_counts_a_reset_inside_a_step() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let samples = [
            (FROM - 60, 90),
            (FROM, 100),
            (FROM + 15, 110),
            (FROM + 30, 3),
            (FROM + 45, 8),
            (FROM + 70, 20),
            (FROM + 100, 25),
        ];
        let counters = samples
            .iter()
            .map(|(seconds, value)| counter_row("a", "p1", *seconds, f64::from(*value)))
            .collect::<Vec<_>>();
        insert(
            &store,
            "otel_metrics_sum",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, IsMonotonic, AggregationTemporality",
            &counters,
        )
        .await;
        let histograms = samples
            .iter()
            .map(|(seconds, value)| {
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({seconds}), {value}, 0, [{value}, 1], [0.1])")
            })
            .collect::<Vec<_>>();
        insert(
            &store,
            "otel_metrics_histogram",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds",
            &histograms,
        )
        .await;

        // Step FROM: 90 -> 100. Step FROM + 60: 100 -> 110 across the
        // boundary, a reset to 3 that counts 3, and 3 -> 8. Step FROM + 120:
        // 8 -> 20 -> 25.
        let expected = vec![
            (u64::from(FROM), 10.0),
            (u64::from(FROM + 60), 18.0),
            (u64::from(FROM + 120), 17.0),
        ];
        for expr in ["increase(a_total[1m])", "sum(increase(a_total[1m]))", "increase(h_count[1m])"] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            let values = rows
                .iter()
                .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(values, expected, "{expr}");
        }

        let buckets = plan("histogram_quantile(0.5, rate(h_bucket[1m]))");
        let rows = run(&store, &buckets.leaves[0].sql, "a").await.unwrap();
        let counts = rows
            .iter()
            .map(|r| (r["t"].as_u64().unwrap(), r["counts"].clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            counts,
            vec![
                (u64::from(FROM), serde_json::json!([10, 0])),
                (u64::from(FROM + 60), serde_json::json!([18, 0])),
                (u64::from(FROM + 120), serde_json::json!([17, 0])),
            ]
        );
    })
    .await;
}

#[tokio::test]
async fn anchored_regex_matchers_filter_paths() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let gauge = |path: &str| {
            format!(
                "('a', map('service.instance.id', 'p1'), 'g', map('path', '{path}'), toDateTime({}), 1, 0)",
                FROM + 10
            )
        };
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, Flags",
            &[gauge("/api/users"), gauge("/web/home")],
        )
        .await;

        for (expr, expected) in [
            (r#"g{path=~"/api/.*$"}"#, "/api/users"),
            (r#"g{path!~"/api/.*$"}"#, "/web/home"),
        ] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            let paths = values_by(&rows, "path")
                .into_iter()
                .map(|(path, _, _)| path)
                .collect::<Vec<_>>();
            assert_eq!(paths, vec![expected.to_owned()], "{expr}");
        }
    })
    .await;
}

fn gauge_values(rows: &[Json]) -> Vec<(u64, f64)> {
    rows.iter()
        .map(|r| (r["t"].as_u64().unwrap(), r["value"].as_f64().unwrap()))
        .collect()
}

#[tokio::test]
async fn steps_end_at_their_label_on_the_grid_from_from() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let gauge = |seconds: u32, value: f64| {
            format!(
                "('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1'), toDateTime({seconds}), {value})"
            )
        };
        let from = FROM + 20;
        let to = from + 2 * STEP;
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value",
            &[
                gauge(from - 10, 0.0),
                gauge(from, 1.0),
                gauge(from + 30, 2.0),
                gauge(from + STEP, 3.0),
                gauge(to, 4.0),
                gauge(to + 10, 5.0),
            ],
        )
        .await;

        // Step `t` holds `(t - step, t]`, so a sample on a boundary is the
        // last of the step it ends and a sample at `to` fills the last step.
        let rows = run_window(&store, &plan("g").leaves[0].sql, "a", from, to)
            .await
            .unwrap();
        assert_eq!(
            gauge_values(&rows),
            vec![
                (u64::from(from), 1.0),
                (u64::from(from + STEP), 3.0),
                (u64::from(to), 4.0),
            ]
        );

        // A `to` off the grid ends at the last step at or before it.
        let rows = run_window(&store, &plan("g").leaves[0].sql, "a", from, to + 30)
            .await
            .unwrap();
        assert_eq!(gauge_values(&rows).last(), Some(&(u64::from(to), 4.0)));
    })
    .await;
}

#[tokio::test]
async fn sub_second_samples_after_a_boundary_belong_to_the_next_step() {
    common::empty_database_test(true, |store| async move {
        add_project_column(&store).await;
        let gauge = |milliseconds: u64, value: f64| {
            format!(
                "('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1'), fromUnixTimestamp64Milli({milliseconds}), {value})"
            )
        };
        let from_ms = u64::from(FROM) * 1000;
        insert(
            &store,
            "otel_metrics_gauge",
            "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value",
            &[gauge(from_ms, 1.0), gauge(from_ms + 300, 2.0)],
        )
        .await;

        let rows = run(&store, &plan("g").leaves[0].sql, "a").await.unwrap();
        assert_eq!(
            gauge_values(&rows),
            vec![(u64::from(FROM), 1.0), (u64::from(FROM + STEP), 2.0)]
        );
    })
    .await;
}

const GAUGE_COLUMNS: &str =
    "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value";
const SUM_COLUMNS: &str = "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Value, IsMonotonic, AggregationTemporality";
const HISTOGRAM_COLUMNS: &str = "ProjectId, ResourceAttributes, MetricName, Attributes, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds";

fn sorted_values(rows: &[Json], label: &str) -> Vec<(String, u64, f64)> {
    let mut values = values_by(rows, label);
    values.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    values
}

#[tokio::test]
async fn series_from_two_jobs_on_one_instance_stay_apart() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let resource =
            |job: &str| format!("map('service.name', '{job}', 'service.instance.id', 'p1')");
        insert(
            &store,
            "otel_metrics_gauge",
            GAUGE_COLUMNS,
            &[
                format!(
                    "('a', {}, 'g', map('pod', 'p1'), toDateTime({FROM}), 1)",
                    resource("engine")
                ),
                format!(
                    "('a', {}, 'g', map('pod', 'p1'), toDateTime({FROM}), 2)",
                    resource("runner")
                ),
            ],
        )
        .await;
        let mut counters = Vec::new();
        for (job, start, per_minute) in [("engine", 0.0, 60.0), ("runner", 1000.0, 6.0)] {
            for offset in [0, 60] {
                counters.push(format!(
                    "('a', {}, 'c_total', map('pod', 'p1'), toDateTime({}), {}, true, 2)",
                    resource(job),
                    base + offset,
                    start + per_minute * f64::from(offset / 60),
                ));
            }
        }
        insert(&store, "otel_metrics_sum", SUM_COLUMNS, &counters).await;

        let t = u64::from(FROM);
        let rows = run(&store, &plan("g").leaves[0].sql, "a").await.unwrap();
        assert_eq!(
            sorted_values(&rows, "job"),
            vec![("engine".to_owned(), t, 1.0), ("runner".to_owned(), t, 2.0)]
        );
        let rows = run(
            &store,
            &plan("sum by (job) (rate(c_total[1m]))").leaves[0].sql,
            "a",
        )
        .await
        .unwrap();
        assert_eq!(
            sorted_values(&rows, "job"),
            vec![("engine".to_owned(), t, 1.0), ("runner".to_owned(), t, 0.1)]
        );
    })
    .await;
}

#[tokio::test]
async fn grouped_buckets_tied_on_members_keep_one_layout() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        let row = |pod: &str, at: u32, counts: &str, bounds: &str| {
            format!("('a', map('service.instance.id', '{pod}'), 'h', map('pod', '{pod}', 'phase', 'x'), toDateTime({at}), 0, 0, {counts}, {bounds})")
        };
        insert(
            &store,
            "otel_metrics_histogram",
            HISTOGRAM_COLUMNS,
            &[
                row("narrow", base, "[1, 0]", "[1]"),
                row("narrow", base + 60, "[3, 0]", "[1]"),
                row("wide", base, "[0, 0, 0]", "[1, 2]"),
                row("wide", base + 60, "[1, 1, 1]", "[1, 2]"),
            ],
        )
        .await;

        let compiled = plan("histogram_quantile(0.5, sum by (le, phase) (rate(h_bucket[1m])))");
        let rows = run(&store, &compiled.leaves[0].sql, "a").await.unwrap();
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0]["bounds"], serde_json::json!([1, 2]));
        assert_eq!(rows[0]["counts"], serde_json::json!([1, 1, 1]));
    })
    .await;
}

#[tokio::test]
async fn count_and_sum_suffixes_read_plain_counters_and_histograms() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        insert(
            &store,
            "otel_metrics_sum",
            SUM_COLUMNS,
            &[
                format!("('a', map('service.instance.id', 'p1'), 'jobs_count', map('pod', 'p1'), toDateTime({base}), 0, true, 2)"),
                format!("('a', map('service.instance.id', 'p1'), 'jobs_count', map('pod', 'p1'), toDateTime({}), 120, true, 2)", base + 60),
            ],
        )
        .await;
        insert(
            &store,
            "otel_metrics_histogram",
            HISTOGRAM_COLUMNS,
            &[
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({base}), 2, 1, [1, 1, 0], [0.1, 1])"),
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({}), 62, 31, [21, 21, 20], [0.1, 1])", base + 60),
            ],
        )
        .await;

        let t = u64::from(FROM);
        for (expr, expected) in [
            ("rate(jobs_count[1m])", 2.0),
            ("rate(h_count[1m])", 1.0),
            ("sum(rate(h_count[1m]))", 1.0),
            ("increase(h_sum[1m])", 30.0),
            (r#"rate({__name__="h_count"}[1m])"#, 1.0),
        ] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            assert_eq!(gauge_values(&rows), vec![(t, expected)], "{expr}");
        }

        let compiled = plan(r#"histogram_quantile(0.5, sum by (le) (rate({__name__="h_bucket"}[1m])))"#);
        let rows = run(&store, &compiled.leaves[0].sql, "a").await.unwrap();
        assert_eq!(rows[0]["counts"], serde_json::json!([20, 20, 20]));
    })
    .await;
}

#[tokio::test]
async fn an_le_matcher_reads_one_cumulative_bucket() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        insert(
            &store,
            "otel_metrics_histogram",
            HISTOGRAM_COLUMNS,
            &[
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({base}), 2, 1, [1, 1, 0], [0.1, 1])"),
                format!("('a', map('service.instance.id', 'p1'), 'h', map('pod', 'p1'), toDateTime({}), 62, 31, [21, 21, 20], [0.1, 1])", base + 60),
                format!("('a', map('service.instance.id', 'p2'), 'h', map('pod', 'p2'), toDateTime({base}), 0, 0, [0, 0], [0.5])"),
                format!("('a', map('service.instance.id', 'p2'), 'h', map('pod', 'p2'), toDateTime({}), 9, 9, [4, 5], [0.5])", base + 60),
            ],
        )
        .await;

        let t = u64::from(FROM);
        for (expr, expected) in [
            (r#"increase(h_bucket{le="0.1"}[1m])"#, vec![("p1".to_owned(), t, 20.0)]),
            (r#"increase(h_bucket{le="1"}[1m])"#, vec![("p1".to_owned(), t, 40.0)]),
            (r#"sum by (pod) (increase(h_bucket{le="1"}[1m]))"#, vec![("p1".to_owned(), t, 40.0)]),
            (r#"increase(h_bucket{le="0.5"}[1m])"#, vec![("p2".to_owned(), t, 4.0)]),
            (
                r#"increase(h_bucket{le="+Inf"}[1m])"#,
                vec![("p1".to_owned(), t, 60.0), ("p2".to_owned(), t, 9.0)],
            ),
        ] {
            let rows = run(&store, &plan(expr).leaves[0].sql, "a").await.unwrap();
            let mut values = values_by(&rows, "pod");
            values.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(values, expected, "{expr}");
        }

        let rows = run(&store, &plan(r#"increase(h_bucket{le="1"}[1m])"#).leaves[0].sql, "a")
            .await
            .unwrap();
        assert_eq!(rows[0]["labels"]["le"], "1");
    })
    .await;
}

#[tokio::test]
async fn group_returns_one_for_every_group() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let gauge = |pod: &str, role: &str, value: f64| {
            format!(
                "('a', map('service.name', 'engine', 'service.instance.id', '{pod}'), 'g', map('pod', '{pod}', 'role', '{role}'), toDateTime({FROM}), {value})"
            )
        };
        insert(
            &store,
            "otel_metrics_gauge",
            GAUGE_COLUMNS,
            &[gauge("p1", "engine", 5.0), gauge("p2", "engine", 7.0), gauge("p3", "runner", 0.0)],
        )
        .await;

        let rows = run(&store, &plan("group by (role) (g)").leaves[0].sql, "a").await.unwrap();
        let mut values = values_by(&rows, "role");
        values.sort_by(|a, b| a.0.cmp(&b.0));
        let t = u64::from(FROM);
        assert_eq!(
            values,
            vec![("engine".to_owned(), t, 1.0), ("runner".to_owned(), t, 1.0)]
        );
    })
    .await;
}

#[tokio::test]
async fn grouping_by_name_keeps_a_selected_metric_name() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        insert(
            &store,
            "otel_metrics_gauge",
            GAUGE_COLUMNS,
            &[format!("('a', map('service.instance.id', 'p1'), 'g', map('pod', 'p1'), toDateTime({FROM}), 4)")],
        )
        .await;
        insert(
            &store,
            "otel_metrics_sum",
            SUM_COLUMNS,
            &[counter_row("a", "p1", base, 0.0), counter_row("a", "p1", base + 60, 60.0)],
        )
        .await;

        let rows = run(&store, &plan("sum by (__name__, pod) (g)").leaves[0].sql, "a")
            .await
            .unwrap();
        assert_eq!(rows[0]["labels"], serde_json::json!({ "__name__": "g", "pod": "p1" }));
        let rows = run(&store, &plan("sum by (__name__) (rate(a_total[1m]))").leaves[0].sql, "a")
            .await
            .unwrap();
        assert_eq!(rows[0]["labels"], serde_json::json!({}));
    })
    .await;
}

#[tokio::test]
async fn a_counter_renamed_between_total_forms_stays_one_series() {
    common::empty_database_test(false, |store| async move {
        add_project_column(&store).await;
        let base = FROM - 60;
        insert(
            &store,
            "otel_metrics_sum",
            SUM_COLUMNS,
            &[
                format!("('a', map('service.instance.id', 'p1'), 'r_total', map('pod', 'p1'), toDateTime({base}), 0, true, 2)"),
                format!("('a', map('service.instance.id', 'p1'), 'r', map('pod', 'p1'), toDateTime({}), 60, true, 2)", base + 60),
            ],
        )
        .await;

        let rows = run(&store, &plan("rate(r_total[1m])").leaves[0].sql, "a")
            .await
            .unwrap();
        assert_eq!(gauge_values(&rows), vec![(u64::from(FROM), 1.0)]);
    })
    .await;
}
