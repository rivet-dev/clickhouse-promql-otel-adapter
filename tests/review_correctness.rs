use clickhouse_promql::{
    engine::Engine,
    model::{Labels, Point, Series},
};

#[test]
fn subquery_enforces_memory_limit_while_building_result() {
    let mut engine = Engine::new(&[], 0, 1_000_000);
    engine.max_samples = 100;
    let error = engine
        .instant("sum_over_time(vector(1)[1000s:1s])", 1_000_000)
        .unwrap_err();
    assert!(error.to_string().contains("too many samples"));
    // Fail at the first over-budget step, not after retaining the entire matrix.
    assert!(engine.peak_samples <= 102, "{}", engine.peak_samples);
}

#[test]
fn matrix_selector_enforces_memory_limit_while_building_result() {
    let data = [Series {
        labels: Labels::from([("__name__".into(), "metric".into())]),
        points: (1..=1000)
            .map(|timestamp| Point {
                timestamp,
                value: 1.0,
                ..Default::default()
            })
            .collect(),
    }];
    let mut engine = Engine::new(&data, 0, 1000);
    engine.max_samples = 100;
    assert!(engine.instant("metric[1s]", 1000).is_err());
    assert!(engine.peak_samples <= 101, "{}", engine.peak_samples);
}

#[test]
fn range_selector_cache_preserves_missing_labels_offsets_and_new_queries() {
    let data = (0..2000)
        .map(|i| Series {
            labels: if i % 2 == 0 {
                Labels::from([
                    ("__name__".into(), "metric".into()),
                    ("instance".into(), i.to_string()),
                ])
            } else {
                Labels::from([
                    ("__name__".into(), "metric".into()),
                    ("instance".into(), i.to_string()),
                    ("zone".into(), "east".into()),
                ])
            },
            points: vec![Point {
                timestamp: 0,
                value: 1.0,
                ..Default::default()
            }],
        })
        .collect::<Vec<_>>();
    let mut engine = Engine::new(&data, 0, 3000);
    let expression =
        Engine::parse("sum(metric{zone=\"\"}) + sum(metric{zone=~\"east\"} offset 1s)").unwrap();
    let result = engine.range(&expression, 1000).unwrap().matrix().unwrap();
    assert_eq!(result[0].points.len(), 3);
    assert!(result[0].points.iter().all(|point| point.value == 2000.0));
    // The public dataset can change between queries; cached matching must not leak.
    engine.series = &[];
    let expression = Engine::parse("metric{zone=\"\"}").unwrap();
    assert!(
        engine
            .range(&expression, 1000)
            .unwrap()
            .matrix()
            .unwrap()
            .is_empty()
    );
}

#[test]
#[ignore = "manual high-cardinality performance comparison; no timing assertions"]
fn high_cardinality_selector_cache_benchmark() {
    let data = (0..50_000)
        .map(|i| Series {
            labels: Labels::from([
                ("__name__".into(), "metric".into()),
                ("instance".into(), format!("node_{i}")),
            ]),
            points: vec![Point {
                timestamp: 0,
                value: 1.0,
                ..Default::default()
            }],
        })
        .collect::<Vec<_>>();
    let query = r#"sum(metric{instance=~"node_(12|34)[0-9]{3}"})"#;
    let expr = Engine::parse(query).unwrap();
    let start = std::time::Instant::now();
    let mut uncached = Vec::new();
    for timestamp in (0..60).map(|t| t * 1000) {
        uncached.push(
            Engine::new(&data, 0, 59_000)
                .evaluate(&expr, timestamp)
                .unwrap()
                .vector()
                .unwrap()[0]
                .value,
        );
    }
    let uncached_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    let cached = Engine::new(&data, 0, 59_000)
        .range(&expr, 1000)
        .unwrap()
        .matrix()
        .unwrap();
    let cached_elapsed = start.elapsed();
    assert_eq!(
        cached[0].points.iter().map(|p| p.value).collect::<Vec<_>>(),
        uncached
    );
    assert!(uncached.iter().all(|value| *value == 2000.0));
    eprintln!(
        "50,000 series / 60 steps: uncached matching {uncached_elapsed:?}, cached {cached_elapsed:?}"
    );
}

#[test]
fn vector_selector_enforces_memory_limit_before_cloning_all_labelsets() {
    let data = (0..1000)
        .map(|i| Series {
            labels: Labels::from([
                ("__name__".into(), "metric".into()),
                ("instance".into(), i.to_string()),
            ]),
            points: vec![Point {
                timestamp: 0,
                value: 1.0,
                ..Default::default()
            }],
        })
        .collect::<Vec<_>>();
    let mut engine = Engine::new(&data, 0, 0);
    engine.max_samples = 100;
    assert!(engine.instant("metric", 0).is_err());
    assert_eq!(engine.peak_samples, 101);
}

#[test]
fn float_point_layout() {
    // Float-heavy classic histogram expansions must not carry an inline native
    // histogram (112 bytes on x86_64) in every individual bucket point.
    assert!(std::mem::size_of::<Point>() <= 40);
    eprintln!(
        "Point={} Histogram={} Sample={}",
        std::mem::size_of::<Point>(),
        std::mem::size_of::<clickhouse_promql::histogram::Histogram>(),
        std::mem::size_of::<clickhouse_promql::model::Sample>()
    );
}

#[test]
#[ignore = "manual classic histogram evaluator benchmark; no timing assertions"]
fn classic_histogram_range_benchmark() {
    use std::collections::{BTreeSet, HashSet};
    let mut data = Vec::with_capacity(544 * 19);
    for host in 0..544 {
        for bucket in 0..19 {
            let labels = Labels::from([
                (
                    "__name__".into(),
                    "foundationdb_transaction_latency_bucket".into(),
                ),
                ("cluster".into(), "foundationdb-regional-production".into()),
                ("deployment_environment".into(), "production".into()),
                ("job".into(), "foundationdb-exporter".into()),
                (
                    "le".into(),
                    if bucket == 18 {
                        "+Inf".into()
                    } else {
                        ((bucket + 1) * 100).to_string()
                    },
                ),
                ("metric_source".into(), "prometheus".into()),
                ("namespace".into(), "rivet-engine".into()),
                (
                    "node_name".into(),
                    format!("aks-engine-pool-vmss-{host:06}"),
                ),
                ("operation".into(), format!("operation_{}", host % 8)),
                ("rivet_datacenter".into(), "us-east-1".into()),
                ("rivet_project".into(), "prod".into()),
            ]);
            data.push(Series {
                labels,
                points: (0..400)
                    .map(|i| Point {
                        timestamp: i * 15_000,
                        value: (i * (bucket + 1)) as f64,
                        ..Default::default()
                    })
                    .collect(),
            });
        }
    }
    let start = std::time::Instant::now();
    for _ in 0..30 {
        let mut seen = BTreeSet::new();
        for series in &data {
            assert!(seen.insert(&series.labels));
        }
        std::hint::black_box(seen);
    }
    let tree_elapsed = start.elapsed();
    let start = std::time::Instant::now();
    for _ in 0..30 {
        let mut seen = HashSet::with_capacity(data.len());
        for series in &data {
            assert!(seen.insert(&series.labels));
        }
        std::hint::black_box(seen);
    }
    let hash_elapsed = start.elapsed();
    let query = "sum by(le, operation)(rate(foundationdb_transaction_latency_bucket[2m]))";
    let expr = Engine::parse(query).unwrap();
    let start = std::time::Instant::now();
    let result = Engine::new(&data, 159 * 15_000, 399 * 15_000)
        .range(&expr, 15_000)
        .unwrap()
        .matrix()
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(result.len(), 19 * 8);
    assert!(result.iter().all(|s| s.points.len() == 241));
    for series in &result {
        let bucket = if series.labels["le"] == "+Inf" {
            19.0
        } else {
            series.labels["le"].parse::<f64>().unwrap() / 100.0
        };
        let expected = 68.0 * bucket / 15.0;
        assert!(
            series
                .points
                .iter()
                .all(|p| (p.value - expected).abs() < 1e-10)
        );
    }
    eprintln!(
        "10,336 labelsets x30: BTreeSet={tree_elapsed:?}; HashSet={hash_elapsed:?}; 1h/15s classic histogram range={elapsed:?}"
    );
}
