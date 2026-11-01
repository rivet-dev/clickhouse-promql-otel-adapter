//! A range query must produce exactly what one instant evaluation per step
//! produces. Instant evaluation never precomputes subtrees, so comparing the
//! two on the same engine checks every series-at-a-time path against the
//! step loop: results, annotations and read statistics.
#[path = "common/corpus.rs"]
mod corpus;
#[allow(dead_code)]
#[path = "common/reference.rs"]
mod reference;
use clickhouse_promql::{
    engine::Engine,
    histogram::Histogram,
    model::{Labels, Point, STALE_BITS, Series, Value},
};
use promql_parser::parser::Expr;
use std::collections::BTreeMap;

#[derive(Clone)]
struct Options {
    lookback: i64,
    replica_labels: Vec<String>,
    max_samples: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            lookback: 300_000,
            replica_labels: Vec::new(),
            max_samples: 5_000_000,
        }
    }
}

fn engine<'a>(
    data: &'a [Series],
    query: &str,
    start: i64,
    end: i64,
    options: &Options,
) -> Engine<'a> {
    let mut engine = Engine::new(data, start, end);
    engine.source = query.into();
    engine.lookback = options.lookback;
    engine.replica_labels = options.replica_labels.clone();
    engine.max_samples = options.max_samples;
    engine
}

/// The range result assembled from one instant evaluation per step, as
/// `Engine::range` assembles it.
fn per_step(
    engine: &mut Engine<'_>,
    expr: &Expr,
    start: i64,
    end: i64,
    step: i64,
) -> anyhow::Result<Value> {
    let mut result: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
    let mut t = start;
    loop {
        match engine.evaluate(expr, t)? {
            Value::Scalar(value) => result.entry(Labels::new()).or_default().push(Point {
                timestamp: t,
                value,
                ..Default::default()
            }),
            Value::Vector(samples) => {
                for s in samples {
                    result.entry(s.labels).or_default().push(Point {
                        timestamp: t,
                        value: s.value,
                        histogram: s.histogram.map(Box::new),
                        start_timestamp: s.start_timestamp,
                        drop_name: s.drop_name,
                    });
                }
            }
            _ => anyhow::bail!("invalid expression type for range query"),
        }
        match t.checked_add(step) {
            Some(next) if next <= end => t = next,
            _ => break,
        }
    }
    Ok(Value::Matrix(
        result
            .into_iter()
            .map(|(labels, points)| Series { labels, points })
            .collect(),
    ))
}

fn assert_equivalent(
    data: &[Series],
    query: &str,
    start: i64,
    end: i64,
    step: i64,
    options: &Options,
) {
    let expr = Engine::parse_at(query, start, end, step).unwrap();
    let mut ranged = engine(data, query, start, end, options);
    let range_result = ranged.range(&expr, step);
    let mut stepped = engine(data, query, start, end, options);
    stepped.is_range = true;
    stepped.query_step = step;
    let step_result = per_step(&mut stepped, &expr, start, end, step);
    match (range_result, step_result) {
        (Ok(range), Ok(steps)) => {
            assert_eq!(range.json(start), steps.json(start), "{query}");
            // Statistics only accompany successful results; a failing query
            // may have read more before the step loop reached the error.
            assert_eq!(ranged.samples_read, stepped.samples_read, "{query}");
        }
        (Err(range), Err(steps)) => assert_eq!(range.to_string(), steps.to_string(), "{query}"),
        (range, steps) => panic!("{query}: range {range:?} but per step {steps:?}"),
    }
    assert_eq!(ranged.warnings, stepped.warnings, "{query}");
    assert_eq!(ranged.infos, stepped.infos, "{query}");
}

fn check(data: &[Series], queries: &[&str], start: i64, end: i64, step: i64) {
    for query in queries {
        assert_equivalent(data, query, start, end, step, &Options::default());
    }
}

fn labels(name: &str, labels: &[(&str, &str)]) -> Labels {
    let mut map = Labels::from([("__name__".into(), name.into())]);
    for (k, v) in labels {
        map.insert(k.to_string(), v.to_string());
    }
    map
}

fn series(name: &str, labels_: &[(&str, &str)], values: &[(i64, f64)]) -> Series {
    Series {
        labels: labels(name, labels_),
        points: values
            .iter()
            .map(|&(timestamp, value)| Point {
                timestamp,
                value,
                ..Default::default()
            })
            .collect(),
    }
}

/// A counter sampled every 15 s, with optional start timestamps.
fn counter(name: &str, labels_: &[(&str, &str)], values: &[(i64, f64, i64)]) -> Series {
    Series {
        labels: labels(name, labels_),
        points: values
            .iter()
            .map(|&(timestamp, value, start_timestamp)| Point {
                timestamp,
                value,
                start_timestamp,
                ..Default::default()
            })
            .collect(),
    }
}

fn ramp(count: usize, slope: f64) -> Vec<(i64, f64)> {
    (0..count)
        .map(|i| (i as i64 * 15_000, i as f64 * slope))
        .collect()
}

#[test]
fn corpus_range_cases_match_per_step_evaluation() {
    let (cases, datasets) = corpus::load(|data| reference::load(data).unwrap());
    let mut checked = 0;
    for case in &cases {
        let step = case["step"].as_i64().unwrap();
        if step == 0 {
            continue;
        }
        let options = Options {
            lookback: case["lookback"].as_i64().unwrap(),
            ..Default::default()
        };
        assert_equivalent(
            &datasets[case["dataset"].as_str().unwrap()],
            case["query"].as_str().unwrap(),
            case["start"].as_i64().unwrap(),
            case["end"].as_i64().unwrap(),
            step,
            &options,
        );
        checked += 1;
    }
    assert!(checked > 10_000, "only {checked} range cases");
}

#[test]
fn range_functions_with_offsets_and_unaligned_steps() {
    let data = [
        series("x_total", &[("job", "a")], &ramp(20, 2.0)),
        series("x_total", &[("job", "b")], &ramp(20, 3.0)),
        series("y", &[("job", "a")], &ramp(20, -1.0)),
    ];
    let queries = [
        "rate(x_total[1m])",
        "increase(x_total[1m] offset 30s)",
        "delta(y[45s] offset -30s)",
        "sum(rate(x_total[1m] offset 10s))",
        "sum by (job) (increase(x_total[2m]))",
        "max without (job) (delta(x_total[1m]))",
        "count(rate(x_total[30s]))",
        "min(rate(x_total[1m] offset 1m))",
    ];
    check(&data, &queries, 0, 300_000, 15_000);
    check(&data, &queries, 7_000, 300_000, 10_000);
    check(&data, &queries, -60_000, 120_000, 20_000);
}

#[test]
fn stale_markers_inside_windows() {
    let stale = f64::from_bits(STALE_BITS);
    let data = [
        series(
            "x_total",
            &[("job", "a")],
            &[
                (0, 0.0),
                (15_000, 1.0),
                (30_000, stale),
                (45_000, 3.0),
                (60_000, stale),
                (75_000, stale),
                (90_000, 6.0),
                (105_000, 7.0),
            ],
        ),
        series("x_total", &[("job", "b")], &ramp(8, 1.0)),
        series("x_total", &[("job", "c")], &[(0, stale), (15_000, stale)]),
    ];
    check(
        &data,
        &[
            "rate(x_total[30s])",
            "sum(rate(x_total[1m]))",
            "count(increase(x_total[45s]))",
            "min by (job) (delta(x_total[1m]))",
        ],
        0,
        120_000,
        15_000,
    );
}

#[test]
fn start_timestamps_count_overlap_warnings_per_window() {
    let data = [
        counter(
            "x_total",
            &[("job", "a")],
            &[
                (0, 0.0, 0),
                (15_000, 5.0, 0),
                (30_000, 2.0, 20_000),
                (45_000, 4.0, 20_000),
                (60_000, 1.0, 50_000),
                (75_000, 3.0, 50_000),
            ],
        ),
        // One sample per window, with a start timestamp inside the window.
        counter(
            "y_total",
            &[("job", "b")],
            &[(10_000, 4.0, 5_000), (70_000, 2.0, 65_000)],
        ),
        // Start timestamps that overlap earlier samples.
        counter(
            "z_total",
            &[("job", "c")],
            &[
                (0, 1.0, 0),
                (15_000, 2.0, 10_000),
                (30_000, 3.0, 10_000),
                (45_000, 9.0, 40_000),
            ],
        ),
    ];
    check(
        &data,
        &[
            "rate(x_total[30s])",
            "increase(y_total[30s])",
            "sum(rate(z_total[45s]))",
            "sum(increase({__name__=~\".+_total\"}[1m]))",
            "delta(z_total[1m])",
        ],
        0,
        90_000,
        15_000,
    );
}

#[test]
fn native_histogram_inputs_use_the_step_loop() {
    let mut mixed = series("x_total", &[("job", "a")], &ramp(6, 1.0));
    mixed.points[3].histogram = Some(Box::new(Histogram {
        schema: 0,
        count: 3.0,
        sum: 4.5,
        positive: BTreeMap::from([(0, 3.0)]),
        ..Default::default()
    }));
    let data = [mixed, series("x_total", &[("job", "b")], &ramp(6, 2.0))];
    check(
        &data,
        &[
            "rate(x_total[1m])",
            "sum(rate(x_total[1m]))",
            "sum by (job) (increase(x_total[30s]))",
        ],
        0,
        90_000,
        15_000,
    );
}

#[test]
fn subqueries_and_nested_shapes() {
    let data = [
        series("x_total", &[("job", "a"), ("le", "1")], &ramp(30, 1.0)),
        series("x_total", &[("job", "a"), ("le", "+Inf")], &ramp(30, 2.0)),
        series("x_total", &[("job", "b"), ("le", "1")], &ramp(30, 0.5)),
        series("x_total", &[("job", "b"), ("le", "+Inf")], &ramp(30, 4.0)),
    ];
    check(
        &data,
        &[
            "max_over_time(rate(x_total[1m])[2m:30s])",
            "sum_over_time(sum(rate(x_total[1m]))[1m:15s])",
            "histogram_quantile(0.9, sum by (le) (rate(x_total[1m])))",
            "histogram_quantile(0.9, sum by (le, job) (rate(x_total[1m]))) and on (job) topk(1, sum by (job) (rate(x_total[1m])))",
            "sum(rate(x_total[1m])) / sum(increase(x_total[2m]))",
            "-sum(rate(x_total[1m]))",
            "(sum by (job) (rate(x_total[1m])))",
            "topk(1, sum by (job) (rate(x_total[1m])))",
            "avg(rate(x_total[1m]))",
            "sum(rate(x_total[1m] @ 60))",
            "sum(rate(x_total[1m])) > 1",
            "label_replace(sum by (job) (rate(x_total[1m])), \"j\", \"$1\", \"job\", \"(.*)\")",
            "timestamp(sum(rate(x_total[1m])))",
        ],
        0,
        300_000,
        15_000,
    );
}

#[test]
fn replica_labels_are_merged_before_the_walk() {
    let data = [
        series("x_total", &[("job", "a"), ("replica", "0")], &ramp(10, 1.0)),
        series(
            "x_total",
            &[("job", "a"), ("replica", "1")],
            &[(5_000, 0.0), (20_000, 1.5), (35_000, 2.0), (50_000, 4.0)],
        ),
        series("x_total", &[("job", "b"), ("replica", "0")], &ramp(10, 3.0)),
    ];
    let options = Options {
        replica_labels: vec!["replica".into()],
        ..Default::default()
    };
    for query in [
        "rate(x_total[1m])",
        "sum(rate(x_total[1m]))",
        "sum by (job) (increase(x_total[30s]))",
    ] {
        assert_equivalent(&data, query, 0, 150_000, 15_000, &options);
    }
}

#[test]
fn name_grouping_and_modifier_free_sums() {
    let data = [
        series("a_total", &[("job", "a")], &ramp(10, 1.0)),
        series("b_total", &[("job", "a")], &ramp(10, 2.0)),
        series("b_total", &[("job", "b")], &ramp(10, 3.0)),
    ];
    check(
        &data,
        &[
            "sum(rate({__name__=~\".+_total\"}[1m]))",
            "sum by (__name__) (rate({__name__=~\".+_total\"}[1m]))",
            "sum by (__name__, job) (rate(b_total[1m]))",
            "rate({__name__=~\".+_total\"}[1m])",
            "count without () (rate(b_total[1m]))",
        ],
        0,
        150_000,
        15_000,
    );
}

#[test]
fn tables_over_budget_fall_back() {
    let data = [
        series("x_total", &[("job", "a")], &ramp(14, 1.0)),
        series("x_total", &[("job", "b")], &ramp(14, 2.0)),
    ];
    // 2 outputs x 13 steps exceeds a quarter of the budget while the result
    // and each step's windows stay within it.
    let options = Options {
        max_samples: 100,
        ..Default::default()
    };
    for query in ["rate(x_total[1m])", "sum(rate(x_total[1m]))"] {
        assert_equivalent(&data, query, 0, 180_000, 15_000, &options);
    }
}

#[test]
fn gaps_nans_and_negative_zero() {
    let data = [
        series(
            "x_total",
            &[("job", "a")],
            &[(0, 0.0), (15_000, 1.0), (120_000, 5.0), (135_000, 6.0)],
        ),
        series(
            "x_total",
            &[("job", "b")],
            &[(60_000, 2.0), (75_000, f64::NAN), (90_000, 3.0)],
        ),
        series(
            "g",
            &[("job", "a")],
            &[(0, -0.0), (15_000, -0.0), (30_000, 0.0), (45_000, -0.0)],
        ),
        series(
            "g",
            &[("job", "b")],
            &[(0, 1.0), (15_000, f64::NAN), (30_000, f64::INFINITY)],
        ),
    ];
    check(
        &data,
        &[
            "count(rate(x_total[30s]))",
            "min by (job) (rate(x_total[30s]))",
            "max(increase(x_total[30s]))",
            "sum(rate(x_total[1m]))",
            "delta(g[30s])",
            "sum(delta(g[30s]))",
            "min(delta(g[30s]))",
            "max(delta(g[30s]))",
            "sum by (job) (delta(g[1m]))",
        ],
        0,
        180_000,
        15_000,
    );
}
