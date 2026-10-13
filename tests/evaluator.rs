use clickhouse_promql::{
    engine::Engine,
    model::{Labels, Point, STALE_BITS, Series, Value},
};
fn series(name: &str, labels: &[(&str, &str)], values: &[(i64, f64)]) -> Series {
    let mut map = Labels::from([("__name__".into(), name.into())]);
    for (k, v) in labels {
        map.insert(k.to_string(), v.to_string());
    }
    Series {
        labels: map,
        points: values
            .iter()
            .map(|(timestamp, value)| Point {
                timestamp: *timestamp,
                value: *value,
            })
            .collect(),
    }
}
fn values(data: &[Series], query: &str, t: i64) -> Vec<f64> {
    Engine::new(data, t, t)
        .instant(query, t)
        .unwrap()
        .vector()
        .unwrap()
        .iter()
        .map(|s| s.value)
        .collect()
}
#[test]
fn scalar_precedence_and_special_values() {
    let mut e = Engine::new(&[], 0, 0);
    assert_eq!(e.instant("1+2*3^2", 0).unwrap().scalar().unwrap(), 19.0);
    assert!(e.instant("0/0", 0).unwrap().scalar().unwrap().is_nan());
    assert_eq!(e.instant("1/0", 0).unwrap().json(0)["result"][1], "+Inf");
}
#[test]
fn lookback_stale_and_left_open_range() {
    let d = [series(
        "x",
        &[],
        &[
            (0, 1.0),
            (60_000, f64::from_bits(STALE_BITS)),
            (120_000, 3.0),
        ],
    )];
    assert_eq!(values(&d, "x", 59_000), vec![1.0]);
    assert!(values(&d, "x", 60_000).is_empty());
    assert!(values(&d, "x", 420_000).is_empty());
    assert_eq!(values(&d, "count_over_time(x[2m])", 120_000), vec![1.0]);
}
#[test]
fn offset_and_at() {
    let d = [series("x", &[], &[(0, 1.0), (60_000, 2.0), (120_000, 3.0)])];
    assert_eq!(values(&d, "x offset 1m", 120_000), vec![2.0]);
    assert_eq!(values(&d, "x @ 0", 120_000), vec![1.0]);
    assert_eq!(values(&d, "x offset -1m", 0), vec![2.0]);
}
#[test]
fn anchored_matchers_and_absent_labels() {
    let d = [
        series("x", &[("route", "api")], &[(0, 1.0)]),
        series("x", &[("route", "api/v1")], &[(0, 2.0)]),
    ];
    assert_eq!(values(&d, "x{route=~\"api\"}", 0), vec![1.0]);
    assert_eq!(values(&d, "x{missing=\"\"}", 0).len(), 2);
    assert!(values(&d, "x{missing!=\"\"}", 0).is_empty());
    let v = Engine::new(&d, 0, 0)
        .instant("absent(y{job=\"a\",instance=~\".*\"})", 0)
        .unwrap()
        .vector()
        .unwrap();
    assert_eq!(v[0].labels, Labels::from([("job".into(), "a".into())]));
}
#[test]
fn rate_with_reset_and_extrapolation() {
    let d = [series(
        "requests_total",
        &[],
        &[
            (0, 0.0),
            (15_000, 10.0),
            (30_000, 20.0),
            (45_000, 5.0),
            (60_000, 15.0),
        ],
    )];
    assert!((values(&d, "rate(requests_total[1m])", 60_000)[0] - 25.0 / 45.0).abs() < 1e-12);
    assert_eq!(values(&d, "resets(requests_total[1m])", 60_000), vec![1.0]);
    assert_eq!(
        values(&d, "irate(requests_total[1m])", 60_000),
        vec![10.0 / 15.0]
    );
}
#[test]
fn grouping_and_matching_cardinality() {
    let d = [
        series("x", &[("job", "a"), ("instance", "1")], &[(0, 2.0)]),
        series("x", &[("job", "a"), ("instance", "2")], &[(0, 3.0)]),
        series("y", &[("job", "a")], &[(0, 10.0)]),
    ];
    assert_eq!(values(&d, "sum by(job)(x)", 0), vec![5.0]);
    assert_eq!(values(&d, "x * on(job) group_left y", 0), vec![20.0, 30.0]);
    assert!(Engine::new(&d, 0, 0).instant("x * on(job) y", 0).is_err());
    assert_eq!(values(&d, "x and on(job) y", 0), vec![2.0, 3.0]);
    assert!(values(&d, "x unless on(job) y", 0).is_empty());
}
#[test]
fn scalar_comparison_preserves_vector_value() {
    let d = [series("x", &[], &[(0, 3.0)])];
    assert_eq!(values(&d, "2 < x", 0), vec![3.0]);
    assert_eq!(values(&d, "2 < bool x", 0), vec![1.0]);
}
#[test]
fn histogram_quantile_interpolates() {
    let d = [
        series("latency_bucket", &[("le", "0.1")], &[(0, 2.0)]),
        series("latency_bucket", &[("le", "1")], &[(0, 8.0)]),
        series("latency_bucket", &[("le", "+Inf")], &[(0, 10.0)]),
    ];
    assert!((values(&d, "histogram_quantile(0.5,latency_bucket)", 0)[0] - 0.55).abs() < 1e-12);
}
#[test]
fn range_scalar_and_subquery_alignment() {
    let mut e = Engine::new(&[], 15_000, 75_000);
    let result = e.range(&Engine::parse("time()").unwrap(), 30_000).unwrap();
    let Value::Matrix(m) = result else { panic!() };
    assert_eq!(
        m[0].points.iter().map(|p| p.value).collect::<Vec<_>>(),
        vec![15.0, 45.0, 75.0]
    );
    assert_eq!(
        values(&[], "sum_over_time(vector(time())[2m:30s])", 75_000),
        vec![60.0]
    );
}
#[test]
fn duplicate_output_labelsets_fail() {
    let d = [series("x", &[], &[(0, 1.0)]), series("y", &[], &[(0, 2.0)])];
    assert!(
        Engine::new(&d, 0, 0)
            .instant("abs({__name__=~\"x|y\"})", 0)
            .is_err()
    );
}
#[test]
fn resource_limits() {
    let d = [series("x", &[], &[(0, 1.0)])];
    let mut e = Engine::new(&d, 0, 0);
    e.max_samples = 0;
    assert!(
        e.instant("x", 0)
            .unwrap_err()
            .to_string()
            .contains("too many samples")
    );
}
