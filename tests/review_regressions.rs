use clickhouse_promql::{
    engine::Engine,
    histogram::Histogram,
    model::{Labels, Point, Series},
};
#[test]
fn re2_reference() {
    let cases: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("conformance/regex.json")).unwrap();
    let mut failures = Vec::new();
    for c in cases {
        let regex = promql_parser::label::compile_regex(c["pattern"].as_str().unwrap());
        if c["error"] == true {
            if regex.is_ok() {
                failures.push(c);
            }
            continue;
        }
        match regex {
            Ok(r)
                if r.is_match(c["value"].as_str().unwrap()) == c["matches"].as_bool().unwrap() => {}
            _ => failures.push(c),
        }
    }
    assert!(
        failures.is_empty(),
        "{}",
        serde_json::to_string_pretty(&failures).unwrap()
    );
}
#[test]
fn parser_comments_fractional_durations_and_utf8_labels() {
    for q in ["foo # [ignored comment", "foo # offset invalid"] {
        assert_eq!(Engine::parse(q).unwrap().to_string(), "foo");
    }
    for q in ["foo{x=\"a\" or x=\"b\"}", "foo{x=\"a\" or x=\"b\"}[1m]"] {
        assert!(Engine::parse(q).is_err());
    }
    for q in [
        "m[1 # ] ignored\n]",
        "m[1 # : ignored\n]",
        "m offset # ignored\n 1m",
    ] {
        assert!(Engine::parse(q).is_ok(), "{q}");
    }
    let series = [Series {
        labels: Labels::from([("__name__".into(), "m".into())]),
        points: (1..=3)
            .map(|t| Point {
                timestamp: t,
                value: t as f64,
                ..Default::default()
            })
            .collect(),
    }];
    for (q, expected) in [("m offset 0.0016", 2.0), ("sum_over_time(m[0.0016])", 3.0)] {
        assert_eq!(
            Engine::new(&series, 3, 3)
                .instant(q, 3)
                .unwrap()
                .vector()
                .unwrap()[0]
                .value,
            expected
        );
    }
    for q in [
        r#"count_values("a.b",vector(1))"#,
        r#"label_replace(vector(1),"a.b","yes","missing",".*")"#,
        r#"label_join(vector(1),"世","-","a.b")"#,
    ] {
        assert!(Engine::new(&[], 0, 0).instant(q, 0).is_ok(), "{q}");
    }
    assert!(
        Engine::new(&[], 0, 0)
            .instant(r#"count_values("",missing)"#, 0)
            .is_err()
    );
    for op in ["topk", "bottomk", "limitk"] {
        assert!(
            Engine::new(&[], 0, 0)
                .instant(&format!("{op}(Inf,vector(1))"), 0)
                .unwrap_err()
                .to_string()
                .contains("overflows int64")
        );
    }
}
#[test]
fn native_zero_threshold_reconciliation() {
    let mut a = Histogram {
        schema: 0,
        count: 1.0,
        sum: 1.5,
        positive: std::collections::BTreeMap::from([(1, 1.0)]),
        ..Default::default()
    };
    let b = Histogram {
        schema: 0,
        count: 1.0,
        sum: 1.0,
        zero_threshold: 1.5,
        zero_count: 1.0,
        ..Default::default()
    };
    a.combine(&b, 1.0).unwrap();
    assert_eq!(a.zero_threshold, 2.0);
    assert_eq!(a.zero_count, 2.0);
    assert!(a.positive.is_empty());
    let mut fine = Histogram {
        schema: 1,
        count: 1.0,
        positive: [(1, 1.0)].into(),
        ..Default::default()
    };
    let coarse = Histogram {
        schema: 0,
        count: 1.0,
        zero_threshold: 1.1,
        zero_count: 1.0,
        ..Default::default()
    };
    fine.combine(&coarse, 1.0).unwrap();
    assert_eq!(fine.zero_threshold, 2.0f64.sqrt());
    assert_eq!(fine.zero_count, 2.0);
}
#[test]
fn long_monotonicity_annotations_do_not_panic() {
    let series = [("1", 2.0), ("2", 1.0), ("+Inf", 1.0)]
        .into_iter()
        .map(|(le, value)| Series {
            labels: Labels::from([
                ("__name__".into(), "h_bucket".into()),
                ("le".into(), le.into()),
            ]),
            points: (0..70)
                .map(|t| Point {
                    timestamp: t * 1000,
                    value,
                    ..Default::default()
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    let mut engine = Engine::new(&series, 0, 69000);
    let expr = Engine::parse("histogram_quantile(0.5,h_bucket)").unwrap();
    assert!(engine.range(&expr, 1000).is_ok());
}

#[test]
fn memory_budget_counts_generated_results_and_releases_windows() {
    let expr = Engine::parse("vector(1)").unwrap();
    let mut engine = Engine::new(&[], 0, 10_000);
    engine.max_samples = 5;
    assert!(
        engine
            .range(&expr, 1000)
            .unwrap_err()
            .to_string()
            .contains("too many samples")
    );
    let series = [Series {
        labels: Labels::from([("__name__".into(), "m".into())]),
        points: (0..=100)
            .map(|t| Point {
                timestamp: t * 1000,
                value: 1.0,
                ..Default::default()
            })
            .collect(),
    }];
    let mut engine = Engine::new(&series, 10_000, 100_000);
    engine.max_samples = 130;
    let expr = Engine::parse("sum_over_time(m[10s])").unwrap();
    assert!(engine.range(&expr, 1000).is_ok());
    assert!(engine.samples_read > engine.max_samples);
    assert!(engine.peak_samples <= engine.max_samples);
}
