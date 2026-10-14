use clickhouse_promql::{
    engine::Engine,
    model::{Labels, Point, STALE_BITS, Series},
};
use serde_json::{Value, json};
use std::collections::BTreeMap;
pub fn load(data: &Value) -> Result<Vec<Series>, String> {
    data.as_array()
        .unwrap()
        .iter()
        .map(|s| {
            let labels =
                serde_json::from_value::<Labels>(s["labels"].clone()).map_err(|e| e.to_string())?;
            let points = s["points"]
                .as_array()
                .unwrap()
                .iter()
                .map(|p| {
                    if let Some(h) = p.get("histogram") {
                        return Ok(Point {
                            timestamp: p["timestamp"].as_i64().unwrap(),
                            start_timestamp: p["start_timestamp"].as_i64().unwrap_or(0),
                            histogram: Some(Box::new(load_histogram(h)?)),
                            ..Default::default()
                        });
                    }
                    let text = p["value"].as_str().unwrap();
                    let value = if text == "stale" {
                        f64::from_bits(STALE_BITS)
                    } else {
                        text.parse()
                            .map_err(|e| format!("invalid float {text}: {e}"))?
                    };
                    Ok(Point {
                        timestamp: p["timestamp"].as_i64().unwrap(),
                        start_timestamp: p["start_timestamp"].as_i64().unwrap_or(0),
                        value,
                        ..Default::default()
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Series { labels, points })
        })
        .collect()
}
pub fn compare(case: &Value, data: Result<&Vec<Series>, &String>) -> Result<(), String> {
    let data = data.map_err(Clone::clone)?;
    let start = case["start"].as_i64().unwrap();
    let end = case["end"].as_i64().unwrap();
    let step = case["step"].as_i64().unwrap();
    let query = case["query"].as_str().unwrap();
    let mut engine = Engine::new(data, start, end);
    engine.source = query.into();
    engine.lookback = case["lookback"].as_i64().unwrap();
    let result = Engine::parse_at(query, start, end, step).and_then(|expr| {
        if step == 0 {
            engine.evaluate(&expr, start)
        } else {
            engine.range(&expr, step)
        }
    });
    match (result, case.get("error")) {
        (Err(error), Some(expected)) => {
            if error.to_string() == expected.as_str().unwrap() {
                return Ok(());
            }
            return Err(format!(
                "error mismatch: actual {error}; expected {expected}"
            ));
        }
        (Err(error), None) => return Err(format!("unexpected error: {error}")),
        (Ok(_), Some(error)) => return Err(format!("expected error: {error}")),
        (Ok(value), None) => {
            let mut actual = value.json(start);
            let mut expected = case["expected"].clone();
            // API vectors are generally unordered. Sort by labels for comparison;
            // sort() ordering is tested separately by evaluator regression tests.
            let ordered = step == 0
                && [
                    "sort(",
                    "sort_desc(",
                    "sort_by_label(",
                    "sort_by_label_desc(",
                ]
                .iter()
                .any(|prefix| query.trim_start().starts_with(prefix));
            normalize(&mut actual, ordered);
            normalize(&mut expected, ordered);
            if actual["resultType"] == "string" && actual["result"][1] != expected["result"][1] {
                return Err(format!(
                    "string result mismatch: actual={actual}; expected={expected}"
                ));
            }
            equal(&actual, &expected)
                .map_err(|e| format!("{e}; actual={actual}; expected={expected}"))?;
        }
    }
    let mut warnings = case["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    warnings.sort();
    let mut infos = case["infos"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    infos.sort();
    let actual_warnings = engine.warnings.into_iter().collect::<Vec<_>>();
    if actual_warnings != warnings {
        return Err(format!(
            "warnings mismatch: actual {actual_warnings:?}; expected {warnings:?}"
        ));
    }
    let actual_infos = engine.infos.into_iter().collect::<Vec<_>>();
    if actual_infos != infos {
        return Err(format!(
            "infos mismatch: actual {actual_infos:?}; expected {infos:?}"
        ));
    }
    Ok(())
}
fn normalize(value: &mut Value, ordered: bool) {
    if matches!(value["resultType"].as_str(), Some("matrix" | "vector")) {
        if value["result"].is_null() {
            value["result"] = json!([])
        }
        if !ordered && let Some(values) = value["result"].as_array_mut() {
            values.sort_by_key(|v| v["metric"].to_string());
        }
    }
}
fn equal(a: &Value, b: &Value) -> Result<(), String> {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                return Err("object key count mismatch".into());
            }
            for (k, a) in a {
                if k == "metric" {
                    if Some(a) != b.get(k) {
                        return Err(format!(
                            "labels mismatch: actual={a}; expected={:?}",
                            b.get(k)
                        ));
                    }
                    continue;
                }
                equal(a, b.get(k).ok_or_else(|| format!("missing key {k}"))?)?;
            }
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Err("array length mismatch".into());
            }
            for (a, b) in a.iter().zip(b) {
                equal(a, b)?
            }
        }
        (Value::String(a), Value::String(b)) => {
            if a != b {
                match (a.parse::<f64>(), b.parse::<f64>()) {
                    (Ok(a), Ok(b)) if floats(a, b) => {}
                    _ => return Err(format!("string mismatch {a:?} vs {b:?}")),
                }
            }
        }
        (Value::Number(a), Value::Number(b)) => {
            if !floats(a.as_f64().unwrap(), b.as_f64().unwrap()) {
                return Err("number mismatch".into());
            }
        }
        _ if a == b => {}
        _ => return Err(format!("value mismatch: {a} vs {b}")),
    }
    Ok(())
}
fn floats(a: f64, b: f64) -> bool {
    a == b
        || (a.is_nan() && b.is_nan())
        || (a.is_finite() && b.is_finite() && (a - b).abs() <= 1e-8 * a.abs().max(b.abs()).max(1.0))
}

fn load_histogram(h: &Value) -> Result<clickhouse_promql::histogram::Histogram, String> {
    fn number(v: &Value) -> f64 {
        v.as_str()
            .and_then(|s| s.parse().ok())
            .or_else(|| v.as_f64())
            .unwrap_or(0.0)
    }
    fn buckets(spans: &Value, counts: &Value) -> BTreeMap<i32, f64> {
        let mut out = BTreeMap::new();
        let mut index = 0i32;
        let mut offset = 0;
        if let Some(spans) = spans.as_array() {
            for span in spans {
                index += span["Offset"].as_i64().unwrap() as i32;
                for _ in 0..span["Length"].as_u64().unwrap() {
                    out.insert(index, number(&counts[offset]));
                    offset += 1;
                    index += 1;
                }
            }
        }
        out
    }
    Ok(clickhouse_promql::histogram::Histogram {
        schema: h["Schema"].as_i64().unwrap() as i32,
        count: number(&h["Count"]),
        sum: number(&h["Sum"]),
        zero_count: number(&h["ZeroCount"]),
        zero_threshold: number(&h["ZeroThreshold"]),
        reset_hint: h["CounterResetHint"].as_u64().unwrap() as u8,
        positive: buckets(&h["PositiveSpans"], &h["PositiveBuckets"]),
        negative: buckets(&h["NegativeSpans"], &h["NegativeBuckets"]),
        custom: h["CustomValues"]
            .as_array()
            .unwrap()
            .iter()
            .map(number)
            .collect(),
    })
}
