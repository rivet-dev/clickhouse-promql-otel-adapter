use super::*;
use crate::model::Labels;
use std::{collections::BTreeMap, time::Duration};
fn encode_value(value: &Value) -> Vec<u8> {
    let cancelled = AtomicBool::new(false);
    let mut encoder = Encoder::new(
        1_000_000,
        Instant::now() + Duration::from_secs(5),
        &cancelled,
    );
    encoder
        .json(&Query {
            status: "success",
            data: Data {
                value,
                timestamp: 1234,
                stats: None,
            },
            warnings: vec![],
            infos: vec![],
        })
        .unwrap_or_else(|_| panic!("encoding failed"));
    encoder.finish()
}
#[test]
fn streamed_query_matches_existing_shapes_and_float_encoding() {
    let histogram = Histogram {
        schema: 0,
        count: 7.0,
        sum: f64::INFINITY,
        zero_count: 2.0,
        zero_threshold: 0.1,
        positive: BTreeMap::from([(1, 3.0), (2, 0.0)]),
        negative: BTreeMap::from([(0, 2.0)]),
        ..Default::default()
    };
    let custom = Histogram {
        schema: -53,
        count: 1.0,
        sum: f64::NEG_INFINITY,
        positive: BTreeMap::from([(0, 1.0), (1, 0.0)]),
        custom: vec![1.0],
        ..Default::default()
    };
    let labels = Labels::from([("unicode".into(), "𝛼\"\\\n".into())]);
    let values = vec![
        Value::Scalar(f64::NAN),
        Value::Scalar(f64::INFINITY),
        Value::String("value\"\n".into()),
        Value::Vector(vec![
            Sample {
                labels: labels.clone(),
                value: f64::NEG_INFINITY,
                ..Default::default()
            },
            Sample {
                histogram: Some(histogram.clone()),
                ..Default::default()
            },
        ]),
        Value::Matrix(vec![
            Series {
                labels: labels.clone(),
                points: vec![
                    Point {
                        timestamp: 1234,
                        value: f64::NAN,
                        ..Default::default()
                    },
                    Point {
                        timestamp: 2345,
                        histogram: Some(histogram.into()),
                        ..Default::default()
                    },
                ],
            },
            Series {
                labels: Labels::new(),
                points: vec![Point {
                    timestamp: -1234,
                    histogram: Some(custom.into()),
                    ..Default::default()
                }],
            },
        ]),
        Value::Vector(vec![]),
        Value::Matrix(vec![]),
    ];
    for value in values {
        let actual: Json = serde_json::from_slice(&encode_value(&value)).unwrap();
        assert_eq!(
            actual,
            serde_json::json!({"status":"success","data":value.json(1234)})
        );
    }
}
#[test]
fn encoder_checks_exact_byte_boundary_and_cancellation_without_oversized_buffer() {
    let value = Value::Scalar(1.0);
    let expected = encode_value(&value);
    let cancelled = AtomicBool::new(false);
    for limit in [1, expected.len() - 1, expected.len()] {
        let mut encoder = Encoder::new(limit, Instant::now() + Duration::from_secs(5), &cancelled);
        let result = encoder.json(&Query {
            status: "success",
            data: Data {
                value: &value,
                timestamp: 1234,
                stats: None,
            },
            warnings: vec![],
            infos: vec![],
        });
        assert_eq!(result.is_ok(), limit == expected.len());
        assert!(encoder.bytes.len() <= limit);
        assert!(encoder.bytes.capacity() <= limit);
    }
    cancelled.store(true, Ordering::Relaxed);
    let mut encoder = Encoder::new(
        1_000_000,
        Instant::now() + Duration::from_secs(5),
        &cancelled,
    );
    assert!(encoder.json(&"never encoded").is_err());
    assert!(encoder.bytes.is_empty());
    assert_eq!(encoder.failure, Some(true));
}
