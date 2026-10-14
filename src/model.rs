use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

pub type Labels = BTreeMap<String, String>;
pub const STALE_BITS: u64 = 0x7ff0000000000002;

#[derive(Clone, Debug, Default)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
    pub histogram: Option<crate::histogram::Histogram>,
    pub start_timestamp: i64,
    /// Preserve names within the expression; remove them when returning results.
    pub drop_name: bool,
}
impl Point {
    pub fn stale(&self) -> bool {
        self.histogram
            .as_ref()
            .map_or(self.value, |h| h.sum)
            .to_bits()
            == STALE_BITS
    }
    pub fn json(&self) -> Json {
        if let Some(h) = &self.histogram {
            h.json(self.timestamp)
        } else {
            json!([self.timestamp as f64 / 1000.0, float_string(self.value)])
        }
    }
}

#[derive(Clone, Debug)]
pub struct Series {
    pub labels: Labels,
    pub points: Vec<Point>,
}

#[derive(Clone, Debug, Default)]
pub struct Sample {
    pub labels: Labels,
    pub value: f64,
    pub histogram: Option<crate::histogram::Histogram>,
    pub start_timestamp: i64,
    /// Preserve names within the expression; remove them when returning results.
    pub drop_name: bool,
    /// Original sample timestamp, used by timestamp(); otherwise query evaluation time.
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub enum Value {
    Scalar(f64),
    String(String),
    Vector(Vec<Sample>),
    Matrix(Vec<Series>),
}
impl Value {
    pub fn json(&self, timestamp: i64) -> Json {
        match self {
            Self::Scalar(value) => {
                json!({"resultType":"scalar", "result": Point{timestamp,value:*value, ..Default::default()}.json()})
            }
            Self::String(value) => {
                json!({"resultType":"string", "result":[timestamp as f64/1000.0,value]})
            }
            Self::Vector(samples) => {
                let result = samples
                    .iter()
                    .map(|s| {
                        let mut value = json!({"metric":s.labels});
                        if let Some(h) = &s.histogram {
                            value["histogram"] = h.json(timestamp);
                        } else {
                            value["value"] =
                                json!([timestamp as f64 / 1000.0, float_string(s.value)]);
                        }
                        value
                    })
                    .collect::<Vec<_>>();
                json!({"resultType":"vector","result":result})
            }
            Self::Matrix(series) => {
                let result = series
                    .iter()
                    .map(|s| {
                        let mut value = json!({"metric":s.labels});
                        let floats = s
                            .points
                            .iter()
                            .filter(|p| p.histogram.is_none())
                            .map(Point::json)
                            .collect::<Vec<_>>();
                        let histograms = s
                            .points
                            .iter()
                            .filter(|p| p.histogram.is_some())
                            .map(Point::json)
                            .collect::<Vec<_>>();
                        if !floats.is_empty() {
                            value["values"] = json!(floats);
                        }
                        if !histograms.is_empty() {
                            value["histograms"] = json!(histograms);
                        }
                        value
                    })
                    .collect::<Vec<_>>();
                json!({"resultType":"matrix","result":result})
            }
        }
    }
    pub fn scalar(&self) -> anyhow::Result<f64> {
        if let Self::Scalar(v) = self {
            Ok(*v)
        } else {
            anyhow::bail!("expected scalar")
        }
    }
    pub fn string(&self) -> anyhow::Result<&str> {
        if let Self::String(v) = self {
            Ok(v)
        } else {
            anyhow::bail!("expected string")
        }
    }
    pub fn vector(self) -> anyhow::Result<Vec<Sample>> {
        if let Self::Vector(v) = self {
            Ok(v)
        } else {
            anyhow::bail!("expected instant vector")
        }
    }
    pub fn matrix(self) -> anyhow::Result<Vec<Series>> {
        if let Self::Matrix(v) = self {
            Ok(v)
        } else {
            anyhow::bail!("expected range vector")
        }
    }
}

pub fn float_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".into()
    } else if value == f64::INFINITY {
        "+Inf".into()
    } else if value == f64::NEG_INFINITY {
        "-Inf".into()
    } else {
        value.to_string()
    }
}

pub fn drop_name(labels: &mut Labels) {
    labels.remove("__name__");
    labels.remove("__type__");
    labels.remove("__unit__");
}

pub fn labels_string(labels: &Labels) -> String {
    format!(
        "{{{}}}",
        labels
            .iter()
            .map(|(k, v)| format!("{k}={}", serde_json::to_string(v).unwrap()))
            .collect::<Vec<_>>()
            .join(", ")
    )
}
