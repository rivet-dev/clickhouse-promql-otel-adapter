use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

pub type Labels = BTreeMap<String, String>;
pub const STALE_BITS: u64 = 0x7ff0000000000002;

#[derive(Clone, Debug)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
}
impl Point {
    pub fn stale(&self) -> bool {
        self.value.to_bits() == STALE_BITS
    }
    pub fn json(&self) -> Json {
        json!([self.timestamp as f64 / 1000.0, float_string(self.value)])
    }
}

#[derive(Clone, Debug)]
pub struct Series {
    pub labels: Labels,
    pub points: Vec<Point>,
}

#[derive(Clone, Debug)]
pub struct Sample {
    pub labels: Labels,
    pub value: f64,
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
                json!({"resultType":"scalar", "result": Point{timestamp,value:*value}.json()})
            }
            Self::String(value) => {
                json!({"resultType":"string", "result":[timestamp as f64/1000.0,value]})
            }
            Self::Vector(samples) => {
                json!({"resultType":"vector", "result":samples.iter().map(|s| json!({"metric":s.labels,"value":Point{timestamp,value:s.value}.json()})).collect::<Vec<_>>()})
            }
            Self::Matrix(series) => {
                json!({"resultType":"matrix", "result":series.iter().map(|s| json!({"metric":s.labels,"values":s.points.iter().map(Point::json).collect::<Vec<_>>()})).collect::<Vec<_>>()})
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
}
