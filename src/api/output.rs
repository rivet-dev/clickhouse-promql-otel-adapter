//! Bounded response encoding without cloning a complete query into a JSON tree.
use super::{ApiError, Result};
use crate::{
    histogram::Histogram,
    model::{Point, Sample, Series, Value, float_string},
};
use serde::{
    Serialize, Serializer,
    ser::{SerializeMap, SerializeSeq},
};
use serde_json::Value as Json;
use std::{
    io::{self, Write},
    sync::atomic::{AtomicBool, Ordering},
    time::Instant,
};

pub(super) struct Encoder<'a> {
    bytes: Vec<u8>,
    limit: usize,
    deadline: Instant,
    cancelled: &'a AtomicBool,
    failure: Option<bool>, // true: deadline/cancellation, false: response budget
}
impl<'a> Encoder<'a> {
    pub(super) fn new(limit: usize, deadline: Instant, cancelled: &'a AtomicBool) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
            deadline,
            cancelled,
            failure: None,
        }
    }
    pub(super) fn json(&mut self, value: &impl Serialize) -> Result<()> {
        serde_json::to_writer(&mut *self, value).map_err(|e| self.error(e))
    }
    pub(super) fn newline(&mut self) -> Result<()> {
        self.write_all(b"\n").map_err(|e| self.error(e))
    }
    fn error(&self, error: impl ToString) -> ApiError {
        match self.failure {
            Some(true) => ApiError::timeout(),
            Some(false) => {
                ApiError::exec("response byte limit exceeded; narrow the query or use limit")
            }
            None => ApiError::exec(error),
        }
    }
    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }
}
impl Write for Encoder<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.deadline || self.cancelled.load(Ordering::Relaxed) {
            self.failure = Some(true);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response encoding cancelled",
            ));
        }
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.failure = Some(false);
            return Err(io::Error::other("response byte limit exceeded"));
        }
        let needed = self.bytes.len() + bytes.len();
        if needed > self.bytes.capacity() {
            let capacity = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .max(1024)
                .min(self.limit);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
pub(super) struct Query<'a> {
    pub status: &'static str,
    pub data: Data<'a>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub infos: Vec<String>,
}
pub(super) struct Data<'a> {
    pub value: &'a Value,
    pub timestamp: i64,
    pub stats: Option<Json>,
}
struct Sequence<I>(I);
impl<I> Serialize for Sequence<I>
where
    I: Iterator + Clone,
    I::Item: Serialize,
{
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(None)?;
        for value in self.0.clone() {
            seq.serialize_element(&value)?;
        }
        seq.end()
    }
}
impl Serialize for Data<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        match self.value {
            Value::Scalar(value) => {
                map.serialize_entry("resultType", "scalar")?;
                map.serialize_entry(
                    "result",
                    &(self.timestamp as f64 / 1000.0, float_string(*value)),
                )?;
            }
            Value::String(value) => {
                map.serialize_entry("resultType", "string")?;
                map.serialize_entry("result", &(self.timestamp as f64 / 1000.0, value))?;
            }
            Value::Vector(values) => {
                map.serialize_entry("resultType", "vector")?;
                map.serialize_entry(
                    "result",
                    &Sequence(values.iter().map(|s| SampleOutput(s, self.timestamp))),
                )?;
            }
            Value::Matrix(values) => {
                map.serialize_entry("resultType", "matrix")?;
                map.serialize_entry("result", &Sequence(values.iter().map(SeriesOutput)))?;
            }
        }
        if let Some(stats) = &self.stats {
            map.serialize_entry("stats", stats)?;
        }
        map.end()
    }
}
struct SampleOutput<'a>(&'a Sample, i64);
impl Serialize for SampleOutput<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("metric", &self.0.labels)?;
        if let Some(h) = &self.0.histogram {
            map.serialize_entry("histogram", &(self.1 as f64 / 1000.0, HistogramOutput(h)))?;
        } else {
            map.serialize_entry(
                "value",
                &(self.1 as f64 / 1000.0, float_string(self.0.value)),
            )?;
        }
        map.end()
    }
}
struct SeriesOutput<'a>(&'a Series);
impl Serialize for SeriesOutput<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("metric", &self.0.labels)?;
        if self.0.points.iter().any(|p| p.histogram.is_none()) {
            map.serialize_entry(
                "values",
                &Sequence(
                    self.0
                        .points
                        .iter()
                        .filter(|p| p.histogram.is_none())
                        .map(|p| (p.timestamp as f64 / 1000.0, float_string(p.value))),
                ),
            )?;
        }
        if self.0.points.iter().any(|p| p.histogram.is_some()) {
            map.serialize_entry(
                "histograms",
                &Sequence(
                    self.0
                        .points
                        .iter()
                        .filter(|p| p.histogram.is_some())
                        .map(HistogramPoint),
                ),
            )?;
        }
        map.end()
    }
}
struct HistogramPoint<'a>(&'a Point);
impl Serialize for HistogramPoint<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        (
            self.0.timestamp as f64 / 1000.0,
            HistogramOutput(self.0.histogram.as_ref().unwrap()),
        )
            .serialize(serializer)
    }
}
struct HistogramOutput<'a>(&'a Histogram);
impl Serialize for HistogramOutput<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("count", &float_string(self.0.count))?;
        map.serialize_entry("sum", &float_string(self.0.sum))?;
        let buckets = self.0.buckets();
        if buckets.iter().any(|b| b.count != 0.0) {
            map.serialize_entry(
                "buckets",
                &Sequence(buckets.iter().filter(|b| b.count != 0.0).map(|b| {
                    (
                        b.boundaries,
                        float_string(b.lower),
                        float_string(b.upper),
                        float_string(b.count),
                    )
                })),
            )?;
        }
        map.end()
    }
}

#[cfg(test)]
#[path = "../../tests/unit/review_api_output.rs"]
mod tests;
