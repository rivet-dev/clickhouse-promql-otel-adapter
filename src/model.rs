use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

/// A series label set. Clones share one allocation, so passing labels from
/// series to samples at every evaluation step costs a reference count instead
/// of copying every key and value. Mutation copies only a shared set. The
/// content hash is computed once per set and reset by any mutation.
#[derive(Clone, Default)]
pub struct Labels(std::sync::Arc<LabelSet>);

#[derive(Default)]
pub struct LabelSet {
    map: BTreeMap<String, String>,
    hash: std::sync::OnceLock<u64>,
}

impl Clone for LabelSet {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            hash: self.hash.clone(),
        }
    }
}

impl Labels {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes ownership of the map, copying it only if it is shared.
    pub fn into_map(self) -> BTreeMap<String, String> {
        std::sync::Arc::unwrap_or_clone(self.0).map
    }

    /// Identifies this shared allocation. Equal identities imply equal labels;
    /// the converse does not hold. Callers that key a cache by identity must
    /// keep a clone alive so the allocation cannot be reused.
    pub fn identity(&self) -> usize {
        std::sync::Arc::as_ptr(&self.0) as usize
    }

    fn content_hash(&self) -> u64 {
        *self.0.hash.get_or_init(|| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::hash::DefaultHasher::new();
            self.0.map.hash(&mut hasher);
            hasher.finish()
        })
    }
}

impl std::ops::Deref for Labels {
    type Target = BTreeMap<String, String>;

    fn deref(&self) -> &Self::Target {
        &self.0.map
    }
}

impl std::ops::DerefMut for Labels {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let set = std::sync::Arc::make_mut(&mut self.0);
        set.hash = std::sync::OnceLock::new();
        &mut set.map
    }
}

impl PartialEq for Labels {
    fn eq(&self, other: &Self) -> bool {
        std::sync::Arc::ptr_eq(&self.0, &other.0)
            || (self.content_hash() == other.content_hash() && self.0.map == other.0.map)
    }
}

impl Eq for Labels {}

impl PartialOrd for Labels {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Labels {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if std::sync::Arc::ptr_eq(&self.0, &other.0) {
            return std::cmp::Ordering::Equal;
        }
        self.0.map.cmp(&other.0.map)
    }
}

impl std::hash::Hash for Labels {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.content_hash())
    }
}

impl serde::Serialize for Labels {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.map.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Labels {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::deserialize(deserializer).map(Self::from)
    }
}

impl std::fmt::Debug for Labels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.map.fmt(f)
    }
}

impl From<BTreeMap<String, String>> for Labels {
    fn from(map: BTreeMap<String, String>) -> Self {
        Self(std::sync::Arc::new(LabelSet {
            map,
            hash: std::sync::OnceLock::new(),
        }))
    }
}

impl<const N: usize> From<[(String, String); N]> for Labels {
    fn from(pairs: [(String, String); N]) -> Self {
        Self::from(BTreeMap::from(pairs))
    }
}

impl FromIterator<(String, String)> for Labels {
    fn from_iter<I: IntoIterator<Item = (String, String)>>(iter: I) -> Self {
        Self::from(BTreeMap::from_iter(iter))
    }
}

impl Extend<(String, String)> for Labels {
    fn extend<I: IntoIterator<Item = (String, String)>>(&mut self, iter: I) {
        std::ops::DerefMut::deref_mut(self).extend(iter)
    }
}

impl IntoIterator for Labels {
    type Item = (String, String);
    type IntoIter = std::collections::btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.into_map().into_iter()
    }
}

impl<'a> IntoIterator for &'a Labels {
    type Item = (&'a String, &'a String);
    type IntoIter = std::collections::btree_map::Iter<'a, String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.map.iter()
    }
}
pub const STALE_BITS: u64 = 0x7ff0000000000002;

#[derive(Clone, Debug, Default)]
pub struct Point {
    pub timestamp: i64,
    pub value: f64,
    /// Keep float points compact; native histogram payloads allocate only when present.
    pub histogram: Option<Box<crate::histogram::Histogram>>,
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
