use crate::model::float_string;
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Prometheus exponential bucket index i has upper bound 2^(i * 2^-schema).
/// Custom-bucket schema uses the same sparse indexes into `custom` boundaries.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Histogram {
    pub schema: i32,
    pub count: f64,
    pub sum: f64,
    pub zero_threshold: f64,
    pub zero_count: f64,
    pub positive: BTreeMap<i32, f64>,
    pub negative: BTreeMap<i32, f64>,
    pub custom: Vec<f64>,
    /// 0 unknown, 1 reset, 2 not reset, 3 gauge (upstream CounterResetHint).
    pub reset_hint: u8,
}
#[derive(Clone, Debug)]
pub struct Bucket {
    pub lower: f64,
    pub upper: f64,
    pub count: f64,
    pub boundaries: u8,
}
impl Histogram {
    pub fn buckets(&self) -> Vec<Bucket> {
        let bound = |i: i32| 2.0f64.powf(i as f64 * 2.0f64.powi(-self.schema));
        let mut buckets = Vec::new();
        if self.schema == -53 {
            for (&i, &count) in &self.positive {
                let lower = if i <= 0 {
                    f64::NEG_INFINITY
                } else {
                    self.custom
                        .get(i as usize - 1)
                        .copied()
                        .unwrap_or(f64::INFINITY)
                };
                let upper = self
                    .custom
                    .get(i as usize)
                    .copied()
                    .unwrap_or(f64::INFINITY);
                buckets.push(Bucket {
                    lower,
                    upper,
                    count,
                    boundaries: if lower == f64::NEG_INFINITY { 3 } else { 0 },
                })
            }
            return buckets;
        }
        for (&i, &count) in self.negative.iter().rev() {
            buckets.push(Bucket {
                lower: -bound(i),
                upper: -bound(i - 1).max(self.zero_threshold),
                count,
                boundaries: 1,
            });
        }
        if self.zero_count != 0.0 {
            buckets.push(Bucket {
                lower: -self.zero_threshold,
                upper: self.zero_threshold,
                count: self.zero_count,
                boundaries: 3,
            })
        }
        for (&i, &count) in &self.positive {
            buckets.push(Bucket {
                lower: bound(i - 1).max(self.zero_threshold),
                upper: bound(i),
                count,
                boundaries: 0,
            });
        }
        buckets
    }
    pub fn json(&self, t: i64) -> Value {
        let buckets = self
            .buckets()
            .into_iter()
            .filter(|b| b.count != 0.0)
            .map(|b| {
                json!([
                    b.boundaries,
                    float_string(b.lower),
                    float_string(b.upper),
                    float_string(b.count)
                ])
            })
            .collect::<Vec<_>>();
        let mut value = json!({"count":float_string(self.count),"sum":float_string(self.sum)});
        if !buckets.is_empty() {
            value["buckets"] = json!(buckets)
        }
        json!([t as f64 / 1000.0, value])
    }
    pub fn scale(&mut self, factor: f64) {
        self.count *= factor;
        self.sum *= factor;
        self.zero_count *= factor;
        for v in self.positive.values_mut().chain(self.negative.values_mut()) {
            *v *= factor
        }
        if factor < 0.0 {
            self.reset_hint = 3
        }
    }
    pub fn reduce(&mut self, schema: i32) -> Result<()> {
        if self.schema == schema {
            return Ok(());
        }
        if self.schema == -53 || schema == -53 {
            bail!("incompatible histogram schemas")
        }
        if schema > self.schema {
            bail!("cannot increase histogram resolution")
        }
        let shift = (self.schema - schema) as u32;
        if shift > 30 {
            bail!("invalid histogram schema")
        }
        for map in [&mut self.positive, &mut self.negative] {
            let mut reduced = BTreeMap::new();
            for (&i, &v) in map.iter() {
                *reduced
                    .entry((i - 1).div_euclid(1 << shift) + 1)
                    .or_insert(0.0) += v;
            }
            *map = reduced;
        }
        self.schema = schema;
        Ok(())
    }
    pub fn combine(&mut self, other: &Self, sign: f64) -> Result<()> {
        if (self.schema == -53) != (other.schema == -53) {
            bail!("incompatible histogram schemas")
        }
        if self.schema == -53 && self.custom != other.custom {
            bail!("incompatible custom buckets")
        }
        let mut other = other.clone();
        let schema = self.schema.min(other.schema);
        self.reduce(schema)?;
        other.reduce(schema)?;
        let threshold = self.zero_threshold.max(other.zero_threshold);
        self.reconcile_zero(threshold);
        other.reconcile_zero(threshold);
        self.count += sign * other.count;
        self.sum += sign * other.sum;
        self.zero_count += sign * other.zero_count;
        for (k, v) in other.positive {
            *self.positive.entry(k).or_default() += sign * v;
        }
        for (k, v) in other.negative {
            *self.negative.entry(k).or_default() += sign * v;
        }
        if sign < 0.0 || self.reset_hint == 3 || other.reset_hint == 3 {
            self.reset_hint = 3
        } else if self.reset_hint != other.reset_hint {
            self.reset_hint = 0
        }
        Ok(())
    }
    fn reconcile_zero(&mut self, threshold: f64) {
        if threshold <= self.zero_threshold || self.schema == -53 {
            return;
        }
        let bound = |i: i32| 2.0f64.powf(i as f64 * 2.0f64.powi(-self.schema));
        for map in [&mut self.positive, &mut self.negative] {
            let keys = map
                .keys()
                .copied()
                .filter(|i| bound(*i) <= threshold)
                .collect::<Vec<_>>();
            for i in keys {
                self.zero_count += map.remove(&i).unwrap();
            }
        }
        self.zero_threshold = threshold;
    }
    pub fn detects_reset(&self, previous: &Self) -> bool {
        if self.reset_hint == 1 {
            return true;
        }
        if self.reset_hint == 2 {
            return false;
        }
        if self.count < previous.count
            || self.schema > previous.schema
            || self.zero_threshold < previous.zero_threshold
            || self.custom != previous.custom
        {
            return true;
        }
        let mut previous = previous.clone();
        if previous.reduce(self.schema).is_err() {
            return true;
        }
        previous.reconcile_zero(self.zero_threshold);
        self.zero_count < previous.zero_count
            || previous
                .positive
                .iter()
                .any(|(k, v)| self.positive.get(k).copied().unwrap_or(0.0) < *v)
            || previous
                .negative
                .iter()
                .any(|(k, v)| self.negative.get(k).copied().unwrap_or(0.0) < *v)
    }
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        if self.count == 0.0 || lower.is_nan() || upper.is_nan() {
            return f64::NAN;
        }
        if lower >= upper {
            return 0.0;
        }
        let cdf = |x: f64| {
            let mut count = 0.0;
            for b in self.buckets() {
                if x >= b.upper {
                    count += b.count;
                    continue;
                }
                if x <= b.lower {
                    continue;
                }
                let fraction = if self.schema == -53 || b.lower <= 0.0 && b.upper >= 0.0 {
                    let lo = if b.lower == f64::NEG_INFINITY {
                        0.0f64.min(b.upper)
                    } else {
                        b.lower
                    };
                    let hi = if b.upper == f64::INFINITY {
                        0.0f64.max(b.lower)
                    } else {
                        b.upper
                    };
                    (x - lo) / (hi - lo)
                } else if b.lower >= 0.0 {
                    (x.log2() - b.lower.log2()) / (b.upper.log2() - b.lower.log2())
                } else {
                    1.0 - ((-x).log2() - (-b.upper).log2())
                        / ((-b.lower).log2() - (-b.upper).log2())
                };
                count += b.count * fraction.clamp(0.0, 1.0);
            }
            count
        };
        (cdf(upper) - cdf(lower)) / self.count
    }
    pub fn quantile(&self, q: f64) -> f64 {
        if q.is_nan() || self.count == 0.0 {
            return f64::NAN;
        }
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        let rank = q * self.count;
        let mut count = 0.0;
        let buckets = self.buckets();
        for b in &buckets {
            if b.count == 0.0 {
                continue;
            }
            count += b.count;
            if count < rank {
                continue;
            }
            let fraction = (rank - (count - b.count)) / b.count;
            let mut lower = b.lower;
            let mut upper = b.upper;
            if self.schema == -53 {
                if lower == f64::NEG_INFINITY {
                    if upper <= 0.0 {
                        return upper;
                    }
                    lower = 0.0
                }
                if upper == f64::INFINITY {
                    return lower;
                }
            }
            if lower < 0.0 && upper > 0.0 {
                if self.negative.is_empty() {
                    lower = 0.0
                }
                if self.positive.is_empty() {
                    upper = 0.0
                }
            }
            if self.schema == -53 || lower <= 0.0 && upper >= 0.0 {
                return lower + (upper - lower) * fraction;
            }
            if lower >= 0.0 {
                return 2.0f64.powf(lower.log2() + (upper.log2() - lower.log2()) * fraction);
            }
            return -2.0f64
                .powf((-upper).log2() + ((-lower).log2() - (-upper).log2()) * (1.0 - fraction));
        }
        f64::NAN
    }
}
