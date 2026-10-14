use crate::model::float_string;
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Prometheus exponential bucket index i has upper bound 2^(i * 2^-schema).
/// Custom-bucket schema uses the same sparse indexes into `custom` boundaries.
#[derive(Clone, Debug, Default)]
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
        let bound = |i: i32| {
            let exponent = i as f64 * 2.0f64.powi(-self.schema);
            if exponent == 1024.0 {
                f64::MAX
            } else {
                2.0f64.powf(exponent)
            }
        };
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
        if self.zero_count > 0.0 {
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
    pub fn aggregate(values: &[&Self], average: bool) -> Result<Self> {
        let mut result = values[0].clone();
        for h in &values[1..] {
            result.combine(h, 1.0)?;
        }
        let mut inputs = Vec::with_capacity(values.len());
        for h in values {
            let mut h = (*h).clone();
            h.reconcile_zero(result.zero_threshold);
            h.reduce(result.schema)?;
            if result.schema == -53 {
                h.coarsen_custom(&result.custom);
            }
            inputs.push(h);
        }
        let sum = |v: Vec<f64>| {
            if average {
                crate::numeric::mean(&v)
            } else {
                crate::numeric::sum(&v)
            }
        };
        result.count = sum(inputs.iter().map(|h| h.count).collect());
        result.sum = sum(inputs.iter().map(|h| h.sum).collect());
        result.zero_count = sum(inputs.iter().map(|h| h.zero_count).collect());
        for (&k, v) in &mut result.positive {
            *v = sum(inputs
                .iter()
                .map(|h| h.positive.get(&k).copied().unwrap_or(0.0))
                .collect());
        }
        for (&k, v) in &mut result.negative {
            *v = sum(inputs
                .iter()
                .map(|h| h.negative.get(&k).copied().unwrap_or(0.0))
                .collect());
        }
        Ok(result)
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
        let mut other = other.clone();
        if self.schema == -53 && self.custom != other.custom {
            let common = self
                .custom
                .iter()
                .copied()
                .filter(|v| other.custom.contains(v))
                .collect::<Vec<_>>();
            self.coarsen_custom(&common);
            other.coarsen_custom(&common);
        }

        let schema = self.schema.min(other.schema);
        let mut threshold = self.zero_threshold.max(other.zero_threshold);
        loop {
            let next = self
                .expanded_zero_threshold(threshold)
                .max(other.expanded_zero_threshold(threshold));
            if next == threshold {
                break;
            }
            threshold = next;
        }
        self.reconcile_zero(threshold);
        other.reconcile_zero(threshold);
        self.reduce(schema)?;
        other.reduce(schema)?;
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
    fn expanded_zero_threshold(&self, mut threshold: f64) -> f64 {
        if self.schema == -53 || threshold <= self.zero_threshold {
            return threshold;
        }
        let bound = |i: i32| {
            let exponent = i as f64 * 2.0f64.powi(-self.schema);
            if exponent == 1024.0 {
                f64::MAX
            } else {
                2.0f64.powf(exponent)
            }
        };
        loop {
            let previous = threshold;
            for (&index, &count) in self.positive.iter().chain(&self.negative) {
                if count != 0.0
                    && bound(index.saturating_sub(1)) < threshold
                    && bound(index) > threshold
                {
                    threshold = bound(index);
                }
            }
            if previous == threshold {
                return threshold;
            }
        }
    }
    fn reconcile_zero(&mut self, threshold: f64) {
        if threshold <= self.zero_threshold || self.schema == -53 {
            return;
        }
        let bound = |i: i32| {
            let exponent = i as f64 * 2.0f64.powi(-self.schema);
            if exponent == 1024.0 {
                f64::MAX
            } else {
                2.0f64.powf(exponent)
            }
        };
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
    fn coarsen_custom(&mut self, bounds: &[f64]) {
        let mut buckets = BTreeMap::new();
        for (&i, &value) in &self.positive {
            let upper = self
                .custom
                .get(i as usize)
                .copied()
                .unwrap_or(f64::INFINITY);
            let index = bounds.partition_point(|b| *b < upper) as i32;
            *buckets.entry(index).or_insert(0.0) += value;
        }
        self.positive = buckets;
        self.custom = bounds.to_vec();
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
            || (self.schema == -53) != (previous.schema == -53)
        {
            return true;
        }
        let mut previous = previous.clone();
        if self.schema == -53 && self.custom != previous.custom {
            let common = self
                .custom
                .iter()
                .copied()
                .filter(|v| previous.custom.contains(v))
                .collect::<Vec<_>>();
            previous.coarsen_custom(&common);
            let mut current = self.clone();
            current.coarsen_custom(&common);
            return current.detects_reset(&previous);
        }
        if previous.buckets().iter().any(|b| {
            b.count != 0.0
                && ((b.lower < self.zero_threshold
                    && b.upper > self.zero_threshold
                    && b.lower > 0.0)
                    || (b.upper > -self.zero_threshold
                        && b.lower < -self.zero_threshold
                        && b.upper < 0.0))
        }) {
            return true;
        }
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
        let buckets = self.buckets();
        let count = if self.sum.is_nan() {
            buckets.iter().map(|b| b.count).sum::<f64>()
        } else {
            self.count
        };
        let cdf = |x: f64| {
            let mut rank = 0.0;
            for b in &buckets {
                let (mut lo, mut hi) = (b.lower, b.upper);
                let zero = lo <= 0.0 && hi >= 0.0;
                if zero {
                    if self.negative.is_empty() && !self.positive.is_empty() {
                        lo = 0.0;
                    }
                    if self.positive.is_empty() && !self.negative.is_empty() {
                        hi = 0.0;
                    }
                }
                if lo >= x {
                    return rank;
                }
                if lo < x && x < hi {
                    if self.schema == -53 || zero {
                        if lo == f64::NEG_INFINITY {
                            return b.count;
                        }
                        return rank + b.count * (x - lo) / (hi - lo);
                    }
                    let fraction = if lo > 0.0 {
                        (x.log2() - lo.log2()) / (hi.log2() - lo.log2())
                    } else {
                        1.0 - ((-x).log2() - (-hi).log2()) / ((-lo).log2() - (-hi).log2())
                    };
                    return rank + b.count * fraction;
                }
                rank += b.count;
            }
            count
        };
        (cdf(upper).min(count) - cdf(lower).min(count)) / self.count
    }
    pub fn quantile(&self, q: f64) -> f64 {
        if q < 0.0 {
            return f64::NEG_INFINITY;
        }
        if q > 1.0 {
            return f64::INFINITY;
        }
        if q.is_nan() || self.count == 0.0 {
            return f64::NAN;
        }
        let reverse = !self.sum.is_nan() && q >= 0.5;
        let rank = if reverse {
            (1.0 - q) * self.count
        } else {
            q * self.count
        };
        let mut count = 0.0;
        let mut buckets = self.buckets();
        if reverse {
            buckets.reverse();
        }
        for b in &buckets {
            if b.count == 0.0 {
                continue;
            }
            count += b.count;
            if count < rank {
                continue;
            }
            let count = count.min(self.count);
            let fraction = if reverse {
                (count - rank) / b.count
            } else {
                (rank - (count - b.count)) / b.count
            };
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
                if self.negative.is_empty() && !self.positive.is_empty() {
                    lower = 0.0
                }
                if self.positive.is_empty() && !self.negative.is_empty() {
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

// Adapted from Prometheus model/histogram/float_histogram.go (Apache-2.0).
// Approximate the sum only when observations are actually trimmed.
impl Histogram {
    pub fn trim(&mut self, threshold: f64, upper_trim: bool) {
        let buckets = self.buckets();
        let mut replacements = Vec::new();
        let mut sum = 0.0;
        let mut count = 0.0;
        let mut changed = false;
        for bucket in buckets {
            if bucket.count == 0.0 {
                replacements.push(0.0);
                continue;
            }
            let zero = self.schema != -53 && bucket.lower <= 0.0 && bucket.upper >= 0.0;
            let mut bucket = bucket;
            if zero {
                if self.negative.values().all(|v| *v == 0.0)
                    && self.positive.values().any(|v| *v != 0.0)
                {
                    bucket.lower = 0.0
                }
                if self.positive.values().all(|v| *v == 0.0)
                    && self.negative.values().any(|v| *v != 0.0)
                {
                    bucket.upper = 0.0
                }
            }
            let (keep, mid) =
                trim_bucket(&bucket, threshold, upper_trim, self.schema == -53 || zero);
            if keep != bucket.count {
                changed = true
            }
            count += keep;
            sum += mid * keep;
            replacements.push(keep);
        }
        if !changed {
            return;
        }
        let mut replacement = replacements.into_iter();
        if self.schema != -53 {
            for value in self.negative.values_mut().rev() {
                *value = replacement.next().unwrap();
            }
            if self.zero_count > 0.0 {
                self.zero_count = replacement.next().unwrap();
            }
        }
        for value in self.positive.values_mut() {
            *value = replacement.next().unwrap();
        }
        self.positive.retain(|_, v| *v != 0.0);
        self.negative.retain(|_, v| *v != 0.0);
        self.count = count;
        self.sum = sum;
    }
}
fn midpoint(lower: f64, upper: f64, linear: bool) -> f64 {
    if lower.is_infinite() {
        if upper.is_infinite() {
            return 0.0;
        }
        return if upper > 0.0 { upper / 2.0 } else { upper };
    }
    if upper.is_infinite() {
        return lower;
    }
    if linear {
        (lower + upper) / 2.0
    } else {
        (if upper > 0.0 { 1.0 } else { -1.0 }) * (lower * upper).abs().sqrt()
    }
}
fn trim_bucket(b: &Bucket, rhs: f64, upper: bool, linear: bool) -> (f64, f64) {
    let all = if upper {
        b.upper <= rhs
    } else {
        b.lower >= rhs
    };
    let partial = if upper { b.lower < rhs } else { b.upper > rhs };
    if all {
        return (b.count, midpoint(b.lower, b.upper, linear));
    }
    if !partial {
        return (0.0, 0.0);
    }
    if b.lower == f64::NEG_INFINITY {
        if upper {
            if rhs > 0.0 && b.upper > 0.0 && b.upper.is_finite() {
                return (b.count * rhs / b.upper, rhs / 2.0);
            }
            if b.upper <= 0.0 {
                return (b.count, rhs);
            }
            return (0.0, 0.0);
        }
        if rhs >= 0.0 && b.upper > rhs && b.upper.is_finite() {
            return (b.count * (1.0 - rhs / b.upper), (rhs + b.upper) / 2.0);
        }
        return (0.0, 0.0);
    }
    if b.upper == f64::INFINITY {
        return if upper { (0.0, 0.0) } else { (b.count, rhs) };
    }
    let fraction = if linear {
        (rhs - b.lower) / (b.upper - b.lower)
    } else if b.lower >= 0.0 {
        (rhs.log2() - b.lower.log2()) / (b.upper.log2() - b.lower.log2())
    } else {
        1.0 - ((-rhs).log2() - (-b.upper).log2()) / ((-b.lower).log2() - (-b.upper).log2())
    };
    if upper {
        (b.count * fraction, midpoint(b.lower, rhs, linear))
    } else {
        (b.count * (1.0 - fraction), midpoint(rhs, b.upper, linear))
    }
}

impl PartialEq for Histogram {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema
            && self.count.to_bits() == other.count.to_bits()
            && self.sum.to_bits() == other.sum.to_bits()
            && self.zero_threshold == other.zero_threshold
            && self.zero_count.to_bits() == other.zero_count.to_bits()
            && self.custom == other.custom
            && self.positive.len() == other.positive.len()
            && self.negative.len() == other.negative.len()
            && self.positive.iter().all(|(k, v)| {
                other
                    .positive
                    .get(k)
                    .is_some_and(|b| b.to_bits() == v.to_bits())
            })
            && self.negative.iter().all(|(k, v)| {
                other
                    .negative
                    .get(k)
                    .is_some_and(|b| b.to_bits() == v.to_bits())
            })
    }
}
impl std::fmt::Display for Histogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{count:{}, sum:{}",
            float_string(self.count),
            float_string(self.sum)
        )?;
        for b in self.buckets().into_iter().filter(|b| b.count != 0.0) {
            write!(
                f,
                ", {}{},{}{}:{}",
                if b.boundaries == 1 || b.boundaries == 3 {
                    "["
                } else {
                    "("
                },
                float_string(b.lower),
                float_string(b.upper),
                if b.boundaries == 0 || b.boundaries == 3 {
                    "]"
                } else {
                    ")"
                },
                float_string(b.count)
            )?;
        }
        write!(f, "}}")
    }
}
