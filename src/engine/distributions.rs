//! Classic/native histogram functions. Algorithms follow pinned Prometheus quantile.go.
use super::Engine;
use crate::{
    histogram::Histogram,
    model::{Labels, Sample, Value},
};
use anyhow::Result;
use promql_parser::parser::Expr;
use std::collections::{BTreeMap, HashMap};

type ClassicGroups = BTreeMap<Labels, Vec<(f64, f64)>>;

/// Where one input label set of a histogram_quantile call site belongs.
#[derive(Clone)]
enum Resolved {
    /// Group index and `le` bound.
    Bucket(usize, f64),
    /// The warning to repeat for a missing or malformed `le`.
    Malformed(String),
}

/// Classic bucket grouping for one histogram_quantile call site. An input
/// label set is resolved to its group and `le` bound once; later steps reuse
/// the resolution by label identity, so the per-step path clones and
/// compares no labels. Input labels are retained so their identity stays
/// unique while cached.
#[derive(Default)]
pub(super) struct BucketSite {
    by_identity: HashMap<usize, (Labels, Resolved)>,
    /// Group label sets with `le` removed, one shared allocation per group.
    groups: Vec<Labels>,
    group_index: HashMap<Labels, usize>,
    /// Group indices in label order, rebuilt when a group is added.
    order: Vec<usize>,
    dirty: bool,
    /// Per group `(bound, count)` for the current step, reused across steps.
    scratch: Vec<Vec<(f64, f64)>>,
}

impl BucketSite {
    const MAX_CACHED_INPUTS: usize = 1 << 20;

    fn resolve(&mut self, labels: &Labels) -> Resolved {
        let mut group_labels = labels.clone();
        let le = group_labels.remove("le").unwrap_or_default();
        match le.parse::<f64>() {
            Ok(bound) => {
                let group = match self.group_index.get(&group_labels) {
                    Some(&group) => group,
                    None => {
                        self.groups.push(group_labels.clone());
                        self.group_index.insert(group_labels, self.groups.len() - 1);
                        self.scratch.push(Vec::new());
                        self.dirty = true;
                        self.groups.len() - 1
                    }
                };
                Resolved::Bucket(group, bound)
            }
            Err(_) => {
                let metric = labels.get("__name__").cloned().unwrap_or_default();
                Resolved::Malformed(format!(
                    "bucket label \"le\" is missing or has a malformed value of {le:?} for metric name {metric:?}"
                ))
            }
        }
    }

    /// Places every sample's `(bound, count)` into its group's scratch
    /// buffer, warning for malformed `le` labels as `split` does.
    fn gather(&mut self, engine: &mut Engine<'_>, vector: &[Sample], expr: &Expr) {
        for buckets in &mut self.scratch {
            buckets.clear();
        }
        for s in vector {
            let id = s.labels.identity();
            let resolved = match self.by_identity.get(&id) {
                Some((_, resolved)) => resolved.clone(),
                None => {
                    let resolved = self.resolve(&s.labels);
                    if self.by_identity.len() < Self::MAX_CACHED_INPUTS {
                        self.by_identity
                            .insert(id, (s.labels.clone(), resolved.clone()));
                    }
                    resolved
                }
            };
            match resolved {
                Resolved::Bucket(group, bound) => self.scratch[group].push((bound, s.value)),
                Resolved::Malformed(message) => engine.annotation(true, message, expr),
            }
        }
        if self.dirty {
            let groups = &self.groups;
            self.order = (0..groups.len()).collect();
            self.order
                .sort_unstable_by(|&a, &b| groups[a].cmp(&groups[b]));
            self.dirty = false;
        }
    }
}

/// Sorts a group's buckets by bound and merges duplicate bounds by adding
/// their counts.
fn merge_buckets(buckets: &mut Vec<(f64, f64)>) {
    buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut merged: Vec<(f64, f64)> = Vec::new();
    for b in buckets.drain(..) {
        if merged.last().is_some_and(|v| v.0 == b.0) {
            merged.last_mut().unwrap().1 += b.1;
        } else {
            merged.push(b);
        }
    }
    *buckets = merged;
}

fn split(
    engine: &mut Engine<'_>,
    vector: Vec<Sample>,
    expr: &Expr,
) -> (Vec<Sample>, ClassicGroups) {
    let mut native = Vec::new();
    let mut groups = BTreeMap::<Labels, Vec<(f64, f64)>>::new();
    for mut s in vector {
        if s.histogram.is_some() {
            native.push(s);
            continue;
        }
        let metric = s.labels.get("__name__").cloned().unwrap_or_default();
        let le = s.labels.remove("le").unwrap_or_default();
        match le.parse::<f64>() {
            Ok(bound) => groups.entry(s.labels).or_default().push((bound,s.value)),
            Err(_) => engine.annotation(true,format!("bucket label \"le\" is missing or has a malformed value of {le:?} for metric name {metric:?}"),expr),
        }
    }
    native.retain(|s| {
        if groups.remove(&s.labels).is_some() {
            let metric=s.labels.get("__name__").cloned().unwrap_or_default();
            engine.annotation(true,format!("vector contains a mix of classic and native histograms for metric name {metric:?}"),expr); false
        } else { true }
    });
    for buckets in groups.values_mut() {
        merge_buckets(buckets);
    }
    (native, groups)
}
fn nan_annotation(
    engine: &mut Engine<'_>,
    h: &Histogram,
    metric: &str,
    expr: &Expr,
    result: f64,
    fraction: bool,
) {
    if h.sum.is_nan() && h.buckets().iter().map(|b| b.count).sum::<f64>() < h.count {
        let message = if fraction {
            "input to histogram_fraction has NaN observations, which are excluded from all fractions"
        } else if result.is_nan() {
            "input to histogram_quantile has NaN observations, result is NaN"
        } else {
            "input to histogram_quantile has NaN observations, result is skewed higher"
        };
        engine.annotation(false, format!("{message} for metric name {metric:?}"), expr);
    }
}
pub(super) fn quantile(
    engine: &mut Engine<'_>,
    q: f64,
    vector: Vec<Sample>,
    t: i64,
    expr: &Expr,
) -> Result<Value> {
    if vector.iter().all(|s| s.histogram.is_none()) {
        return Ok(Value::Vector(classic_quantile(engine, q, &vector, t, expr)));
    }
    let (native, groups) = split(engine, vector, expr);
    let mut result = Vec::new();
    for mut s in native {
        let h = s.histogram.take().unwrap();
        s.value = h.quantile(q);
        s.drop_name = true;
        s.timestamp = t;
        nan_annotation(
            engine,
            &h,
            s.labels.get("__name__").map(String::as_str).unwrap_or(""),
            expr,
            s.value,
            false,
        );
        result.push(s);
    }
    for (labels, mut buckets) in groups {
        result.push(classic_group_quantile(
            engine,
            q,
            labels,
            &mut buckets,
            t,
            expr,
        ));
    }
    Ok(Value::Vector(result))
}
/// Classic buckets only: groups are resolved by label identity through the
/// call site's `BucketSite`, then reduced per group in label order exactly
/// as the `split` path does.
fn classic_quantile(
    engine: &mut Engine<'_>,
    q: f64,
    vector: &[Sample],
    t: i64,
    expr: &Expr,
) -> Vec<Sample> {
    // The site is taken out of the engine while annotations are produced.
    let key = std::ptr::from_ref(expr) as usize;
    let mut site = engine.bucket_sites.remove(&key).unwrap_or_default();
    site.gather(engine, vector, expr);
    let mut result = Vec::new();
    for &group in &site.order {
        let buckets = &mut site.scratch[group];
        if buckets.is_empty() {
            continue;
        }
        merge_buckets(buckets);
        result.push(classic_group_quantile(
            engine,
            q,
            site.groups[group].clone(),
            buckets,
            t,
            expr,
        ));
    }
    engine.bucket_sites.insert(key, site);
    result
}
/// Quantile of one classic histogram from its merged, bound-sorted buckets,
/// after the monotonicity fix. Follows pinned Prometheus quantile.go.
fn classic_group_quantile(
    engine: &mut Engine<'_>,
    q: f64,
    labels: Labels,
    buckets: &mut [(f64, f64)],
    t: i64,
    expr: &Expr,
) -> Sample {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut diff = 0.0f64;
    if buckets.len() >= 2 && buckets.last().unwrap().0 == f64::INFINITY {
        for i in 1..buckets.len() {
            let delta = buckets[i - 1].1 - buckets[i].1;
            if delta.abs() <= 1e-12 * (buckets[i - 1].1 + buckets[i].1).abs() {
                buckets[i].1 = buckets[i - 1].1;
            } else if delta > 0.0 {
                min = min.min(buckets[i].0);
                max = max.max(buckets[i].0);
                diff = diff.max(delta);
                buckets[i].1 = buckets[i - 1].1;
            }
        }
    }
    if min != f64::INFINITY {
        engine.monotonic_annotation(
            labels.get("__name__").map(String::as_str).unwrap_or(""),
            t,
            min,
            max,
            diff,
            expr,
        );
    }
    let value = if q.is_nan() {
        f64::NAN
    } else if q < 0.0 {
        f64::NEG_INFINITY
    } else if q > 1.0 {
        f64::INFINITY
    } else if buckets.len() < 2
        || buckets.last().unwrap().0 != f64::INFINITY
        || buckets.last().unwrap().1 == 0.0
    {
        f64::NAN
    } else {
        let rank = q * buckets.last().unwrap().1;
        let i = buckets
            .partition_point(|b| b.1 < rank)
            .min(buckets.len() - 1);
        if i == buckets.len() - 1 {
            buckets[i - 1].0
        } else if i == 0 && buckets[0].0 <= 0.0 {
            buckets[0].0
        } else {
            let (low, previous) = if i == 0 { (0.0, 0.0) } else { buckets[i - 1] };
            low + (buckets[i].0 - low) * (rank - previous) / (buckets[i].1 - previous)
        }
    };
    Sample {
        labels,
        value,
        timestamp: t,
        drop_name: true,
        ..Default::default()
    }
}
pub(super) fn fraction(
    engine: &mut Engine<'_>,
    lower: f64,
    upper: f64,
    vector: Vec<Sample>,
    t: i64,
    expr: &Expr,
) -> Result<Value> {
    let (native, groups) = split(engine, vector, expr);
    let mut result = Vec::new();
    for mut s in native {
        let h = s.histogram.take().unwrap();
        s.value = h.fraction(lower, upper);
        s.drop_name = true;
        s.timestamp = t;
        nan_annotation(
            engine,
            &h,
            s.labels.get("__name__").map(String::as_str).unwrap_or(""),
            expr,
            s.value,
            true,
        );
        result.push(s);
    }
    for (labels, buckets) in groups {
        let count = buckets.last().map(|b| b.1).unwrap_or(0.0);
        let cdf = |x: f64| {
            let mut previous = if buckets[0].0 <= 0.0 {
                (f64::NEG_INFINITY, 0.0)
            } else {
                (0.0, 0.0)
            };
            for &(bound, count) in &buckets {
                if previous.0 >= x {
                    return previous.1;
                }
                if previous.0 < x && x < bound {
                    if previous.0 == f64::NEG_INFINITY {
                        return count;
                    }
                    return previous.1
                        + (count - previous.1) * (x - previous.0) / (bound - previous.0);
                }
                previous = (bound, count);
            }
            count
        };
        let value = if count == 0.0
            || lower.is_nan()
            || upper.is_nan()
            || buckets.last().unwrap().0 != f64::INFINITY
        {
            f64::NAN
        } else if lower >= upper {
            0.0
        } else {
            (cdf(upper).min(count) - cdf(lower).min(count)) / count
        };
        result.push(Sample {
            labels,
            value,
            timestamp: t,
            drop_name: true,
            ..Default::default()
        });
    }
    Ok(Value::Vector(result))
}
