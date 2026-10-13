use super::{
    Engine,
    aggregate::{quantile, valid_label, variance},
};
use crate::model::{Labels, Sample, Series, Value, drop_name};
use anyhow::{Result, bail};
use chrono::{Datelike, Timelike};
use promql_parser::{
    label::MatchOp,
    parser::{Call, Expr},
};
use std::collections::BTreeMap;

impl Engine<'_> {
    pub(super) fn call(&mut self, c: &Call, t: i64) -> Result<Value> {
        let name = c.func.name;
        if name == "time" {
            return Ok(Value::Scalar(t as f64 / 1000.0));
        }
        if name == "pi" {
            return Ok(Value::Scalar(std::f64::consts::PI));
        }
        let mut args = c
            .args
            .args
            .iter()
            .map(|a| self.eval(a, t))
            .collect::<Result<Vec<_>>>()?;
        match name {
            "vector" => {
                return Ok(Value::Vector(vec![Sample {
                    labels: Labels::new(),
                    value: args[0].scalar()?,
                    timestamp: t,
                }]));
            }
            "scalar" => {
                let v = args.remove(0).vector()?;
                return Ok(Value::Scalar(if v.len() == 1 {
                    v[0].value
                } else {
                    f64::NAN
                }));
            }
            "absent" | "absent_over_time" => {
                let absent = match &args[0] {
                    Value::Vector(v) => v.is_empty(),
                    Value::Matrix(v) => v.is_empty(),
                    _ => false,
                };
                return Ok(Value::Vector(if absent {
                    vec![Sample {
                        labels: absent_labels(&c.args.args[0]),
                        value: 1.0,
                        timestamp: t,
                    }]
                } else {
                    vec![]
                }));
            }
            "label_replace" | "label_join" => return label_function(name, args, t),
            "histogram_quantile" => return histogram_quantile(args, t, &mut self.infos),
            _ => {}
        }
        if args.iter().any(|a| matches!(a, Value::Matrix(_))) {
            return self.range_function(c, args, t);
        }
        let vi = args.iter().position(|a| matches!(a, Value::Vector(_)));
        let calendar = matches!(
            name,
            "day_of_month"
                | "day_of_week"
                | "day_of_year"
                | "days_in_month"
                | "hour"
                | "minute"
                | "month"
                | "year"
        );
        let vector = if let Some(i) = vi {
            args[i].clone().vector()?
        } else if calendar {
            vec![Sample {
                labels: Labels::new(),
                value: t as f64 / 1000.0,
                timestamp: t,
            }]
        } else {
            bail!("function {name} is not implemented")
        };
        if matches!(name, "sort" | "sort_desc") {
            let mut result = vector;
            result.sort_by(|a, b| match (a.value.is_nan(), b.value.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => {
                    if name == "sort" {
                        a.value.total_cmp(&b.value)
                    } else {
                        b.value.total_cmp(&a.value)
                    }
                }
            });
            return Ok(Value::Vector(result));
        }
        let scalar = |i: usize, default: f64| -> Result<f64> {
            args.get(i).map_or(Ok(default), Value::scalar)
        };
        let mut result = Vec::new();
        for mut s in vector {
            let x = s.value;
            s.value = match name {
                "abs" => x.abs(),
                "ceil" => x.ceil(),
                "floor" => x.floor(),
                "sqrt" => x.sqrt(),
                "exp" => x.exp(),
                "ln" => x.ln(),
                "log2" => x.log2(),
                "log10" => x.log10(),
                "sgn" => {
                    if x.is_nan() {
                        x
                    } else if x == 0.0 {
                        0.0
                    } else {
                        x.signum()
                    }
                }
                "sin" => x.sin(),
                "cos" => x.cos(),
                "tan" => x.tan(),
                "asin" => x.asin(),
                "acos" => x.acos(),
                "atan" => x.atan(),
                "sinh" => x.sinh(),
                "cosh" => x.cosh(),
                "tanh" => x.tanh(),
                "asinh" => x.asinh(),
                "acosh" => x.acosh(),
                "atanh" => x.atanh(),
                "deg" => x.to_degrees(),
                "rad" => x.to_radians(),
                "round" => {
                    let nearest = scalar(1, 1.0)?;
                    let inverse = 1.0 / nearest;
                    (x * inverse + 0.5).floor() / inverse
                }
                "clamp" => {
                    let lo = scalar(1, 0.0)?;
                    let hi = scalar(2, 0.0)?;
                    if lo > hi {
                        continue;
                    }
                    if x.is_nan() || lo.is_nan() || hi.is_nan() {
                        f64::NAN
                    } else {
                        x.max(lo).min(hi)
                    }
                }
                "clamp_min" => {
                    let lo = scalar(1, 0.0)?;
                    if lo.is_nan() || x.is_nan() {
                        f64::NAN
                    } else {
                        x.max(lo)
                    }
                }
                "clamp_max" => {
                    let hi = scalar(1, 0.0)?;
                    if hi.is_nan() || x.is_nan() {
                        f64::NAN
                    } else {
                        x.min(hi)
                    }
                }
                "timestamp" => s.timestamp as f64 / 1000.0,
                _ if calendar => calendar_value(name, x),
                _ => bail!("function {name} is not implemented"),
            };
            drop_name(&mut s.labels);
            s.timestamp = t;
            result.push(s);
        }
        Ok(Value::Vector(result))
    }
    fn range_function(&mut self, c: &Call, mut args: Vec<Value>, t: i64) -> Result<Value> {
        let name = c.func.name;
        let mi = args
            .iter()
            .position(|a| matches!(a, Value::Matrix(_)))
            .unwrap();
        let matrix = std::mem::replace(&mut args[mi], Value::Scalar(0.0)).matrix()?;
        let mut result = Vec::new();
        for Series { mut labels, points } in matrix {
            let values = points.iter().map(|p| p.value).collect::<Vec<_>>();
            if values.is_empty() {
                continue;
            }
            let mut omit = false;
            let value = match name {
                "sum_over_time" => values.iter().sum(),
                "avg_over_time" => values.iter().map(|v| v / values.len() as f64).sum(),
                "min_over_time" => values.iter().copied().reduce(f64::min).unwrap(),
                "max_over_time" => values.iter().copied().reduce(f64::max).unwrap(),
                "count_over_time" => values.len() as f64,
                "present_over_time" => 1.0,
                "last_over_time" => *values.last().unwrap(),
                "first_over_time" => values[0],
                "stddev_over_time" => variance(&values).sqrt(),
                "stdvar_over_time" => variance(&values),
                "quantile_over_time" => quantile(args[0].scalar()?, &mut values.clone()),
                "changes" => values
                    .windows(2)
                    .filter(|p| p[0] != p[1] && !(p[0].is_nan() && p[1].is_nan()))
                    .count() as f64,
                "resets" => values.windows(2).filter(|p| p[1] < p[0]).count() as f64,
                "ts_of_last_over_time" => points.last().unwrap().timestamp as f64 / 1000.0,
                "ts_of_first_over_time" => points[0].timestamp as f64 / 1000.0,
                "ts_of_max_over_time" | "ts_of_min_over_time" => {
                    let mut best = &points[0];
                    for p in &points[1..] {
                        if best.value.is_nan()
                            || (if name == "ts_of_max_over_time" {
                                p.value >= best.value
                            } else {
                                p.value <= best.value
                            })
                        {
                            best = p
                        }
                    }
                    best.timestamp as f64 / 1000.0
                }
                "mad_over_time" => {
                    let median = quantile(0.5, &mut values.clone());
                    quantile(
                        0.5,
                        &mut values
                            .iter()
                            .map(|x| (x - median).abs())
                            .collect::<Vec<_>>(),
                    )
                }
                "irate" | "idelta" => {
                    if points.len() < 2 {
                        continue;
                    }
                    let a = &points[points.len() - 2];
                    let b = points.last().unwrap();
                    let mut delta = b.value - a.value;
                    if name == "irate" && b.value < a.value {
                        delta = b.value
                    }
                    if name == "irate" {
                        delta / ((b.timestamp - a.timestamp) as f64 / 1000.0)
                    } else {
                        delta
                    }
                }
                "rate" | "increase" | "delta" => {
                    if points.len() < 2 {
                        continue;
                    }
                    let (start, end) = self.range_bounds(&c.args.args[mi], t)?;
                    extrapolate(&points, start, end, name != "delta", name == "rate")
                }
                "deriv" | "predict_linear" => {
                    if points.len() < 2 {
                        continue;
                    }
                    let reference = if name == "deriv" {
                        points[0].timestamp
                    } else {
                        t
                    };
                    let (slope, intercept) = regression(&points, reference);
                    if name == "deriv" {
                        slope
                    } else {
                        intercept + slope * args[1].scalar()?
                    }
                }
                "double_exponential_smoothing" | "holt_winters" => {
                    if points.len() < 2 {
                        continue;
                    }
                    let sf = args[1].scalar()?;
                    let tf = args[2].scalar()?;
                    if !(0.0..1.0).contains(&sf)
                        || sf == 0.0
                        || !(0.0..1.0).contains(&tf)
                        || tf == 0.0
                    {
                        bail!("smoothing and trend factors must be between 0 and 1")
                    }
                    let mut smooth = values[0];
                    let mut trend = values[1] - values[0];
                    for v in &values[1..] {
                        let old = smooth;
                        smooth = sf * v + (1.0 - sf) * (smooth + trend);
                        trend = tf * (smooth - old) + (1.0 - tf) * trend;
                    }
                    smooth
                }
                // Native-only functions have no float result.
                "histogram_count" | "histogram_sum" | "histogram_avg" => {
                    omit = true;
                    0.0
                }
                _ => bail!("function {name} is not implemented"),
            };
            if !omit {
                if !matches!(name, "last_over_time" | "first_over_time") {
                    drop_name(&mut labels)
                }
                result.push(Sample {
                    labels,
                    value,
                    timestamp: t,
                });
            }
        }
        Ok(Value::Vector(result))
    }
    fn range_bounds(&self, expr: &Expr, t: i64) -> Result<(i64, i64)> {
        let (end, range) = match expr {
            Expr::MatrixSelector(m) => (self.target(t, &m.vs.at, &m.vs.offset)?, m.range),
            Expr::Subquery(s) => (self.target(t, &s.at, &s.offset)?, s.range),
            Expr::Paren(p) => return self.range_bounds(&p.expr, t),
            _ => bail!("expected range selector"),
        };
        Ok((end.saturating_sub(range.as_millis().try_into()?), end))
    }
}
fn absent_labels(expr: &Expr) -> Labels {
    let vs = match expr {
        Expr::VectorSelector(v) => Some(v),
        Expr::MatrixSelector(m) => Some(&m.vs),
        _ => None,
    };
    let mut labels = Labels::new();
    let mut used = std::collections::BTreeSet::new();
    if let Some(v) = vs {
        for m in &v.matchers.matchers {
            if m.name == "__name__" {
                continue;
            }
            if !used.insert(&m.name) {
                labels.remove(&m.name);
            } else if matches!(m.op, MatchOp::Equal) {
                labels.insert(m.name.clone(), m.value.clone());
            }
        }
    }
    labels
}
fn label_function(name: &str, mut args: Vec<Value>, t: i64) -> Result<Value> {
    let mut v = args.remove(0).vector()?;
    let dest = args[0].string()?;
    if !valid_label(dest) {
        bail!("invalid destination label name {dest}")
    }
    if name == "label_replace" {
        let replacement = args[1].string()?;
        let source = args[2].string()?;
        let re = regex::Regex::new(&format!("(?s)^(?:{})$", args[3].string()?))?;
        for s in &mut v {
            let input = s.labels.get(source).map(String::as_str).unwrap_or("");
            if re.is_match(input) {
                let value = re.replace(input, replacement).to_string();
                if value.is_empty() {
                    s.labels.remove(dest);
                } else {
                    s.labels.insert(dest.into(), value);
                }
            }
        }
    } else {
        let separator = args[1].string()?;
        let sources = args[2..]
            .iter()
            .map(Value::string)
            .collect::<Result<Vec<_>>>()?;
        for source in &sources {
            if !valid_label(source) {
                bail!("invalid source label name {source}")
            }
        }
        for s in &mut v {
            let value = sources
                .iter()
                .map(|k| s.labels.get(*k).map(String::as_str).unwrap_or(""))
                .collect::<Vec<_>>()
                .join(separator);
            if value.is_empty() {
                s.labels.remove(dest);
            } else {
                s.labels.insert(dest.into(), value);
            }
        }
    }
    for s in &mut v {
        s.timestamp = t;
    }
    Ok(Value::Vector(v))
}
fn calendar_value(name: &str, value: f64) -> f64 {
    if !value.is_finite() || value < i64::MIN as f64 || value > i64::MAX as f64 {
        return f64::NAN;
    }
    let Some(date) = chrono::DateTime::from_timestamp(value as i64, 0) else {
        return f64::NAN;
    };
    match name {
        "day_of_month" => date.day() as f64,
        "day_of_week" => date.weekday().num_days_from_sunday() as f64,
        "day_of_year" => date.ordinal() as f64,
        "hour" => date.hour() as f64,
        "minute" => date.minute() as f64,
        "month" => date.month() as f64,
        "year" => date.year() as f64,
        "days_in_month" => {
            let (year, month) = if date.month() == 12 {
                (date.year() + 1, 1)
            } else {
                (date.year(), date.month() + 1)
            };
            chrono::NaiveDate::from_ymd_opt(year, month, 1)
                .and_then(|d| d.pred_opt())
                .map(|d| d.day() as f64)
                .unwrap_or(f64::NAN)
        }
        _ => f64::NAN,
    }
}
fn extrapolate(
    points: &[crate::model::Point],
    start: i64,
    end: i64,
    counter: bool,
    rate: bool,
) -> f64 {
    let first = &points[0];
    let last = points.last().unwrap();
    let mut result = last.value - first.value;
    if counter {
        for pair in points.windows(2) {
            if pair[1].value < pair[0].value {
                result += pair[0].value
            }
        }
    }
    let sampled = (last.timestamp - first.timestamp) as f64 / 1000.0;
    let average = sampled / (points.len() - 1) as f64;
    let mut to_start = (first.timestamp - start) as f64 / 1000.0;
    let mut to_end = (end - last.timestamp) as f64 / 1000.0;
    if to_start >= average * 1.1 {
        to_start = average / 2.0
    }
    if counter && result > 0.0 && first.value >= 0.0 {
        let zero = sampled * first.value / result;
        if zero < to_start {
            to_start = zero
        }
    }
    if to_end >= average * 1.1 {
        to_end = average / 2.0
    }
    let mut factor = (sampled + to_start + to_end) / sampled;
    if rate {
        factor /= (end - start) as f64 / 1000.0
    }
    result * factor
}
fn regression(points: &[crate::model::Point], reference: i64) -> (f64, f64) {
    if points.iter().all(|p| p.value == points[0].value) {
        return if points[0].value.is_finite() {
            (0.0, points[0].value)
        } else {
            (f64::NAN, f64::NAN)
        };
    }
    let n = points.len() as f64;
    let mut sx = 0.0;
    let mut sy = 0.0;
    let mut sxy = 0.0;
    let mut sxx = 0.0;
    for p in points {
        let x = (p.timestamp - reference) as f64 / 1000.0;
        sx += x;
        sy += p.value;
        sxy += x * p.value;
        sxx += x * x;
    }
    let slope = (sxy - sx * sy / n) / (sxx - sx * sx / n);
    (slope, sy / n - slope * sx / n)
}
fn histogram_quantile(
    mut args: Vec<Value>,
    t: i64,
    infos: &mut std::collections::BTreeSet<String>,
) -> Result<Value> {
    let q = args[0].scalar()?;
    let v = args.remove(1).vector()?;
    let mut groups: BTreeMap<Labels, Vec<(f64, f64)>> = BTreeMap::new();
    for s in v {
        let mut labels = s.labels;
        let bound = labels.remove("le").and_then(|x| x.parse::<f64>().ok());
        drop_name(&mut labels);
        if let Some(bound) = bound {
            groups.entry(labels).or_default().push((bound, s.value));
        }
    }
    let mut result = Vec::new();
    for (labels, mut buckets) in groups {
        buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
        let mut merged: Vec<(f64, f64)> = Vec::new();
        for b in buckets {
            if merged.last().is_some_and(|last| last.0 == b.0) {
                merged.last_mut().unwrap().1 += b.1
            } else {
                merged.push(b)
            }
        }
        let buckets = &mut merged;
        for i in 1..buckets.len() {
            if buckets[i].1 < buckets[i - 1].1 {
                let diff = buckets[i - 1].1 - buckets[i].1;
                if diff > 1e-12 * (buckets[i - 1].1 + buckets[i].1).abs() {
                    infos.insert("PromQL info: input to histogram_quantile needed to be fixed for monotonicity".into());
                }
                buckets[i].1 = buckets[i - 1].1
            }
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
            let index = buckets
                .partition_point(|b| b.1 < rank)
                .min(buckets.len() - 1);
            if index == buckets.len() - 1 {
                buckets[index - 1].0
            } else if index == 0 && buckets[0].0 <= 0.0 {
                buckets[0].0
            } else {
                let (low, previous) = if index == 0 {
                    (0.0, 0.0)
                } else {
                    buckets[index - 1]
                };
                low + (buckets[index].0 - low) * (rank - previous) / (buckets[index].1 - previous)
            }
        };
        result.push(Sample {
            labels,
            value,
            timestamp: t,
        });
    }
    Ok(Value::Vector(result))
}
