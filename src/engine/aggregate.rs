use super::Engine;
use crate::model::{Labels, Sample, Value, float_string};
use anyhow::{Result, bail};
use promql_parser::parser::{AggregateExpr, LabelModifier};
use std::collections::BTreeMap;

pub(super) fn grouping(labels: &Labels, modifier: &Option<LabelModifier>) -> Labels {
    match modifier {
        Some(LabelModifier::Include(l)) => labels
            .iter()
            .filter(|(k, _)| l.labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        Some(LabelModifier::Exclude(l)) => labels
            .iter()
            .filter(|(k, _)| k.as_str() != "__name__" && !l.labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        None => Labels::new(),
    }
}
pub(super) fn quantile(q: f64, values: &mut [f64]) -> f64 {
    if q.is_nan() || values.is_empty() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(|a, b| {
        if a.is_nan() {
            if b.is_nan() {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Less
            }
        } else if b.is_nan() {
            std::cmp::Ordering::Greater
        } else {
            a.total_cmp(b)
        }
    });
    let rank = q * (values.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = (lo + 1).min(values.len() - 1);
    let weight = rank - rank.floor();
    values[lo] * (1.0 - weight) + values[hi] * weight
}
pub(super) fn variance(values: &[f64]) -> f64 {
    let mut mean = 0.0;
    let mut sum = 0.0;
    for (i, v) in values.iter().enumerate() {
        let delta = v - mean;
        mean += delta / (i + 1) as f64;
        sum += delta * (v - mean);
    }
    sum / values.len() as f64
}
impl Engine<'_> {
    pub(super) fn aggregate(&mut self, a: &AggregateExpr, t: i64) -> Result<Value> {
        let vector = self.eval(&a.expr, t)?.vector()?;
        let parameter = match &a.param {
            Some(p) => Some(self.eval(p, t)?),
            None => None,
        };
        let op = a.op.to_string();
        let mut groups: BTreeMap<Labels, Vec<Sample>> = BTreeMap::new();
        for mut s in vector {
            if op == "count_values" {
                let name = parameter.as_ref().unwrap().string()?;
                if !valid_label(name) {
                    bail!("invalid label name {name}")
                }
                s.labels.insert(name.into(), float_string(s.value));
            }
            let mut key = grouping(&s.labels, &a.modifier);
            if op == "count_values" {
                let name = parameter.as_ref().unwrap().string()?;
                key.insert(name.into(), s.labels[name].clone());
            }
            groups.entry(key).or_default().push(s);
        }
        let mut result = Vec::new();
        for (labels, mut samples) in groups {
            if op == "topk" || op == "bottomk" {
                let k = parameter.as_ref().unwrap().scalar()?;
                if k.is_nan() {
                    bail!("parameter to {op} is NaN")
                }
                samples.sort_by(|a, b| match (a.value.is_nan(), b.value.is_nan()) {
                    (true, true) => std::cmp::Ordering::Equal,
                    (true, false) => std::cmp::Ordering::Greater,
                    (false, true) => std::cmp::Ordering::Less,
                    _ => {
                        if op == "topk" {
                            b.value.total_cmp(&a.value)
                        } else {
                            a.value.total_cmp(&b.value)
                        }
                    }
                });
                result.extend(samples.into_iter().take(k.max(0.0) as usize));
                continue;
            }
            let mut values = samples.iter().map(|s| s.value).collect::<Vec<_>>();
            let value = match op.as_str() {
                "sum" => values.iter().sum(),
                "avg" => {
                    let n = values.len() as f64;
                    values.iter().map(|v| v / n).sum()
                }
                "count" | "count_values" => values.len() as f64,
                "group" => 1.0,
                "min" => values.iter().copied().reduce(f64::min).unwrap(),
                "max" => values.iter().copied().reduce(f64::max).unwrap(),
                "stdvar" => variance(&values),
                "stddev" => variance(&values).sqrt(),
                "quantile" => quantile(parameter.as_ref().unwrap().scalar()?, &mut values),
                _ => bail!("aggregation {op} is not implemented"),
            };
            result.push(Sample {
                labels,
                value,
                timestamp: t,
            });
        }
        Ok(Value::Vector(result))
    }
}
pub(super) fn valid_label(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}
