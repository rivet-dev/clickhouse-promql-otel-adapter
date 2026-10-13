use super::Engine;
use crate::model::{Labels, Sample, Value, drop_name};
use anyhow::{Result, bail};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};
use std::collections::BTreeMap;

fn operation(op: &str, a: f64, b: f64) -> Result<(f64, bool)> {
    Ok(match op {
        "+" => (a + b, true),
        "-" => (a - b, true),
        "*" => (a * b, true),
        "/" => (a / b, true),
        "%" => (a % b, true),
        "^" => (a.powf(b), true),
        "atan2" => (a.atan2(b), true),
        "==" => (a, a == b),
        "!=" => (a, a != b),
        ">" => (a, a > b),
        "<" => (a, a < b),
        ">=" => (a, a >= b),
        "<=" => (a, a <= b),
        _ => bail!("unsupported binary operator {op}"),
    })
}
fn key(labels: &Labels, modifier: &Option<LabelModifier>) -> Labels {
    match modifier {
        Some(LabelModifier::Include(l)) => labels
            .iter()
            .filter(|(k, _)| l.labels.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        _ => labels
            .iter()
            .filter(|(k, _)| {
                k.as_str() != "__name__"
                    && !modifier
                        .as_ref()
                        .is_some_and(|m| m.labels().labels.contains(k))
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    }
}
impl Engine<'_> {
    pub(super) fn binary(&mut self, b: &BinaryExpr, t: i64) -> Result<Value> {
        let lhs = self.eval(&b.lhs, t)?;
        let rhs = self.eval(&b.rhs, t)?;
        let op = b.op.to_string();
        let comparison = matches!(op.as_str(), "==" | "!=" | ">" | "<" | ">=" | "<=");
        let modifier = b.modifier.clone().unwrap_or_default();
        match (lhs, rhs) {
            (Value::Scalar(a), Value::Scalar(b)) => {
                let (v, keep) = operation(&op, a, b)?;
                Ok(Value::Scalar(if comparison {
                    if keep { 1.0 } else { 0.0 }
                } else {
                    v
                }))
            }
            (Value::Vector(v), Value::Scalar(s)) => {
                scalar_vector(v, s, false, &op, comparison, modifier.return_bool, t)
            }
            (Value::Scalar(s), Value::Vector(v)) => {
                scalar_vector(v, s, true, &op, comparison, modifier.return_bool, t)
            }
            (Value::Vector(mut left), Value::Vector(mut right)) => {
                let left_keys = left
                    .iter()
                    .map(|s| key(&s.labels, &modifier.matching))
                    .collect::<std::collections::BTreeSet<_>>();
                let right_keys = right
                    .iter()
                    .map(|s| key(&s.labels, &modifier.matching))
                    .collect::<std::collections::BTreeSet<_>>();
                if let Some(value) = modifier.fill_values.lhs {
                    let mut added = std::collections::BTreeSet::new();
                    for s in &right {
                        let signature = key(&s.labels, &modifier.matching);
                        if !left_keys.contains(&signature) && added.insert(signature) {
                            let mut labels = s.labels.clone();
                            drop_name(&mut labels);
                            left.push(Sample {
                                labels,
                                value,
                                timestamp: t,
                                ..Default::default()
                            });
                        }
                    }
                }
                if let Some(value) = modifier.fill_values.rhs {
                    let mut added = std::collections::BTreeSet::new();
                    for s in &left {
                        let signature = key(&s.labels, &modifier.matching);
                        if !right_keys.contains(&signature) && added.insert(signature) {
                            let mut labels = s.labels.clone();
                            drop_name(&mut labels);
                            right.push(Sample {
                                labels,
                                value,
                                timestamp: t,
                                ..Default::default()
                            });
                        }
                    }
                }

                let mut rhs: BTreeMap<Labels, Vec<&Sample>> = BTreeMap::new();
                let mut lhs: BTreeMap<Labels, Vec<&Sample>> = BTreeMap::new();
                for s in &right {
                    rhs.entry(key(&s.labels, &modifier.matching))
                        .or_default()
                        .push(s);
                }
                for s in &left {
                    lhs.entry(key(&s.labels, &modifier.matching))
                        .or_default()
                        .push(s);
                }
                if matches!(op.as_str(), "and" | "or" | "unless") {
                    let mut result = Vec::new();
                    for s in &left {
                        let present = rhs.contains_key(&key(&s.labels, &modifier.matching));
                        if op == "or" || (op == "and" && present) || (op == "unless" && !present) {
                            result.push(s.clone())
                        }
                    }
                    if op == "or" {
                        for s in &right {
                            if !lhs.contains_key(&key(&s.labels, &modifier.matching)) {
                                result.push(s.clone())
                            }
                        }
                    }
                    return Ok(Value::Vector(result));
                }
                let one_to_many = matches!(modifier.card, VectorMatchCardinality::OneToMany(_));
                let (many, one) = if one_to_many {
                    (&right, &lhs)
                } else {
                    (&left, &rhs)
                };
                // Uniqueness is validated even if the other side has no matching series.
                if one.values().any(|v| v.len() > 1) {
                    bail!(
                        "found duplicate series for the match group; many-to-many matching not allowed: matching labels must be unique on one side"
                    )
                }
                let mut matched = BTreeMap::<Labels, usize>::new();
                let mut result = Vec::new();
                for m in many {
                    let signature = key(&m.labels, &modifier.matching);
                    let Some(matches) = one.get(&signature) else {
                        continue;
                    };
                    let o = matches[0];
                    let count = matched.entry(signature).or_default();
                    *count += 1;
                    if *count > 1 && matches!(modifier.card, VectorMatchCardinality::OneToOne) {
                        bail!(
                            "multiple matches for labels: many-to-one matching must be explicit (group_left/group_right)"
                        )
                    }
                    let (a, c) = if one_to_many {
                        (o.value, m.value)
                    } else {
                        (m.value, o.value)
                    };
                    let (sa, sb) = if one_to_many { (o, m) } else { (m, o) };
                    let mut histogram = None;
                    let (value, keep) = if sa.histogram.is_some() || sb.histogram.is_some() {
                        if sa.histogram.is_some()
                            && sb.histogram.is_some()
                            && (op == "==" || op == "!=")
                        {
                            let equal = sa.histogram == sb.histogram;
                            histogram = sa.histogram.clone();
                            (0.0, if op == "==" { equal } else { !equal })
                        } else {
                            histogram = match super::native::histogram_binary(&op, sa, sb) {
                                Ok(h) => h,
                                Err(_) => {
                                    self.annotation(true,format!("incompatible bucket layout encountered for binary operator {op}"),&promql_parser::parser::Expr::Binary(b.clone()));
                                    continue;
                                }
                            };
                            if histogram.is_none() {
                                continue;
                            }
                            (0.0, true)
                        }
                    } else {
                        operation(&op, a, c)?
                    };
                    if modifier.return_bool {
                        histogram = None;
                    }
                    if !keep && !modifier.return_bool {
                        continue;
                    }
                    let mut labels = m.labels.clone();
                    if !comparison && !matches!(op.as_str(), "</" | ">/") {
                        drop_name(&mut labels)
                    }
                    if matches!(modifier.card, VectorMatchCardinality::OneToOne) {
                        match &modifier.matching {
                            Some(LabelModifier::Include(l)) => {
                                labels.retain(|k, _| l.labels.contains(k))
                            }
                            Some(LabelModifier::Exclude(l)) => {
                                labels.retain(|k, _| !l.labels.contains(k))
                            }
                            None => {}
                        }
                    }
                    if let Some(include) = modifier.card.labels() {
                        for label in &include.labels {
                            match o.labels.get(label) {
                                Some(v) => {
                                    labels.insert(label.clone(), v.clone());
                                }
                                None => {
                                    labels.remove(label);
                                }
                            }
                        }
                    }
                    let value = if modifier.return_bool {
                        if keep { 1.0 } else { 0.0 }
                    } else {
                        value
                    };
                    result.push(Sample {
                        drop_name: modifier.return_bool,
                        labels,
                        value,
                        histogram,
                        timestamp: t,
                        ..Default::default()
                    });
                }
                Ok(Value::Vector(result))
            }
            _ => bail!("binary expression requires scalars or instant vectors"),
        }
    }
}
fn scalar_vector(
    v: Vec<Sample>,
    scalar: f64,
    swap: bool,
    op: &str,
    comparison: bool,
    bool_mode: bool,
    t: i64,
) -> Result<Value> {
    let mut result = Vec::new();
    for mut s in v {
        if s.histogram.is_some() {
            let scalar_sample = Sample {
                value: scalar,
                ..Default::default()
            };
            let (a, b) = if swap {
                (&scalar_sample, &s)
            } else {
                (&s, &scalar_sample)
            };
            let h = super::native::histogram_binary(op, a, b)?;
            if h.is_none() {
                continue;
            }
            s.histogram = h;
            s.timestamp = t;
            if !matches!(op, "</" | ">/") {
                s.drop_name = true;
            }
            result.push(s);
            continue;
        }
        let (a, b) = if swap {
            (scalar, s.value)
        } else {
            (s.value, scalar)
        };
        let (value, keep) = operation(op, a, b)?;
        if !keep && !bool_mode {
            continue;
        }
        s.value = if bool_mode {
            if keep { 1.0 } else { 0.0 }
        } else if comparison {
            s.value
        } else {
            value
        };
        if !comparison || bool_mode {
            s.drop_name = true
        }
        s.timestamp = t;
        result.push(s);
    }
    Ok(Value::Vector(result))
}
