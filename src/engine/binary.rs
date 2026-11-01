use super::Engine;
use crate::model::{Labels, Sample, Value, drop_name};
use anyhow::{Result, bail};
use promql_parser::parser::{BinaryExpr, LabelModifier, VectorMatchCardinality};
use std::collections::{HashMap, HashSet};

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
    /// Match signature of `labels` at binary node `site`, cached per input
    /// label set and interned like aggregation group keys.
    fn match_key(
        &mut self,
        site: usize,
        labels: &Labels,
        modifier: &Option<LabelModifier>,
    ) -> Labels {
        self.node_key(site, labels, |labels| key(labels, modifier))
    }

    fn incompatible(&mut self, b: &BinaryExpr, left: bool, right: bool) {
        let lhs = if left { "histogram" } else { "float" };
        let rhs = if right { "histogram" } else { "float" };
        let op = b.op.to_string();
        self.annotation(
            false,
            format!(
                "incompatible sample types encountered for binary operator {op:?}: {lhs} {op} {rhs}"
            ),
            &promql_parser::parser::Expr::Binary(b.clone()),
        );
    }

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
                if v.iter().any(|s| s.histogram.is_some())
                    && !matches!(op.as_str(), "*" | "/" | "</" | ">/")
                {
                    self.incompatible(b, true, false);
                }
                scalar_vector(v, s, false, &op, comparison, modifier.return_bool, t)
            }
            (Value::Scalar(s), Value::Vector(v)) => {
                if v.iter().any(|s| s.histogram.is_some()) && op != "*" {
                    self.incompatible(b, false, true);
                }
                scalar_vector(v, s, true, &op, comparison, modifier.return_bool, t)
            }
            (Value::Vector(mut left), Value::Vector(mut right)) => {
                // Match signatures are derived once per input label set and
                // shared, so every lookup below hashes a cached value and
                // compares by pointer.
                let site = std::ptr::from_ref(b) as usize;
                let mut left_keys = Vec::with_capacity(left.len());
                for s in &left {
                    left_keys.push(self.match_key(site, &s.labels, &modifier.matching));
                }
                let mut right_keys = Vec::with_capacity(right.len());
                for s in &right {
                    right_keys.push(self.match_key(site, &s.labels, &modifier.matching));
                }
                let one_to_many = matches!(modifier.card, VectorMatchCardinality::OneToMany(_));
                let (fill_left, fill_right) = if one_to_many {
                    (modifier.fill_values.rhs, modifier.fill_values.lhs)
                } else {
                    (modifier.fill_values.lhs, modifier.fill_values.rhs)
                };
                // Indexes are built only where an operator consults them.
                if fill_left.is_some() || fill_right.is_some() {
                    let left_present = left_keys.iter().cloned().collect::<HashSet<_>>();
                    let right_present = right_keys.iter().cloned().collect::<HashSet<_>>();
                    if let Some(value) = fill_left {
                        let mut added = HashSet::new();
                        for signature in &right_keys {
                            if !left_present.contains(signature) && added.insert(signature.clone())
                            {
                                left.push(Sample {
                                    labels: signature.clone(),
                                    value,
                                    timestamp: t,
                                    ..Default::default()
                                });
                                left_keys.push(signature.clone());
                            }
                        }
                    }
                    if let Some(value) = fill_right {
                        let mut added = HashSet::new();
                        for signature in &left_keys {
                            if !right_present.contains(signature) && added.insert(signature.clone())
                            {
                                right.push(Sample {
                                    labels: signature.clone(),
                                    value,
                                    timestamp: t,
                                    ..Default::default()
                                });
                                right_keys.push(signature.clone());
                            }
                        }
                    }
                }

                if matches!(op.as_str(), "and" | "or" | "unless") {
                    let mut result = Vec::new();
                    if op == "or" {
                        result.extend(left.iter().cloned());
                        let lhs = left_keys.iter().cloned().collect::<HashSet<_>>();
                        for (s, signature) in right.iter().zip(&right_keys) {
                            if !lhs.contains(signature) {
                                result.push(s.clone())
                            }
                        }
                    } else {
                        let rhs = right_keys.iter().cloned().collect::<HashSet<_>>();
                        for (s, signature) in left.iter().zip(&left_keys) {
                            let present = rhs.contains(signature);
                            if (op == "and" && present) || (op == "unless" && !present) {
                                result.push(s.clone())
                            }
                        }
                    }
                    return Ok(Value::Vector(result));
                }
                let (many, many_keys, one_side, one_keys) = if one_to_many {
                    (&right, &right_keys, &left, &left_keys)
                } else {
                    (&left, &left_keys, &right, &right_keys)
                };
                let mut one: HashMap<Labels, Vec<&Sample>> = HashMap::new();
                for (s, signature) in one_side.iter().zip(one_keys) {
                    one.entry(signature.clone()).or_default().push(s);
                }
                if many.is_empty() || one.is_empty() {
                    return Ok(Value::Vector(vec![]));
                }
                // Report the first duplicate group in label order.
                if let Some((group, values)) = one
                    .iter()
                    .filter(|(_, v)| v.len() > 1)
                    .min_by(|a, b| a.0.cmp(b.0))
                {
                    let mut duplicates = values
                        .iter()
                        .map(|s| crate::model::labels_string(&s.labels))
                        .collect::<Vec<_>>();
                    duplicates.sort();
                    bail!(
                        "found duplicate series for the match group {} on the {} hand-side of the operation: [{}, {}];many-to-many matching not allowed: matching labels must be unique on one side",
                        crate::model::labels_string(group),
                        if one_to_many { "left" } else { "right" },
                        duplicates[0],
                        duplicates[1]
                    );
                }
                let mut matched = HashMap::<Labels, usize>::new();
                let mut result = Vec::new();
                for (m, signature) in many.iter().zip(many_keys) {
                    let Some(matches) = one.get(signature) else {
                        continue;
                    };
                    let o = matches[0];
                    let count = matched.entry(signature.clone()).or_default();
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
                    if let (Some(a), Some(c)) = (&sa.histogram, &sb.histogram)
                        && matches!(op.as_str(), "+" | "-")
                    {
                        let operation = if op == "+" { "addition" } else { "subtraction" };
                        if (a.reset_hint == 1 && c.reset_hint == 2)
                            || (a.reset_hint == 2 && c.reset_hint == 1)
                        {
                            self.annotation(
                                true,
                                format!("conflicting counter resets during histogram {operation}"),
                                &promql_parser::parser::Expr::Binary(b.clone()),
                            );
                        }
                        if a.schema == -53 && c.schema == -53 && a.custom != c.custom {
                            self.annotation(
                                false,
                                format!(
                                    "mismatched custom buckets were reconciled during {operation}"
                                ),
                                &promql_parser::parser::Expr::Binary(b.clone()),
                            );
                        }
                    }
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
                                self.incompatible(
                                    b,
                                    sa.histogram.is_some(),
                                    sb.histogram.is_some(),
                                );
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
