use super::Engine;
use crate::model::{Sample, Value};
use anyhow::{Result, bail};
use promql_parser::{
    label::{MatchOp, Matcher, Matchers},
    parser::{Call, Expr, VectorSelector},
};
use std::collections::BTreeMap;
impl Engine<'_> {
    fn info_references(
        &self,
        expr: &Expr,
        t: i64,
        anchored: bool,
        refs: &mut Vec<Option<(bool, i64)>>,
    ) -> Result<bool> {
        use promql_parser::parser::value::ValueType;
        if !matches!(expr.value_type(), ValueType::Vector | ValueType::Matrix) {
            return Ok(false);
        }
        let selector = match expr {
            Expr::VectorSelector(v) => Some(v),
            Expr::MatrixSelector(m) => Some(&m.vs),
            _ => None,
        };
        if let Some(v) = selector {
            refs.push(Some((
                anchored || v.at.is_some(),
                self.target(t, &v.at, &v.offset)?,
            )));
            return Ok(true);
        }
        let found = match expr {
            Expr::Subquery(s) => self.info_references(
                &s.expr,
                self.target(t, &s.at, &s.offset)?,
                anchored || s.at.is_some(),
                refs,
            )?,
            Expr::Call(c) => {
                let mut found = false;
                for a in c
                    .args
                    .args
                    .iter()
                    .take(if c.func.name == "info" { 1 } else { usize::MAX })
                {
                    found |= self.info_references(a, t, anchored, refs)?;
                }
                found
            }
            Expr::Aggregate(a) => self.info_references(&a.expr, t, anchored, refs)?,
            Expr::Binary(b) => {
                let a = self.info_references(&b.lhs, t, anchored, refs)?;
                let b = self.info_references(&b.rhs, t, anchored, refs)?;
                a || b
            }
            Expr::Paren(p) => self.info_references(&p.expr, t, anchored, refs)?,
            Expr::Unary(u) => self.info_references(&u.expr, t, anchored, refs)?,
            _ => false,
        };
        if !found {
            refs.push(None);
        }
        Ok(found)
    }
    pub(super) fn info(&mut self, c: &Call, t: i64) -> Result<Value> {
        let base = self.eval(&c.args.args[0], t)?.vector()?;
        let mut selector = info_selector(c);
        let data_matchers = selector
            .matchers
            .matchers
            .iter()
            .filter(|m| m.name != "__name__")
            .cloned()
            .collect::<Vec<_>>();
        let required = data_matchers.iter().any(|m| !m.is_match(""));
        let name_matchers = selector
            .matchers
            .matchers
            .iter()
            .filter(|m| m.name == "__name__")
            .cloned()
            .collect::<Vec<_>>();
        let mut references = Vec::new();
        self.info_references(&c.args.args[0], t, false, &mut references)?;
        let at = references
            .first()
            .copied()
            .flatten()
            .filter(|r| references.iter().all(|v| *v == Some(*r)))
            .map(|r| r.1)
            .unwrap_or(t);
        selector.at = None;
        selector.offset = None;
        let infos = self.eval(&Expr::VectorSelector(selector), at)?.vector()?;
        let mut index = BTreeMap::<(String, String, String), Sample>::new();
        for sample in infos {
            if sample.histogram.is_some() {
                bail!(
                    "this should be an info metric, with float samples: {}",
                    crate::model::labels_string(&sample.labels)
                )
            }
            let key = (
                sample.labels.get("__name__").cloned().unwrap_or_default(),
                sample.labels.get("job").cloned().unwrap_or_default(),
                sample.labels.get("instance").cloned().unwrap_or_default(),
            );
            if let Some(previous) = index.get(&key) {
                if previous.timestamp == sample.timestamp {
                    bail!("found multiple matching info series with the same timestamp")
                };
                if previous.timestamp > sample.timestamp {
                    continue;
                }
            }
            index.insert(key, sample);
        }
        let mut result = Vec::new();
        for mut sample in base {
            let name = sample
                .labels
                .get("__name__")
                .map(String::as_str)
                .unwrap_or("");
            if name_matchers.iter().all(|m| m.is_match(name)) {
                result.push(sample);
                continue;
            }
            let job = sample.labels.get("job").cloned().unwrap_or_default();
            let instance = sample.labels.get("instance").cloned().unwrap_or_default();
            let mut joined = false;
            let original = sample.labels.clone();
            let mut added = BTreeMap::new();
            for ((_, j, i), info) in &index {
                if j != &job || i != &instance {
                    continue;
                }
                joined = true;
                for (k, v) in &info.labels {
                    if matches!(k.as_str(), "__name__" | "job" | "instance") {
                        continue;
                    }
                    if (data_matchers.is_empty() || data_matchers.iter().any(|m| &m.name == k))
                        && !original.contains_key(k)
                    {
                        if added.get(k).is_some_and(|previous| previous != v) {
                            bail!("conflicting label: {k}");
                        }
                        added.insert(k.clone(), v.clone());
                        sample.labels.insert(k.clone(), v.clone());
                    }
                }
            }
            if joined || !required {
                result.push(sample)
            }
        }
        Ok(Value::Vector(result))
    }
}
pub(crate) fn info_selector(c: &Call) -> VectorSelector {
    let mut matchers = if let Some(arg) = c.args.args.get(1) {
        if let Expr::VectorSelector(v) = &**arg {
            v.matchers.matchers.clone()
        } else {
            vec![]
        }
    } else {
        vec![]
    };
    let names = matchers
        .iter()
        .filter(|m| m.name == "__name__")
        .collect::<Vec<_>>();
    if names.is_empty() {
        matchers.push(Matcher::new(MatchOp::Equal, "__name__", "target_info"));
    } else if !names
        .iter()
        .any(|m| matches!(m.op, MatchOp::Equal | MatchOp::Re(_)))
    {
        matchers.push(Matcher::new(
            MatchOp::Re(regex::Regex::new("^.+_info$").unwrap()),
            "__name__",
            ".+_info",
        ));
    }
    VectorSelector::new(None, Matchers::new(matchers))
}
