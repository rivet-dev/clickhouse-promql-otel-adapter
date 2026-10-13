use super::Engine;
use crate::model::{Sample, Value};
use anyhow::{Result, bail};
use promql_parser::{
    label::{MatchOp, Matcher, Matchers},
    parser::{Call, Expr, VectorSelector},
};
use std::collections::BTreeMap;
impl Engine<'_> {
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
        let (_, _, refs) = crate::storage::plan(&c.args.args[0], t, t, self.lookback)?;
        let targets = refs
            .iter()
            .map(|v| self.target(t, &v.at, &v.offset))
            .collect::<Result<Vec<_>>>()?;
        let at = targets
            .first()
            .copied()
            .filter(|v| targets.iter().all(|t| t == v))
            .unwrap_or(t);
        selector.at = None;
        selector.offset = None;
        let infos = self.eval(&Expr::VectorSelector(selector), at)?.vector()?;
        let mut index = BTreeMap::<(String, String, String), Sample>::new();
        for sample in infos {
            if sample.histogram.is_some() {
                bail!("info metric must be a float sample")
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
            for ((_, j, i), info) in &index {
                if j != &job || i != &instance {
                    continue;
                }
                joined = true;
                for (k, v) in &info.labels {
                    if matches!(k.as_str(), "__name__" | "job" | "instance") {
                        continue;
                    }
                    if data_matchers.is_empty() || data_matchers.iter().any(|m| &m.name == k) {
                        sample.labels.entry(k.clone()).or_insert(v.clone());
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
