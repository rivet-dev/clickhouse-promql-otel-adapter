mod aggregate;
mod binary;
mod functions;

use crate::model::{Labels, Point, Sample, Series, Value};
use anyhow::{Result, bail};
use promql_parser::parser::{self, AtModifier, Expr, Offset, VectorSelector};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant, UNIX_EPOCH},
};

pub struct Engine<'a> {
    pub series: &'a [Series],
    pub start: i64,
    pub end: i64,
    pub lookback: i64,
    pub default_step: i64,
    pub max_samples: usize,
    pub deadline: Instant,
    samples: usize,
    depth: usize,
    pub warnings: BTreeSet<String>,
    pub infos: BTreeSet<String>,
}
impl<'a> Engine<'a> {
    pub fn new(series: &'a [Series], start: i64, end: i64) -> Self {
        Self {
            series,
            start,
            end,
            lookback: 300_000,
            default_step: 60_000,
            max_samples: 5_000_000,
            deadline: Instant::now() + Duration::from_secs(120),
            samples: 0,
            depth: 0,
            warnings: BTreeSet::new(),
            infos: BTreeSet::new(),
        }
    }
    pub fn evaluate(&mut self, expr: &Expr, t: i64) -> Result<Value> {
        self.eval(expr, t)
    }
    pub fn parse(query: &str) -> Result<Expr> {
        parser::parse(query).map_err(anyhow::Error::msg)
    }
    pub fn instant(&mut self, query: &str, timestamp: i64) -> Result<Value> {
        self.eval(&Self::parse(query)?, timestamp)
    }
    pub fn range(&mut self, expr: &Expr, step: i64) -> Result<Value> {
        if step <= 0 {
            bail!("zero or negative query resolution step widths are not accepted")
        }
        if self.end < self.start {
            bail!("end timestamp must not be before start time")
        }
        if (self.end as i128 - self.start as i128) / step as i128 > 11000 {
            bail!("exceeded maximum resolution of 11,000 points per timeseries")
        }
        let mut result: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
        let mut timestamp = self.start;
        loop {
            match self.eval(expr, timestamp)? {
                Value::Scalar(value) => result
                    .entry(Labels::new())
                    .or_default()
                    .push(Point { timestamp, value }),
                Value::Vector(samples) => {
                    for s in samples {
                        result.entry(s.labels).or_default().push(Point {
                            timestamp,
                            value: s.value,
                        });
                    }
                }
                _ => bail!(
                    "invalid expression type for range query, must be Scalar or instant Vector"
                ),
            }
            match timestamp.checked_add(step) {
                Some(next) if next <= self.end => timestamp = next,
                _ => break,
            }
        }
        Ok(Value::Matrix(
            result
                .into_iter()
                .map(|(labels, points)| Series { labels, points })
                .collect(),
        ))
    }
    fn charge(&mut self, count: usize) -> Result<()> {
        self.samples = self.samples.saturating_add(count);
        if self.samples > self.max_samples {
            bail!("query processing would load too many samples into memory in query execution")
        }
        if Instant::now() > self.deadline {
            bail!("query timed out in expression evaluation")
        }
        Ok(())
    }
    fn eval(&mut self, expr: &Expr, timestamp: i64) -> Result<Value> {
        self.charge(0)?;
        if self.depth >= 128 {
            bail!("expression nesting exceeds limit")
        }
        self.depth += 1;
        let result = self.eval_inner(expr, timestamp);
        self.depth -= 1;
        if let Ok(Value::Vector(v)) = &result {
            let mut seen = BTreeSet::new();
            for sample in v {
                if !seen.insert(&sample.labels) {
                    bail!("vector cannot contain metrics with the same labelset")
                }
            }
        }
        result
    }
    fn eval_inner(&mut self, expr: &Expr, timestamp: i64) -> Result<Value> {
        match expr {
            Expr::NumberLiteral(n) => Ok(Value::Scalar(n.val)),
            Expr::StringLiteral(s) => Ok(Value::String(s.val.clone())),
            Expr::Paren(p) => self.eval(&p.expr, timestamp),
            Expr::Unary(u) => match self.eval(&u.expr, timestamp)? {
                Value::Scalar(v) => Ok(Value::Scalar(-v)),
                Value::Vector(mut v) => {
                    for s in &mut v {
                        s.value = -s.value;
                        crate::model::drop_name(&mut s.labels);
                    }
                    Ok(Value::Vector(v))
                }
                _ => bail!("unary expression requires scalar or instant vector"),
            },
            Expr::VectorSelector(v) => {
                let target = self.target(timestamp, &v.at, &v.offset)?;
                let mut samples = Vec::new();
                for s in self.series.iter().filter(|s| matches(v, &s.labels)) {
                    let index = s.points.partition_point(|p| p.timestamp <= target);
                    if index > 0 {
                        let p = &s.points[index - 1];
                        if p.timestamp > target.saturating_sub(self.lookback) && !p.stale() {
                            samples.push(Sample {
                                labels: s.labels.clone(),
                                timestamp: p.timestamp,
                                value: p.value,
                            })
                        }
                    }
                }
                self.charge(samples.len())?;
                Ok(Value::Vector(samples))
            }
            Expr::MatrixSelector(m) => {
                let target = self.target(timestamp, &m.vs.at, &m.vs.offset)?;
                let begin = target.saturating_sub(m.range.as_millis().try_into()?);
                let mut result = Vec::new();
                for s in self.series.iter().filter(|s| matches(&m.vs, &s.labels)) {
                    let a = s.points.partition_point(|p| p.timestamp <= begin);
                    let b = s.points.partition_point(|p| p.timestamp <= target);
                    let points = s.points[a..b]
                        .iter()
                        .filter(|p| !p.stale())
                        .cloned()
                        .collect::<Vec<_>>();
                    if !points.is_empty() {
                        result.push(Series {
                            labels: s.labels.clone(),
                            points,
                        });
                    }
                }
                self.charge(result.iter().map(|s| s.points.len()).sum())?;
                Ok(Value::Matrix(result))
            }
            Expr::Subquery(s) => {
                let target = self.target(timestamp, &s.at, &s.offset)?;
                let range: i64 = s.range.as_millis().try_into()?;
                let step = match s.step {
                    Some(v) => v.as_millis().try_into()?,
                    None => self.default_step,
                };
                if step <= 0 {
                    bail!("subquery resolution must be positive")
                }
                let begin = target
                    .checked_sub(range)
                    .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;
                let mut t = begin
                    .div_euclid(step)
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(step))
                    .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;
                let mut result: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
                while t <= target {
                    for s in self.eval(&s.expr, t)?.vector()? {
                        result.entry(s.labels).or_default().push(Point {
                            timestamp: t,
                            value: s.value,
                        });
                        self.charge(1)?;
                    }
                    t = match t.checked_add(step) {
                        Some(t) => t,
                        None => break,
                    };
                }
                Ok(Value::Matrix(
                    result
                        .into_iter()
                        .map(|(labels, points)| Series { labels, points })
                        .collect(),
                ))
            }
            Expr::Aggregate(a) => self.aggregate(a, timestamp),
            Expr::Binary(b) => self.binary(b, timestamp),
            Expr::Call(c) => self.call(c, timestamp),
            _ => bail!("unsupported expression"),
        }
    }
    pub fn selector_time(
        &self,
        timestamp: i64,
        at: &Option<AtModifier>,
        offset: &Option<Offset>,
    ) -> Result<i64> {
        self.target(timestamp, at, offset)
    }
    fn target(
        &self,
        timestamp: i64,
        at: &Option<AtModifier>,
        offset: &Option<Offset>,
    ) -> Result<i64> {
        let t = match at {
            Some(AtModifier::Start) => self.start,
            Some(AtModifier::End) => self.end,
            Some(AtModifier::At(t)) => match t.duration_since(UNIX_EPOCH) {
                Ok(d) => i64::try_from(d.as_millis())?,
                Err(e) => -i64::try_from(e.duration().as_millis())?,
            },
            None => timestamp,
        };
        let delta = match offset {
            Some(Offset::Pos(d)) => i64::try_from(d.as_millis())?,
            Some(Offset::Neg(d)) => -i64::try_from(d.as_millis())?,
            None => 0,
        };
        t.checked_sub(delta)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))
    }
}

pub fn matches(selector: &VectorSelector, labels: &Labels) -> bool {
    if selector
        .name
        .as_ref()
        .is_some_and(|n| labels.get("__name__") != Some(n))
    {
        return false;
    }
    let check = |ms: &[promql_parser::label::Matcher]| {
        ms.iter()
            .all(|m| m.is_match(labels.get(&m.name).map(String::as_str).unwrap_or("")))
    };
    if selector.matchers.or_matchers.is_empty() {
        check(&selector.matchers.matchers)
    } else {
        selector.matchers.or_matchers.iter().any(|ms| check(ms))
    }
}
