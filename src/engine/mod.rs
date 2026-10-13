mod aggregate;
mod binary;
mod distributions;
mod extended;
mod frontend;
mod functions;
pub(crate) mod info;
mod native;

use crate::model::{Labels, Point, Sample, Series, Value};
use anyhow::{Result, bail};
use promql_parser::parser::{AtModifier, Expr, Offset, VectorSelector};
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
    pub samples_read: usize,
    pub peak_samples: usize,
    depth: usize,
    pub warnings: BTreeSet<String>,
    pub infos: BTreeSet<String>,
    pub source: String,
    pub is_range: bool,
    pub query_step: i64,
    invariant_cache: BTreeMap<String, Value>,
    overlaps: BTreeMap<String, usize>,
    monotonic: BTreeMap<String, (i64, i64, f64, f64, f64, usize)>,
    histogram_stats: bool,
    histogram_stats_blocked: bool,
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
            samples_read: 0,
            peak_samples: 0,
            depth: 0,
            warnings: BTreeSet::new(),
            infos: BTreeSet::new(),
            source: String::new(),
            is_range: false,
            query_step: 0,
            invariant_cache: BTreeMap::new(),
            overlaps: BTreeMap::new(),
            monotonic: BTreeMap::new(),
            histogram_stats: false,
            histogram_stats_blocked: false,
        }
    }
    pub fn evaluate(&mut self, expr: &Expr, t: i64) -> Result<Value> {
        self.eval(expr, t)
    }
    pub fn parse(query: &str) -> Result<Expr> {
        frontend::syntax(query)
    }
    pub fn parse_at(query: &str, start: i64, end: i64, step: i64) -> Result<Expr> {
        frontend::parse(query, start, end, step)
    }
    pub fn instant(&mut self, query: &str, timestamp: i64) -> Result<Value> {
        self.source = query.into();
        self.eval(&Self::parse_at(query, self.start, self.end, 0)?, timestamp)
    }
    pub fn range(&mut self, expr: &Expr, step: i64) -> Result<Value> {
        self.is_range = true;
        self.query_step = step;
        if self.source.is_empty() {
            self.source = expr.to_string();
        }
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
                Value::Scalar(value) => result.entry(Labels::new()).or_default().push(Point {
                    timestamp,
                    value,
                    ..Default::default()
                }),
                Value::Vector(samples) => {
                    for s in samples {
                        result.entry(s.labels).or_default().push(Point {
                            timestamp,
                            value: s.value,
                            histogram: s.histogram,
                            start_timestamp: s.start_timestamp,
                            drop_name: s.drop_name,
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
    pub(super) fn position(&self, expr: &Expr) -> String {
        let text = expr.to_string();
        let needle = text
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect::<String>();
        let mut compact = String::new();
        let mut indexes = Vec::new();
        for (i, c) in self.source.char_indices() {
            if !c.is_whitespace() {
                for _ in 0..c.len_utf8() {
                    indexes.push(i);
                }
                compact.push(c);
            }
        }
        let index = compact
            .find(&needle)
            .and_then(|i| indexes.get(i).copied())
            .or_else(|| {
                let selector = match expr {
                    Expr::MatrixSelector(m) => Some(&m.vs),
                    Expr::VectorSelector(v) => Some(v),
                    _ => None,
                };
                if let Some(v) = selector
                    && let Some(name) = &v.name
                {
                    for (idx, _) in compact.match_indices(name) {
                        let tail = &compact[idx + name.len()..];
                        if !tail.starts_with('{') {
                            if v.matchers.matchers.is_empty() {
                                return indexes.get(idx).copied();
                            }
                            continue;
                        }
                        let end = tail.find('}')?;
                        let candidate = &tail[..=end];
                        if v.matchers.matchers.iter().all(|m| {
                            candidate.contains(
                                &m.to_string()
                                    .chars()
                                    .filter(|c| !c.is_whitespace())
                                    .collect::<String>(),
                            )
                        }) {
                            return indexes.get(idx).copied();
                        }
                    }
                }
                let prefix = match expr {
                    Expr::MatrixSelector(m) => m.vs.name.clone(),
                    Expr::VectorSelector(v) => v.name.clone(),
                    Expr::Call(c) => Some(format!("{}(", c.func.name)),
                    Expr::Aggregate(a) => Some(a.op.to_string()),
                    _ => None,
                }?;
                let idx = compact.find(&prefix)?;
                indexes.get(idx).copied()
            })
            .unwrap_or(0);
        let before = &self.source[..index];
        let line = before.bytes().filter(|&b| b == b'\n').count() + 1;
        let column = before.rsplit('\n').next().unwrap_or("").chars().count() + 1;
        format!("{line}:{column}")
    }
    pub(super) fn annotation(&mut self, warning: bool, message: impl AsRef<str>, expr: &Expr) {
        let text = format!(
            "PromQL {}: {} ({})",
            if warning { "warning" } else { "info" },
            message.as_ref(),
            self.position(expr)
        );
        let annotations = if warning {
            &mut self.warnings
        } else {
            &mut self.infos
        };
        let base = text.rsplit_once(" (").map(|x| x.0).unwrap_or(&text);
        let position = |s: &str| -> (usize, usize) {
            s.rsplit_once(" (")
                .and_then(|(_, p)| p.trim_end_matches(')').split_once(':'))
                .map(|(l, c)| (l.parse().unwrap_or(0), c.parse().unwrap_or(0)))
                .unwrap_or_default()
        };
        if annotations.iter().any(|old| {
            old.rsplit_once(" (").map(|x| x.0) == Some(base) && position(old) > position(&text)
        }) {
            return;
        }
        annotations.retain(|old| old.rsplit_once(" (").map(|x| x.0) != Some(base));
        annotations.insert(text);
    }
    pub(super) fn monotonic_annotation(
        &mut self,
        metric: &str,
        t: i64,
        min: f64,
        max: f64,
        diff: f64,
        expr: &Expr,
    ) {
        let base = format!(
            "PromQL info: input to histogram_quantile needed to be fixed for monotonicity (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile) for metric name {metric:?}"
        );
        let (start, end, min, max, diff, count) = {
            let e = self
                .monotonic
                .entry(base.clone())
                .or_insert((t, t, min, max, diff, 0));
            if e.5 > 0 && e.1 == t {
                return;
            }
            e.0 = e.0.min(t);
            e.1 = e.1.max(t);
            e.2 = e.2.min(min);
            e.3 = e.3.max(max);
            e.4 = e.4.max(diff);
            e.5 = if e.5 == 0 { 1 } else { 2 * (e.5 + 1) }; // Upstream rangeEval merges the accumulated annotation twice per subsequent step.
            *e
        };
        let date = |t: i64| {
            chrono::DateTime::from_timestamp(t / 1000, 0)
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        };
        self.infos.retain(|v| !v.starts_with(&base));
        self.infos.insert(format!("{base}, from buckets {} to {}, with a max diff of {}, over {count} samples from {} to {} ({})",crate::model::float_string(min),crate::model::float_string(max),crate::model::float_string(diff),date(start),date(end),self.position(expr)));
    }
    fn charge(&mut self, count: usize) -> Result<()> {
        self.samples = self.samples.saturating_add(count);
        self.samples_read = self.samples_read.saturating_add(count);
        self.peak_samples = self.peak_samples.max(count);
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
        let old_stats = (self.histogram_stats, self.histogram_stats_blocked);
        if matches!(expr, Expr::Subquery(_))
            || matches!(expr, Expr::Binary(b) if matches!(b.op.to_string().as_str(),"</"|">/"))
            || matches!(expr, Expr::Call(c) if matches!(c.func.name,"histogram_quantile"|"histogram_quantiles"|"histogram_fraction"|"histogram_stddev"|"histogram_stdvar"))
        {
            self.histogram_stats_blocked = true;
            self.histogram_stats = false;
        }
        if !self.histogram_stats_blocked
            && matches!(expr, Expr::Call(c) if matches!(c.func.name,"histogram_count"|"histogram_sum"|"histogram_avg"))
        {
            self.histogram_stats = true;
        }
        let cache_key = if self.is_range && invariant(expr) {
            Some(format!(
                "{}:{}:{expr}",
                self.histogram_stats, self.histogram_stats_blocked
            ))
        } else {
            None
        };
        let mut result = if let Some(value) = cache_key
            .as_ref()
            .and_then(|key| self.invariant_cache.get(key))
        {
            let mut value = value.clone();
            if !matches!(expr, Expr::VectorSelector(_))
                && let Value::Vector(v) = &mut value
            {
                for s in v {
                    s.timestamp = timestamp;
                }
            }
            Ok(value)
        } else {
            let result = self.eval_inner(expr, timestamp);
            if let (Some(key), Ok(value)) = (cache_key, &result) {
                self.invariant_cache.insert(key, value.clone());
            }
            result
        };
        (self.histogram_stats, self.histogram_stats_blocked) = old_stats;
        self.depth -= 1;
        if self.depth == 0 {
            match &mut result {
                Ok(Value::Vector(v)) => {
                    for s in v {
                        if s.drop_name {
                            crate::model::drop_name(&mut s.labels);
                        }
                    }
                }
                Ok(Value::Matrix(v)) => {
                    for s in v {
                        if s.points.iter().any(|p| p.drop_name) {
                            crate::model::drop_name(&mut s.labels);
                        }
                    }
                }
                _ => {}
            }
        }
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
            Expr::Unary(u) if u.positive => self.eval(&u.expr, timestamp),
            Expr::Unary(u) => match self.eval(&u.expr, timestamp)? {
                Value::Scalar(v) => Ok(Value::Scalar(-v)),
                Value::Vector(mut v) => {
                    for s in &mut v {
                        s.value = -s.value;
                        if let Some(h) = &mut s.histogram {
                            h.scale(-1.0);
                            h.reset_hint = 3;
                        }
                        s.drop_name = true;
                    }
                    Ok(Value::Vector(v))
                }
                _ => bail!("unary expression requires scalar or instant vector"),
            },
            Expr::VectorSelector(v) => {
                let target = self.target(timestamp, &v.at, &v.offset)?;
                let mut samples = Vec::new();
                if v.smoothed {
                    let selected = self
                        .series
                        .iter()
                        .filter(|s| matches(v, &s.labels))
                        .collect::<Vec<_>>();
                    for s in selected {
                        match extended::smooth(s, target, self.lookback) {
                            Ok(Some(p)) => samples.push(Sample {
                                labels: s.labels.clone(),
                                value: p.value,
                                histogram: p.histogram,
                                timestamp: target,
                                ..Default::default()
                            }),
                            Ok(None) => {}
                            Err(e) => {
                                let metric = s.labels.get("__name__").cloned().unwrap_or_default();
                                let message = if e.to_string() == "mixed sample types" {
                                    format!(
                                        "encountered a mix of histograms and floats for metric name {metric:?}"
                                    )
                                } else {
                                    format!(
                                        "vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric:?}"
                                    )
                                };
                                self.annotation(true, message, expr);
                            }
                        }
                    }
                    self.charge(samples.len())?;
                    return Ok(Value::Vector(samples));
                }
                for s in self.series.iter().filter(|s| matches(v, &s.labels)) {
                    let index = s.points.partition_point(|p| p.timestamp <= target);
                    if index > 0 {
                        let p = &s.points[index - 1];
                        if p.timestamp > target.saturating_sub(self.lookback) && !p.stale() {
                            samples.push(Sample {
                                labels: s.labels.clone(),
                                timestamp: p.timestamp,
                                value: p.value,
                                histogram: resolved_histogram(s, index - 1, self.histogram_stats),
                                start_timestamp: p.start_timestamp,
                                ..Default::default()
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
                    let mut a = s.points.partition_point(|p| p.timestamp <= begin);
                    let mut b = s.points.partition_point(|p| p.timestamp <= target);
                    if (m.anchored || m.smoothed)
                        && a > 0
                        && s.points[a - 1].timestamp > begin.saturating_sub(self.lookback)
                    {
                        a -= 1;
                    }
                    if m.smoothed
                        && b < s.points.len()
                        && s.points[b].timestamp <= target.saturating_add(self.lookback)
                    {
                        b += 1;
                    }
                    let points = (a..b)
                        .filter(|&i| !s.points[i].stale())
                        .map(|i| {
                            let mut p = s.points[i].clone();
                            p.histogram = resolved_histogram(s, i, self.histogram_stats);
                            p
                        })
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
                            histogram: s.histogram.map(|mut h| {
                                if h.reset_hint != 3 {
                                    h.reset_hint = 0
                                }
                                h
                            }),
                            start_timestamp: 0,
                            drop_name: s.drop_name,
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

fn resolved_histogram(
    series: &Series,
    index: usize,
    stats: bool,
) -> Option<crate::histogram::Histogram> {
    let mut h = series.points[index].histogram.clone()?;
    if !stats {
        return Some(h);
    }
    if h.reset_hint == 0
        && index > 0
        && let Some(previous) = &series.points[index - 1].histogram
        && !series.points[index - 1].stale()
    {
        h.reset_hint = if h.detects_reset(previous) { 1 } else { 2 };
    }
    h.positive.clear();
    h.negative.clear();
    h.custom.clear();
    h.zero_count = 0.0;
    h.zero_threshold = 0.0;
    Some(h)
}

pub(super) fn invariant(expr: &Expr) -> bool {
    match expr {
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) => true,
        Expr::VectorSelector(v) => v.at.is_some(),
        Expr::MatrixSelector(m) => m.vs.at.is_some(),
        Expr::Subquery(s) => s.at.is_some(),
        Expr::Paren(p) => invariant(&p.expr),
        Expr::Unary(u) => invariant(&u.expr),
        Expr::Aggregate(a) => invariant(&a.expr) && a.param.as_ref().is_none_or(|p| invariant(p)),
        Expr::Binary(b) => invariant(&b.lhs) && invariant(&b.rhs),
        Expr::Call(c) => {
            !matches!(c.func.name, "time" | "info" | "predict_linear")
                && (c.func.name != "timestamp"
                    || matches!(&*c.args.args[0], Expr::VectorSelector(_)))
                && !matches!(
                    c.func.name,
                    "day_of_month"
                        | "day_of_week"
                        | "day_of_year"
                        | "days_in_month"
                        | "hour"
                        | "minute"
                        | "month"
                        | "year"
                )
                && c.args.args.iter().all(|a| invariant(a))
        }
        _ => false,
    }
}
