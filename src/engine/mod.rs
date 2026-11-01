mod aggregate;
mod binary;
mod columnar;
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
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    time::{Duration, Instant, UNIX_EPOCH},
};

pub struct Engine<'a> {
    pub replica_labels: Vec<String>,
    counter_hint: bool,
    pub series: &'a [Series],
    pub start: i64,
    pub end: i64,
    pub lookback: i64,
    pub default_step: i64,
    pub max_samples: usize,
    pub deadline: Instant,
    pub cancelled: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    resident_samples: usize,
    cached_samples: usize,
    pub samples_read: usize,
    pub peak_samples: usize,
    depth: usize,
    pub warnings: BTreeSet<String>,
    pub infos: BTreeSet<String>,
    pub source: String,
    pub is_range: bool,
    pub query_step: i64,
    invariant_cache: BTreeMap<String, Value>,
    // Match labels once per query, not once per evaluation timestamp.
    // Keyed by the selector's address, which is stable for one evaluation;
    // a hit is confirmed by structural equality, since temporary selectors
    // can reuse an address.
    selector_cache: HashMap<usize, Vec<(VectorSelector, Vec<&'a Series>)>>,
    selector_cache_entries: usize,
    overlaps: BTreeMap<String, usize>,
    monotonic: BTreeMap<String, (i64, i64, f64, f64, f64, i64)>,
    histogram_stats: bool,
    histogram_stats_blocked: bool,
    /// Group or match key per (aggregation or binary node, input label set),
    /// reused across steps. The input labels are retained so their identity
    /// stays unique.
    node_keys: HashMap<(usize, usize), (Labels, Labels)>,
    /// One shared allocation per distinct group or match key, so equal keys
    /// compare by pointer.
    interned_keys: HashSet<Labels>,
    /// Per selector edge, each selected series' last (bound, index), so steps
    /// that move forward in time advance instead of binary searching.
    cursors: HashMap<(usize, u8), Vec<(i64, usize)>>,
    /// Compiled label_replace patterns.
    regexes: HashMap<String, regex::Regex>,
    /// label_replace/label_join output per (call site, input label set). The
    /// input labels are retained so their identity stays unique.
    relabeled: HashMap<(usize, usize), (Labels, Labels, bool)>,
    charges: u32,
    /// Subtrees evaluated for every step before the step loop, by node
    /// address. Addresses are stable for one evaluation; the map is cleared
    /// when a new expression is evaluated.
    precomputed: HashMap<usize, columnar::Columnar>,
    /// Evaluation timestamps of the current range query, in order.
    step_times: Vec<i64>,
    /// Classic bucket grouping per histogram_quantile call site, by the
    /// address of the call's vector argument.
    bucket_sites: HashMap<usize, distributions::BucketSite>,
    /// Result labels with the metric name removed, per input label set, so a
    /// result series keeps one shared label allocation across steps. The
    /// input labels are retained so their identity stays unique.
    dropped_names: HashMap<usize, (Labels, Labels)>,
}
impl<'a> Engine<'a> {
    pub fn new(series: &'a [Series], start: i64, end: i64) -> Self {
        Self {
            series,
            replica_labels: Vec::new(),
            counter_hint: false,
            start,
            end,
            lookback: 300_000,
            default_step: 60_000,
            max_samples: 5_000_000,
            deadline: Instant::now() + Duration::from_secs(120),
            cancelled: None,
            resident_samples: 0,
            cached_samples: 0,
            samples_read: 0,
            peak_samples: 0,
            depth: 0,
            warnings: BTreeSet::new(),
            infos: BTreeSet::new(),
            source: String::new(),
            is_range: false,
            query_step: 0,
            invariant_cache: BTreeMap::new(),
            selector_cache: HashMap::new(),
            selector_cache_entries: 0,
            overlaps: BTreeMap::new(),
            monotonic: BTreeMap::new(),
            histogram_stats: false,
            histogram_stats_blocked: false,
            node_keys: HashMap::new(),
            interned_keys: HashSet::new(),
            cursors: HashMap::new(),
            regexes: HashMap::new(),
            relabeled: HashMap::new(),
            charges: 0,
            precomputed: HashMap::new(),
            step_times: Vec::new(),
            bucket_sites: HashMap::new(),
            dropped_names: HashMap::new(),
        }
    }
    pub fn evaluate(&mut self, expr: &Expr, t: i64) -> Result<Value> {
        self.selector_cache.clear();
        self.selector_cache_entries = 0;
        self.precomputed.clear();
        self.bucket_sites.clear();
        self.dropped_names.clear();
        self.eval(expr, t)
    }
    pub fn parse(query: &str) -> Result<Expr> {
        frontend::syntax(query)
    }
    pub fn parse_at(query: &str, start: i64, end: i64, step: i64) -> Result<Expr> {
        frontend::parse(query, start, end, step)
    }
    pub fn instant(&mut self, query: &str, timestamp: i64) -> Result<Value> {
        self.selector_cache.clear();
        self.selector_cache_entries = 0;
        self.precomputed.clear();
        self.bucket_sites.clear();
        self.dropped_names.clear();
        self.source = query.into();
        self.eval(&Self::parse_at(query, self.start, self.end, 0)?, timestamp)
    }
    pub fn range(&mut self, expr: &Expr, step: i64) -> Result<Value> {
        self.selector_cache.clear();
        self.selector_cache_entries = 0;
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
        self.precomputed.clear();
        self.bucket_sites.clear();
        self.dropped_names.clear();
        self.step_times = step_grid(self.start, self.end, step);
        self.precompute(expr)?;
        // Result series are indexed by label hash while steps are appended and
        // sorted by labels once at the end, the order a BTreeMap would give.
        // Steps that reuse an output's label allocation hit the index by
        // pointer equality, without comparing label content.
        let mut index: HashMap<Labels, usize> = HashMap::new();
        let mut result: Vec<(Labels, Vec<Point>)> = Vec::new();
        let mut retained = 0usize;
        for step_index in 0..self.step_times.len() {
            let timestamp = self.step_times[step_index];
            self.resident_samples = retained;
            let value = self.eval(expr, timestamp)?;
            retained = retained.saturating_add(value_samples(&value));
            match value {
                Value::Scalar(value) => {
                    let slot = result_slot(&mut index, &mut result, Labels::new());
                    result[slot].1.push(Point {
                        timestamp,
                        value,
                        ..Default::default()
                    })
                }
                Value::Vector(samples) => {
                    for s in samples {
                        let slot = result_slot(&mut index, &mut result, s.labels);
                        result[slot].1.push(Point {
                            timestamp,
                            value: s.value,
                            histogram: s.histogram.map(Box::new),
                            start_timestamp: s.start_timestamp,
                            drop_name: s.drop_name,
                        });
                    }
                }
                _ => bail!(
                    "invalid expression type for range query, must be Scalar or instant Vector"
                ),
            }
            self.account(retained)?;
        }
        result.sort_by(|a, b| a.0.cmp(&b.0));
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
            e.5 = if e.5 == 0 {
                1
            } else {
                e.5.wrapping_add(1).wrapping_mul(2)
            }; // Upstream rangeEval merges the accumulated annotation twice per subsequent step.
            *e
        };
        let date = |t: i64| timestamp_date(t);
        self.infos.retain(|v| !v.starts_with(&base));
        self.infos.insert(format!("{base}, from buckets {} to {}, with a max diff of {}, over {count} samples from {} to {} ({})",crate::model::float_string(min),crate::model::float_string(max),crate::model::float_string(diff),date(start),date(end),self.position(expr)));
    }
    /// `labels` without the metric name, computed once per input label set.
    fn without_name(&mut self, labels: &Labels) -> Labels {
        const MAX_CACHED_LABELS: usize = 1 << 20;
        let id = labels.identity();
        if let Some((_, dropped)) = self.dropped_names.get(&id) {
            return dropped.clone();
        }
        let mut dropped = labels.clone();
        crate::model::drop_name(&mut dropped);
        if self.dropped_names.len() < MAX_CACHED_LABELS {
            self.dropped_names
                .insert(id, (labels.clone(), dropped.clone()));
        }
        dropped
    }
    fn charge(&mut self, count: usize) -> Result<()> {
        self.samples_read = self.samples_read.saturating_add(count);
        // Reading the clock per sample was a measurable share of evaluation;
        // a timeout or cancellation is noticed on the first charge and then
        // within every 256 charges.
        let check = self.charges.is_multiple_of(256);
        self.charges = self.charges.wrapping_add(1);
        if !check {
            return Ok(());
        }
        if Instant::now() > self.deadline
            || self
                .cancelled
                .as_ref()
                .is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        {
            bail!("query timed out in expression evaluation")
        }
        Ok(())
    }
    fn account(&mut self, samples: usize) -> Result<()> {
        self.resident_samples = samples;
        let total = samples.saturating_add(self.cached_samples);
        self.peak_samples = self.peak_samples.max(total);
        if total > self.max_samples {
            bail!("query processing would load too many samples into memory in query execution");
        }
        self.charge(0)
    }
    fn eval(&mut self, expr: &Expr, timestamp: i64) -> Result<Value> {
        self.charge(0)?;
        let resident_before = self.resident_samples;
        if self.depth >= 128 {
            bail!("expression nesting exceeds limit")
        }
        self.depth += 1;
        let old_stats = (self.histogram_stats, self.histogram_stats_blocked);
        let old_counter = self.counter_hint;
        if let Expr::Call(c) = expr {
            self.counter_hint = matches!(c.func.name, "rate" | "irate" | "increase" | "resets");
        }
        if matches!(expr, Expr::Aggregate(_) | Expr::Binary(_)) {
            self.counter_hint = false;
        }
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
        // Literals are cheaper to evaluate than to look up by their text.
        let literal = matches!(expr, Expr::NumberLiteral(_) | Expr::StringLiteral(_));
        let cache_key = if self.is_range && !literal && invariant(expr) {
            Some(format!(
                "{}:{}:{}:{expr}",
                self.histogram_stats, self.histogram_stats_blocked, self.counter_hint
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
        } else if let Some(step) = self.step_index(timestamp)
            && let Some(columnar) = self.precomputed.get(&(std::ptr::from_ref(expr) as usize))
        {
            Ok(columnar.materialize(step, timestamp))
        } else {
            let result = self.eval_inner(expr, timestamp);
            if let (Some(key), Ok(value)) = (cache_key, &result) {
                self.cached_samples = self.cached_samples.saturating_add(value_samples(value));
                self.invariant_cache.insert(key, value.clone());
            }
            result
        };
        (self.histogram_stats, self.histogram_stats_blocked) = old_stats;
        self.counter_hint = old_counter;
        self.depth -= 1;
        if self.depth == 0 {
            match &mut result {
                Ok(Value::Vector(v)) => {
                    for s in v {
                        if s.drop_name {
                            s.labels = self.without_name(&s.labels);
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
        // Selectors, range functions and aggregations produce unique label
        // sets by construction. Only vector matching, relabeling and added
        // labels can collide, and the root once names are dropped.
        let may_collide = self.depth == 0
            || match expr {
                Expr::Binary(_) => true,
                Expr::Call(c) => matches!(
                    c.func.name,
                    "label_replace" | "label_join" | "histogram_quantiles" | "info"
                ),
                _ => false,
            };
        if may_collide && let Ok(Value::Vector(v)) = &result {
            // Only membership matters here. Hash complete labelsets once instead
            // of repeatedly comparing their shared prefixes; preserve v's order.
            let mut seen = HashSet::with_capacity(v.len());
            for sample in v {
                if !seen.insert(&sample.labels) {
                    bail!("vector cannot contain metrics with the same labelset")
                }
            }
        }
        if let Ok(value) = &result {
            self.account(resident_before.saturating_add(value_samples(value)))?;
        } else {
            self.resident_samples = resident_before;
        }
        result
    }
    fn selected(&mut self, v: &VectorSelector) -> Result<Vec<std::borrow::Cow<'a, Series>>> {
        let key = std::ptr::from_ref(v) as usize;
        let cached = self
            .selector_cache
            .get(&key)
            .and_then(|entries| entries.iter().position(|(selector, _)| selector == v));
        if let Some(index) = cached {
            return Ok(self.select_replicas(&self.selector_cache[&key][index].1));
        }
        {
            let mut selected = Vec::new();
            for (index, series) in self.series.iter().enumerate() {
                if index % 1024 == 0 {
                    self.charge(0)?;
                }
                if matches(v, &series.labels) {
                    selected.push(series);
                }
            }
            // Bound reference storage for queries with many broad selectors. A
            // cache miss is safe; failing an otherwise small result is not.
            if selected.len() > self.max_samples.saturating_sub(self.selector_cache_entries) {
                return Ok(self.select_replicas(&selected));
            }
            self.selector_cache_entries += selected.len();
            let result = self.select_replicas(&selected);
            self.selector_cache
                .entry(key)
                .or_default()
                .push((v.clone(), selected));
            Ok(result)
        }
    }
    /// Cursors for one selector edge, reset if the selection changed size.
    /// Callers put them back after use; an early error simply drops them.
    fn take_cursors(&mut self, key: (usize, u8), len: usize) -> Vec<(i64, usize)> {
        match self.cursors.remove(&key) {
            Some(cursors) if cursors.len() == len => cursors,
            _ => vec![(i64::MIN, 0); len],
        }
    }
    fn select_replicas(&self, selected: &[&'a Series]) -> Vec<std::borrow::Cow<'a, Series>> {
        if self.replica_labels.is_empty() {
            selected
                .iter()
                .map(|&series| std::borrow::Cow::Borrowed(series))
                .collect()
        } else {
            crate::dedup::merge(selected, &self.replica_labels, self.counter_hint)
                .into_iter()
                .map(std::borrow::Cow::Owned)
                .collect()
        }
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
                let resident_before = self.resident_samples;
                let mut retained = 0usize;
                if v.smoothed {
                    let selected = self.selected(v)?;
                    for (index, s) in selected.into_iter().enumerate() {
                        if index % 1024 == 0 {
                            self.charge(0)?;
                        }
                        match extended::smooth(&s, target, self.lookback) {
                            Ok(Some(p)) => {
                                retained =
                                    retained.saturating_add(sample_size(p.histogram.as_deref()));
                                self.account(resident_before.saturating_add(retained))?;
                                samples.push(Sample {
                                    labels: s.labels.clone(),
                                    value: p.value,
                                    histogram: p.histogram.map(|h| *h),
                                    timestamp: target,
                                    ..Default::default()
                                });
                            }
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
                let selected = self.selected(v)?;
                let mut cursors =
                    self.take_cursors((std::ptr::from_ref(v) as usize, 0), selected.len());
                for (series_index, s) in selected.into_iter().enumerate() {
                    if series_index % 1024 == 0 {
                        self.charge(0)?;
                    }
                    let index = seek(&s.points, &mut cursors[series_index], target);
                    if index > 0 {
                        let p = &s.points[index - 1];
                        if p.timestamp > target.saturating_sub(self.lookback) && !p.stale() {
                            retained = retained.saturating_add(if self.histogram_stats {
                                1
                            } else {
                                sample_size(p.histogram.as_deref())
                            });
                            self.account(resident_before.saturating_add(retained))?;
                            samples.push(Sample {
                                labels: s.labels.clone(),
                                timestamp: p.timestamp,
                                value: p.value,
                                histogram: resolved_histogram(&s, index - 1, self.histogram_stats),
                                start_timestamp: p.start_timestamp,
                                ..Default::default()
                            })
                        }
                    }
                }
                self.cursors
                    .insert((std::ptr::from_ref(v) as usize, 0), cursors);
                self.charge(samples.len())?;
                Ok(Value::Vector(samples))
            }
            Expr::MatrixSelector(m) => {
                let target = self.target(timestamp, &m.vs.at, &m.vs.offset)?;
                let begin = target.saturating_sub(m.range.as_millis().try_into()?);
                let mut result = Vec::new();
                let resident_before = self.resident_samples;
                let mut retained = 0usize;
                let selected = self.selected(&m.vs)?;
                let key = std::ptr::from_ref(&m.vs) as usize;
                let mut begins = self.take_cursors((key, 1), selected.len());
                let mut ends = self.take_cursors((key, 2), selected.len());
                for (series_index, s) in selected.into_iter().enumerate() {
                    if series_index % 1024 == 0 {
                        self.charge(0)?;
                    }
                    let mut a = seek(&s.points, &mut begins[series_index], begin);
                    let mut b = seek(&s.points, &mut ends[series_index], target);
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
                    let mut points = Vec::new();
                    for i in a..b {
                        if i % 1024 == 0 {
                            self.charge(0)?;
                        }
                        if s.points[i].stale() {
                            continue;
                        }
                        retained = retained.saturating_add(if self.histogram_stats {
                            1
                        } else {
                            sample_size(s.points[i].histogram.as_deref())
                        });
                        // Reject over-budget ranges before cloning their samples/buckets.
                        self.account(resident_before.saturating_add(retained))?;
                        let p = &s.points[i];
                        points.push(Point {
                            timestamp: p.timestamp,
                            value: p.value,
                            histogram: resolved_histogram(&s, i, self.histogram_stats)
                                .map(Box::new),
                            start_timestamp: p.start_timestamp,
                            drop_name: p.drop_name,
                        });
                    }
                    if !points.is_empty() {
                        result.push(Series {
                            labels: s.labels.clone(),
                            points,
                        });
                    }
                }
                self.cursors.insert((key, 1), begins);
                self.cursors.insert((key, 2), ends);
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
                                Box::new(h)
                            }),
                            start_timestamp: 0,
                            drop_name: s.drop_name,
                        });
                        self.charge(0)?;
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
        t.checked_sub(offset_millis(offset)?)
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))
    }
    /// Index of `timestamp` on the current range query's step grid, if it is
    /// an evaluation step. Subquery steps and instant queries are not.
    fn step_index(&self, timestamp: i64) -> Option<usize> {
        if self.query_step <= 0 {
            return None;
        }
        let elapsed = timestamp.checked_sub(self.start)?;
        if elapsed < 0 || elapsed % self.query_step != 0 {
            return None;
        }
        let index = usize::try_from(elapsed / self.query_step).ok()?;
        if index < self.step_times.len() {
            Some(index)
        } else {
            None
        }
    }
}

/// Milliseconds an `offset` modifier moves the evaluation time back.
fn offset_millis(offset: &Option<Offset>) -> Result<i64> {
    Ok(match offset {
        Some(Offset::Pos(d)) => i64::try_from(d.as_millis())?,
        Some(Offset::Neg(d)) => -i64::try_from(d.as_millis())?,
        None => 0,
    })
}

/// Evaluation timestamps of a range query: `start` and every following step
/// up to `end`. Bounded by the resolution limit checked before use.
fn step_grid(start: i64, end: i64, step: i64) -> Vec<i64> {
    let mut times = vec![start];
    let mut t = start;
    while let Some(next) = t.checked_add(step)
        && next <= end
    {
        times.push(next);
        t = next;
    }
    times
}

/// Slot of `labels` in `result`, appended on first use.
fn result_slot(
    index: &mut HashMap<Labels, usize>,
    result: &mut Vec<(Labels, Vec<Point>)>,
    labels: Labels,
) -> usize {
    if let Some(&slot) = index.get(&labels) {
        return slot;
    }
    result.push((labels.clone(), Vec::new()));
    index.insert(labels, result.len() - 1);
    result.len() - 1
}

/// Number of points with `timestamp <= bound`, like `partition_point` on
/// sorted points. A cursor from an earlier, smaller bound advances a few
/// points instead of searching again. The result is verified against its
/// neighbours, so a stale or foreign cursor costs a binary search, never a
/// wrong index.
fn seek(points: &[Point], cursor: &mut (i64, usize), bound: i64) -> usize {
    const MAX_ADVANCE: usize = 8;
    let (previous, mut index) = *cursor;
    let advanced = bound >= previous
        && index <= points.len()
        && (index == 0 || points[index - 1].timestamp <= bound)
        && {
            let limit = (index + MAX_ADVANCE).min(points.len());
            while index < limit && points[index].timestamp <= bound {
                index += 1;
            }
            index == points.len() || points[index].timestamp > bound
        };
    if !advanced {
        index = points.partition_point(|p| p.timestamp <= bound);
    }
    *cursor = (bound, index);
    index
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
    let source = series.points[index].histogram.as_deref()?;
    if !stats {
        return Some(source.clone());
    }
    // Statistics-only functions need count/sum, not copies of every bucket.
    let mut h = crate::histogram::Histogram {
        schema: source.schema,
        count: source.count,
        sum: source.sum,
        reset_hint: source.reset_hint,
        ..Default::default()
    };
    if h.reset_hint == 0
        && index > 0
        && let Some(previous) = &series.points[index - 1].histogram
        && !series.points[index - 1].stale()
    {
        h.reset_hint = if source.detects_reset(previous) { 1 } else { 2 };
    }
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

// Proleptic Gregorian UTC rendering across Prometheus's full int64-ms domain.
fn timestamp_date(ms: i64) -> String {
    let seconds = ms / 1000;
    let day = seconds.div_euclid(86400);
    let time = seconds.rem_euclid(86400);
    let z = day + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(m <= 2);
    let year = if year < 0 {
        format!("-{:04}", -year)
    } else {
        format!("{year:04}")
    };
    format!(
        "{year}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        time / 3600,
        time % 3600 / 60,
        time % 60
    )
}

fn sample_size(histogram: Option<&crate::histogram::Histogram>) -> usize {
    // Account for histogram bucket storage as well as the sample itself.
    histogram.map_or(1, |h| {
        1 + h.positive.len() + h.negative.len() + h.custom.len()
    })
}
fn value_samples(value: &Value) -> usize {
    match value {
        Value::Scalar(_) => 1,
        Value::String(_) => 0,
        Value::Vector(v) => v.iter().map(|s| sample_size(s.histogram.as_ref())).sum(),
        Value::Matrix(v) => v
            .iter()
            .flat_map(|s| s.points.iter())
            .map(|p| sample_size(p.histogram.as_deref()))
            .sum(),
    }
}

#[cfg(test)]
mod annotation_dates {
    #[test]
    fn negative_fraction_and_year() {
        assert_eq!(super::timestamp_date(-1), "1970-01-01T00:00:00Z");
        assert_eq!(
            super::timestamp_date(-62_198_755_200_000),
            "-0001-01-01T00:00:00Z"
        );
        assert!(super::timestamp_date(i64::MIN).ends_with('Z'));
    }
}
