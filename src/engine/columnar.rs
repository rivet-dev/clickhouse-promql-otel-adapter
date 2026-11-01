//! Series-at-a-time evaluation of range-function subtrees for range queries.
//!
//! The step loop evaluates the whole expression once per step, so a node such
//! as `sum by (le) (rate(bucket[1m]))` touches every selected series' points
//! and labels again at every step, long after they left the cache. Eligible
//! subtrees are instead walked once per series with sliding window cursors
//! before the step loop and their per-step results kept in a dense
//! (output x step) table, which `Engine::eval` materializes for each step.
//!
//! Results are identical to the step loop's: every window is selected by the
//! same bounds and stale filtering, reduced by the same function, and
//! accumulated into its group in the same input order. Only the loop order
//! changes. Annotations and `samples_read` are produced from the same
//! per-window predicates and are order independent.
use super::{Engine, functions::extrapolate, seek};
use crate::model::{Labels, Point, Sample, Value};
use anyhow::Result;
use promql_parser::parser::{AggregateExpr, Call, Expr, MatrixSelector};
use std::collections::HashMap;

/// Per-step results of one precomputed node.
pub(super) struct Columnar {
    /// Output label sets in the order the step loop would emit them.
    labels: Vec<Labels>,
    /// `values[output * steps + step]`, meaningful where `present` is set.
    values: Vec<f64>,
    present: Vec<bool>,
    steps: usize,
}

impl Columnar {
    /// The node's instant vector at step `step`, as the step loop would have
    /// built it.
    pub(super) fn materialize(&self, step: usize, timestamp: i64) -> Value {
        let mut samples = Vec::new();
        for (output, labels) in self.labels.iter().enumerate() {
            let cell = output * self.steps + step;
            if self.present[cell] {
                samples.push(Sample {
                    labels: labels.clone(),
                    value: self.values[cell],
                    timestamp,
                    drop_name: true,
                    ..Default::default()
                });
            }
        }
        Value::Vector(samples)
    }
}

/// How a series' per-window values reach the output table.
#[derive(Clone, Copy)]
enum Reduce {
    /// One output per series.
    Each,
    Sum,
    Min,
    Max,
    Count,
}

/// Sum, correction and count per cell while series are walked; finalized
/// into a `Columnar` once every series has been seen.
struct Cells {
    reduce: Reduce,
    steps: usize,
    sums: Vec<f64>,
    corrections: Vec<f64>,
    counts: Vec<u32>,
}

impl Cells {
    fn new(reduce: Reduce, outputs: usize, steps: usize) -> Self {
        let cells = outputs * steps;
        Self {
            reduce,
            steps,
            sums: vec![0.0; cells],
            corrections: vec![0.0; cells],
            counts: vec![0; cells],
        }
    }

    fn accumulate(&mut self, output: usize, step: usize, value: f64) {
        let cell = output * self.steps + step;
        match self.reduce {
            Reduce::Each => self.sums[cell] = value,
            Reduce::Sum => {
                let (sum, correction) =
                    crate::numeric::add(value, self.sums[cell], self.corrections[cell]);
                self.sums[cell] = sum;
                self.corrections[cell] = correction;
            }
            Reduce::Min => {
                self.sums[cell] = if self.counts[cell] == 0 {
                    value
                } else {
                    self.sums[cell].min(value)
                }
            }
            Reduce::Max => {
                self.sums[cell] = if self.counts[cell] == 0 {
                    value
                } else {
                    self.sums[cell].max(value)
                }
            }
            Reduce::Count => {}
        }
        self.counts[cell] += 1;
    }

    /// Finalizes the table with outputs reordered by `order`.
    fn finish(self, labels: &[Labels], order: &[usize]) -> Columnar {
        let mut values = Vec::with_capacity(self.sums.len());
        let mut present = Vec::with_capacity(self.sums.len());
        for &output in order {
            for cell in output * self.steps..(output + 1) * self.steps {
                present.push(self.counts[cell] > 0);
                values.push(match self.reduce {
                    Reduce::Each | Reduce::Min | Reduce::Max => self.sums[cell],
                    Reduce::Sum => self.sums[cell] + self.corrections[cell],
                    Reduce::Count => f64::from(self.counts[cell]),
                });
            }
        }
        Columnar {
            labels: order.iter().map(|&output| labels[output].clone()).collect(),
            values,
            present,
            steps: self.steps,
        }
    }
}

/// The matrix selector of an eligible range function call. Modifiers that
/// take the extended path (`@`, anchored, smoothed) are left to the step loop.
fn range_call(call: &Call) -> Option<&MatrixSelector> {
    if !matches!(call.func.name, "rate" | "increase" | "delta") || call.args.args.len() != 1 {
        return None;
    }
    let Expr::MatrixSelector(selector) = &*call.args.args[0] else {
        return None;
    };
    if selector.vs.at.is_some() || selector.anchored || selector.smoothed {
        return None;
    }
    Some(selector)
}

fn reduction(aggregate: &AggregateExpr) -> Option<Reduce> {
    if aggregate.param.is_some() {
        return None;
    }
    Some(match aggregate.op.to_string().as_str() {
        "sum" => Reduce::Sum,
        "min" => Reduce::Min,
        "max" => Reduce::Max,
        "count" => Reduce::Count,
        _ => return None,
    })
}

/// True when a counter window carries a start timestamp that overlaps the
/// previous sample, which `range_function` reports as a warning.
fn overlapping_starts(window: &[Point]) -> bool {
    window.windows(2).any(|pair| {
        pair[1].start_timestamp != 0
            && pair[1].start_timestamp < pair[0].timestamp
            && pair[1].start_timestamp != pair[0].start_timestamp
    })
}

impl Engine<'_> {
    /// Precomputes every eligible subtree of `expr` for all steps of the
    /// current range query. Subqueries are never entered: their inner
    /// evaluations are not on the step grid.
    pub(super) fn precompute(&mut self, expr: &Expr) -> Result<()> {
        match expr {
            Expr::Aggregate(aggregate) => {
                if let Some(reduce) = reduction(aggregate)
                    && let Expr::Call(call) = &*aggregate.expr
                    && let Some(selector) = range_call(call)
                    && self.precompute_range_call(expr, Some(aggregate), reduce, call, selector)?
                {
                    return Ok(());
                }
                self.precompute(&aggregate.expr)?;
                if let Some(param) = &aggregate.param {
                    self.precompute(param)?;
                }
                Ok(())
            }
            Expr::Call(call) => {
                if let Some(selector) = range_call(call)
                    && self.precompute_range_call(expr, None, Reduce::Each, call, selector)?
                {
                    return Ok(());
                }
                for arg in &call.args.args {
                    self.precompute(arg)?;
                }
                Ok(())
            }
            Expr::Binary(binary) => {
                self.precompute(&binary.lhs)?;
                self.precompute(&binary.rhs)
            }
            Expr::Paren(paren) => self.precompute(&paren.expr),
            Expr::Unary(unary) => self.precompute(&unary.expr),
            Expr::Subquery(_)
            | Expr::VectorSelector(_)
            | Expr::MatrixSelector(_)
            | Expr::NumberLiteral(_)
            | Expr::StringLiteral(_)
            | Expr::Extension(_) => Ok(()),
        }
    }

    /// Precomputes `node`, a range function call or an aggregation of one.
    /// Returns false, before producing any annotation, when the node must
    /// stay on the step loop: a selected series holds native histograms, or
    /// the table would not fit the sample budget.
    fn precompute_range_call(
        &mut self,
        node: &Expr,
        aggregate: Option<&AggregateExpr>,
        reduce: Reduce,
        call: &Call,
        selector: &MatrixSelector,
    ) -> Result<bool> {
        let name = call.func.name;
        let counter = name != "delta";
        let rate = name == "rate";
        let range: i64 = selector.range.as_millis().try_into()?;
        let offset = super::offset_millis(&selector.vs.offset)?;
        let selector_expr = &*call.args.args[0];

        // Select as the step loop would, with the call's counter hint.
        let hint = self.counter_hint;
        self.counter_hint = matches!(name, "rate" | "increase");
        let selected = self.selected(&selector.vs);
        self.counter_hint = hint;
        let selected = selected?;

        // One sequential pass per series decides eligibility and remembers
        // which series need stale markers filtered from their windows.
        let mut has_stale = Vec::with_capacity(selected.len());
        for series in &selected {
            let mut stale = false;
            for point in &series.points {
                if point.histogram.is_some() {
                    return Ok(false);
                }
                stale |= point.stale();
            }
            has_stale.push(stale);
        }

        // Assign outputs before allocating, so the budget check sees the
        // table size.
        let mut labels: Vec<Labels> = Vec::new();
        let mut output_of = Vec::with_capacity(selected.len());
        let mut index: HashMap<Labels, usize> = HashMap::new();
        for series in &selected {
            let key = match aggregate {
                Some(aggregate) => self.group_key(aggregate, &series.labels),
                None => series.labels.clone(),
            };
            let output = match index.get(&key) {
                Some(&output) => output,
                None => {
                    labels.push(key.clone());
                    index.insert(key, labels.len() - 1);
                    labels.len() - 1
                }
            };
            output_of.push(output);
        }
        let steps = self.step_times.clone();
        if labels.len().saturating_mul(steps.len()) > self.max_samples / 4 {
            return Ok(false);
        }
        let mut cells = Cells::new(reduce, labels.len(), steps.len());

        let mut scratch: Vec<Point> = Vec::new();
        for (series_index, series) in selected.iter().enumerate() {
            let output = output_of[series_index];
            let points = &series.points;
            let mut begin_cursor = (i64::MIN, 0usize);
            let mut end_cursor = (i64::MIN, 0usize);
            let mut read = 0usize;
            let mut produced = false;
            for (step, &t) in steps.iter().enumerate() {
                let end = t
                    .checked_sub(offset)
                    .ok_or_else(|| anyhow::anyhow!("timestamp overflow"))?;
                let start = end.saturating_sub(range);
                let begin = seek(points, &mut begin_cursor, start);
                let until = seek(points, &mut end_cursor, end);
                if begin >= until {
                    continue;
                }
                let window: &[Point] = if has_stale[series_index] {
                    scratch.clear();
                    scratch.extend(points[begin..until].iter().filter(|p| !p.stale()).cloned());
                    &scratch
                } else {
                    &points[begin..until]
                };
                if window.is_empty() {
                    continue;
                }
                read += window.len();
                if counter && overlapping_starts(window) {
                    self.overlap_annotation(&series.labels, selector_expr);
                }
                let starts_inside = counter
                    && window[0].start_timestamp > start
                    && window[0].start_timestamp < window[0].timestamp
                    && window[0].start_timestamp != 0;
                if window.len() < 2 && !starts_inside {
                    continue;
                }
                cells.accumulate(output, step, extrapolate(window, start, end, counter, rate));
                produced = true;
            }
            self.charge(read)?;
            if produced && counter {
                self.counter_name_annotation(&series.labels, selector_expr);
            }
        }

        // Aggregates are emitted in label order, like the step loop's groups;
        // per-series outputs keep selection order.
        let mut order = (0..labels.len()).collect::<Vec<_>>();
        if !matches!(reduce, Reduce::Each) {
            order.sort_unstable_by(|&a, &b| labels[a].cmp(&labels[b]));
        }
        let columnar = cells.finish(&labels, &order);
        self.cached_samples = self.cached_samples.saturating_add(columnar.values.len());
        self.precomputed
            .insert(std::ptr::from_ref(node) as usize, columnar);
        Ok(true)
    }
}
