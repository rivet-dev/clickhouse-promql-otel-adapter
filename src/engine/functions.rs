use super::{
    Engine,
    aggregate::{quantile, valid_label, variance},
};
use crate::model::{Labels, Sample, Series, Value};
use anyhow::{Result, bail};
use chrono::{Datelike, Timelike};
use promql_parser::{
    label::MatchOp,
    parser::{Call, Expr},
};

impl Engine<'_> {
    pub(super) fn call(&mut self, c: &Call, t: i64) -> Result<Value> {
        let name = c.func.name;
        if matches!(name, "label_replace" | "label_join") {
            if super::aggregate::invalid_utf8_literal(&c.args.args[1]) {
                let text = self.eval(&c.args.args[1], t)?.string()?.to_owned();
                bail!("invalid destination label name in {name}(): {text}");
            }
            if name == "label_join" {
                for arg in c.args.args.iter().skip(3) {
                    if super::aggregate::invalid_utf8_literal(arg) {
                        let text = self.eval(arg, t)?.string()?.to_owned();
                        bail!("invalid source label name in label_join(): {text}");
                    }
                }
            }
        }
        if name == "start_timestamp" && !matches!(&*c.args.args[0], Expr::VectorSelector(_)) {
            return Ok(Value::Vector(vec![]));
        }
        if name == "info" {
            return self.info(c, t);
        }
        if self.is_range
            && !super::invariant(&Expr::Call(c.clone()))
            && matches!(
                name,
                "sort" | "sort_desc" | "sort_by_label" | "sort_by_label_desc"
            )
        {
            self.annotation(
                true,
                "sort is ineffective for range queries since results are always ordered by labels",
                &Expr::Call(c.clone()),
            );
        }

        if matches!(name, "start" | "end" | "step" | "range") {
            return Ok(Value::Scalar(
                match name {
                    "start" => self.start,
                    "end" => self.end,
                    "step" => self.query_step,
                    _ => self.end - self.start,
                } as f64
                    / 1000.0,
            ));
        }
        if name == "time" {
            return Ok(Value::Scalar(t as f64 / 1000.0));
        }
        if name == "pi" {
            return Ok(Value::Scalar(std::f64::consts::PI));
        }
        for arg in &c.args.args {
            if let Expr::MatrixSelector(m) = &**arg {
                if m.smoothed && !matches!(name, "rate" | "increase" | "delta") {
                    bail!(
                        "smoothed modifier can only be used with: delta, increase, rate - not with {name}"
                    );
                }
                if m.anchored
                    && !matches!(name, "rate" | "increase" | "delta" | "changes" | "resets")
                {
                    bail!(
                        "anchored modifier can only be used with: changes, delta, increase, rate, resets - not with {name}"
                    );
                }
            }
        }
        let mut args = c
            .args
            .args
            .iter()
            .map(|a| self.eval(a, t))
            .collect::<Result<Vec<_>>>()?;
        if matches!(name, "histogram_quantile" | "quantile_over_time")
            && (name != "quantile_over_time"
                || matches!(&args[1], Value::Matrix(series) if !series.is_empty()))
        {
            let q = args[0].scalar()?;
            if q.is_nan() || !(0.0..=1.0).contains(&q) {
                self.annotation(
                    true,
                    format!(
                        "quantile value should be between 0 and 1, got {}",
                        crate::model::float_string(q)
                    ),
                    &c.args.args[0],
                );
            }
        }
        match name {
            "min_of" | "max_of" => {
                let a = args[0].scalar()?;
                let b = args[1].scalar()?;
                return Ok(Value::Scalar(if a.is_nan() || b.is_nan() {
                    f64::NAN
                } else if name == "min_of" {
                    a.min(b)
                } else {
                    a.max(b)
                }));
            }
            "histogram_quantiles" => {
                let vector = args[0].clone();
                let label = args[1].string()?;
                let mut result = Vec::new();
                for (index, q) in args[2..].iter().enumerate() {
                    let q = q.scalar()?;
                    if q.is_nan() || !(0.0..=1.0).contains(&q) {
                        self.annotation(
                            true,
                            format!(
                                "quantile value should be between 0 and 1, got {}",
                                crate::model::float_string(q)
                            ),
                            &c.args.args[index + 2],
                        );
                    }
                    let mut values = super::distributions::quantile(
                        self,
                        q,
                        vector.clone().vector()?,
                        t,
                        &c.args.args[0],
                    )?
                    .vector()?;
                    for s in &mut values {
                        let mut text = crate::model::float_string(q);
                        if q.is_finite() && !text.contains(['.', 'e', 'E']) {
                            text.push_str(".0");
                        }
                        s.labels.insert(label.into(), text);
                    }
                    result.extend(values);
                }
                return Ok(Value::Vector(result));
            }
            "histogram_fraction" => {
                return super::distributions::fraction(
                    self,
                    args[0].scalar()?,
                    args[1].scalar()?,
                    args.remove(2).vector()?,
                    t,
                    &c.args.args[2],
                );
            }
            "vector" => {
                return Ok(Value::Vector(vec![Sample {
                    labels: Labels::new(),
                    value: args[0].scalar()?,
                    timestamp: t,
                    ..Default::default()
                }]));
            }
            "scalar" => {
                let v = args
                    .remove(0)
                    .vector()?
                    .into_iter()
                    .filter(|s| s.histogram.is_none())
                    .collect::<Vec<_>>();
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
                        ..Default::default()
                    }]
                } else {
                    vec![]
                }));
            }
            "label_replace" | "label_join" => return self.label_function(c, name, args, t),
            "histogram_quantile" => {
                return super::distributions::quantile(
                    self,
                    args[0].scalar()?,
                    args.remove(1).vector()?,
                    t,
                    &c.args.args[1],
                );
            }
            _ => {}
        }
        if args.iter().any(|a| matches!(a, Value::Matrix(_))) {
            return self.range_function(c, args, t);
        }
        if matches!(
            name,
            "histogram_count"
                | "histogram_sum"
                | "histogram_avg"
                | "histogram_stddev"
                | "histogram_stdvar"
        ) {
            let mut result = Vec::new();
            for mut s in args.remove(0).vector()? {
                let Some(h) = s.histogram.take() else {
                    continue;
                };
                s.value = match name {
                    "histogram_count" => h.count,
                    "histogram_sum" => h.sum,
                    "histogram_avg" => h.sum / h.count,
                    _ => {
                        let mean = h.sum / h.count;
                        let variance = h
                            .buckets()
                            .iter()
                            .filter(|b| b.count != 0.0)
                            .map(|b| {
                                let midpoint = if h.schema == -53 {
                                    if b.lower == f64::NEG_INFINITY {
                                        b.upper
                                    } else if b.upper == f64::INFINITY {
                                        b.lower
                                    } else {
                                        (b.lower + b.upper) / 2.0
                                    }
                                } else if b.lower <= 0.0 && b.upper >= 0.0 {
                                    0.0
                                } else {
                                    b.lower.signum() * (b.lower * b.upper).sqrt()
                                };
                                b.count * (midpoint - mean).powi(2)
                            })
                            .sum::<f64>()
                            / h.count;
                        if name == "histogram_stddev" {
                            variance.sqrt()
                        } else {
                            variance
                        }
                    }
                };
                s.drop_name = true;
                s.timestamp = t;
                result.push(s);
            }
            return Ok(Value::Vector(result));
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
            std::mem::replace(&mut args[i], Value::Scalar(0.0)).vector()?
        } else if calendar {
            vec![Sample {
                labels: Labels::new(),
                value: t as f64 / 1000.0,
                timestamp: t,
                ..Default::default()
            }]
        } else {
            bail!("function {name} is not implemented")
        };
        if matches!(name, "sort_by_label" | "sort_by_label_desc") {
            let labels = args[1..]
                .iter()
                .map(Value::string)
                .collect::<Result<Vec<_>>>()?;
            let mut result = vector;
            result.sort_by(|a, b| {
                let mut order = std::cmp::Ordering::Equal;
                for label in &labels {
                    order = natural_cmp(
                        a.labels.get(*label).map(String::as_str).unwrap_or(""),
                        b.labels.get(*label).map(String::as_str).unwrap_or(""),
                    );
                    if order != std::cmp::Ordering::Equal {
                        break;
                    }
                }
                if order == std::cmp::Ordering::Equal {
                    order = a.labels.cmp(&b.labels)
                }
                if name == "sort_by_label_desc" {
                    order.reverse()
                } else {
                    order
                }
            });
            return Ok(Value::Vector(result));
        }
        if matches!(name, "sort" | "sort_desc") {
            let mut result = vector
                .into_iter()
                .filter(|s| s.histogram.is_none())
                .collect::<Vec<_>>();
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
            if s.histogram.is_some() && !matches!(name, "timestamp" | "start_timestamp") {
                continue;
            }
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
                "start_timestamp" => s.start_timestamp as f64 / 1000.0,
                _ if calendar => calendar_value(name, x),
                _ => bail!("function {name} is not implemented"),
            };
            s.drop_name = true;
            s.histogram = None;
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
        for Series { labels, points } in matrix {
            if let Expr::MatrixSelector(m) = &*c.args.args[mi]
                && m.anchored
                && matches!(name, "changes" | "resets")
                && points.last().is_none_or(|p| {
                    p.timestamp
                        <= self
                            .range_bounds(&c.args.args[mi], t)
                            .map(|x| x.0)
                            .unwrap_or(i64::MAX)
                })
            {
                continue;
            }
            let mut drop_name = points.iter().any(|p| p.drop_name);
            // Only warnings use the name; look it up when one is formatted.
            let metric = MetricName(&labels);
            if matches!(name, "rate" | "increase" | "irate" | "resets")
                && points.windows(2).any(|p| {
                    p[1].start_timestamp != 0
                        && p[1].start_timestamp < p[0].timestamp
                        && p[1].start_timestamp != p[0].start_timestamp
                })
            {
                let base = format!(
                    "sample has start time that overlaps with previous sample timestamp for metric {metric:?}"
                );
                let count = self.overlaps.entry(base.clone()).or_default();
                *count += 1;
                let count = *count;
                self.warnings
                    .retain(|s| !s.starts_with(&format!("PromQL warning: {base}")));
                self.annotation(
                    true,
                    if count > 1 {
                        format!("{base} ({count} occurrences)")
                    } else {
                        base
                    },
                    &c.args.args[mi],
                );
            }
            let points = if matches!(name, "irate" | "idelta") {
                points[points.len().saturating_sub(2)..].to_vec()
            } else {
                points
            };
            let hist_count = points.iter().filter(|p| p.histogram.is_some()).count();
            if hist_count > 0 {
                let histograms = points
                    .iter()
                    .filter_map(|p| p.histogram.as_ref())
                    .collect::<Vec<_>>();
                if histograms.iter().all(|h| h.schema == -53)
                    && histograms.iter().any(|h| h.custom != histograms[0].custom)
                {
                    let operation = if matches!(name, "sum_over_time" | "avg_over_time") {
                        Some("aggregation")
                    } else if matches!(name, "rate" | "increase" | "delta" | "irate" | "idelta") {
                        Some("subtraction")
                    } else {
                        None
                    };
                    if let Some(operation) = operation.filter(|_| {
                        name != "irate"
                            || histograms.len() < 2
                            || !histograms[histograms.len() - 1]
                                .detects_reset(histograms[histograms.len() - 2])
                    }) {
                        self.annotation(
                            false,
                            format!("mismatched custom buckets were reconciled during {operation}"),
                            &c.args.args[mi],
                        );
                    }
                }
                if matches!(name, "rate" | "increase" | "irate")
                    && points
                        .iter()
                        .any(|p| p.histogram.as_ref().is_some_and(|h| h.reset_hint == 3))
                {
                    self.annotation(
                        true,
                        format!("this native histogram metric is not a counter: {metric:?}"),
                        &c.args.args[mi],
                    );
                }
                if matches!(name, "delta" | "idelta")
                    && points
                        .iter()
                        .any(|p| p.histogram.as_ref().is_some_and(|h| h.reset_hint != 3))
                {
                    self.annotation(
                        true,
                        format!("this native histogram metric is not a gauge: {metric:?}"),
                        &c.args.args[mi],
                    );
                }
                if hist_count < points.len() {
                    if matches!(
                        name,
                        "rate"
                            | "increase"
                            | "delta"
                            | "irate"
                            | "idelta"
                            | "sum_over_time"
                            | "avg_over_time"
                    ) {
                        self.annotation(true,format!("encountered a mix of histograms and floats for metric name {metric:?}"),&c.args.args[mi]);
                    } else if !matches!(
                        name,
                        "irate"
                            | "idelta"
                            | "last_over_time"
                            | "first_over_time"
                            | "count_over_time"
                            | "present_over_time"
                            | "changes"
                            | "resets"
                            | "ts_of_last_over_time"
                            | "ts_of_first_over_time"
                    ) {
                        self.annotation(false,format!("ignored histograms in a range containing both floats and histograms for metric name {metric:?}"),&c.args.args[0]);
                    }
                }

                if matches!(name, "sum_over_time" | "avg_over_time")
                    && points
                        .iter()
                        .any(|p| p.histogram.as_ref().is_some_and(|h| h.reset_hint == 1))
                    && points
                        .iter()
                        .any(|p| p.histogram.as_ref().is_some_and(|h| h.reset_hint == 2))
                {
                    self.annotation(
                        true,
                        "conflicting counter resets during histogram aggregation",
                        &c.args.args[mi],
                    );
                }
                if matches!(name, "rate" | "increase" | "delta")
                    && hist_count == points.len()
                    && let Expr::MatrixSelector(m) = &*c.args.args[mi]
                    && (m.anchored || m.smoothed)
                {
                    let (start, end) = self.range_bounds(&c.args.args[mi], t)?;
                    if let Some(mut sample) = super::extended::rate(
                                &points,
                                start,
                                end,
                                m.smoothed,
                                name != "delta",
                                name == "rate",
                            ).unwrap_or_else(|_| { self.annotation(true,format!("vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric:?}"),&c.args.args[mi]); None }) {
                                drop_name = true;
                                sample.labels = labels;
                                sample.drop_name = true;
                                sample.timestamp = t;
                                result.push(sample);
                            }
                    continue;
                }
                let mut output = None;
                if matches!(name, "last_over_time" | "first_over_time") {
                    let p = if name == "last_over_time" {
                        points.last().unwrap()
                    } else {
                        &points[0]
                    };
                    output = Some(Sample {
                        labels: labels.clone(),
                        value: p.value,
                        histogram: p.histogram.as_deref().cloned(),
                        timestamp: t,
                        drop_name,
                        ..Default::default()
                    });
                } else if matches!(
                    name,
                    "sum_over_time"
                        | "avg_over_time"
                        | "rate"
                        | "increase"
                        | "delta"
                        | "irate"
                        | "idelta"
                ) {
                    let (start, end) = self.range_bounds(&c.args.args[mi], t)?;
                    let native = match super::native::histogram_range(name, &points, start, end) {
                        Ok(v) => v,
                        Err(_) => {
                            self.annotation(true,format!("vector contains a mix of histograms with exponential and custom buckets schemas for metric name {metric:?}"),&c.args.args[mi]);
                            continue;
                        }
                    };
                    if let Some(h) = native {
                        let labels = labels.clone();
                        drop_name = true;
                        output = Some(Sample {
                            labels,
                            histogram: Some(h),
                            timestamp: t,
                            drop_name,
                            ..Default::default()
                        });
                    }
                }
                if let Some(s) = output {
                    result.push(s);
                    continue;
                }
                if matches!(
                    name,
                    "sum_over_time"
                        | "avg_over_time"
                        | "rate"
                        | "increase"
                        | "delta"
                        | "irate"
                        | "idelta"
                ) {
                    continue;
                }
            }
            if matches!(name, "changes" | "resets") && hist_count > 0 {
                let count = points
                    .windows(2)
                    .filter(|p| match (&p[0].histogram, &p[1].histogram) {
                        (Some(a), Some(b)) => {
                            if name == "changes" {
                                a != b
                            } else {
                                b.detects_reset(a) || start_reset(&p[0], &p[1])
                            }
                        }
                        (None, None) => {
                            if name == "changes" {
                                p[0].value != p[1].value
                                    && !(p[0].value.is_nan() && p[1].value.is_nan())
                            } else {
                                p[1].value < p[0].value || start_reset(&p[0], &p[1])
                            }
                        }
                        _ => true,
                    })
                    .count();
                drop_name = true;
                result.push(Sample {
                    labels,
                    value: count as f64,
                    timestamp: t,
                    drop_name,
                    ..Default::default()
                });
                continue;
            }
            if hist_count == 0
                && matches!(name, "rate" | "increase" | "delta")
                && let Expr::MatrixSelector(m) = &*c.args.args[mi]
                && (m.anchored || m.smoothed)
            {
                let (start, end) = self.range_bounds(&c.args.args[mi], t)?;
                if let Some(mut sample) = super::extended::rate(
                    &points,
                    start,
                    end,
                    m.smoothed,
                    name != "delta",
                    name == "rate",
                )? {
                    drop_name = true;
                    sample.labels = labels;
                    sample.drop_name = true;
                    sample.timestamp = t;
                    result.push(sample);
                }
                continue;
            }
            let points = if hist_count == 0
                || matches!(
                    name,
                    "count_over_time"
                        | "present_over_time"
                        | "ts_of_last_over_time"
                        | "ts_of_first_over_time"
                ) {
                points
            } else {
                points
                    .into_iter()
                    .filter(|p| p.histogram.is_none())
                    .collect::<Vec<_>>()
            };
            if points.is_empty() {
                continue;
            }
            // These functions read the points directly; skip copying values.
            let values = if matches!(
                name,
                "rate" | "increase" | "delta" | "irate" | "idelta" | "deriv" | "predict_linear"
            ) {
                Vec::new()
            } else {
                points.iter().map(|p| p.value).collect::<Vec<_>>()
            };
            let mut omit = false;
            let value = match name {
                "sum_over_time" => super::aggregate::compensated_sum(&values),
                "avg_over_time" => crate::numeric::mean(&values),
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
                "resets" => points
                    .windows(2)
                    .filter(|p| p[1].value < p[0].value || start_reset(&p[0], &p[1]))
                    .count() as f64,
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
                    if values.iter().any(|v| v.is_nan()) {
                        f64::NAN
                    } else {
                        let median = quantile(0.5, &mut values.clone());
                        quantile(
                            0.5,
                            &mut values
                                .iter()
                                .map(|x| (x - median).abs())
                                .collect::<Vec<_>>(),
                        )
                    }
                }
                "irate" | "idelta" => {
                    if points.len() < 2 {
                        continue;
                    }
                    let a = &points[points.len() - 2];
                    let b = points.last().unwrap();
                    let mut delta = b.value - a.value;
                    if name == "irate" && (b.value < a.value || start_reset(a, b)) {
                        delta = b.value
                    }
                    if name == "irate" {
                        delta / ((b.timestamp - a.timestamp) as f64 / 1000.0)
                    } else {
                        delta
                    }
                }
                "rate" | "increase" | "delta" => {
                    let (start, end) = self.range_bounds(&c.args.args[mi], t)?;
                    if points.len() < 2
                        && !(name != "delta"
                            && points[0].start_timestamp > start
                            && points[0].start_timestamp < points[0].timestamp
                            && points[0].start_timestamp != 0)
                    {
                        continue;
                    }
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
                    drop_name = true
                }
                result.push(Sample {
                    labels,
                    value,
                    timestamp: t,
                    drop_name,
                    ..Default::default()
                });
            }
        }
        if matches!(name, "rate" | "increase") {
            for sample in &result {
                if sample.histogram.is_none() {
                    let metric = sample
                        .labels
                        .get("__name__")
                        .map(String::as_str)
                        .unwrap_or_default();
                    if !metric.is_empty()
                        && !["_total", "_sum", "_count", "_bucket"]
                            .iter()
                            .any(|s| metric.ends_with(s))
                    {
                        let annotation_expr = if matches!(&*c.args.args[mi], Expr::Subquery(_)) {
                            Expr::Call(c.clone())
                        } else {
                            (*c.args.args[mi]).clone()
                        };
                        self.annotation(false,format!("metric might not be a counter, name does not end in _total/_sum/_count/_bucket: {metric:?}"),&annotation_expr);
                    }
                }
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
            if pair[1].value < pair[0].value || start_reset(&pair[0], &pair[1]) {
                result += pair[0].value
            }
        }
    }
    let mut sampled = (last.timestamp - first.timestamp) as f64 / 1000.0;
    let average = if points.len() > 1 {
        sampled / (points.len() - 1) as f64
    } else {
        0.0
    };
    let mut to_start = (first.timestamp - start) as f64 / 1000.0;
    let mut to_end = (end - last.timestamp) as f64 / 1000.0;
    if counter
        && first.start_timestamp != 0
        && first.start_timestamp > start
        && first.start_timestamp < first.timestamp
    {
        to_start = 0.0;
        sampled = (last.timestamp - first.start_timestamp) as f64 / 1000.0;
        result += first.value;
    } else {
        if to_start >= average * 1.1 {
            to_start = average / 2.0
        }
        if counter && result > 0.0 && first.value >= 0.0 {
            let zero = sampled * first.value / result;
            if zero < to_start {
                to_start = zero
            }
        }
    }
    if to_end >= average * 1.1 {
        to_end = average / 2.0
    }
    let mut factor = if sampled == 0.0 {
        1.0
    } else {
        (sampled + to_start + to_end) / sampled
    };
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
fn natural_cmp(mut a: &str, mut b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    while !a.is_empty() && !b.is_empty() {
        let ac = a.chars().next().unwrap();
        let bc = b.chars().next().unwrap();
        if ac.is_ascii_digit() && bc.is_ascii_digit() {
            let an = a.bytes().take_while(u8::is_ascii_digit).count();
            let bn = b.bytes().take_while(u8::is_ascii_digit).count();
            let av = a[..an].trim_start_matches('0');
            let bv = b[..bn].trim_start_matches('0');
            let order = av.len().cmp(&bv.len()).then_with(|| av.cmp(bv));
            if order != Ordering::Equal {
                return order;
            }
            a = &a[an..];
            b = &b[bn..];
        } else {
            let order = ac.cmp(&bc);
            if order != Ordering::Equal {
                return order;
            }
            a = &a[ac.len_utf8()..];
            b = &b[bc.len_utf8()..];
        }
    }
    a.len().cmp(&b.len())
}
pub(super) fn start_reset(a: &crate::model::Point, b: &crate::model::Point) -> bool {
    if a.start_timestamp == b.start_timestamp
        || b.start_timestamp == 0
        || b.start_timestamp >= b.timestamp
    {
        return false;
    }
    if b.start_timestamp < a.timestamp {
        return false;
    }
    if b.start_timestamp > a.timestamp {
        return true;
    }
    a.start_timestamp <= a.timestamp && a.start_timestamp != 0 && a.start_timestamp != a.timestamp
}

impl Engine<'_> {
    /// label_replace and label_join. Their arguments are literals, so each
    /// call site maps an input label set to the same output at every step;
    /// the output is cached per input set and keeps one shared allocation.
    fn label_function(
        &mut self,
        c: &Call,
        name: &str,
        mut args: Vec<Value>,
        t: i64,
    ) -> Result<Value> {
        const MAX_CACHED_LABELS: usize = 1 << 20;
        let mut v = args.remove(0).vector()?;
        let dest = args[0].string()?;
        if !valid_label(dest) {
            bail!("invalid destination label name in {name}(): {dest}")
        }
        let site = std::ptr::from_ref(c) as usize;
        if name == "label_replace" {
            let replacement = args[1].string()?;
            let source = args[2].string()?;
            let pattern = args[3].string()?;
            if !self.regexes.contains_key(pattern) {
                let re = promql_parser::label::compile_regex(pattern).map_err(|_| {
                    anyhow::anyhow!("invalid regular expression in label_replace(): {pattern}")
                })?;
                self.regexes.insert(pattern.to_owned(), re);
            }
            for s in &mut v {
                let id = (site, s.labels.identity());
                if let Some((_, labels, keeps_name)) = self.relabeled.get(&id) {
                    s.labels = labels.clone();
                    if *keeps_name {
                        s.drop_name = false;
                    }
                    continue;
                }
                let input = s.labels.clone();
                let re = &self.regexes[pattern];
                let source_value = input.get(source).map(String::as_str).unwrap_or("");
                let mut keeps_name = false;
                if re.is_match(source_value) {
                    keeps_name = dest == "__name__";
                    let value =
                        promql_parser::label::expand_regex(re, source_value, replacement).unwrap();
                    if value.is_empty() {
                        s.labels.remove(dest);
                    } else {
                        s.labels.insert(dest.into(), value);
                    }
                }
                if keeps_name {
                    s.drop_name = false;
                }
                if self.relabeled.len() < MAX_CACHED_LABELS {
                    self.relabeled
                        .insert(id, (input, s.labels.clone(), keeps_name));
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
                if dest == "__name__" {
                    s.drop_name = false;
                }
                let id = (site, s.labels.identity());
                if let Some((_, labels, _)) = self.relabeled.get(&id) {
                    s.labels = labels.clone();
                    continue;
                }
                let input = s.labels.clone();
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
                if self.relabeled.len() < MAX_CACHED_LABELS {
                    self.relabeled.insert(id, (input, s.labels.clone(), false));
                }
            }
        }
        for s in &mut v {
            s.timestamp = t;
        }
        Ok(Value::Vector(v))
    }
}

/// Formats a series' metric name like `{:?}` of the name string, reading the
/// labels only when formatted.
struct MetricName<'a>(&'a crate::model::Labels);

impl std::fmt::Debug for MetricName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = self
            .0
            .get("__name__")
            .map(String::as_str)
            .unwrap_or_default();
        std::fmt::Debug::fmt(name, f)
    }
}
