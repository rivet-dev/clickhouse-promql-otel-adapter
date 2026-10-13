use crate::{
    histogram::Histogram,
    model::{Point, Sample},
};
use anyhow::Result;

pub(super) fn histogram_range(
    name: &str,
    points: &[Point],
    start: i64,
    end: i64,
) -> Result<Option<Histogram>> {
    let points = if name == "irate" || name == "idelta" {
        &points[points.len().saturating_sub(2)..]
    } else {
        points
    };
    if points.is_empty() {
        return Ok(None);
    }
    let histograms = points
        .iter()
        .filter_map(|p| p.histogram.as_ref())
        .collect::<Vec<_>>();
    if histograms.len() != points.len() {
        return Ok(None);
    }
    let mut result = histograms[0].clone();
    match name {
        "sum_over_time" | "avg_over_time" => {
            for h in &histograms[1..] {
                result.combine(h, 1.0)?
            }
            if name == "avg_over_time" {
                result.scale(1.0 / histograms.len() as f64)
            }
            result.reset_hint = 3;
        }
        "rate" | "increase" | "delta" | "irate" | "idelta" => {
            if histograms.len() < 2 {
                return Ok(None);
            }
            let instant = name == "irate" || name == "idelta";
            let counter = name != "delta" && name != "idelta";
            let first = if instant { histograms.len() - 2 } else { 0 };
            let last = histograms.len() - 1;
            result = histograms[last].clone();
            if !counter
                || !(histograms[last].detects_reset(histograms[first])
                    || super::functions::start_reset(&points[first], &points[last]))
                || !instant
            {
                result.combine(histograms[first], -1.0)?
            }
            if counter && !instant {
                for (i, pair) in histograms[first..].windows(2).enumerate() {
                    if pair[1].detects_reset(pair[0])
                        || super::functions::start_reset(&points[first + i], &points[first + i + 1])
                    {
                        result.combine(pair[0], 1.0)?
                    }
                }
            }
            let mut sampled = (points[last].timestamp - points[first].timestamp) as f64 / 1000.0;
            let mut factor = 1.0;
            if !instant {
                let avg = sampled / (last - first) as f64;
                let mut left = (points[first].timestamp - start) as f64 / 1000.0;
                let mut right = (end - points[last].timestamp) as f64 / 1000.0;
                if counter
                    && points[first].start_timestamp != 0
                    && points[first].start_timestamp > start
                    && points[first].start_timestamp < points[first].timestamp
                {
                    left = 0.0;
                    sampled =
                        (points[last].timestamp - points[first].start_timestamp) as f64 / 1000.0;
                    result.combine(histograms[first], 1.0)?;
                    if right >= avg * 1.1 {
                        right = avg / 2.0
                    }
                } else {
                    if left >= avg * 1.1 {
                        left = avg / 2.0
                    }
                    if right >= avg * 1.1 {
                        right = avg / 2.0
                    }
                    if counter && result.count > 0.0 && histograms[first].count >= 0.0 {
                        left = left.min(sampled * histograms[first].count / result.count)
                    }
                }
                factor = (sampled + left + right) / sampled;
            }
            if name == "rate" {
                factor /= (end - start) as f64 / 1000.0
            } else if name == "irate" {
                factor /= sampled
            }
            result.scale(factor);
            result.reset_hint = 3;
        }
        _ => return Ok(None),
    }
    Ok(Some(result))
}
pub(super) fn histogram_binary(op: &str, a: &Sample, b: &Sample) -> Result<Option<Histogram>> {
    match (&a.histogram, &b.histogram) {
        (Some(h), None) if op == "</" || op == ">/" => {
            let mut h = h.clone();
            h.trim(b.value, op == "</");
            Ok(Some(h))
        }
        (Some(a), Some(b)) if op == "+" || op == "-" => {
            let mut h = a.clone();
            h.combine(b, if op == "+" { 1.0 } else { -1.0 })?;
            Ok(Some(h))
        }
        (Some(h), None) if op == "*" || op == "/" => {
            let mut h = h.clone();
            if op == "/" && b.value == 0.0 {
                h.positive.clear();
                h.negative.clear();
            }
            h.scale(if op == "*" { b.value } else { 1.0 / b.value });
            Ok(Some(h))
        }
        (None, Some(h)) if op == "*" => {
            let mut h = h.clone();
            h.scale(a.value);
            Ok(Some(h))
        }
        _ => Ok(None),
    }
}
