use crate::model::{Point, Sample};
use anyhow::Result;
pub(super) fn interpolate(a: &Point, b: &Point, t: i64, counter: bool) -> Result<Point> {
    let fraction = (t - a.timestamp) as f64 / (b.timestamp - a.timestamp) as f64;
    if let (Some(ha), Some(hb)) = (&a.histogram, &b.histogram) {
        let mut left = ha.clone();
        if counter && hb.detects_reset(ha) {
            left.scale(0.0)
        }
        let mut difference = hb.clone();
        difference.combine(&left, -1.0)?;
        difference.scale(fraction);
        left.combine(&difference, 1.0)?;
        Ok(Point {
            timestamp: t,
            histogram: Some(left),
            ..Default::default()
        })
    } else {
        let left = if counter && b.value < a.value {
            0.0
        } else {
            a.value
        };
        Ok(Point {
            timestamp: t,
            value: left + (b.value - left) * fraction,
            ..Default::default()
        })
    }
}
pub(super) fn rate(
    points: &[Point],
    start: i64,
    end: i64,
    smoothed: bool,
    counter: bool,
    rate: bool,
) -> Result<Option<Sample>> {
    if points.is_empty() {
        return Ok(None);
    }
    let first = points
        .partition_point(|p| p.timestamp <= start)
        .saturating_sub(1);
    let last = if smoothed {
        points
            .partition_point(|p| p.timestamp < end)
            .min(points.len() - 1)
    } else {
        points.len() - 1
    };
    if points[last].timestamp <= start || smoothed && points[first].timestamp > end {
        return Ok(None);
    }
    let left = if smoothed && points[first].timestamp < start && first + 1 < points.len() {
        interpolate(&points[first], &points[first + 1], start, counter)?
    } else {
        points[first].clone()
    };
    let right = if smoothed && points[last].timestamp > end && last > 0 {
        interpolate(&points[last - 1], &points[last], end, counter)?
    } else {
        points[last].clone()
    };
    let middle = points[first..=last]
        .iter()
        .filter(|p| p.timestamp > start && p.timestamp < end);
    if let (Some(a), Some(b)) = (&left.histogram, &right.histogram) {
        let mut histogram = b.clone();
        histogram.combine(a, -1.0)?;
        if counter {
            let mut previous = a;
            for p in middle {
                let Some(h) = &p.histogram else {
                    return Ok(None);
                };
                if h.detects_reset(previous) {
                    histogram.combine(previous, 1.0)?
                }
                previous = h;
            }
            if b.detects_reset(previous) {
                histogram.combine(previous, 1.0)?
            }
        }
        if rate {
            histogram.scale(1000.0 / (end - start) as f64)
        }
        histogram.reset_hint = 3;
        Ok(Some(Sample {
            histogram: Some(histogram),
            ..Default::default()
        }))
    } else {
        let mut value = right.value - left.value;
        if counter {
            let mut previous = left.value;
            for p in middle {
                if p.value < previous {
                    value += previous
                }
                previous = p.value;
            }
            if right.value < previous {
                value += previous
            }
        }
        if rate {
            value /= (end - start) as f64 / 1000.0
        }
        Ok(Some(Sample {
            value,
            ..Default::default()
        }))
    }
}

pub(super) fn smooth(
    series: &crate::model::Series,
    t: i64,
    lookback: i64,
) -> Result<Option<Point>> {
    let points = series
        .points
        .iter()
        .filter(|p| {
            p.timestamp > t.saturating_sub(lookback)
                && p.timestamp <= t.saturating_add(lookback)
                && !p.stale()
        })
        .collect::<Vec<_>>();
    if points.is_empty() {
        return Ok(None);
    }
    if points.iter().any(|p| p.histogram.is_some()) && points.iter().any(|p| p.histogram.is_none())
    {
        anyhow::bail!("mixed sample types")
    }
    let index = points.partition_point(|p| p.timestamp < t);
    if index < points.len() && points[index].timestamp == t {
        return Ok(Some(points[index].clone()));
    }
    if index == 0 {
        return Ok(None);
    }
    if index == points.len() {
        return Ok(Some(points[index - 1].clone()));
    }
    let a = points[index - 1];
    let b = points[index];
    let counter = a
        .histogram
        .as_ref()
        .zip(b.histogram.as_ref())
        .is_some_and(|(a, b)| a.reset_hint != 3 || b.reset_hint != 3);
    interpolate(a, b, t, counter).map(Some)
}
