//! Thanos penalty-based HA replica selection, including counter adjustment on
//! replica switches. The iterator tree preserves the behavior for >2 replicas.
use crate::model::{Labels, Point, Series};
use std::collections::BTreeMap;
enum Cursor<'a> {
    Leaf {
        points: &'a [Point],
        index: Option<usize>,
        counter: bool,
        adjust: f64,
    },
    Merge {
        a: Box<Cursor<'a>>,
        b: Box<Cursor<'a>>,
        last: i64,
        pen_a: i64,
        pen_b: i64,
        use_a: bool,
    },
}
impl<'a> Cursor<'a> {
    fn leaf(points: &'a [Point], counter: bool) -> Self {
        Self::Leaf {
            points,
            index: None,
            counter,
            adjust: 0.0,
        }
    }
    fn merge(mut a: Self, mut b: Self) -> Self {
        a.next();
        b.next();
        Self::Merge {
            a: Box::new(a),
            b: Box::new(b),
            last: i64::MIN,
            pen_a: 0,
            pen_b: 0,
            use_a: true,
        }
    }
    fn current(&self) -> Option<Point> {
        match self {
            Self::Leaf {
                points,
                index,
                adjust,
                ..
            } => {
                let mut p = points.get((*index)?)?.clone();
                if p.histogram.is_none() {
                    p.value += adjust;
                }
                Some(p)
            }
            Self::Merge { a, b, use_a, .. } => {
                if *use_a {
                    a.current()
                } else {
                    b.current()
                }
            }
        }
    }
    fn adjust(&mut self, value: f64) {
        match self {
            Self::Leaf {
                points,
                index,
                counter,
                adjust,
            } => {
                if *counter
                    && let Some(p) = index.and_then(|i| points.get(i))
                    && p.histogram.is_none()
                    && value > p.value + *adjust
                {
                    *adjust += value - (p.value + *adjust);
                }
            }
            Self::Merge { a, b, .. } => {
                a.adjust(value);
                b.adjust(value);
            }
        }
    }
    fn seek(&mut self, t: i64) -> bool {
        if let Self::Leaf { points, index, .. } = self {
            let start = index.unwrap_or(0).min(points.len());
            let n = points[start..].partition_point(|p| p.timestamp < t) + start;
            *index = Some(n);
            return n < points.len();
        }
        loop {
            if self.current().is_some_and(|p| p.timestamp >= t) {
                return true;
            }
            if !self.next() {
                return false;
            }
        }
    }
    fn next(&mut self) -> bool {
        let previous = self.current();
        match self {
            Self::Leaf { points, index, .. } => {
                *index = Some(index.map_or(0, |i| i.saturating_add(1)));
                index.unwrap() < points.len()
            }
            Self::Merge {
                a,
                b,
                last,
                pen_a,
                pen_b,
                use_a,
            } => {
                let old = *use_a;
                let alive_a = a.seek(last.saturating_add(1).saturating_add(*pen_a));
                let alive_b = b.seek(last.saturating_add(1).saturating_add(*pen_b));
                if !alive_a && !alive_b {
                    return false;
                }
                let time;
                if !alive_a {
                    *use_a = false;
                    time = b.current().unwrap().timestamp;
                    *pen_b = 0;
                } else if !alive_b {
                    *use_a = true;
                    time = a.current().unwrap().timestamp;
                    *pen_a = 0;
                } else {
                    let ta = a.current().unwrap().timestamp;
                    let tb = b.current().unwrap().timestamp;
                    *use_a = ta <= tb;
                    time = if *use_a { ta } else { tb };
                    let penalty = if *last == i64::MIN {
                        5000
                    } else {
                        time.saturating_sub(*last).saturating_mul(2)
                    };
                    if *use_a {
                        *pen_a = 0;
                        *pen_b = penalty;
                    } else {
                        *pen_b = 0;
                        *pen_a = penalty;
                    }
                }
                *last = time;
                if old != *use_a
                    && let Some(p) = previous.filter(|p| p.histogram.is_none())
                {
                    a.adjust(p.value);
                    b.adjust(p.value);
                }
                true
            }
        }
    }
}
pub(crate) fn merge(replicas: &[&Series], replica_labels: &[String], counter: bool) -> Vec<Series> {
    let mut groups: BTreeMap<Labels, Vec<&Series>> = BTreeMap::new();
    for s in replicas {
        let mut labels = s.labels.clone();
        for l in replica_labels {
            labels.remove(l);
        }
        groups.entry(labels).or_default().push(s);
    }
    groups
        .into_iter()
        .map(|(labels, series)| {
            let mut cursor = Cursor::leaf(&series[0].points, counter);
            for s in &series[1..] {
                cursor = Cursor::merge(cursor, Cursor::leaf(&s.points, counter));
            }
            let mut points = Vec::new();
            while cursor.next() {
                points.push(cursor.current().unwrap());
            }
            Series { labels, points }
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_thanos_dedup_reference() {
        let cases: Vec<serde_json::Value> = serde_json::from_reader(flate2::read::GzDecoder::new(
            &include_bytes!("../tests/conformance/thanos-dedup.json.gz")[..],
        ))
        .unwrap();
        for (n, c) in cases.iter().enumerate() {
            let series = c["replicas"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| Series {
                    labels: Labels::new(),
                    points: r
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|p| Point {
                            timestamp: p[0].as_f64().unwrap() as i64,
                            value: p[1].as_f64().unwrap(),
                            ..Default::default()
                        })
                        .collect(),
                })
                .collect::<Vec<_>>();
            let result = merge(
                &series.iter().collect::<Vec<_>>(),
                &[],
                c["counter"].as_bool().unwrap(),
            );
            let actual = result[0]
                .points
                .iter()
                .map(|p| [p.timestamp as f64, p.value])
                .collect::<Vec<_>>();
            let expected: Vec<[f64; 2]> = serde_json::from_value(c["expected"].clone()).unwrap();
            assert_eq!(actual, expected, "case {n}");
        }
    }
}
