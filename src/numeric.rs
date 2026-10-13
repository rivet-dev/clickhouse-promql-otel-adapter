// Adapted from Prometheus util/kahansum and promql/functions.go.
// Copyright The Prometheus Authors; licensed under Apache-2.0.
pub fn add(value: f64, sum: f64, mut correction: f64) -> (f64, f64) {
    let next = sum + value;
    if next.is_infinite() {
        correction = 0.0;
    } else if sum.abs() >= value.abs() {
        correction += (sum - next) + value;
    } else {
        correction += (value - next) + sum;
    }
    (next, correction)
}
pub fn sum(values: &[f64]) -> f64 {
    let (mut sum, mut correction) = (0.0, 0.0);
    for &v in values {
        (sum, correction) = add(v, sum, correction);
    }
    sum + correction
}
pub fn mean(values: &[f64]) -> f64 {
    let Some(&first) = values.first() else {
        return f64::NAN;
    };
    let (mut sum, mut correction, mut mean, mut incremental) = (first, 0.0, 0.0, false);
    for (i, &value) in values.iter().enumerate().skip(1) {
        let count = (i + 1) as f64;
        if !incremental {
            let (s, c) = add(value, sum, correction);
            if !s.is_infinite() {
                sum = s;
                correction = c;
                continue;
            }
            incremental = true;
            mean = sum / (count - 1.0);
            correction /= count - 1.0;
        }
        let q = (count - 1.0) / count;
        (mean, correction) = add(value / count, q * mean, q * correction);
    }
    if incremental {
        mean + correction
    } else {
        sum / values.len() as f64 + correction / values.len() as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancellation_preserves_small_terms() {
        assert_eq!(sum(&[1e16, 1.0, -1e16]), 1.0);
        assert_eq!(mean(&[1e16, 1.0, -1e16]), 1.0 / 3.0);
    }
    #[test]
    fn mean_survives_sum_overflow() {
        assert_eq!(mean(&[f64::MAX, f64::MAX]), f64::MAX);
        assert!(mean(&[f64::INFINITY, f64::NEG_INFINITY]).is_nan());
    }
}
