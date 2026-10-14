//! OTLP naming compatible with prometheus/otlptranslator v1.0.0's default
//! underscore escaping with suffixes and permissive label sanitization.
//! Prometheus receiver data bypasses this conversion to preserve scrape names.
use anyhow::{Result, bail};
fn valid(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == ':'
}
fn clean_unit(unit: &str) -> String {
    let mut result = String::new();
    let mut underscore = false;
    for c in unit.chars() {
        if valid(c) {
            result.push(c);
            underscore = false;
        } else if !underscore {
            result.push('_');
            underscore = true;
        }
    }
    result.strip_prefix('_').unwrap_or(&result).to_owned()
}
fn unit_suffixes(unit: &str) -> (String, String) {
    let (main, per) = unit.split_once('/').unwrap_or((unit, ""));
    let main = main.trim();
    let per = per.trim();
    let main = if main.contains(['{', '}']) {
        ""
    } else {
        match main {
            "d" => "days",
            "h" => "hours",
            "min" => "minutes",
            "s" => "seconds",
            "ms" => "milliseconds",
            "us" => "microseconds",
            "ns" => "nanoseconds",
            "By" => "bytes",
            "KiBy" => "kibibytes",
            "MiBy" => "mebibytes",
            "GiBy" => "gibibytes",
            "TiBy" => "tibibytes",
            "KBy" => "kilobytes",
            "MBy" => "megabytes",
            "GBy" => "gigabytes",
            "TBy" => "terabytes",
            "m" => "meters",
            "V" => "volts",
            "A" => "amperes",
            "J" => "joules",
            "W" => "watts",
            "g" => "grams",
            "Cel" => "celsius",
            "Hz" => "hertz",
            "1" => "",
            "%" => "percent",
            x => x,
        }
    };
    let per = if per.contains(['{', '}']) {
        ""
    } else {
        match per {
            "s" => "second",
            "m" => "minute",
            "h" => "hour",
            "d" => "day",
            "w" => "week",
            "mo" => "month",
            "y" => "year",
            x => x,
        }
    };
    (
        clean_unit(main),
        if per.is_empty() {
            String::new()
        } else {
            clean_unit(&format!("per_{per}"))
        },
    )
}
pub fn metric_name(name: &str, unit: &str, kind: &str, monotonic: bool) -> Result<String> {
    let mut tokens = name
        .split(|c| !valid(c))
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let (mut main, mut per) = unit_suffixes(unit);
    if tokens.contains(&main) {
        main.clear();
    }
    if per == "per_" {
        per.clear();
    } else {
        per = per.trim_end_matches('_').to_owned();
        if tokens.contains(&per) {
            per.clear();
        }
    }
    if !per.is_empty() {
        main = main.trim_end_matches('_').to_owned();
    }
    if !main.is_empty() {
        tokens.push(main);
    }
    if !per.is_empty() {
        tokens.push(per);
    }
    let suffix = if kind == "sum" && monotonic {
        Some("total")
    } else if kind == "gauge" && unit == "1" {
        Some("ratio")
    } else {
        None
    };
    if let Some(suffix) = suffix {
        tokens.retain(|v| v != suffix);
        tokens.push(suffix.into());
    }
    let mut result = tokens.join("_");
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert(0, '_');
    }
    if result.is_empty() {
        bail!("normalization for metric {name:?} resulted in empty name");
    }
    if result != name && result.chars().all(|c| c == '_') {
        bail!("normalization for metric {name:?} resulted in invalid name {result:?}");
    }
    Ok(result)
}
pub fn label_name(name: &str) -> Result<String> {
    if name.is_empty() {
        bail!("label name is empty");
    }
    let mut result = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect::<String>();
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert_str(0, "key_");
    }
    if result.chars().all(|c| c == '_') {
        bail!("normalization for label name {name:?} resulted in invalid name {result:?}");
    }
    Ok(result)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_otel_naming_reference() {
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../tests/conformance/otel-names.json")).unwrap();
        for c in corpus["metrics"].as_array().unwrap() {
            let kind = c["kind"].as_u64().unwrap();
            let result = metric_name(
                c["name"].as_str().unwrap(),
                c["unit"].as_str().unwrap(),
                match kind {
                    1 | 2 => "sum",
                    3 => "gauge",
                    _ => "histogram",
                },
                kind == 2,
            );
            check(result, c);
        }
        for c in corpus["labels"].as_array().unwrap() {
            check(label_name(c["name"].as_str().unwrap()), c);
        }
    }
    fn check(result: Result<String>, c: &serde_json::Value) {
        match result {
            Ok(value) => {
                assert!(c.get("error").is_none(), "{c}");
                assert_eq!(value, c["result"].as_str().unwrap(), "{c}");
            }
            Err(e) => assert_eq!(Some(e.to_string().as_str()), c["error"].as_str(), "{c}"),
        }
    }
}
