//! Prometheus 3.15 search protocol and Unicode scoring algorithms.
use super::*;
fn boolean(p: &Params, name: &str, default: bool) -> Result<bool> {
    match p.get(name).unwrap_or("") {
        "" => Ok(default),
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        v => Err(ApiError::bad(format!(
            "invalid {name} {v:?}: must be boolean"
        ))),
    }
}
fn positive(p: &Params, name: &str, default: usize) -> Result<usize> {
    p.get(name)
        .filter(|v| !v.is_empty())
        .map_or(Ok(default), |v| {
            v.parse::<i64>()
                .ok()
                .filter(|v| *v > 0)
                .map(|v| v as usize)
                .ok_or_else(|| {
                    ApiError::bad(format!("invalid {name} {v:?}: must be a positive integer"))
                })
        })
}
pub(super) async fn run(
    app: &App,
    p: &Params,
    endpoint: &str,
    deadline: Instant,
    permit: Arc<OwnedSemaphorePermit>,
) -> Result<JsonValue> {
    let now = chrono::Utc::now().timestamp_millis();
    let start = p.time("start", now - 3_600_000)?;
    let end = p.time("end", now)?;
    if end < start {
        return Err(ApiError::bad(
            "end timestamp must not be before start timestamp",
        ));
    }
    let selectors = p.selectors(false)?;
    let searches = p.all("search[]");
    if searches.len() > 32 {
        return Err(ApiError::bad(format!(
            "too many search[] terms: got {}, maximum is 32",
            searches.len()
        )));
    }
    if searches.iter().any(|s| s.len() > 1024) {
        return Err(ApiError::bad("search term exceeds 1024 byte limit"));
    }
    let threshold = match p.get("fuzz_threshold").unwrap_or("") {
        "" => 0,
        v => v
            .parse::<i64>()
            .ok()
            .filter(|n| (0..=100).contains(n))
            .ok_or_else(|| ApiError::bad(format!("invalid fuzz_threshold {v:?}: must be 0-100")))?,
    };
    let algorithm = p
        .get("fuzz_alg")
        .filter(|v| !v.is_empty())
        .unwrap_or("subsequence");
    if !matches!(algorithm, "subsequence" | "jarowinkler") {
        return Err(ApiError::bad(format!(
            "unsupported fuzz_alg {algorithm:?}: must be one of [subsequence jarowinkler]"
        )));
    }
    let sensitive = boolean(p, "case_sensitive", true)?;
    let include_score = boolean(p, "include_score", false)?;
    let sort = p.get("sort_by").unwrap_or("");
    let direction = p.get("sort_dir").unwrap_or("");
    if !direction.is_empty() && sort.is_empty() {
        return Err(ApiError::bad("sort_dir is only valid when sort_by is set"));
    }
    if !direction.is_empty() && sort == "score" {
        return Err(ApiError::bad("sort_dir is not supported for sort_by=score"));
    }
    if !matches!(direction, "" | "asc" | "dsc") {
        return Err(ApiError::bad(format!(
            "invalid sort_dir {direction:?}: must be \"asc\" or \"dsc\""
        )));
    }
    if sort == "score" && searches.is_empty() {
        return Err(ApiError::bad("sort_by=score requires search[] to be set"));
    }
    let limit = positive(p, "limit", 100)?;
    if limit > 1000 {
        return Err(ApiError::bad(format!(
            "limit {limit} exceeds the configured maximum (1000, see --web.search.max-limit)"
        )));
    }
    let batch = positive(p, "batch_size", 100)?;
    if !matches!(sort, "" | "alpha" | "score") {
        return Err(ApiError::bad(format!(
            "invalid sort_by {sort:?} for {endpoint}: must be \"alpha\" or \"score\""
        )));
    }
    let metadata = endpoint == "metric_names" && boolean(p, "include_metadata", false)?;
    let label = p.get("label").unwrap_or("");
    if endpoint == "label_values" && label.is_empty() {
        return Err(ApiError::bad("missing required parameter \"label\""));
    }
    let loaded = app
        .store
        .discover(start, end, &selectors)
        .await
        .map_err(ApiError::storage)?;
    let terms = searches
        .into_iter()
        .filter(|s| !s.is_empty())
        .map(|s| {
            if sensitive {
                s.to_owned()
            } else {
                s.to_lowercase()
            }
        })
        .collect::<Vec<_>>();
    let label = label.to_owned();
    let algorithm = algorithm.to_owned();
    let endpoint = endpoint.to_owned();
    let sort = sort.to_owned();
    let direction = direction.to_owned();
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let check = || {
            if Instant::now() >= deadline || cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                Err(ApiError::timeout())
            } else {
                Ok(())
            }
        };
        let mut names = BTreeSet::new();
        for series in loaded.series.iter() {
            check()?;
            if endpoint == "label_names" {
                names.extend(series.labels.keys().cloned());
            } else if let Some(name) = series.labels.get(if endpoint == "metric_names" {
                "__name__"
            } else {
                &label
            }) {
                names.insert(name.clone());
            }
        }
        let names: Box<dyn Iterator<Item = String>> = if direction == "dsc" {
            Box::new(names.into_iter().rev())
        } else {
            Box::new(names.into_iter())
        };
        let mut best = std::collections::BinaryHeap::new();
        let mut matched = 0usize;
        let mut work = 0usize;
        let mut results = Vec::new();
        for name in names {
            if Instant::now() > deadline || cancelled.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(ApiError::timeout());
            }
            let text = if sensitive {
                name.clone()
            } else {
                name.to_lowercase()
            };
            if !terms.is_empty() {
                work = work.saturating_add(
                    text.len()
                        .saturating_mul(text.len())
                        .saturating_mul(terms.len()),
                );
                if work > 100_000_000 {
                    return Err(ApiError::exec(
                        "search scoring work limit exceeded; narrow match[] or search[]",
                    ));
                }
            }
            let result = if terms.is_empty() {
                Some(1.0)
            } else {
                terms
                    .iter()
                    .filter_map(|q| score(q, &text, &algorithm, threshold as f64 / 100.0))
                    .max_by(f64::total_cmp)
            };
            if let Some(score) = result {
                matched += 1;
                if sort == "score" {
                    best.push(Ranked { name, score });
                    if best.len() > limit {
                        best.pop();
                    }
                } else {
                    results.push((name, score));
                    if results.len() > limit {
                        break;
                    }
                }
            }
        }
        if sort == "score" {
            results = best
                .into_sorted_vec()
                .into_iter()
                .map(|r| (r.name, r.score))
                .collect();
        }
        let has_more = matched > limit;
        results.truncate(limit);
        let records = results
            .into_iter()
            .map(|(name, score)| {
                let mut record = if endpoint == "label_values" {
                    json!({"value":name})
                } else {
                    json!({"name":name})
                };
                if include_score {
                    record["score"] = json!(score);
                }
                if metadata && let Some(m) = loaded.metadata.get(&name).and_then(|v| v.first()) {
                    for field in ["type", "help", "unit"] {
                        if m[field].as_str().is_some_and(|v| !v.is_empty()) {
                            record[field] = m[field].clone();
                        }
                    }
                }
                record
            })
            .collect::<Vec<_>>();
        let mut lines = records
            .chunks(batch)
            .map(|r| json!({"results":r}))
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(json!({"results":[]}));
        }
        lines.push(json!({"status":"success","has_more":has_more}));
        Ok(json!(lines))
    })
    .await
    .map_err(ApiError::exec)?
}
// The heap root is the worst retained match: lowest score, then largest name.
// Keep only the requested top-k instead of sorting every matched label value.
#[derive(PartialEq)]
struct Ranked {
    name: String,
    score: f64,
}
impl Eq for Ranked {}
impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then(self.name.cmp(&other.name))
    }
}
fn score(query: &str, text: &str, algorithm: &str, threshold: f64) -> Option<f64> {
    if text.starts_with(query) {
        return Some(1.0);
    }
    if algorithm == "subsequence" {
        let score = subsequence(query, text);
        return (score > 0.0 && score >= threshold).then_some(score);
    }
    if let Some(idx) = text.find(query) {
        let pos = text[..idx].chars().count();
        let max = text.chars().count() - query.chars().count();
        return Some(1.0 - 0.9 * pos as f64 / max as f64);
    }
    let score = jaro(query, text);
    (threshold > 0.0 && score >= threshold).then_some(score)
}
fn subsequence(pattern: &str, text: &str) -> f64 {
    let p = pattern.chars().collect::<Vec<_>>();
    let t = text.chars().collect::<Vec<_>>();
    if p.is_empty() {
        return 1.0;
    }
    if p.len() > t.len() {
        return 0.0;
    }
    let inv = 1.0 / t.len() as f64;
    let mut best: f64 = -1.0;
    for start in 0..=t.len() - p.len() {
        if t[start] != p[0] {
            continue;
        }
        let (mut i, mut pi, mut prev, mut score) = (start, 0, 0, 0.0);
        while i < t.len() && pi < p.len() {
            if t[i] != p[pi] {
                i += 1;
                continue;
            }
            let from = i;
            while i < t.len() && pi < p.len() && t[i] == p[pi] {
                i += 1;
                pi += 1;
            }
            score -= (from - prev) as f64 * inv;
            score += ((i - from) * (i - from)) as f64;
            prev = i;
        }
        if pi < p.len() {
            break;
        }
        score -= (t.len() - prev) as f64 * inv * 0.5;
        best = best.max(score);
    }
    if best < 0.0 {
        0.0
    } else {
        best / (p.len() * p.len()) as f64 * 0.999
    }
}
fn jaro(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    let (mut a, mut b) = (a.chars().collect::<Vec<_>>(), b.chars().collect::<Vec<_>>());
    if a.len() > b.len() {
        std::mem::swap(&mut a, &mut b);
    }
    if a.is_empty() {
        return 0.0;
    }
    let d = (b.len() / 2).saturating_sub(1);
    let mut am = vec![false; a.len()];
    let mut bm = vec![false; b.len()];
    let mut matched = 0.0;
    for i in 0..a.len() {
        for j in i.saturating_sub(d)..(i + d + 1).min(b.len()) {
            if !bm[j] && a[i] == b[j] {
                am[i] = true;
                bm[j] = true;
                matched += 1.0;
                break;
            }
        }
    }
    if matched == 0.0 {
        return 0.0;
    }
    let trans = a
        .iter()
        .zip(&am)
        .filter(|(_, m)| **m)
        .map(|(c, _)| c)
        .zip(b.iter().zip(&bm).filter(|(_, m)| **m).map(|(c, _)| c))
        .filter(|(a, b)| a != b)
        .count() as f64;
    let jaro = (matched * (1.0 / a.len() as f64)
        + matched * (1.0 / b.len() as f64)
        + (matched - trans * 0.5) / matched)
        / 3.0;
    let prefix = a.iter().zip(&b).take(4).take_while(|(a, b)| a == b).count();
    jaro + prefix as f64 * 0.1 * (1.0 - jaro)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn upstream_unicode_search_scores() {
        let cases: Vec<JsonValue> =
            serde_json::from_str(include_str!("../../tests/conformance/search-scores.json"))
                .unwrap();
        for case in cases {
            let actual = score(
                case["query"].as_str().unwrap(),
                case["text"].as_str().unwrap(),
                case["algorithm"].as_str().unwrap(),
                case["threshold"].as_f64().unwrap(),
            );
            assert_eq!(
                actual.is_some(),
                case["accepted"].as_bool().unwrap(),
                "{case}"
            );
            if let Some(actual) = actual {
                assert!(
                    (actual - case["score"].as_f64().unwrap()).abs() < 1e-14,
                    "{case}: {actual}"
                );
            }
        }
    }
}
