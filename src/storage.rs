use crate::{
    engine::matches,
    model::{Labels, Point, STALE_BITS, Series, float_string},
};
use anyhow::{Context, Result, bail};
use promql_parser::parser::{Expr, VectorSelector};
use serde_json::Value as Json;
use std::{collections::BTreeMap, time::Duration};

const TABLES: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "summary",
    "exponential_histogram",
];
#[derive(Clone)]
pub struct Store {
    pub client: reqwest::Client,
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_rows: usize,
}
#[derive(Default)]
pub struct Loaded {
    pub series: Vec<Series>,
    pub metadata: BTreeMap<String, Vec<Json>>,
    pub warnings: Vec<String>,
}
impl Store {
    pub fn new(url: String, database: String, username: String, password: String) -> Result<Self> {
        validate_identifier(&database)?;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;
        Ok(Self {
            client,
            url,
            database,
            username,
            password,
            max_rows: 1_000_000,
        })
    }
    pub async fn query(&self, sql: &str, params: &[(&str, String)]) -> Result<String> {
        let response = self
            .client
            .post(&self.url)
            .basic_auth(&self.username, Some(&self.password))
            .query(params)
            .body(sql.to_owned())
            .send()
            .await
            .context("ClickHouse request failed")?;
        let status = response.status();
        let text = response.text().await?;
        if !status.is_success() {
            bail!(
                "ClickHouse returned {status}: {}",
                text.chars().take(500).collect::<String>()
            )
        }
        Ok(text)
    }
    pub async fn ready(&self) -> Result<()> {
        self.query("SELECT 1", &[]).await?;
        Ok(())
    }
    pub async fn load(&self, start: i64, end: i64, selectors: &[VectorSelector]) -> Result<Loaded> {
        self.load_inner(start, end, selectors, true).await
    }
    pub async fn discover(
        &self,
        start: i64,
        end: i64,
        selectors: &[VectorSelector],
    ) -> Result<Loaded> {
        self.load_inner(start, end, selectors, false).await
    }
    async fn load_inner(
        &self,
        start: i64,
        end: i64,
        selectors: &[VectorSelector],
        samples: bool,
    ) -> Result<Loaded> {
        let mut groups: BTreeMap<Labels, Vec<Point>> = BTreeMap::new();
        let mut loaded = Loaded::default();
        let mut total = 0usize;
        for kind in TABLES {
            let sql = format!(
                "SELECT *, toUnixTimestamp64Milli(toDateTime64(TimeUnix, 3)) AS _timestamp, toUnixTimestamp64Milli(toDateTime64(StartTimeUnix, 3)) AS _start_timestamp FROM {}.otel_metrics_{} WHERE TimeUnix >= fromUnixTimestamp64Milli({{start:Int64}}) AND TimeUnix <= fromUnixTimestamp64Milli({{end:Int64}}) LIMIT {} SETTINGS output_format_json_quote_denormals=1 FORMAT JSONEachRow",
                self.database,
                kind,
                self.max_rows + 1
            );
            let text = self
                .query(
                    &sql,
                    &[
                        ("param_start", start.to_string()),
                        ("param_end", end.to_string()),
                    ],
                )
                .await?;
            for line in text.lines() {
                total += 1;
                if total > self.max_rows {
                    bail!("ClickHouse sample scan limit exceeded")
                }
                let row: Json =
                    serde_json::from_str(line).context("invalid ClickHouse metric row")?;
                let name = metric_name(
                    string(&row, "MetricName"),
                    string(&row, "MetricUnit"),
                    kind,
                    row["IsMonotonic"].as_bool().unwrap_or(false),
                );
                let mut labels = row_labels(&row)?;
                labels.insert("__name__".into(), name.clone());
                let candidates = derived_names(&name, kind);
                let selected = selectors.is_empty()
                    || candidates.iter().any(|n| {
                        let mut l = labels.clone();
                        l.insert("__name__".into(), n.clone());
                        if kind == "histogram" {
                            l.insert("le".into(), "+Inf".into());
                        }
                        selectors
                            .iter()
                            .any(|s| matches(s, &l) || potential_bucket_match(s, &l, kind))
                    });
                if !selected {
                    continue;
                }
                let typ = match kind {
                    "sum" if row["IsMonotonic"].as_bool() == Some(true) => "counter",
                    "sum" | "gauge" => "gauge",
                    "histogram" | "exponential_histogram" => "histogram",
                    _ => "summary",
                };
                let metadata = serde_json::json!({"type":typ,"help":string(&row,"MetricDescription"),"unit":string(&row,"MetricUnit")});
                let entries = loaded.metadata.entry(name.clone()).or_default();
                if !entries.contains(&metadata) {
                    entries.push(metadata)
                }
                if !samples {
                    for n in &candidates {
                        let mut ls = labels.clone();
                        ls.insert("__name__".into(), n.clone());
                        let extra: Vec<Option<(&str, String)>> =
                            if n.ends_with("_bucket") && kind == "histogram" {
                                let mut bounds = row["ExplicitBounds"]
                                    .as_array()
                                    .context("missing bounds")?
                                    .iter()
                                    .map(|v| number(v).map(|x| Some(("le", float_string(x)))))
                                    .collect::<Result<Vec<_>>>()?;
                                bounds.push(Some(("le", "+Inf".into())));
                                bounds
                            } else if n == &name && kind == "summary" {
                                row["ValueAtQuantiles.Quantile"]
                                    .as_array()
                                    .context("missing quantiles")?
                                    .iter()
                                    .map(|v| number(v).map(|x| Some(("quantile", float_string(x)))))
                                    .collect::<Result<Vec<_>>>()?
                            } else {
                                vec![None]
                            };
                        for e in extra {
                            let mut ls = ls.clone();
                            if let Some((k, v)) = e {
                                ls.insert(k.into(), v);
                            }
                            if selectors.is_empty() || selectors.iter().any(|s| matches(s, &ls)) {
                                groups.entry(ls).or_default();
                            }
                        }
                    }
                    continue;
                }
                if matches!(kind, "sum" | "histogram" | "exponential_histogram")
                    && integer(&row["AggregationTemporality"])? == 1
                {
                    bail!(
                        "delta temporality for {name} requires cumulative conversion; configure the collector deltatocumulative processor"
                    )
                }
                let timestamp = integer(&row["_timestamp"])?;
                let stale = integer(&row["Flags"]).unwrap_or(0) & 1 != 0;
                let start_timestamp = integer(&row["_start_timestamp"])?;
                if kind == "exponential_histogram" {
                    let mut h = crate::histogram::Histogram {
                        schema: integer(&row["Scale"])? as i32,
                        count: number(&row["Count"])?,
                        sum: number(&row["Sum"])?,
                        zero_count: number(&row["ZeroCount"])?,
                        zero_threshold: row
                            .get("ZeroThreshold")
                            .map(number)
                            .transpose()?
                            .unwrap_or(0.0),
                        ..Default::default()
                    };
                    for (prefix, buckets) in
                        [("Positive", &mut h.positive), ("Negative", &mut h.negative)]
                    {
                        let offset = integer(&row[format!("{prefix}Offset")])? as i32;
                        for (i, v) in row[format!("{prefix}BucketCounts")]
                            .as_array()
                            .context("missing native bucket counts")?
                            .iter()
                            .enumerate()
                        {
                            buckets.insert(offset + i as i32 + 1, number(v)?);
                        }
                    }
                    if row.get("ZeroThreshold").is_none() {
                        let warning="Collector schema does not store exponential histogram ZeroThreshold; assuming zero".to_owned();
                        if !loaded.warnings.contains(&warning) {
                            loaded.warnings.push(warning)
                        }
                    }
                    if stale {
                        h.sum = f64::from_bits(STALE_BITS)
                    }
                    groups.entry(labels).or_default().push(Point {
                        timestamp,
                        start_timestamp,
                        histogram: Some(h),
                        ..Default::default()
                    });
                    continue;
                }
                let mut push = |suffix: &str, extra: Option<(&str, String)>, value: f64| {
                    let mut ls = labels.clone();
                    ls.insert("__name__".into(), format!("{name}{suffix}"));
                    if let Some((k, v)) = extra {
                        ls.insert(k.into(), v);
                    }
                    if selectors.is_empty() || selectors.iter().any(|s| matches(s, &ls)) {
                        groups.entry(ls).or_default().push(Point {
                            timestamp,
                            start_timestamp,
                            value: if stale {
                                f64::from_bits(STALE_BITS)
                            } else {
                                value
                            },
                            ..Default::default()
                        });
                    }
                };
                match kind {
                    "gauge" | "sum" => push("", None, number(&row["Value"])?),
                    "histogram" => {
                        let counts = row["BucketCounts"]
                            .as_array()
                            .context("missing BucketCounts")?;
                        let bounds = row["ExplicitBounds"]
                            .as_array()
                            .context("missing ExplicitBounds")?;
                        if counts.len() != bounds.len() + 1 {
                            bail!("invalid histogram bucket count for {name}")
                        }
                        let mut cumulative = 0.0;
                        for (i, count) in counts.iter().enumerate() {
                            cumulative += number(count)?;
                            let bound = if i < bounds.len() {
                                number(&bounds[i])?
                            } else {
                                f64::INFINITY
                            };
                            push("_bucket", Some(("le", float_string(bound))), cumulative);
                        }
                        push("_sum", None, number(&row["Sum"])?);
                        push("_count", None, number(&row["Count"])?);
                    }
                    "summary" => {
                        let quantiles = row["ValueAtQuantiles.Quantile"]
                            .as_array()
                            .context("missing quantiles")?;
                        let values = row["ValueAtQuantiles.Value"]
                            .as_array()
                            .context("missing quantile values")?;
                        if quantiles.len() != values.len() {
                            bail!("invalid summary quantiles for {name}")
                        }
                        for (q, v) in quantiles.iter().zip(values) {
                            push("", Some(("quantile", float_string(number(q)?))), number(v)?)
                        }
                        push("_sum", None, number(&row["Sum"])?);
                        push("_count", None, number(&row["Count"])?);
                    }
                    _ => unreachable!(),
                }
            }
        }
        for (labels, mut points) in groups {
            points.sort_by_key(|p| p.timestamp);
            for pair in points.windows(2) {
                if pair[0].timestamp == pair[1].timestamp
                    && (pair[0].value.to_bits() != pair[1].value.to_bits()
                        || pair[0].histogram != pair[1].histogram)
                {
                    bail!(
                        "conflicting samples at timestamp {} for {labels:?}; default DateTime columns lose subsecond precision",
                        pair[0].timestamp
                    )
                }
            }
            points.dedup_by(|a, b| {
                a.timestamp == b.timestamp
                    && a.value.to_bits() == b.value.to_bits()
                    && a.histogram == b.histogram
            });
            loaded.series.push(Series { labels, points });
        }
        Ok(loaded)
    }
}
fn potential_bucket_match(selector: &VectorSelector, labels: &Labels, kind: &str) -> bool {
    if kind != "histogram" && kind != "summary" {
        return false;
    }
    let mut selector = selector.clone();
    selector.matchers.matchers.retain(|m| {
        m.name
            != if kind == "histogram" {
                "le"
            } else {
                "quantile"
            }
    });
    matches(&selector, labels)
}
pub fn validate_identifier(s: &str) -> Result<()> {
    if s.is_empty()
        || !s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
        || s.as_bytes()[0].is_ascii_digit()
    {
        bail!("invalid ClickHouse database identifier")
    };
    Ok(())
}
fn string<'a>(row: &'a Json, key: &str) -> &'a str {
    row[key].as_str().unwrap_or("")
}
fn number(v: &Json) -> Result<f64> {
    if let Some(n) = v.as_f64() {
        Ok(n)
    } else if let Some(s) = v.as_str() {
        s.parse().context("invalid numeric metric value")
    } else {
        bail!("missing or invalid numeric metric value {v}")
    }
}
fn integer(v: &Json) -> Result<i64> {
    if let Some(n) = v.as_i64() {
        Ok(n)
    } else if let Some(s) = v.as_str() {
        s.parse().context("invalid integer metric value")
    } else {
        bail!("missing or invalid integer metric value")
    }
}
fn row_labels(row: &Json) -> Result<Labels> {
    let mut labels = Labels::new();
    if let Some(attrs) = row["Attributes"].as_object() {
        for (k, v) in attrs {
            let key = label_name(k);
            let value = v.as_str().context("attribute is not a string")?;
            if !value.is_empty() && labels.insert(key.clone(), value.into()).is_some() {
                bail!("attribute normalization collision at {key}")
            }
        }
    }
    let resource = &row["ResourceAttributes"];
    let service = resource["service.name"].as_str().unwrap_or("");
    let namespace = resource["service.namespace"].as_str().unwrap_or("");
    if !service.is_empty() {
        labels.entry("job".into()).or_insert_with(|| {
            if namespace.is_empty() {
                service.into()
            } else {
                format!("{namespace}/{service}")
            }
        });
    }
    if let Some(instance) = resource["service.instance.id"]
        .as_str()
        .filter(|s| !s.is_empty())
    {
        labels.entry("instance".into()).or_insert(instance.into());
    }
    // Scraped Prometheus labels already live in Attributes. Scope is ingestion
    // metadata and must not introduce extra labels into those series.
    Ok(labels)
}
fn label_name(name: &str) -> String {
    let mut result = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if result.starts_with(|c: char| c.is_ascii_digit()) {
        result.insert_str(0, "key_");
    }
    result
}
fn metric_name(name: &str, unit: &str, kind: &str, monotonic: bool) -> String {
    let mut tokens = name
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let unit = match unit {
        "s" => "seconds",
        "ms" => "milliseconds",
        "us" => "microseconds",
        "ns" => "nanoseconds",
        "By" => "bytes",
        "1" | "" => "",
        other => other,
    };
    if !unit.is_empty() && !tokens.iter().any(|t| t == unit) {
        tokens.push(unit.into())
    }
    if kind == "sum" && monotonic {
        tokens.retain(|t| t != "total");
        tokens.push("total".into());
    }
    let mut name = tokens.join("_");
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        name.insert(0, '_');
    }
    name
}
fn derived_names(name: &str, kind: &str) -> Vec<String> {
    match kind {
        "histogram" => vec![
            format!("{name}_bucket"),
            format!("{name}_sum"),
            format!("{name}_count"),
        ],
        "summary" => vec![name.into(), format!("{name}_sum"), format!("{name}_count")],
        _ => vec![name.into()],
    }
}

/// Gather selectors and safe, conservative input bounds, including nested
/// subqueries/offset/@. This does not assume a fixed lookback for all queries.
pub fn plan(
    expr: &Expr,
    start: i64,
    end: i64,
    lookback: i64,
) -> Result<(i64, i64, Vec<VectorSelector>)> {
    fn visit(
        e: &Expr,
        a: i64,
        b: i64,
        root_a: i64,
        root_b: i64,
        lookback: i64,
        out: &mut Vec<(i64, i64, VectorSelector)>,
    ) -> Result<()> {
        let engine = crate::engine::Engine::new(&[], root_a, root_b);
        match e {
            Expr::VectorSelector(v) => {
                let a = engine.selector_time(a, &v.at, &v.offset)?;
                let b = engine.selector_time(b, &v.at, &v.offset)?;
                out.push((
                    a.saturating_sub(lookback),
                    b.saturating_add(if v.smoothed { lookback } else { 0 }),
                    v.clone(),
                ));
            }
            Expr::MatrixSelector(m) => {
                let a = engine.selector_time(a, &m.vs.at, &m.vs.offset)?;
                let b = engine.selector_time(b, &m.vs.at, &m.vs.offset)?;
                out.push((
                    a.saturating_sub(m.range.as_millis().try_into()?)
                        .saturating_sub(if m.anchored || m.smoothed {
                            lookback
                        } else {
                            0
                        }),
                    b.saturating_add(if m.smoothed { lookback } else { 0 }),
                    m.vs.clone(),
                ));
            }
            Expr::Subquery(s) => {
                let a = engine
                    .selector_time(a, &s.at, &s.offset)?
                    .saturating_sub(s.range.as_millis().try_into()?);
                let b = engine.selector_time(b, &s.at, &s.offset)?;
                visit(&s.expr, a, b, root_a, root_b, lookback, out)?
            }
            Expr::Paren(p) => visit(&p.expr, a, b, root_a, root_b, lookback, out)?,
            Expr::Unary(u) => visit(&u.expr, a, b, root_a, root_b, lookback, out)?,
            Expr::Binary(v) => {
                visit(&v.lhs, a, b, root_a, root_b, lookback, out)?;
                visit(&v.rhs, a, b, root_a, root_b, lookback, out)?
            }
            Expr::Aggregate(v) => {
                visit(&v.expr, a, b, root_a, root_b, lookback, out)?;
                if let Some(p) = &v.param {
                    visit(p, a, b, root_a, root_b, lookback, out)?
                }
            }
            Expr::Call(c) => {
                if c.func.name == "info" {
                    out.push((
                        a.saturating_sub(lookback),
                        b,
                        crate::engine::info::info_selector(c),
                    ));
                }
                for arg in &c.args.args {
                    visit(arg, a, b, root_a, root_b, lookback, out)?
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut out = Vec::new();
    visit(expr, start, end, start, end, lookback, &mut out)?;
    Ok((
        out.iter().map(|x| x.0).min().unwrap_or(start),
        out.iter().map(|x| x.1).max().unwrap_or(end),
        out.into_iter().map(|x| x.2).collect(),
    ))
}
