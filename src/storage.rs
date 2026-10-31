use crate::{
    engine::matches,
    model::{Labels, Point, STALE_BITS, Series, float_string},
    translation::{label_name, metric_name},
};
use anyhow::{Context, Result, bail};
use promql_parser::{
    label::MatchOp,
    parser::{Expr, VectorSelector},
};
use serde_json::Value as Json;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

type Dictionary = Arc<Vec<Json>>;
type DictionaryEntry = Option<NameCoverage>;

/// Late or clock-skewed samples older than this when the dictionary was last
/// extended forward are rescanned by the next forward extension.
const NAME_LATE_ARRIVAL_MS: i64 = 5 * 60 * 1000;

/// Distinct metric names seen in `[from_ms, scanned_at_ms)` plus anything newer
/// that the last forward scan returned. Queries starting at or after `from_ms`
/// reuse it; earlier starts extend it backwards, and expiry extends it forward.
/// Names are only ever added, so an extension never hides a name.
/// A sample's field from its grouped tuple, or from the group row for fields
/// shared by every sample in the group.
fn sample_field<'a>(row: &'a Json, values: &'a [Json], fields: &[&str], name: &str) -> &'a Json {
    match fields.iter().position(|field| *field == name) {
        Some(index) if index < values.len() => &values[index],
        _ => &row[name],
    }
}

/// Hashes the per-group output cache's small keys (a static suffix and a
/// float's bits) without SipHash. The cache lives for one request and its
/// keys are not attacker-chosen, so collision resistance is not needed.
type KeyHasher = std::hash::BuildHasherDefault<FnvHasher>;

#[derive(Default)]
struct FnvHasher(u64);

impl std::hash::Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf29ce484222325
        } else {
            self.0
        };
        for byte in bytes {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
        }
        self.0 = hash;
    }
}

/// One table's sample or discovery query, prepared before any are sent.
struct TableQuery {
    kind: &'static str,
    grouped: bool,
    sample_fields: Vec<&'static str>,
    sql: String,
    params: Vec<(String, String)>,
}

struct NameCoverage {
    identity: String,
    from_ms: i64,
    scanned_at_ms: i64,
    refreshed: Instant,
    /// When a background forward scan started, if one is running.
    refreshing: Option<Instant>,
    seen: std::collections::HashSet<String>,
    rows: Dictionary,
}

pub(crate) const TABLES: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "summary",
    "exponential_histogram",
];
#[derive(Debug)]
pub struct BackendError(String);
impl std::fmt::Display for BackendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for BackendError {}
#[derive(Clone)]
pub struct Store {
    pub client: reqwest::Client,
    pub url: String,
    pub database: String,
    pub username: String,
    pub password: String,
    pub max_rows: usize,
    pub max_bytes: usize,
    /// Maximum retained samples, native buckets, or discovered series per read.
    pub max_samples: usize,
    /// Maximum age of metric-name metadata; zero disables caching.
    pub metric_cache_ttl: Duration,
    /// Resource attributes copied onto every series as labels, as
    /// (attribute, label) pairs. This mirrors Prometheus'
    /// `promote_resource_attributes` for producers that identify a target with
    /// an attribute other than `service.instance.id`.
    pub promoted_resource_attributes: Vec<(String, String)>,
    /// Labels whose value is also stored in a table column, as (label,
    /// column) pairs. Positive matchers on such a label also filter the
    /// column, so a sort-key column skips other values' rows. The operator
    /// asserts that any row carrying the label has the same value in the column.
    pub label_columns: Vec<(String, String)>,
    dictionaries: Arc<[Mutex<DictionaryEntry>; 5]>,
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
            max_bytes: 64 * 1024 * 1024,
            promoted_resource_attributes: Vec::new(),
            label_columns: Vec::new(),
            max_samples: 5_000_000,
            metric_cache_ttl: Duration::ZERO,
            dictionaries: Arc::new(std::array::from_fn(|_| Mutex::new(None))),
        })
    }
    pub async fn query(&self, sql: &str, params: &[(&str, String)]) -> Result<String> {
        self.query_inner(sql, params)
            .await
            .map_err(|e| BackendError(format!("{e:#}")).into())
    }
    async fn query_inner(&self, sql: &str, params: &[(&str, String)]) -> Result<String> {
        let started = Instant::now();
        let mut response = self
            .client
            .post(&self.url)
            .basic_auth(&self.username, Some(&self.password))
            .query(params)
            .query(&[
                ("cancel_http_readonly_queries_on_client_close", "1"),
                ("max_execution_time", "120"),
            ])
            .body(sql.to_owned())
            .send()
            .await
            .context("ClickHouse request failed")?;
        let headers_ms = started.elapsed().as_millis() as u64;
        let status = response.status();
        if response
            .content_length()
            .is_some_and(|n| n > self.max_bytes as u64)
        {
            bail!("ClickHouse response byte limit exceeded");
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > self.max_bytes.saturating_sub(bytes.len()) {
                bail!("ClickHouse response byte limit exceeded");
            }
            bytes.extend_from_slice(&chunk);
        }
        tracing::debug!(
            headers_ms,
            total_ms = started.elapsed().as_millis() as u64,
            bytes = bytes.len(),
            query = sql.get(..160).unwrap_or(sql),
            "clickhouse round trip"
        );
        let text = String::from_utf8(bytes).context("ClickHouse returned invalid UTF-8")?;
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
    // Resolve OTel name/unit normalization from a small distinct-name dictionary
    // before fetching samples. Filtering raw names directly would drop SDK names
    // such as http.request.duration (exported as http_request_duration_seconds).
    pub(crate) async fn metric_filter(
        &self,
        kind: &str,
        start: i64,
        selectors: &[VectorSelector],
    ) -> Result<Option<Vec<String>>> {
        let name_selectors = selectors
            .iter()
            .map(|s| {
                let mut s = s.clone();
                retain_matchers(&mut s, |name| name == "__name__");
                s
            })
            .collect::<Vec<_>>();
        let accepts = |name: &str| {
            let labels = Labels::from([("__name__".into(), name.into())]);
            name_selectors.iter().any(|s| matches(s, &labels))
        };
        if selectors.is_empty() || accepts("target_info") {
            return Ok(None);
        }
        let rows = self.metric_dictionary(kind, start).await?;
        let mut names = std::collections::BTreeSet::new();
        for (index, row) in rows.iter().enumerate() {
            if index.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
            let Ok(name) = row_metric_name(row, kind) else {
                continue;
            };
            if derived_names(&name, kind).iter().any(|n| accepts(n)) {
                names.insert(string(row, "MetricName").to_owned());
            }
        }
        Ok(Some(names.into_iter().collect()))
    }
    /// Distinct name metadata for rows at or after `start` (milliseconds).
    /// Only the uncovered part of the time range is scanned: a full-table
    /// DISTINCT reads every stored row and takes tens of seconds at scale.
    async fn metric_dictionary(&self, kind: &str, start: i64) -> Result<Dictionary> {
        let index = TABLES
            .iter()
            .position(|table| *table == kind)
            .context("unknown metric table")?;
        // Hold the per-table lock during a scan so concurrent panels share it.
        // Samples are never cached. Failed/cancelled scans remain retryable.
        let mut cache = self.dictionaries[index].lock().await;
        let identity = format!(
            "{}\n{}\n{}\n{kind}",
            self.url, self.username, self.max_bytes
        );
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or_default();
        let reusable = !self.metric_cache_ttl.is_zero()
            && cache.as_ref().is_some_and(|c| c.identity == identity);
        if !reusable {
            let mut coverage = NameCoverage {
                identity,
                from_ms: start,
                scanned_at_ms: now_ms,
                refreshed: Instant::now(),
                refreshing: None,
                seen: Default::default(),
                rows: Default::default(),
            };
            let text = self.fetch_names(kind, start, None).await?;
            self.merge_names(&text, &mut coverage)?;
            let rows = coverage.rows.clone();
            *cache = if self.metric_cache_ttl.is_zero() {
                None
            } else {
                Some(coverage)
            };
            return Ok(rows);
        }
        let coverage = cache.as_mut().context("metric dictionary missing")?;
        // An earlier start needs those names for a correct answer, so the
        // query waits for that scan.
        if start < coverage.from_ms {
            let until = coverage.from_ms;
            let text = self.fetch_names(kind, start, Some(until)).await?;
            self.merge_names(&text, coverage)?;
            coverage.from_ms = start;
        }
        // New names may appear up to one TTL late, so expiry serves the cached
        // names and extends them forward in the background. A scan that never
        // reports back is retried after two minutes.
        let stuck = coverage
            .refreshing
            .is_some_and(|started| started.elapsed() >= Duration::from_secs(120));
        if coverage.refreshed.elapsed() >= self.metric_cache_ttl
            && (coverage.refreshing.is_none() || stuck)
        {
            coverage.refreshing = Some(Instant::now());
            let since = coverage
                .scanned_at_ms
                .saturating_sub(NAME_LATE_ARRIVAL_MS)
                .max(coverage.from_ms);
            let store = self.clone();
            tokio::spawn(async move { store.refresh_names(index, since, now_ms).await });
        }
        Ok(coverage.rows.clone())
    }

    /// Extends table `index`'s cached names forward from `since`. A failure
    /// leaves the cache expired, so the next query starts another scan.
    async fn refresh_names(&self, index: usize, since: i64, now_ms: i64) {
        let text = self.fetch_names(TABLES[index], since, None).await;
        let mut cache = self.dictionaries[index].lock().await;
        let Some(coverage) = cache.as_mut() else {
            return;
        };
        coverage.refreshing = None;
        let merged = text.and_then(|text| self.merge_names(&text, coverage));
        match merged {
            Ok(()) => {
                coverage.scanned_at_ms = now_ms;
                coverage.refreshed = Instant::now();
            }
            Err(error) => {
                tracing::warn!(
                    ?error,
                    table = TABLES[index],
                    "metric name refresh failed; retrying on the next query"
                );
            }
        }
    }

    /// Distinct name metadata rows in `[from, until)`, one JSON object per line.
    async fn fetch_names(&self, kind: &str, from: i64, until: Option<i64>) -> Result<String> {
        let monotonic = if kind == "sum" { ", IsMonotonic" } else { "" };
        let upper = if until.is_some() {
            " AND TimeUnix < fromUnixTimestamp64Milli({until:Int64})"
        } else {
            ""
        };
        let sql = format!(
            "SELECT DISTINCT MetricName, MetricUnit, ScopeName{monotonic} FROM {}.otel_metrics_{kind} WHERE TimeUnix >= fromUnixTimestamp64Milli({{from:Int64}}){upper} LIMIT {} FORMAT JSONEachRow",
            self.database,
            self.max_rows.saturating_add(1)
        );
        let mut params = vec![("param_from", from.to_string())];
        if let Some(until) = until {
            params.push(("param_until", until.to_string()));
        }
        self.query(&sql, &params).await
    }

    /// Adds name rows from `fetch_names` that `coverage` has not seen. Nothing
    /// is recorded unless every new row decodes.
    fn merge_names(&self, text: &str, coverage: &mut NameCoverage) -> Result<()> {
        let mut lines = Vec::new();
        let mut rows = coverage.rows.as_ref().clone();
        for line in text.lines() {
            if coverage.seen.contains(line) || lines.contains(&line) {
                continue;
            }
            if rows.len() >= self.max_rows {
                bail!("ClickHouse metric dictionary limit exceeded");
            }
            rows.push(serde_json::from_str(line)?);
            lines.push(line);
        }
        coverage.seen.extend(lines.into_iter().map(str::to_owned));
        coverage.rows = Arc::new(rows);
        Ok(())
    }

    /// Fills the metric name cache for rows at or after `start`, so the first
    /// dashboard queries after startup do not each wait for the scans. Errors
    /// are only logged: queries scan on demand as usual.
    pub async fn warm_names(&self, start: i64) {
        if self.metric_cache_ttl.is_zero() {
            return;
        }
        let warm = |kind| self.metric_dictionary(kind, start);
        let results = tokio::join!(
            warm(TABLES[0]),
            warm(TABLES[1]),
            warm(TABLES[2]),
            warm(TABLES[3]),
            warm(TABLES[4]),
        );
        for result in [results.0, results.1, results.2, results.3, results.4] {
            if let Err(error) = result {
                tracing::warn!(
                    ?error,
                    "metric name cache warm-up failed; names load on demand"
                );
            }
        }
    }

    /// Resolves every table's metric-name filter concurrently.
    pub(crate) async fn metric_filters(
        &self,
        start: i64,
        selectors: &[VectorSelector],
    ) -> Result<[Option<Vec<String>>; 5]> {
        let filter = |kind| self.metric_filter(kind, start, selectors);
        let (gauge, sum, histogram, summary, exponential) = tokio::try_join!(
            filter(TABLES[0]),
            filter(TABLES[1]),
            filter(TABLES[2]),
            filter(TABLES[3]),
            filter(TABLES[4]),
        )?;
        Ok([gauge, sum, histogram, summary, exponential])
    }

    /// Sends the per-table queries concurrently and returns their responses in
    /// request order. Each response is individually bounded by `max_bytes`.
    async fn fetch_concurrently(&self, requests: &[TableQuery]) -> Result<Vec<String>> {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, request) in requests.iter().enumerate() {
            let store = self.clone();
            let sql = request.sql.clone();
            let params = request.params.clone();
            tasks.spawn(async move {
                let params = params
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.clone()))
                    .collect::<Vec<_>>();
                (index, store.query(&sql, &params).await)
            });
        }
        let mut texts = vec![String::new(); requests.len()];
        while let Some(joined) = tasks.join_next().await {
            let (index, text) = joined.context("ClickHouse query task failed")?;
            texts[index] = text?;
        }
        Ok(texts)
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
        let mut bytes_read = 0usize;
        let mut retained = 0usize;
        let filters = self.metric_filters(start, selectors).await?;
        let mut requests = Vec::new();
        for (kind, filter) in TABLES.into_iter().zip(filters) {
            if filter.as_ref().is_some_and(|v| v.is_empty()) {
                continue;
            }
            let predicate = if filter.is_some() {
                " AND MetricName IN {names:Array(String)}"
            } else {
                ""
            };
            let (labels_predicate, labels_params) = label_filter(
                kind,
                selectors,
                &self.promoted_resource_attributes,
                &self.label_columns,
            );
            let projection = if samples {
                // Keep optional schema additions such as ZeroThreshold while
                // avoiding unused strings and potentially huge exemplar arrays.
                let mut excluded = vec![
                    "ResourceSchemaUrl",
                    "ScopeDroppedAttrCount",
                    "ServiceName",
                    "TimeUnix",
                    "StartTimeUnix",
                ];
                if kind != "summary" {
                    excluded.extend([
                        "`Exemplars.FilteredAttributes`",
                        "`Exemplars.TimeUnix`",
                        "`Exemplars.Value`",
                        "`Exemplars.SpanId`",
                        "`Exemplars.TraceId`",
                    ]);
                }
                if matches!(kind, "histogram" | "exponential_histogram") {
                    excluded.extend(["Min", "Max"]);
                }
                // Selecting only _count/_sum still reads histogram rows; empty
                // buckets decode as a lone +Inf bucket that no selector reads.
                let mut placeholders = "";
                if kind == "histogram" && !reads_buckets(selectors) {
                    excluded.extend(["BucketCounts", "ExplicitBounds"]);
                    placeholders = ", emptyArrayUInt64() AS BucketCounts, emptyArrayFloat64() AS ExplicitBounds";
                }
                let columns = format!("* EXCEPT ({}){placeholders}", excluded.join(", "));
                format!(
                    "{columns}, toUnixTimestamp64Milli(toDateTime64(TimeUnix, 3)) AS _timestamp, toUnixTimestamp64Milli(toDateTime64(StartTimeUnix, 3)) AS _start_timestamp"
                )
            } else {
                let extra = match kind {
                    "sum" => ", IsMonotonic",
                    "histogram" => ", ExplicitBounds",
                    "summary" => ", `ValueAtQuantiles.Quantile`",
                    _ => "",
                };
                format!(
                    "DISTINCT MetricName, MetricDescription, MetricUnit, ResourceAttributes, ScopeName, ScopeVersion, ScopeSchemaUrl, ScopeAttributes, Attributes{extra}"
                )
            };
            let grouped = samples;
            let mut sample_fields = vec!["_timestamp", "_start_timestamp", "Flags"];
            sample_fields.extend(match kind {
                "gauge" | "sum" => vec!["Value"],
                // Bucket bounds rarely change within a series, so they stay in
                // the grouping key and are sent once per group, not per sample.
                "histogram" => vec!["Count", "Sum", "BucketCounts"],
                "summary" => vec![
                    "Count",
                    "Sum",
                    "ValueAtQuantiles.Quantile",
                    "ValueAtQuantiles.Value",
                ],
                "exponential_histogram" => vec![
                    "Count",
                    "Sum",
                    "Scale",
                    "ZeroCount",
                    "PositiveOffset",
                    "PositiveBucketCounts",
                    "NegativeOffset",
                    "NegativeBucketCounts",
                ],
                _ => unreachable!(),
            });
            // Tables are read concurrently, so each may use the whole row
            // budget; the shared limit is enforced while decoding.
            let row_limit = self.max_rows.saturating_add(1);
            let source = format!(
                "{}.otel_metrics_{kind} WHERE TimeUnix >= fromUnixTimestamp64Milli({{start:Int64}}) AND TimeUnix <= fromUnixTimestamp64Milli({{end:Int64}}){predicate}{labels_predicate}",
                self.database
            );
            // Unquoted 64-bit integers parse without a string allocation per
            // timestamp and bucket count; number() and integer() accept both.
            let sql = if grouped {
                let fields = sample_fields
                    .iter()
                    .map(|field| format!("`{field}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                // Bound raw rows BEFORE aggregation. The extra row survives as
                // a tuple and causes the same scan-limit error during decoding.
                // Every other projected field remains part of the grouping key,
                // including optional schema additions such as ZeroThreshold.
                // No values are aggregated away: only repeated labels/metadata
                // are removed from the transport representation.
                format!(
                    "SELECT * EXCEPT ({fields}), groupArray(tuple({fields})) AS _samples FROM (SELECT {projection} FROM {source} LIMIT {row_limit}) GROUP BY ALL SETTINGS output_format_json_quote_denormals=1, output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow"
                )
            } else {
                format!(
                    "SELECT {projection} FROM {source} LIMIT {row_limit} SETTINGS output_format_json_quote_denormals=1, output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow"
                )
            };
            let mut params = vec![
                ("param_start".to_owned(), start.to_string()),
                ("param_end".to_owned(), end.to_string()),
                (
                    "param_names".to_owned(),
                    string_array(&filter.unwrap_or_default()),
                ),
            ];
            params.extend(labels_params);
            requests.push(TableQuery {
                kind,
                grouped,
                sample_fields,
                sql,
                params,
            });
        }
        let texts = self.fetch_concurrently(&requests).await?;
        for (request, text) in requests.iter().zip(texts) {
            let (kind, grouped, sample_fields) =
                (request.kind, request.grouped, &request.sample_fields);
            // The byte budget is shared across sample/discovery tables, not
            // reset for each response. Dictionary metadata has its own cache.
            bytes_read += text.len();
            if bytes_read > self.max_bytes {
                bail!("ClickHouse response byte limit exceeded");
            }
            for (group_index, line) in text.lines().enumerate() {
                if group_index.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                let mut row: Json =
                    serde_json::from_str(line).context("invalid ClickHouse metric row")?;
                let tuples = if grouped {
                    match row
                        .as_object_mut()
                        .context("invalid grouped metric row")?
                        .remove("_samples")
                    {
                        Some(Json::Array(tuples)) => tuples,
                        _ => bail!("missing grouped metric samples"),
                    }
                } else {
                    vec![Json::Null]
                };
                total = total
                    .checked_add(tuples.len())
                    .context("ClickHouse sample scan limit exceeded")?;
                if total > self.max_rows {
                    bail!("ClickHouse sample scan limit exceeded");
                }
                // Group identity is immutable. Normalize/match labels and
                // metadata once, then decode only point fields inside the loop.
                let name = match row_metric_name(&row, kind) {
                    Ok(name) => name,
                    Err(e) => {
                        let warning = format!(
                            "skipped invalid OTel metric name {:?}: {e}",
                            string(&row, "MetricName")
                        );
                        if !loaded.warnings.contains(&warning) {
                            loaded.warnings.push(warning);
                        }
                        continue;
                    }
                };
                let mut labels = row_labels(&row, &self.promoted_resource_attributes)?;
                labels.insert("__name__".into(), name.clone());
                let info = target_info(&row)?.filter(|info| {
                    selectors.is_empty() || selectors.iter().any(|s| matches(s, info))
                });
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
                if selected {
                    let typ = match kind {
                        "sum" if row["IsMonotonic"].as_bool() == Some(true) => "counter",
                        "sum" | "gauge" => "gauge",
                        "histogram" | "exponential_histogram" => "histogram",
                        _ => "summary",
                    };
                    let help = string(&row, "MetricDescription");
                    let unit = string(&row, "MetricUnit");
                    // Most sample rows repeat the same metadata. Compare borrowed
                    // strings before allocating another JSON object for every point.
                    let known = loaded.metadata.get(&name).is_some_and(|entries| {
                        entries.iter().any(|entry| {
                            entry["type"] == typ && entry["help"] == help && entry["unit"] == unit
                        })
                    });
                    if !known {
                        loaded
                            .metadata
                            .entry(name.clone())
                            .or_default()
                            .push(serde_json::json!({"type":typ,"help":help,"unit":unit}));
                    }
                }
                // One output buffer per derived series within this transport
                // group. Keys contain only suffix + generated label bits, so the
                // hot point loop neither clones nor compares full label maps.
                type OutputKey = (&'static str, Option<(&'static str, u64)>);
                let mut outputs: Vec<(Labels, Vec<Point>)> = Vec::new();
                let mut output_cache: HashMap<OutputKey, Option<usize>, KeyHasher> =
                    HashMap::default();
                let mut info_points = Vec::new();
                let mut native_points = Vec::new();
                for (sample_index, tuple) in tuples.into_iter().enumerate() {
                    if sample_index.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                    // Sample fields are read from the tuple by position; writing
                    // them into the row allocated a key per field per sample.
                    let mut tuple_values = Vec::new();
                    if grouped {
                        let Json::Array(values) = tuple else {
                            bail!("invalid grouped metric sample")
                        };
                        if values.len() != sample_fields.len() {
                            bail!("invalid grouped metric sample length")
                        }
                        tuple_values = values;
                    }
                    let sample =
                        |name: &str| sample_field(&row, &tuple_values, sample_fields, name);
                    if let Some(info) = &info {
                        if !samples && !groups.contains_key(info) {
                            charge_samples(&mut retained, 1, self.max_samples)?;
                        }
                        if !samples {
                            groups.entry(info.clone()).or_default();
                        }
                        if samples && integer(sample("Flags")).unwrap_or(0) & 1 == 0 {
                            charge_samples(&mut retained, 1, self.max_samples)?;
                            info_points.push(Point {
                                timestamp: integer(sample("_timestamp"))?,
                                value: 1.0,
                                ..Default::default()
                            });
                        }
                    }
                    if !selected {
                        continue;
                    }
                    if !samples {
                        for n in &candidates {
                            let mut ls = labels.clone();
                            ls.insert("__name__".into(), n.clone());
                            let extra: Box<
                                dyn Iterator<Item = Result<Option<(&str, String)>>> + Send + '_,
                            > = if n.ends_with("_bucket") && kind == "histogram" {
                                Box::new(
                                    sample("ExplicitBounds")
                                        .as_array()
                                        .context("missing bounds")?
                                        .iter()
                                        .map(|v| number(v).map(|x| Some(("le", float_string(x)))))
                                        .chain(std::iter::once(Ok(Some(("le", "+Inf".into()))))),
                                )
                            } else if n == &name && kind == "summary" {
                                Box::new(
                                    sample("ValueAtQuantiles.Quantile")
                                        .as_array()
                                        .context("missing quantiles")?
                                        .iter()
                                        .map(|v| {
                                            number(v).map(|x| Some(("quantile", float_string(x))))
                                        }),
                                )
                            } else {
                                Box::new(std::iter::once(Ok(None)))
                            };
                            for (index, e) in extra.enumerate() {
                                if index.is_multiple_of(1024) {
                                    tokio::task::yield_now().await;
                                }
                                let mut ls = ls.clone();
                                if let Some((k, v)) = e? {
                                    ls.insert(k.into(), v);
                                }
                                if selectors.is_empty() || selectors.iter().any(|s| matches(s, &ls))
                                {
                                    if !groups.contains_key(&ls) {
                                        charge_samples(&mut retained, 1, self.max_samples)?;
                                    }
                                    groups.entry(ls).or_default();
                                }
                            }
                        }
                        continue;
                    }
                    if matches!(kind, "sum" | "histogram" | "exponential_histogram")
                        && integer(sample("AggregationTemporality"))? == 1
                    {
                        bail!(
                            "delta temporality for {name} requires cumulative conversion; configure the collector deltatocumulative processor"
                        )
                    }
                    let timestamp = integer(sample("_timestamp"))?;
                    let stale = integer(sample("Flags")).unwrap_or(0) & 1 != 0;
                    let start_timestamp = integer(sample("_start_timestamp"))?;
                    if kind == "exponential_histogram" {
                        charge_samples(&mut retained, 1, self.max_samples)?;
                        for field in ["PositiveBucketCounts", "NegativeBucketCounts"] {
                            charge_samples(
                                &mut retained,
                                sample(field)
                                    .as_array()
                                    .context("missing native bucket counts")?
                                    .len(),
                                self.max_samples,
                            )?;
                        }
                        let mut h = crate::histogram::Histogram {
                            schema: integer(sample("Scale"))? as i32,
                            count: number(sample("Count"))?,
                            sum: number(sample("Sum"))?,
                            zero_count: number(sample("ZeroCount"))?,
                            zero_threshold: row
                                .get("ZeroThreshold")
                                .map(number)
                                .transpose()?
                                .unwrap_or(0.0),
                            ..Default::default()
                        };
                        if !(-4..=20).contains(&h.schema) {
                            bail!(
                                "OTLP exponential histogram scale {} cannot be represented as a Prometheus native histogram",
                                h.schema
                            );
                        }
                        for (prefix, buckets) in
                            [("Positive", &mut h.positive), ("Negative", &mut h.negative)]
                        {
                            let offset = integer(sample(&format!("{prefix}Offset")))? as i32;
                            for (i, v) in sample(&format!("{prefix}BucketCounts"))
                                .as_array()
                                .context("missing native bucket counts")?
                                .iter()
                                .enumerate()
                            {
                                if i.is_multiple_of(1024) {
                                    tokio::task::yield_now().await;
                                }
                                let index = i64::from(offset) + i as i64 + 1;
                                let index = i32::try_from(index)
                                    .context("native histogram bucket index overflow")?;
                                buckets.insert(index, number(v)?);
                            }
                        }
                        if h.schema > 8 {
                            h.reduce(8)?;
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
                        native_points.push(Point {
                            timestamp,
                            start_timestamp,
                            histogram: Some(Box::new(h)),
                            ..Default::default()
                        });
                        continue;
                    }
                    let mut push = |suffix: &'static str,
                                    extra: Option<(&'static str, f64)>,
                                    value: f64|
                     -> Result<()> {
                        let key = (suffix, extra.map(|(label, value)| (label, value.to_bits())));
                        let index = if let Some(index) = output_cache.get(&key) {
                            *index
                        } else {
                            let mut ls = labels.clone();
                            ls.insert("__name__".into(), format!("{name}{suffix}"));
                            if let Some((label, value)) = extra {
                                ls.insert(label.into(), float_string(value));
                            }
                            let index = if selectors.is_empty()
                                || selectors.iter().any(|s| matches(s, &ls))
                            {
                                let index = outputs.len();
                                outputs.push((ls, Vec::new()));
                                Some(index)
                            } else {
                                None
                            };
                            // Retained outputs are bounded by max_samples. Cap
                            // negative cache entries independently when arbitrary
                            // changing bounds never match the requested selector.
                            if index.is_some() || output_cache.len() < 4096 {
                                output_cache.insert(key, index);
                            }
                            index
                        };
                        if let Some(index) = index {
                            charge_samples(&mut retained, 1, self.max_samples)?;
                            outputs[index].1.push(Point {
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
                        Ok(())
                    };
                    match kind {
                        "gauge" | "sum" => push("", None, number(sample("Value"))?)?,
                        "histogram" => {
                            let counts = sample("BucketCounts")
                                .as_array()
                                .context("missing BucketCounts")?;
                            let bounds = sample("ExplicitBounds")
                                .as_array()
                                .context("missing ExplicitBounds")?;
                            if (counts.is_empty() && !bounds.is_empty())
                                || (!counts.is_empty() && counts.len() != bounds.len() + 1)
                            {
                                bail!("invalid histogram bucket count for {name}")
                            }
                            if counts.is_empty() {
                                push(
                                    "_bucket",
                                    Some(("le", f64::INFINITY)),
                                    number(sample("Count"))?,
                                )?;
                            }
                            let mut cumulative = 0.0;
                            for (i, count) in counts.iter().enumerate() {
                                if i.is_multiple_of(1024) {
                                    tokio::task::yield_now().await;
                                }
                                cumulative += number(count)?;
                                let bound = if i < bounds.len() {
                                    number(&bounds[i])?
                                } else {
                                    f64::INFINITY
                                };
                                push("_bucket", Some(("le", bound)), cumulative)?;
                            }
                            push("_sum", None, number(sample("Sum"))?)?;
                            push("_count", None, number(sample("Count"))?)?;
                        }
                        "summary" => {
                            let quantiles = sample("ValueAtQuantiles.Quantile")
                                .as_array()
                                .context("missing quantiles")?;
                            let values = sample("ValueAtQuantiles.Value")
                                .as_array()
                                .context("missing quantile values")?;
                            if quantiles.len() != values.len() {
                                bail!("invalid summary quantiles for {name}")
                            }
                            for (index, (q, v)) in quantiles.iter().zip(values).enumerate() {
                                if index.is_multiple_of(1024) {
                                    tokio::task::yield_now().await;
                                }
                                push("", Some(("quantile", number(q)?)), number(v)?)?;
                            }
                            push("_sum", None, number(sample("Sum"))?)?;
                            push("_count", None, number(sample("Count"))?)?;
                        }
                        _ => unreachable!(),
                    }
                }
                if samples {
                    if let Some(info) = info {
                        outputs.push((info, info_points));
                    }
                    if !native_points.is_empty() {
                        outputs.push((labels, native_points));
                    }
                    for (labels, mut points) in outputs {
                        match groups.entry(labels) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(points);
                            }
                            std::collections::btree_map::Entry::Occupied(mut entry) => {
                                entry.get_mut().append(&mut points);
                            }
                        }
                    }
                }
            }
        }
        for (index, (labels, mut points)) in groups.into_iter().enumerate() {
            if index.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
            points.sort_by_key(|p| p.timestamp);
            for (index, pair) in points.windows(2).enumerate() {
                if index.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                if pair[0].timestamp == pair[1].timestamp
                    && (pair[0].value.to_bits() != pair[1].value.to_bits()
                        || pair[0].histogram != pair[1].histogram
                        || pair[0].start_timestamp != pair[1].start_timestamp)
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
                    && a.start_timestamp == b.start_timestamp
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
    retain_matchers(&mut selector, |name| {
        name != if kind == "histogram" {
            "le"
        } else {
            "quantile"
        }
    });
    matches(&selector, labels)
}
/// Removing a matcher must preserve every OR alternative; an empty alternative
/// remains true, rather than accidentally becoming an empty selector.
fn retain_matchers(selector: &mut VectorSelector, keep: impl Fn(&str) -> bool) {
    selector.matchers.matchers.retain(|m| keep(&m.name));
    for branch in &mut selector.matchers.or_matchers {
        branch.retain(|m| keep(&m.name));
    }
}

/// Conservative SQL prefilter. Rust remains the authority for final matching.
/// General regex, generated labels and scope labels stay in Rust to avoid
/// ClickHouse RE2/normalization differences changing which series are returned.
/// Each OR branch contributes independently; an unfilterable branch admits all.
pub(crate) fn label_filter(
    kind: &str,
    selectors: &[VectorSelector],
    promoted: &[(String, String)],
    columns: &[(String, String)],
) -> (String, Vec<(String, String)>) {
    let mut params = Vec::new();
    let mut alternatives = Vec::new();
    for selector in selectors {
        let branches = if selector.matchers.or_matchers.is_empty() {
            vec![selector.matchers.matchers.as_slice()]
        } else {
            selector
                .matchers
                .or_matchers
                .iter()
                .map(Vec::as_slice)
                .collect()
        };
        for branch in branches {
            // target_info has resource attributes, not point attributes. A
            // branch that could select it cannot use ordinary label pushdown.
            if selector.name.as_deref().is_none_or(|n| n == "target_info")
                && branch
                    .iter()
                    .filter(|m| m.name == "__name__")
                    .all(|m| m.is_match("target_info"))
            {
                return (String::new(), Vec::new());
            }
            let mut terms = Vec::new();
            for matcher in branch {
                if let Some(term) = column_term(matcher, columns, &mut params) {
                    terms.push(term);
                }
                // A promoted label that shadows job or instance follows their
                // precedence in row_labels; filter it in Rust only.
                let promotion = promoted.iter().find(|(_, label)| *label == matcher.name);
                if matcher.name == "__name__"
                    || matcher.name.starts_with("otel_scope_")
                    || (promotion.is_some() && matches!(matcher.name.as_str(), "job" | "instance"))
                    || (kind == "histogram" && matcher.name == "le")
                    || (kind == "summary" && matcher.name == "quantile")
                {
                    continue;
                }
                let (op, value_type, parameter) = match &matcher.op {
                    MatchOp::Equal => ("=", "String", string_param(&matcher.value)),
                    MatchOp::NotEqual => ("!=", "String", string_param(&matcher.value)),
                    MatchOp::Re(_) | MatchOp::NotRe(_) => {
                        // Prometheus regexes are fully anchored. Bare literals
                        // and literal alternatives are exactly an equality set;
                        // all other regex syntax remains in Rust.
                        if matcher.value.chars().any(|c| ".*+?()[]{}\\^$".contains(c)) {
                            continue;
                        }
                        let values = matcher
                            .value
                            .split('|')
                            .map(str::to_owned)
                            .collect::<Vec<_>>();
                        (
                            if matches!(matcher.op, MatchOp::Re(_)) {
                                "IN"
                            } else {
                                "NOT IN"
                            },
                            "Array(String)",
                            string_array(&values),
                        )
                    }
                };
                let index = params.len();
                let key = format!("label_key_{index}");
                let value = format!("label_value_{index}");
                params.push((format!("param_{key}"), string_param(&matcher.name)));
                params.push((format!("param_{value}"), parameter));
                // RE2 replaces each Unicode codepoint, matching label_name's
                // permissive escaping; consecutive punctuation is not collapsed.
                let normalized = "concat(if(match(k, '^[0-9]'), 'key_', ''), replaceRegexpAll(k, '[^a-zA-Z0-9]', '_'))";
                let attribute = format!(
                    "if(ScopeName = 'github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver', Attributes[{{{key}:String}}], ifNull(arrayFirstOrNull((v, k) -> {normalized} = {{{key}:String}} AND v != '', mapValues(Attributes), mapKeys(Attributes)), ''))"
                );
                let expression = match matcher.name.as_str() {
                    _ if promotion.is_some() => {
                        let resource = format!("label_resource_{index}");
                        params.push((
                            format!("param_{resource}"),
                            string_param(
                                promotion
                                    .map(|(attribute, _)| attribute.as_str())
                                    .unwrap_or_default(),
                            ),
                        ));
                        format!(
                            "if(notEmpty({attribute}), {attribute}, ResourceAttributes[{{{resource}:String}}])"
                        )
                    }
                    "job" => format!(
                        "if(notEmpty({attribute}), {attribute}, if(empty(ResourceAttributes['service.name']), '', concat(if(empty(ResourceAttributes['service.namespace']), '', concat(ResourceAttributes['service.namespace'], '/')), ResourceAttributes['service.name'])))"
                    ),
                    "instance" => format!(
                        "if(notEmpty({attribute}), {attribute}, ResourceAttributes['service.instance.id'])"
                    ),
                    _ => attribute,
                };
                // A normalized-key collision is an error, not a choice between
                // values. Admit ambiguous rows so row_labels reports that error.
                let collision = format!(
                    "(ScopeName != 'github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver' AND arrayCount((v, k) -> {normalized} = {{{key}:String}} AND v != '', mapValues(Attributes), mapKeys(Attributes)) > 1)"
                );
                terms.push(format!(
                    "(({expression} {op} {{{value}:{value_type}}}) OR {collision})"
                ));
            }
            if terms.is_empty() {
                return (String::new(), Vec::new());
            }
            alternatives.push(format!("({})", terms.join(" AND ")));
        }
    }
    if alternatives.is_empty() {
        (String::new(), Vec::new())
    } else {
        (format!(" AND ({})", alternatives.join(" OR ")), params)
    }
}

/// Whether any selector could select a classic histogram `_bucket` series.
/// Only selectors that name their metric exactly can rule buckets out.
fn reads_buckets(selectors: &[VectorSelector]) -> bool {
    selectors.is_empty()
        || selectors.iter().any(|selector| {
            let branches = if selector.matchers.or_matchers.is_empty() {
                vec![selector.matchers.matchers.as_slice()]
            } else {
                selector
                    .matchers
                    .or_matchers
                    .iter()
                    .map(Vec::as_slice)
                    .collect()
            };
            branches.into_iter().any(|branch| {
                let exact = branch
                    .iter()
                    .find(|m| m.name == "__name__" && matches!(m.op, MatchOp::Equal))
                    .map(|m| m.value.as_str())
                    .or(selector.name.as_deref());
                exact.is_none_or(|name| name.ends_with("_bucket"))
            })
        })
}

/// Restricts a configured column to the values a positive matcher accepts.
/// Matchers that can select rows without the label (an empty value, negation
/// or a general regex) are left to the attribute filter alone.
fn column_term(
    matcher: &promql_parser::label::Matcher,
    columns: &[(String, String)],
    params: &mut Vec<(String, String)>,
) -> Option<String> {
    let (_, column) = columns.iter().find(|(label, _)| *label == matcher.name)?;
    let literal = !matcher.value.chars().any(|c| ".*+?()[]{}\\^$".contains(c));
    let values = match &matcher.op {
        MatchOp::Equal => vec![matcher.value.clone()],
        MatchOp::Re(_) if literal => matcher.value.split('|').map(str::to_owned).collect(),
        MatchOp::Re(_) | MatchOp::NotEqual | MatchOp::NotRe(_) => return None,
    };
    if values.iter().any(String::is_empty) {
        return None;
    }
    let key = format!("label_column_{}", params.len());
    params.push((format!("param_{key}"), string_array(&values)));
    Some(format!("({column} IN {{{key}:Array(String)}})"))
}

// ClickHouse HTTP String parameters use escaped text; URL encoding alone does
// not preserve a literal backslash followed by an escape character.
fn string_param(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\0', "\\0")
}

pub(crate) fn charge_samples(total: &mut usize, amount: usize, limit: usize) -> Result<()> {
    *total = total
        .checked_add(amount)
        .context("ClickHouse expanded sample limit exceeded")?;
    if *total > limit {
        bail!("ClickHouse expanded sample limit exceeded");
    }
    Ok(())
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
pub(crate) fn number(v: &Json) -> Result<f64> {
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
pub(crate) fn row_metric_name(row: &Json, kind: &str) -> Result<String> {
    if prometheus_scope(row) {
        Ok(string(row, "MetricName").into())
    } else {
        metric_name(
            string(row, "MetricName"),
            string(row, "MetricUnit"),
            kind,
            row["IsMonotonic"].as_bool().unwrap_or(false),
        )
    }
}
fn target_info(row: &Json) -> Result<Option<Labels>> {
    let resource = &row["ResourceAttributes"];
    let service = resource["service.name"].as_str().unwrap_or("");
    let instance = resource["service.instance.id"].as_str().unwrap_or("");
    if service.is_empty() && instance.is_empty() {
        return Ok(None);
    }
    let mut labels = Labels::new();
    labels.insert("__name__".into(), "target_info".into());
    if !service.is_empty() {
        let ns = resource["service.namespace"].as_str().unwrap_or("");
        labels.insert(
            "job".into(),
            if ns.is_empty() {
                service.into()
            } else {
                format!("{ns}/{service}")
            },
        );
    }
    if !instance.is_empty() {
        labels.insert("instance".into(), instance.into());
    }
    if let Some(attrs) = resource.as_object() {
        for (key, value) in attrs {
            if matches!(
                key.as_str(),
                "service.name" | "service.namespace" | "service.instance.id"
            ) {
                continue;
            }
            let key = if prometheus_scope(row) {
                key.clone()
            } else {
                label_name(key)?
            };
            let value = value
                .as_str()
                .context("resource attribute is not a string")?;
            if value.is_empty() {
                continue;
            }
            if labels.insert(key.clone(), value.into()).is_some() {
                bail!("resource attribute normalization collision at {key}");
            }
        }
    }
    Ok(Some(labels))
}
pub(crate) fn prometheus_scope(row: &Json) -> bool {
    string(row, "ScopeName")
        == "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver"
}
pub(crate) fn row_labels(row: &Json, promoted: &[(String, String)]) -> Result<Labels> {
    let mut labels = Labels::new();
    if let Some(attrs) = row["Attributes"].as_object() {
        for (k, v) in attrs {
            let key = if prometheus_scope(row) {
                k.clone()
            } else {
                label_name(k)?
            };
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
    for (attribute, label) in promoted {
        if let Some(value) = resource[attribute.as_str()]
            .as_str()
            .filter(|s| !s.is_empty())
        {
            labels.entry(label.clone()).or_insert(value.into());
        }
    }
    if !prometheus_scope(row) {
        for (field, label) in [
            ("ScopeName", "otel_scope_name"),
            ("ScopeVersion", "otel_scope_version"),
            ("ScopeSchemaUrl", "otel_scope_schema_url"),
        ] {
            let value = string(row, field);
            if !value.is_empty() && labels.insert(label.into(), value.into()).is_some() {
                bail!("scope attribute collision at {label}");
            }
        }
        if let Some(attrs) = row["ScopeAttributes"].as_object() {
            for (key, value) in attrs {
                if matches!(key.as_str(), "name" | "version" | "schema_url") {
                    continue;
                }
                let label = label_name(&format!("otel_scope_{key}"))?;
                let value = value.as_str().context("scope attribute is not a string")?;
                if !value.is_empty() && labels.insert(label.clone(), value.into()).is_some() {
                    bail!("scope attribute collision at {label}");
                }
            }
        }
    }
    Ok(labels)
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
                        engine
                            .info_time(&c.args.args[0], a)?
                            .saturating_sub(lookback),
                        engine.info_time(&c.args.args[0], b)?,
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

/// ClickHouse query-parameter text encoding, never interpolated into SQL.
pub(crate) fn string_array(values: &[String]) -> String {
    format!(
        "[{}]",
        values
            .iter()
            .map(|v| format!("'{}'", v.replace('\\', "\\\\").replace('\'', "\\'")))
            .collect::<Vec<_>>()
            .join(",")
    )
}
