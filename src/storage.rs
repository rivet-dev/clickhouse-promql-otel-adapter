use crate::{
    engine::matches,
    model::{Labels, Point, STALE_BITS, Series, float_string},
    rowbinary::{Column, Reader, Ty, Unsupported, trailing_exception},
    translation::{label_name, metric_name},
};
use anyhow::{Context, Result, bail};
use promql_parser::{
    label::{MatchOp, Matcher},
    parser::{Expr, VectorSelector},
};
use serde_json::Value as Json;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

type Dictionary = Arc<Vec<Json>>;
type DictionaryEntry = Option<NameCoverage>;

/// Late or clock-skewed samples older than this when the dictionary was last
/// extended forward are rescanned by the next forward extension.
const NAME_LATE_ARRIVAL_MS: i64 = 5 * 60 * 1000;

/// Logged once per process: the same schema produces the same fallback on
/// every query.
static ROWBINARY_FALLBACK_WARNING: std::sync::Once = std::sync::Once::new();

/// One table's sample or discovery query, prepared before any are sent.
#[derive(Clone)]
struct TableQuery {
    kind: &'static str,
    /// Sample queries return one row per label set with its samples as an
    /// array of tuples in `sample_fields` order; discovery rows are distinct
    /// label sets without samples.
    grouped: bool,
    sample_fields: Vec<&'static str>,
    /// The statement without its output format.
    sql: String,
    params: Vec<(String, String)>,
}

#[derive(Clone, Copy)]
enum Format {
    RowBinary,
    Json,
}

impl TableQuery {
    fn statement(&self, format: Format) -> String {
        match format {
            Format::RowBinary => format!("{} FORMAT RowBinaryWithNamesAndTypes", self.sql),
            // Unquoted 64-bit integers parse without a string allocation per
            // timestamp and bucket count; number() and integer() accept both.
            Format::Json => format!(
                "{} SETTINGS output_format_json_quote_denormals=1, output_format_json_quote_64bit_integers=0 FORMAT JSONEachRow",
                self.sql
            ),
        }
    }
}

/// A table's response, ready to decode.
enum TablePayload {
    /// Grouped sample rows, with the header parsed and the sample tuple
    /// layout checked.
    RowBinary {
        bytes: Vec<u8>,
        columns: Vec<Column>,
        layout: SampleLayout,
        /// Offset of the first row.
        body: usize,
    },
    /// One JSON object per line: discovery rows, or grouped sample rows when
    /// the RowBinary header names a column type the reader does not support.
    Json(String),
}

impl TablePayload {
    fn len(&self) -> usize {
        match self {
            TablePayload::RowBinary { bytes, .. } => bytes.len(),
            TablePayload::Json(text) => text.len(),
        }
    }
}

/// Position and element types of the sample tuple in a RowBinary group row,
/// checked once per response so per-sample reads cannot hit a type mismatch.
struct SampleLayout {
    column: usize,
    elements: Vec<Ty>,
}

impl SampleLayout {
    fn new(columns: &[Column], fields: &[&str]) -> Result<Self> {
        let column = columns
            .iter()
            .position(|column| column.name == "_samples")
            .context("missing grouped metric samples")?;
        let elements = match &columns[column].ty {
            Ty::Array(tuple) => match tuple.as_ref() {
                Ty::Tuple(elements) => elements.clone(),
                _ => bail!("invalid grouped metric sample"),
            },
            _ => bail!("invalid grouped metric samples"),
        };
        if elements.len() != fields.len() {
            bail!("invalid grouped metric sample length");
        }
        for (field, ty) in fields.iter().zip(&elements) {
            let readable = match *field {
                "_timestamp" | "_start_timestamp" | "Flags" | "Scale" | "PositiveOffset"
                | "NegativeOffset" => ty.is_integer(),
                "Value" | "Count" | "Sum" | "ZeroCount" => ty.is_number(),
                _ => ty.is_number_array(),
            };
            if !readable {
                return Err(Unsupported(format!("{ty:?} for sample field {field}")).into());
            }
        }
        Ok(Self { column, elements })
    }
}

/// One sample of a grouped row, decoded by column type.
struct Sample {
    timestamp: i64,
    start_timestamp: i64,
    flags: i64,
    measurement: Measurement,
}

impl Sample {
    fn stale(&self) -> bool {
        self.flags & 1 != 0
    }

    /// Reads the next tuple of a RowBinary sample array.
    fn read(reader: &mut Reader, layout: &SampleLayout, kind: &str) -> Result<Self> {
        let ty = |index: usize| &layout.elements[index];
        let timestamp = reader.integer(ty(0))?;
        let start_timestamp = reader.integer(ty(1))?;
        let flags = reader.integer(ty(2))?;
        let measurement = match kind {
            "gauge" | "sum" => Measurement::Float(reader.float(ty(3))?),
            "histogram" => {
                let count = reader.float(ty(3))?;
                let sum = reader.float(ty(4))?;
                let mut bucket_counts = Vec::new();
                reader.floats(ty(5), &mut bucket_counts)?;
                Measurement::Histogram {
                    count,
                    sum,
                    bucket_counts,
                }
            }
            "summary" => {
                let count = reader.float(ty(3))?;
                let sum = reader.float(ty(4))?;
                let mut quantiles = Vec::new();
                reader.floats(ty(5), &mut quantiles)?;
                let mut values = Vec::new();
                reader.floats(ty(6), &mut values)?;
                Measurement::Summary {
                    count,
                    sum,
                    quantiles,
                    values,
                }
            }
            "exponential_histogram" => {
                let count = reader.float(ty(3))?;
                let sum = reader.float(ty(4))?;
                let scale = reader.integer(ty(5))?;
                let zero_count = reader.float(ty(6))?;
                let positive_offset = reader.integer(ty(7))?;
                let mut positive = Vec::new();
                reader.floats(ty(8), &mut positive)?;
                let negative_offset = reader.integer(ty(9))?;
                let mut negative = Vec::new();
                reader.floats(ty(10), &mut negative)?;
                Measurement::Exponential(Box::new(Exponential {
                    count,
                    sum,
                    scale,
                    zero_count,
                    positive_offset,
                    positive,
                    negative_offset,
                    negative,
                }))
            }
            _ => unreachable!(),
        };
        Ok(Self {
            timestamp,
            start_timestamp,
            flags,
            measurement,
        })
    }

    /// Decodes one tuple of a JSON `_samples` array, in `fields` order.
    fn from_json(values: &[Json], fields: &[&str], kind: &str) -> Result<Self> {
        if values.len() != fields.len() {
            bail!("invalid grouped metric sample length")
        }
        let measurement = match kind {
            "gauge" | "sum" => Measurement::Float(number(&values[3])?),
            "histogram" => Measurement::Histogram {
                count: number(&values[3])?,
                sum: number(&values[4])?,
                bucket_counts: numbers(&values[5]).context("missing BucketCounts")?,
            },
            "summary" => Measurement::Summary {
                count: number(&values[3])?,
                sum: number(&values[4])?,
                quantiles: numbers(&values[5]).context("missing quantiles")?,
                values: numbers(&values[6]).context("missing quantile values")?,
            },
            "exponential_histogram" => Measurement::Exponential(Box::new(Exponential {
                count: number(&values[3])?,
                sum: number(&values[4])?,
                scale: integer(&values[5])?,
                zero_count: number(&values[6])?,
                positive_offset: integer(&values[7])?,
                positive: numbers(&values[8]).context("missing native bucket counts")?,
                negative_offset: integer(&values[9])?,
                negative: numbers(&values[10]).context("missing native bucket counts")?,
            })),
            _ => unreachable!(),
        };
        Ok(Self {
            timestamp: integer(&values[0])?,
            start_timestamp: integer(&values[1])?,
            flags: integer(&values[2]).unwrap_or(0),
            measurement,
        })
    }
}

enum Measurement {
    /// A gauge or sum `Value`.
    Float(f64),
    Histogram {
        count: f64,
        sum: f64,
        /// Per-bound counts, or empty when the row stores no buckets.
        bucket_counts: Vec<f64>,
    },
    Summary {
        count: f64,
        sum: f64,
        quantiles: Vec<f64>,
        values: Vec<f64>,
    },
    Exponential(Box<Exponential>),
}

struct Exponential {
    count: f64,
    sum: f64,
    scale: i64,
    zero_count: f64,
    positive_offset: i64,
    positive: Vec<f64>,
    negative_offset: i64,
    negative: Vec<f64>,
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
    /// Maximum decompressed response bytes across a request's tables.
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
    /// column) pairs. A positive matcher on such a label filters the column
    /// instead of the attribute maps, so a sort-key column skips other
    /// values' rows. The operator asserts that the column mirrors the label
    /// on every row: a row carrying the label has the same value in the
    /// column, and a row whose column holds a value carries the label with
    /// that value. Rows that break the second half are still rejected by the
    /// Rust matcher, but are read from ClickHouse first.
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
        // ClickHouse closes idle HTTP connections after its keep_alive_timeout
        // (30 s by default). Dropping pooled connections first means a
        // dashboard refresh never reuses a socket the server just closed, and
        // keepalive probes keep the rest open through a NAT or load balancer.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .pool_idle_timeout(Duration::from_secs(25))
            .tcp_keepalive(Duration::from_secs(10))
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
        let bytes = self.query_bytes(sql, params).await?;
        String::from_utf8(bytes)
            .map_err(|_| BackendError("ClickHouse returned invalid UTF-8".into()).into())
    }
    pub(crate) async fn query_bytes(
        &self,
        sql: &str,
        params: &[(&str, String)],
    ) -> Result<Vec<u8>> {
        self.query_inner(sql, params)
            .await
            .map_err(|e| BackendError(format!("{e:#}")).into())
    }
    /// Sends one statement. Responses are requested zstd-compressed and
    /// decompressed while streaming, so `max_bytes` bounds decompressed bytes.
    async fn query_inner(&self, sql: &str, params: &[(&str, String)]) -> Result<Vec<u8>> {
        let started = Instant::now();
        let mut response = self
            .client
            .post(&self.url)
            .basic_auth(&self.username, Some(&self.password))
            .query(params)
            .query(&[
                ("cancel_http_readonly_queries_on_client_close", "1"),
                ("max_execution_time", "120"),
                ("enable_http_compression", "1"),
            ])
            .body(sql.to_owned())
            .send()
            .await
            .context("ClickHouse request failed")?;
        let headers_ms = started.elapsed().as_millis() as u64;
        let status = response.status();
        let summary = Summary::from_headers(response.headers());
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
        // clickhouse_ms is the server's execution time up to its response
        // headers, which for a grouped sample query is the whole aggregation.
        // headers_ms minus it is the connection, request and queueing cost.
        tracing::debug!(
            headers_ms,
            clickhouse_ms = summary.elapsed_ms,
            read_rows = summary.read_rows,
            total_ms = started.elapsed().as_millis() as u64,
            bytes = bytes.len(),
            query = sql.get(..160).unwrap_or(sql),
            "clickhouse round trip"
        );
        if !status.is_success() {
            bail!(
                "ClickHouse returned {status}: {}",
                String::from_utf8_lossy(&bytes)
                    .chars()
                    .take(500)
                    .collect::<String>()
            )
        }
        Ok(bytes)
    }
    pub async fn ready(&self) -> Result<()> {
        self.query("SELECT 1", &[]).await?;
        Ok(())
    }
    /// For each selector, the stored metric names it can match in this
    /// table, or none when it cannot be narrowed by name. OTel name/unit
    /// normalization is resolved from a small distinct-name dictionary before
    /// fetching samples: filtering raw names directly would drop SDK names
    /// such as http.request.duration (exported as http_request_duration_seconds).
    async fn metric_names(
        &self,
        kind: &str,
        start: i64,
        selectors: &[VectorSelector],
    ) -> Result<Vec<Option<Vec<String>>>> {
        let name_selectors = selectors
            .iter()
            .map(|s| {
                let mut s = s.clone();
                retain_matchers(&mut s, |name| name == "__name__");
                s
            })
            .collect::<Vec<_>>();
        let accepts = |selector: &VectorSelector, name: &str| {
            let labels = Labels::from([("__name__".into(), name.into())]);
            matches(selector, &labels)
        };
        // A selector that could read target_info reads every row's resource.
        let mut names: Vec<Option<BTreeSet<String>>> = name_selectors
            .iter()
            .map(|selector| {
                if accepts(selector, "target_info") {
                    None
                } else {
                    Some(BTreeSet::new())
                }
            })
            .collect();
        if names.iter().all(Option::is_none) {
            return Ok(vec![None; names.len()]);
        }
        let rows = self.metric_dictionary(kind, start).await?;
        for (index, row) in rows.iter().enumerate() {
            if index.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
            let Ok(name) = row_metric_name(row, kind) else {
                continue;
            };
            let derived = derived_names(&name, kind);
            for (selector, names) in name_selectors.iter().zip(&mut names) {
                if let Some(names) = names
                    && derived.iter().any(|n| accepts(selector, n))
                {
                    names.insert(string(row, "MetricName").to_owned());
                }
            }
        }
        Ok(names
            .into_iter()
            .map(|names| names.map(|names| names.into_iter().collect()))
            .collect())
    }

    /// The predicate selecting the rows of one table the selectors can
    /// match, or none when no selector can match a metric stored there.
    pub(crate) async fn table_filter(
        &self,
        kind: &str,
        start: i64,
        selectors: &[VectorSelector],
    ) -> Result<Option<Predicate>> {
        let names = self.metric_names(kind, start, selectors).await?;
        Ok(selector_filter(
            kind,
            selectors,
            &names,
            &self.promoted_resource_attributes,
            &self.label_columns,
        ))
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

    /// Resolves every table's row filter concurrently.
    async fn table_filters(
        &self,
        start: i64,
        selectors: &[VectorSelector],
    ) -> Result<[Option<Predicate>; 5]> {
        let filter = |kind| self.table_filter(kind, start, selectors);
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
    async fn fetch_concurrently(&self, requests: &[TableQuery]) -> Result<Vec<TablePayload>> {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, request) in requests.iter().enumerate() {
            let store = self.clone();
            let request = request.clone();
            tasks.spawn(async move { (index, store.fetch_table(&request).await) });
        }
        let mut payloads: Vec<Option<TablePayload>> = (0..requests.len()).map(|_| None).collect();
        while let Some(joined) = tasks.join_next().await {
            let (index, payload) = joined.context("ClickHouse query task failed")?;
            payloads[index] = Some(payload?);
        }
        payloads
            .into_iter()
            .map(|payload| payload.context("missing ClickHouse table response"))
            .collect()
    }

    /// Fetches sample rows as RowBinary, or as JSON when the response holds a
    /// column type the RowBinary reader cannot decode, so an unknown schema
    /// addition slows queries down instead of failing them.
    async fn fetch_table(&self, request: &TableQuery) -> Result<TablePayload> {
        let params = request
            .params
            .iter()
            .map(|(key, value)| (key.as_str(), value.clone()))
            .collect::<Vec<_>>();
        if !request.grouped {
            let text = self
                .query(&request.statement(Format::Json), &params)
                .await?;
            return Ok(TablePayload::Json(text));
        }
        let bytes = self
            .query_bytes(&request.statement(Format::RowBinary), &params)
            .await?;
        match prepare_rowbinary(bytes, &request.sample_fields) {
            Ok(payload) => Ok(payload),
            Err(error) if error.is::<Unsupported>() => {
                ROWBINARY_FALLBACK_WARNING.call_once(|| {
                    tracing::warn!(
                        ?error,
                        table = request.kind,
                        "reading samples as JSON because of an unsupported column type; this is slower"
                    );
                });
                let text = self
                    .query(&request.statement(Format::Json), &params)
                    .await?;
                Ok(TablePayload::Json(text))
            }
            Err(error) => Err(error),
        }
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
        let filters = self.table_filters(start, selectors).await?;
        let mut requests = Vec::new();
        for (kind, filter) in TABLES.into_iter().zip(filters) {
            let Some(filter) = filter else {
                continue;
            };
            requests.push(self.table_query(kind, filter, start, end, selectors, samples));
        }
        let payloads = self.fetch_concurrently(&requests).await?;
        let mut decoder = Decoder::new(self, selectors);
        let mut bytes_read = 0usize;
        for (request, payload) in requests.iter().zip(payloads) {
            // The byte budget is shared across sample/discovery tables, not
            // reset for each response. Dictionary metadata has its own cache.
            bytes_read += payload.len();
            if bytes_read > self.max_bytes {
                bail!("ClickHouse response byte limit exceeded");
            }
            match payload {
                TablePayload::RowBinary {
                    bytes,
                    columns,
                    layout,
                    body,
                } => {
                    let mut reader = Reader::at(&bytes, body);
                    let mut index = 0usize;
                    while !reader.is_empty() {
                        if index.is_multiple_of(1024) {
                            tokio::task::yield_now().await;
                        }
                        let (row, group) = read_group(&mut reader, &columns, &layout, request.kind)
                            .with_context(|| decode_failure(&bytes))?;
                        decoder.charge_rows(group.len())?;
                        decoder.sample_group(request.kind, row, group).await?;
                        index += 1;
                    }
                }
                TablePayload::Json(text) => {
                    for (index, line) in text.lines().enumerate() {
                        if index.is_multiple_of(1024) {
                            tokio::task::yield_now().await;
                        }
                        let mut row: Json =
                            serde_json::from_str(line).context("invalid ClickHouse metric row")?;
                        if request.grouped {
                            let group = decoder.json_group(&mut row, request)?;
                            decoder.sample_group(request.kind, row, group).await?;
                        } else {
                            decoder.discovery_row(request.kind, row).await?;
                        }
                    }
                }
            }
        }
        decoder.finish().await
    }

    /// Builds the query for one table.
    fn table_query(
        &self,
        kind: &'static str,
        (predicate, filter_params): Predicate,
        start: i64,
        end: i64,
        selectors: &[VectorSelector],
        samples: bool,
    ) -> TableQuery {
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
                placeholders =
                    ", emptyArrayUInt64() AS BucketCounts, emptyArrayFloat64() AS ExplicitBounds";
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
            "{}.otel_metrics_{kind} WHERE TimeUnix >= fromUnixTimestamp64Milli({{start:Int64}}) AND TimeUnix <= fromUnixTimestamp64Milli({{end:Int64}}){predicate}",
            self.database
        );
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
                "SELECT * EXCEPT ({fields}), groupArray(tuple({fields})) AS _samples FROM (SELECT {projection} FROM {source} LIMIT {row_limit}) GROUP BY ALL"
            )
        } else {
            format!("SELECT {projection} FROM {source} LIMIT {row_limit}")
        };
        let mut params = vec![
            ("param_start".to_owned(), start.to_string()),
            ("param_end".to_owned(), end.to_string()),
        ];
        params.extend(filter_params);
        TableQuery {
            kind,
            grouped,
            sample_fields,
            sql,
            params,
        }
    }
}

/// Server-side figures from the `X-ClickHouse-Summary` response header.
#[derive(Debug, Default, PartialEq)]
struct Summary {
    elapsed_ms: Option<u64>,
    read_rows: Option<u64>,
}

impl Summary {
    /// Missing or unparseable headers leave the fields unset; the summary
    /// only feeds logs.
    fn from_headers(headers: &reqwest::header::HeaderMap) -> Self {
        let Some(text) = headers
            .get("x-clickhouse-summary")
            .and_then(|value| value.to_str().ok())
        else {
            return Self::default();
        };
        let Ok(summary) = serde_json::from_str::<Json>(text) else {
            return Self::default();
        };
        let number = |key: &str| summary[key].as_str().and_then(|v| v.parse::<u64>().ok());
        Self {
            elapsed_ms: number("elapsed_ns").map(|ns| ns / 1_000_000),
            read_rows: number("read_rows"),
        }
    }
}

/// Parses and checks a RowBinary sample response's header.
fn prepare_rowbinary(bytes: Vec<u8>, fields: &[&str]) -> Result<TablePayload> {
    let mut reader = Reader::new(&bytes);
    let columns = reader.header().with_context(|| decode_failure(&bytes))?;
    let layout = SampleLayout::new(&columns, fields)?;
    let body = reader.offset();
    Ok(TablePayload::RowBinary {
        bytes,
        columns,
        layout,
        body,
    })
}

/// Reads one grouped RowBinary row: its label and metadata columns as the
/// JSON object the JSON transport would present, and its samples by type.
/// The SQL row limit bounds the samples a group can hold.
fn read_group(
    reader: &mut Reader,
    columns: &[Column],
    layout: &SampleLayout,
    kind: &str,
) -> Result<(Json, Vec<Sample>)> {
    let mut row = serde_json::Map::new();
    let mut samples = Vec::new();
    for (index, column) in columns.iter().enumerate() {
        if index == layout.column {
            let count = reader.length()?;
            samples.reserve_exact(count.min(1024));
            for _ in 0..count {
                samples.push(Sample::read(reader, layout, kind)?);
            }
        } else {
            row.insert(column.name.clone(), reader.cell(&column.ty)?);
        }
    }
    Ok((Json::Object(row), samples))
}

/// Context for a RowBinary decoding failure. A query that fails after
/// ClickHouse started streaming its response, for example on a memory or
/// execution time limit, ends with the exception text instead of rows.
fn decode_failure(bytes: &[u8]) -> String {
    match trailing_exception(bytes) {
        Some(text) => format!(
            "ClickHouse failed while streaming the response: {}",
            text.chars().take(500).collect::<String>()
        ),
        None => "invalid ClickHouse metric row".to_owned(),
    }
}

/// Decodes table responses into series, enforcing the row and sample budgets
/// across every table of one request.
struct Decoder<'a> {
    store: &'a Store,
    selectors: &'a [VectorSelector],
    groups: BTreeMap<Labels, Vec<Point>>,
    metadata: BTreeMap<String, Vec<Json>>,
    warnings: Vec<String>,
    /// Raw rows decoded so far, bounded by `max_rows`.
    rows: usize,
    /// Retained points, native buckets or discovered series, bounded by
    /// `max_samples`.
    retained: usize,
}

/// Output series of one transport group, created on first use so a selector
/// match runs once per derived series instead of once per sample.
struct Outputs<'a> {
    labels: &'a Labels,
    name: &'a str,
    selectors: &'a [VectorSelector],
    /// Samples in the group, the capacity of every output.
    capacity: usize,
    series: Vec<(Labels, Vec<Point>)>,
}

impl Outputs<'_> {
    /// The output for `name` plus `suffix` and an optional generated label,
    /// or none when no selector reads it.
    fn resolve(&mut self, suffix: &str, extra: Option<(&str, f64)>) -> Option<usize> {
        let mut labels = self.labels.clone();
        labels.insert("__name__".into(), format!("{}{suffix}", self.name));
        if let Some((label, value)) = extra {
            labels.insert(label.into(), float_string(value));
        }
        if self.selectors.is_empty() || self.selectors.iter().any(|s| matches(s, &labels)) {
            self.series
                .push((labels, Vec::with_capacity(self.capacity)));
            Some(self.series.len() - 1)
        } else {
            None
        }
    }
}

impl<'a> Decoder<'a> {
    fn new(store: &'a Store, selectors: &'a [VectorSelector]) -> Self {
        Self {
            store,
            selectors,
            groups: BTreeMap::new(),
            metadata: BTreeMap::new(),
            warnings: Vec::new(),
            rows: 0,
            retained: 0,
        }
    }

    fn charge_rows(&mut self, amount: usize) -> Result<()> {
        self.rows = self
            .rows
            .checked_add(amount)
            .context("ClickHouse sample scan limit exceeded")?;
        if self.rows > self.store.max_rows {
            bail!("ClickHouse sample scan limit exceeded");
        }
        Ok(())
    }

    fn charge(&mut self, amount: usize) -> Result<()> {
        charge_samples(&mut self.retained, amount, self.store.max_samples)
    }

    fn warn(&mut self, warning: String) {
        if !self.warnings.contains(&warning) {
            self.warnings.push(warning);
        }
    }

    /// Takes the samples out of a grouped JSON row.
    fn json_group(&mut self, row: &mut Json, request: &TableQuery) -> Result<Vec<Sample>> {
        let tuples = match row
            .as_object_mut()
            .context("invalid grouped metric row")?
            .remove("_samples")
        {
            Some(Json::Array(tuples)) => tuples,
            _ => bail!("missing grouped metric samples"),
        };
        self.charge_rows(tuples.len())?;
        tuples
            .iter()
            .map(|tuple| {
                let values = tuple.as_array().context("invalid grouped metric sample")?;
                Sample::from_json(values, &request.sample_fields, request.kind)
            })
            .collect()
    }

    /// The Prometheus name of a row's metric, or none after recording why the
    /// row is skipped.
    fn metric_name(&mut self, row: &Json, kind: &str) -> Option<String> {
        match row_metric_name(row, kind) {
            Ok(name) => Some(name),
            Err(error) => {
                self.warn(format!(
                    "skipped invalid OTel metric name {:?}: {error}",
                    string(row, "MetricName")
                ));
                None
            }
        }
    }

    /// Whether any selector can read a series derived from this row.
    fn selected(&self, kind: &str, name: &str, labels: &Labels) -> bool {
        self.selectors.is_empty()
            || derived_names(name, kind).iter().any(|n| {
                let mut l = labels.clone();
                l.insert("__name__".into(), n.clone());
                if kind == "histogram" {
                    l.insert("le".into(), "+Inf".into());
                }
                self.selectors
                    .iter()
                    .any(|s| matches(s, &l) || potential_bucket_match(s, &l, kind))
            })
    }

    fn record_metadata(&mut self, kind: &str, row: &Json, name: &str) {
        let typ = match kind {
            "sum" if row["IsMonotonic"].as_bool() == Some(true) => "counter",
            "sum" | "gauge" => "gauge",
            "histogram" | "exponential_histogram" => "histogram",
            _ => "summary",
        };
        let help = string(row, "MetricDescription");
        let unit = string(row, "MetricUnit");
        // Most sample rows repeat the same metadata. Compare borrowed
        // strings before allocating another JSON object for every point.
        let known = self.metadata.get(name).is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry["type"] == typ && entry["help"] == help && entry["unit"] == unit)
        });
        if !known {
            self.metadata
                .entry(name.to_owned())
                .or_default()
                .push(serde_json::json!({"type":typ,"help":help,"unit":unit}));
        }
    }

    /// Decodes one transport group of samples into its derived series.
    async fn sample_group(
        &mut self,
        kind: &'static str,
        row: Json,
        samples: Vec<Sample>,
    ) -> Result<()> {
        // Group identity is immutable. Normalize/match labels and
        // metadata once, then decode only point fields inside the loops.
        let Some(name) = self.metric_name(&row, kind) else {
            return Ok(());
        };
        let mut labels = row_labels(&row, &self.store.promoted_resource_attributes)?;
        labels.insert("__name__".into(), name.clone());
        let info = target_info(&row)?.filter(|info| {
            self.selectors.is_empty() || self.selectors.iter().any(|s| matches(s, info))
        });
        let selected = self.selected(kind, &name, &labels);
        if selected {
            self.record_metadata(kind, &row, &name);
        }
        let mut outputs: Vec<(Labels, Vec<Point>)> = Vec::new();
        if let Some(info) = info {
            let mut points = Vec::with_capacity(samples.len());
            for sample in &samples {
                if !sample.stale() {
                    self.charge(1)?;
                    points.push(Point {
                        timestamp: sample.timestamp,
                        value: 1.0,
                        ..Default::default()
                    });
                }
            }
            outputs.push((info, points));
        }
        if selected {
            if matches!(kind, "sum" | "histogram" | "exponential_histogram")
                && integer(&row["AggregationTemporality"])? == 1
            {
                bail!(
                    "delta temporality for {name} requires cumulative conversion; configure the collector deltatocumulative processor"
                )
            }
            if kind == "exponential_histogram" {
                let points = self.native_points(&row, &samples).await?;
                outputs.push((labels, points));
            } else {
                let mut resolver = Outputs {
                    labels: &labels,
                    name: &name,
                    selectors: self.selectors,
                    capacity: samples.len(),
                    series: outputs,
                };
                self.float_points(kind, &row, &samples, &mut resolver)
                    .await?;
                outputs = resolver.series;
            }
        }
        for (labels, mut points) in outputs {
            match self.groups.entry(labels) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(points);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().append(&mut points);
                }
            }
        }
        Ok(())
    }

    /// Appends a float point to `output`, if any selector reads that series.
    fn push(
        &mut self,
        series: &mut [(Labels, Vec<Point>)],
        output: Option<usize>,
        sample: &Sample,
        value: f64,
    ) -> Result<()> {
        if let Some(index) = output {
            self.charge(1)?;
            series[index].1.push(Point {
                timestamp: sample.timestamp,
                start_timestamp: sample.start_timestamp,
                value: if sample.stale() {
                    f64::from_bits(STALE_BITS)
                } else {
                    value
                },
                ..Default::default()
            });
        }
        Ok(())
    }

    /// Decodes gauge, sum, histogram and summary samples into float series.
    async fn float_points(
        &mut self,
        kind: &str,
        row: &Json,
        samples: &[Sample],
        outputs: &mut Outputs<'_>,
    ) -> Result<()> {
        let name = outputs.name;
        match kind {
            "gauge" | "sum" => {
                let output = outputs.resolve("", None);
                for (index, sample) in samples.iter().enumerate() {
                    if index.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                    let Measurement::Float(value) = sample.measurement else {
                        bail!("invalid metric sample for {name}")
                    };
                    self.push(&mut outputs.series, output, sample, value)?;
                }
            }
            "histogram" => {
                let bounds = numbers(&row["ExplicitBounds"]).context("missing ExplicitBounds")?;
                // Bounds are part of the group, so every sample with buckets
                // maps onto the same output series.
                let mut bucket_outputs: Option<Vec<Option<usize>>> = None;
                let mut infinity_output = None;
                let sum_output = outputs.resolve("_sum", None);
                let count_output = outputs.resolve("_count", None);
                for (index, sample) in samples.iter().enumerate() {
                    if index.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                    let Measurement::Histogram {
                        count,
                        sum,
                        bucket_counts,
                    } = &sample.measurement
                    else {
                        bail!("invalid histogram sample for {name}")
                    };
                    if (bucket_counts.is_empty() && !bounds.is_empty())
                        || (!bucket_counts.is_empty() && bucket_counts.len() != bounds.len() + 1)
                    {
                        bail!("invalid histogram bucket count for {name}")
                    }
                    if bucket_counts.is_empty() {
                        let output = *infinity_output.get_or_insert_with(|| {
                            outputs.resolve("_bucket", Some(("le", f64::INFINITY)))
                        });
                        self.push(&mut outputs.series, output, sample, *count)?;
                    } else {
                        if bucket_outputs.is_none() {
                            let mut resolved = Vec::with_capacity(bounds.len() + 1);
                            for bound in &bounds {
                                resolved.push(outputs.resolve("_bucket", Some(("le", *bound))));
                            }
                            resolved.push(*infinity_output.get_or_insert_with(|| {
                                outputs.resolve("_bucket", Some(("le", f64::INFINITY)))
                            }));
                            bucket_outputs = Some(resolved);
                        }
                        let mut cumulative = 0.0;
                        for (output, bucket_count) in
                            bucket_outputs.iter().flatten().zip(bucket_counts)
                        {
                            cumulative += bucket_count;
                            self.push(&mut outputs.series, *output, sample, cumulative)?;
                        }
                    }
                    self.push(&mut outputs.series, sum_output, sample, *sum)?;
                    self.push(&mut outputs.series, count_output, sample, *count)?;
                }
            }
            "summary" => {
                // Quantiles can differ between samples, so their outputs are
                // resolved per distinct quantile.
                let mut quantile_outputs: HashMap<u64, Option<usize>> = HashMap::new();
                let sum_output = outputs.resolve("_sum", None);
                let count_output = outputs.resolve("_count", None);
                for (index, sample) in samples.iter().enumerate() {
                    if index.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                    let Measurement::Summary {
                        count,
                        sum,
                        quantiles,
                        values,
                    } = &sample.measurement
                    else {
                        bail!("invalid summary sample for {name}")
                    };
                    if quantiles.len() != values.len() {
                        bail!("invalid summary quantiles for {name}")
                    }
                    for (quantile, value) in quantiles.iter().zip(values) {
                        let output = *quantile_outputs
                            .entry(quantile.to_bits())
                            .or_insert_with(|| outputs.resolve("", Some(("quantile", *quantile))));
                        self.push(&mut outputs.series, output, sample, *value)?;
                    }
                    self.push(&mut outputs.series, sum_output, sample, *sum)?;
                    self.push(&mut outputs.series, count_output, sample, *count)?;
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Converts exponential histogram samples into native histogram points.
    async fn native_points(&mut self, row: &Json, samples: &[Sample]) -> Result<Vec<Point>> {
        let zero_threshold = row
            .get("ZeroThreshold")
            .map(number)
            .transpose()?
            .unwrap_or(0.0);
        if row.get("ZeroThreshold").is_none() {
            self.warn(
                "Collector schema does not store exponential histogram ZeroThreshold; assuming zero"
                    .to_owned(),
            );
        }
        let mut points = Vec::with_capacity(samples.len());
        for (index, sample) in samples.iter().enumerate() {
            if index.is_multiple_of(1024) {
                tokio::task::yield_now().await;
            }
            let Measurement::Exponential(exponential) = &sample.measurement else {
                bail!("invalid exponential histogram sample")
            };
            self.charge(1)?;
            self.charge(exponential.positive.len())?;
            self.charge(exponential.negative.len())?;
            let schema = i32::try_from(exponential.scale)
                .ok()
                .filter(|schema| (-4..=20).contains(schema))
                .with_context(|| {
                    format!(
                        "OTLP exponential histogram scale {} cannot be represented as a Prometheus native histogram",
                        exponential.scale
                    )
                })?;
            let mut histogram = crate::histogram::Histogram {
                schema,
                count: exponential.count,
                sum: exponential.sum,
                zero_count: exponential.zero_count,
                zero_threshold,
                ..Default::default()
            };
            for (offset, buckets, counts) in [
                (
                    exponential.positive_offset,
                    &mut histogram.positive,
                    &exponential.positive,
                ),
                (
                    exponential.negative_offset,
                    &mut histogram.negative,
                    &exponential.negative,
                ),
            ] {
                for (i, count) in counts.iter().enumerate() {
                    let index = offset.saturating_add(i as i64).saturating_add(1);
                    let index =
                        i32::try_from(index).context("native histogram bucket index overflow")?;
                    buckets.insert(index, *count);
                }
            }
            if histogram.schema > 8 {
                histogram.reduce(8)?;
            }
            if sample.stale() {
                histogram.sum = f64::from_bits(STALE_BITS)
            }
            points.push(Point {
                timestamp: sample.timestamp,
                start_timestamp: sample.start_timestamp,
                histogram: Some(Box::new(histogram)),
                ..Default::default()
            });
        }
        Ok(points)
    }

    /// Records the series a discovery row describes, one per derived name
    /// and generated label value.
    async fn discovery_row(&mut self, kind: &str, row: Json) -> Result<()> {
        self.charge_rows(1)?;
        let Some(name) = self.metric_name(&row, kind) else {
            return Ok(());
        };
        let mut labels = row_labels(&row, &self.store.promoted_resource_attributes)?;
        labels.insert("__name__".into(), name.clone());
        let info = target_info(&row)?.filter(|info| {
            self.selectors.is_empty() || self.selectors.iter().any(|s| matches(s, info))
        });
        let selected = self.selected(kind, &name, &labels);
        if selected {
            self.record_metadata(kind, &row, &name);
        }
        if let Some(info) = info {
            if !self.groups.contains_key(&info) {
                self.charge(1)?;
            }
            self.groups.entry(info).or_default();
        }
        if !selected {
            return Ok(());
        }
        for derived in derived_names(&name, kind) {
            let generated: Vec<Option<(&str, String)>> =
                if derived.ends_with("_bucket") && kind == "histogram" {
                    numbers(&row["ExplicitBounds"])
                        .context("missing bounds")?
                        .into_iter()
                        .map(|bound| Some(("le", float_string(bound))))
                        .chain(std::iter::once(Some(("le", "+Inf".into()))))
                        .collect()
                } else if derived == name && kind == "summary" {
                    numbers(&row["ValueAtQuantiles.Quantile"])
                        .context("missing quantiles")?
                        .into_iter()
                        .map(|quantile| Some(("quantile", float_string(quantile))))
                        .collect()
                } else {
                    vec![None]
                };
            for (index, extra) in generated.into_iter().enumerate() {
                if index.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                let mut series_labels = labels.clone();
                series_labels.insert("__name__".into(), derived.clone());
                if let Some((label, value)) = extra {
                    series_labels.insert(label.into(), value);
                }
                if self.selectors.is_empty()
                    || self.selectors.iter().any(|s| matches(s, &series_labels))
                {
                    if !self.groups.contains_key(&series_labels) {
                        self.charge(1)?;
                    }
                    self.groups.entry(series_labels).or_default();
                }
            }
        }
        Ok(())
    }

    /// Orders every series' points, rejecting rows that disagree about a
    /// timestamp, and builds the loaded result.
    async fn finish(self) -> Result<Loaded> {
        let mut loaded = Loaded {
            series: Vec::with_capacity(self.groups.len()),
            metadata: self.metadata,
            warnings: self.warnings,
        };
        for (index, (labels, mut points)) in self.groups.into_iter().enumerate() {
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

/// A SQL `WHERE` fragment starting with ` AND`, or empty to admit every row,
/// with its bound parameters.
pub(crate) type Predicate = (String, Vec<(String, String)>);

/// Conservative SQL prefilter for one table. Rust remains the authority for
/// final matching. Each selector admits the rows whose metric name it can
/// match (`names`, resolved per selector) and whose pushable matchers hold,
/// so a selector without pushable matchers admits only its own metrics, not
/// every row of the other selectors' metrics. General regex, generated labels
/// and scope labels stay in Rust to avoid ClickHouse RE2/normalization
/// differences changing which series are returned. Each OR branch contributes
/// independently; an unfilterable branch admits its selector's names.
/// Returns none when no selector can match a metric stored in the table.
pub(crate) fn selector_filter(
    kind: &str,
    selectors: &[VectorSelector],
    names: &[Option<Vec<String>>],
    promoted: &[(String, String)],
    columns: &[(String, String)],
) -> Option<Predicate> {
    if selectors.is_empty() {
        return Some((String::new(), Vec::new()));
    }
    let mut params = Vec::new();
    let mut alternatives = Vec::new();
    for (selector, names) in selectors.iter().zip(names) {
        let name_term = match names {
            Some(names) if names.is_empty() => continue,
            Some(names) => {
                let key = format!("names_{}", params.len());
                params.push((format!("param_{key}"), string_array(names)));
                Some(format!("MetricName IN {{{key}:Array(String)}}"))
            }
            None => None,
        };
        let before = params.len();
        let label_term = label_term(kind, selector, promoted, columns, &mut params);
        if label_term.is_none() {
            params.truncate(before);
        }
        let alternative = match (name_term, label_term) {
            (Some(name), Some(labels)) => format!("({name} AND {labels})"),
            (Some(name), None) => format!("({name})"),
            (None, Some(labels)) => labels,
            // Nothing narrows this selector, so nothing narrows the table.
            (None, None) => return Some((String::new(), Vec::new())),
        };
        alternatives.push(alternative);
    }
    if alternatives.is_empty() {
        return None;
    }
    Some((format!(" AND ({})", alternatives.join(" OR ")), params))
}

/// The label part of one selector's filter, its OR branches' terms, or none
/// when some branch admits every row.
fn label_term(
    kind: &str,
    selector: &VectorSelector,
    promoted: &[(String, String)],
    columns: &[(String, String)],
    params: &mut Vec<(String, String)>,
) -> Option<String> {
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
    let mut terms = Vec::new();
    for branch in branches {
        let branch_terms = branch_terms(kind, selector, branch, promoted, columns, params)?;
        terms.push(format!("({})", branch_terms.join(" AND ")));
    }
    Some(format!("({})", terms.join(" OR ")))
}

/// The terms of one OR branch, or none when the branch admits every row.
fn branch_terms(
    kind: &str,
    selector: &VectorSelector,
    branch: &[Matcher],
    promoted: &[(String, String)],
    columns: &[(String, String)],
    params: &mut Vec<(String, String)>,
) -> Option<Vec<String>> {
    // target_info has resource attributes, not point attributes. A
    // branch that could select it cannot use ordinary label pushdown.
    if selector.name.as_deref().is_none_or(|n| n == "target_info")
        && branch
            .iter()
            .filter(|m| m.name == "__name__")
            .all(|m| m.is_match("target_info"))
    {
        return None;
    }
    let mut terms = Vec::new();
    for matcher in branch {
        if let Some(term) = column_term(matcher, columns, params) {
            // The column mirrors the label, so it decides a positive matcher
            // by itself; an attribute term would repeat the check on the
            // slower map column.
            terms.push(term);
            continue;
        }
        if let Some(term) = attribute_term(kind, matcher, promoted, params) {
            terms.push(term);
        }
    }
    if terms.is_empty() { None } else { Some(terms) }
}

/// Filters one matcher on the attribute maps, or none for matchers that
/// stay in Rust.
fn attribute_term(
    kind: &str,
    matcher: &Matcher,
    promoted: &[(String, String)],
    params: &mut Vec<(String, String)>,
) -> Option<String> {
    // A promoted label that shadows job or instance follows their
    // precedence in row_labels; filter it in Rust only.
    let promotion = promoted.iter().find(|(_, label)| *label == matcher.name);
    if matcher.name == "__name__"
        || matcher.name.starts_with("otel_scope_")
        || (promotion.is_some() && matches!(matcher.name.as_str(), "job" | "instance"))
        || (kind == "histogram" && matcher.name == "le")
        || (kind == "summary" && matcher.name == "quantile")
    {
        return None;
    }
    let (op, value_type, parameter) = match &matcher.op {
        MatchOp::Equal => ("=", "String", string_param(&matcher.value)),
        MatchOp::NotEqual => ("!=", "String", string_param(&matcher.value)),
        MatchOp::Re(_) | MatchOp::NotRe(_) => {
            // Prometheus regexes are fully anchored. Bare literals
            // and literal alternatives are exactly an equality set;
            // all other regex syntax remains in Rust.
            if matcher.value.chars().any(|c| ".*+?()[]{}\\^$".contains(c)) {
                return None;
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
    let normalized =
        "concat(if(match(k, '^[0-9]'), 'key_', ''), replaceRegexpAll(k, '[^a-zA-Z0-9]', '_'))";
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
    Some(format!(
        "(({expression} {op} {{{value}:{value_type}}}) OR {collision})"
    ))
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
    matcher: &Matcher,
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
/// Every element of a JSON array as a number.
fn numbers(v: &Json) -> Result<Vec<f64>> {
    v.as_array()
        .context("missing numeric array")?
        .iter()
        .map(number)
        .collect()
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

#[cfg(test)]
#[path = "../tests/unit/storage.rs"]
mod tests;
