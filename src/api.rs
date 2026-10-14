mod search;
use crate::{
    engine::Engine,
    storage::{Store, plan},
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use serde_json::{Value as JsonValue, json};
use std::{
    collections::BTreeSet,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::{compression::CompressionLayer, cors::CorsLayer};

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub concurrency: Arc<Semaphore>,
    pub replica_labels: Vec<String>,
    pub store_labels: crate::model::Labels,
    pub request_timeout: Duration,
    pub thanos_default_step: Option<Duration>,
}
impl App {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            concurrency: Arc::new(Semaphore::new(16)),
            replica_labels: Vec::new(),
            store_labels: Default::default(),
            request_timeout: Duration::from_secs(120),
            thanos_default_step: None,
        }
    }
}
pub fn router(app: App) -> Router {
    Router::new().route("/-/healthy",get(||async{"ClickHouse PromQL adapter is healthy\n"}))
        .route("/-/ready",get(ready)).route("/metrics",get(||async{"# HELP clickhouse_promql_build_info Build information.\n# TYPE clickhouse_promql_build_info gauge\nclickhouse_promql_build_info{version=\"0.1.0\"} 1\n"}))
        .route("/api/v1/{*path}",any(handle)).with_state(app).layer(CompressionLayer::new()).layer(CorsLayer::permissive()).layer(axum::middleware::from_fn(options_status))
}
async fn options_status(request: Request, next: axum::middleware::Next) -> Response {
    let options = request.method() == Method::OPTIONS;
    let mut response = next.run(request).await;
    if options {
        *response.status_mut() = StatusCode::NO_CONTENT;
    }
    response
}
async fn ready(State(app): State<App>) -> Response {
    match app.store.ready().await {
        Ok(()) => (StatusCode::OK, "Ready\n").into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "ClickHouse unavailable\n").into_response(),
    }
}
struct ApiError {
    status: StatusCode,
    kind: &'static str,
    message: String,
}
impl ApiError {
    fn bad(message: impl ToString) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            kind: "bad_data",
            message: message.to_string(),
        }
    }
    fn parameter(name: &str, message: impl ToString) -> Self {
        Self::bad(format!(
            "invalid parameter {name:?}: {}",
            message.to_string()
        ))
    }
    fn exec(message: impl ToString) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            kind: "execution",
            message: message.to_string(),
        }
    }
    fn storage(error: anyhow::Error) -> Self {
        if error
            .downcast_ref::<crate::storage::BackendError>()
            .is_some()
        {
            Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                kind: "internal",
                message: error.to_string(),
            }
        } else {
            Self::exec(error)
        }
    }
    fn unavailable(message: impl ToString) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "unavailable",
            message: message.to_string(),
        }
    }
    fn timeout() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            kind: "timeout",
            message: "query timed out in query execution".into(),
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"status":"error","errorType":self.kind,"error":self.message})),
        )
            .into_response()
    }
}
struct CancelOnDrop(Arc<std::sync::atomic::AtomicBool>);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}
type Result<T> = std::result::Result<T, ApiError>;
#[derive(Default, serde::Deserialize)]
#[serde(default)]
struct Shard {
    shard_index: i64,
    total_shards: i64,
    by: bool,
    labels: Vec<String>,
}
impl Shard {
    fn matches(&self, labels: &crate::model::Labels) -> bool {
        if self.total_shards < 1 {
            return true;
        }
        let mut bytes = Vec::new();
        for (name, value) in labels {
            if self.labels.contains(name) == self.by {
                bytes.extend_from_slice(name.as_bytes());
                bytes.push(255);
                bytes.extend_from_slice(value.as_bytes());
                bytes.push(255);
            }
        }
        xxhash_rust::xxh64::xxh64(&bytes, 0) % self.total_shards as u64 == self.shard_index as u64
    }
}
struct QueryOptions {
    replica_labels: Vec<String>,
    store_matches: bool,
    shard: Option<Shard>,
}
impl QueryOptions {
    fn parse(app: &App, p: &Params) -> Result<Self> {
        let boolean = |name, default| match p.get(name).unwrap_or("") {
            "" => Ok(default),
            "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
            "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
            v => Err(ApiError::bad(format!(
                "'{name}' parameter: strconv.ParseBool: parsing {v:?}: invalid syntax"
            ))),
        };
        let dedup = boolean("dedup", true)?;
        boolean("partial_response", false)?;

        match p.get("engine").unwrap_or("") {
            "" | "prometheus" | "thanos" => (),
            v => return Err(ApiError::bad(format!("'{v}' bad engine"))),
        }
        if let Some(v) = p
            .get("max_source_resolution")
            .filter(|v| !v.is_empty() && *v != "auto")
        {
            let d = parse_duration_ns(v).map_err(|e| {
                ApiError::bad(format!("'max_source_resolution' parameter: {}", e.message))
            })?;
            if d < 0 {
                return Err(ApiError::bad(
                    "negative 'max_source_resolution' is not accepted. Try a positive integer",
                ));
            }
        }
        let mut replica_labels = app.replica_labels.clone();
        let labels = p.all("replicaLabels[]");
        if !labels.is_empty() {
            replica_labels = labels.into_iter().map(str::to_owned).collect();
        }
        if !dedup {
            replica_labels.clear();
        }
        let stores = p.all("storeMatch[]");
        let mut store_matches = stores.is_empty();
        for selector in stores {
            // Store selectors may deliberately match empty external label sets.
            let selector = promql_parser::parser::parse(&format!("__store__{selector}"))
                .or_else(|_| Engine::parse(selector))
                .map_err(ApiError::bad)?;
            let promql_parser::parser::Expr::VectorSelector(mut selector) = selector else {
                return Err(ApiError::bad("storeMatch[] must be a metric selector"));
            };
            if selector.name.as_deref() == Some("__store__") {
                selector.name = None;
            }
            store_matches |= crate::engine::matches(&selector, &app.store_labels);
        }
        let shard = p
            .get("shard_info")
            .filter(|v| !v.is_empty())
            .map(serde_json::from_str)
            .transpose()
            .map_err(ApiError::bad)?;
        Ok(Self {
            replica_labels,
            store_matches,
            shard,
        })
    }
}
#[derive(Default)]
struct Params(Vec<(String, String)>);
impl Params {
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
    fn all(&self, name: &str) -> Vec<&str> {
        self.0
            .iter()
            .filter(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
            .collect()
    }
    fn limit(&self) -> Result<usize> {
        let v = self.get("limit").unwrap_or("");
        if v.is_empty() {
            return Ok(0);
        }
        let n = v.parse::<i64>().map_err(|_| {
            ApiError::parameter(
                "limit",
                format!("strconv.Atoi: parsing {v:?}: invalid syntax"),
            )
        })?;
        if n < 0 {
            return Err(ApiError::parameter("limit", "limit must be non-negative"));
        }
        Ok(n as usize)
    }
    fn time(&self, name: &str, default: i64) -> Result<i64> {
        self.get(name)
            .filter(|v| !v.is_empty())
            .map_or(Ok(default), |v| {
                parse_time(v).map_err(|e| {
                    ApiError::parameter(
                        name,
                        format!("invalid time value for '{name}': {}", e.message),
                    )
                })
            })
    }
    fn selectors(&self, required: bool) -> Result<Vec<promql_parser::parser::VectorSelector>> {
        let values = self.all("match[]");
        if required && values.is_empty() {
            return Err(ApiError::bad("no match[] parameter provided"));
        }
        values
            .into_iter()
            .map(|v| {
                let result = match Engine::parse(v) {
                    Ok(promql_parser::parser::Expr::VectorSelector(v)) => Ok(v),
                    Ok(_) => {
                        let pos = v.find('(').unwrap_or(0);
                        Err(format!("1:{}: parse error: unexpected \"(\"", pos + 1))
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        Err(if msg.contains("at least one non-empty matcher") {
                            "match[] must contain at least one non-empty matcher".into()
                        } else {
                            parse_error(v, &msg)
                        })
                    }
                };
                result.map_err(|e| {
                    if required {
                        ApiError::parameter("match[]", e)
                    } else {
                        ApiError::bad(e)
                    }
                })
            })
            .collect()
    }
}
async fn handle(State(app): State<App>, request: Request) -> Response {
    if request.method() == Method::OPTIONS {
        return StatusCode::NO_CONTENT.into_response();
    }
    if request.method() != Method::GET && request.method() != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed\n").into_response();
    }
    let search = request.uri().path().starts_with("/api/v1/search/");
    let timeout = app.request_timeout;
    let result = tokio::time::timeout(timeout, async {
        let permit = app
            .concurrency
            .clone()
            .acquire_owned()
            .await
            .map_err(ApiError::unavailable)?;
        dispatch(app, request, permit).await
    })
    .await
    .unwrap_or_else(|_| Err(ApiError::timeout()));
    match result {
        Ok(value) if search => {
            let text = value
                .as_array()
                .unwrap()
                .iter()
                .map(|v| format!("{v}\n"))
                .collect::<String>();
            (
                [("content-type", "application/x-ndjson; charset=utf-8")],
                text,
            )
                .into_response()
        }
        Ok(value) if value.get("data").is_none() => StatusCode::NO_CONTENT.into_response(),
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    }
}
async fn dispatch(app: App, request: Request, permit: OwnedSemaphorePermit) -> Result<JsonValue> {
    let method = request.method().clone();
    if method != Method::GET && method != Method::POST {
        return Err(ApiError {
            status: StatusCode::METHOD_NOT_ALLOWED,
            kind: "bad_data",
            message: "method not allowed".into(),
        });
    }
    let path = request
        .uri()
        .path()
        .trim_start_matches("/api/v1/")
        .to_owned();
    let query = request.uri().query().unwrap_or("").to_owned();
    let mut params = Params::default();
    if method == Method::POST
        && request
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("application/x-www-form-urlencoded")
    {
        let body = to_bytes(request.into_body(), 1024 * 1024)
            .await
            .map_err(ApiError::bad)?;
        params
            .0
            .extend(url::form_urlencoded::parse(&body).into_owned());
    }
    params
        .0
        .extend(url::form_urlencoded::parse(query.as_bytes()).into_owned());
    if path == "status/buildinfo" {
        return Ok(success(
            json!({"version":"3.15.0","revision":"","branch":"","buildUser":"","buildDate":"","goVersion":"","adapterVersion":env!("CARGO_PKG_VERSION")}),
        ));
    }
    if path == "format_query" || path == "parse_query" {
        let query = params.get("query").unwrap_or("");
        let expr = Engine::parse(query)
            .map_err(|e| ApiError::parameter("query", parse_error(query, &e.to_string())))?;
        return Ok(success(if path == "format_query" {
            json!(promql_parser::parser::Prettier::pretty(&expr, 0, 100))
        } else {
            api_ast(serde_json::to_value(expr).map_err(ApiError::exec)?)
        }));
    }
    let timeout = params
        .get("timeout")
        .filter(|v| !v.is_empty())
        .map(|v| parse_duration(v).map_err(|e| ApiError::parameter("timeout", e.message)))
        .transpose()?
        .unwrap_or(120_000);
    if timeout <= 0 {
        return Err(ApiError::timeout());
    }
    let duration = Duration::from_millis(timeout.min(120_000) as u64);
    let deadline = Instant::now() + duration;
    tokio::time::timeout(duration, async {
        match path.as_str() {
            "query" | "query_range" => {
                query_api(&app, &params, path == "query_range", deadline, permit).await
            }
            "query_explain" | "query_range_explain" => {
                QueryOptions::parse(&app, &params)?;
                Err(ApiError::bad(if params.get("engine") == Some("thanos") {
                    "Query not explainable"
                } else {
                    "engine type must be 'thanos'"
                }))
            }
            "search/metric_names" | "search/label_names" | "search/label_values" => {
                search::run(&app, &params, &path[7..], deadline, permit).await
            }
            "query_exemplars" => exemplar_api(&app, &params).await,
            "labels" | "series" | "metadata" => discovery_api(&app, &params, &path).await,
            _ if path.starts_with("label/") && path.ends_with("/values") && path.len() > 13 => {
                discovery_api(&app, &params, &path).await
            }
            _ => Err(ApiError {
                status: StatusCode::NOT_FOUND,
                kind: "not_found",
                message: format!("endpoint /api/v1/{path} is not implemented"),
            }),
        }
    })
    .await
    .map_err(|_| ApiError::timeout())?
}
fn cap_annotations(mut values: Vec<String>, kind: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values.retain(|v| seen.insert(v.clone()));
    if values.len() > 10 {
        let omitted = values.len() - 10;
        values.truncate(10);
        values.push(format!("{omitted} more {kind} annotations omitted"));
    }
    values
}
fn success(data: JsonValue) -> JsonValue {
    json!({"status":"success","data":data})
}
async fn query_api(
    app: &App,
    p: &Params,
    range: bool,
    deadline: Instant,
    permit: OwnedSemaphorePermit,
) -> Result<JsonValue> {
    let preparation_time = Instant::now();
    let query = p.get("query").unwrap_or("");
    let limit = p.limit()?;
    let options = QueryOptions::parse(app, p)?;

    let now = chrono::Utc::now().timestamp_millis();
    let (start, end, step) = if range {
        let start = parse_time(p.get("start").unwrap_or(""))
            .map_err(|e| ApiError::parameter("start", e.message))?;
        let end = parse_time(p.get("end").unwrap_or(""))
            .map_err(|e| ApiError::parameter("end", e.message))?;
        let step_ns = if let Some(default_step) = app
            .thanos_default_step
            .filter(|_| p.get("step").is_none_or(str::is_empty))
        {
            let seconds =
                ((end as i128 - start as i128) / 1000 / 250).max(default_step.as_secs() as i128);
            i64::try_from(seconds.saturating_mul(1_000_000_000))
                .map_err(|_| ApiError::parameter("step", "duration out of range"))?
        } else {
            parse_duration_ns(p.get("step").unwrap_or(""))
                .map_err(|e| ApiError::parameter("step", e.message))?
        };
        if end < start {
            return Err(ApiError::bad("end timestamp must not be before start time"));
        }
        if step_ns <= 0 {
            return Err(ApiError::parameter(
                "step",
                "zero or negative query resolution step widths are not accepted. Try a positive integer",
            ));
        }
        if (end as i128 - start as i128) * 1_000_000 / step_ns as i128 > 11000 {
            return Err(ApiError::bad(
                "exceeded maximum resolution of 11,000 points per timeseries. Try decreasing the query resolution (?step=XX)",
            ));
        }
        (start, end, (step_ns / 1_000_000).max(1))
    } else {
        let t = p.time("time", now)?;
        (t, t, 0)
    };
    let expr = Engine::parse_at(query, start, end, step)
        .map_err(|e| ApiError::parameter("query", parse_error(query, &e.to_string())))?;
    if range
        && !matches!(
            expr.value_type(),
            promql_parser::parser::value::ValueType::Scalar
                | promql_parser::parser::value::ValueType::Vector
        )
    {
        let kind = if expr.value_type() == promql_parser::parser::value::ValueType::Matrix {
            "range vector"
        } else {
            "string"
        };
        return Err(ApiError::parameter(
            "query",
            format!(
                "invalid expression type {kind:?} for range query, must be Scalar or instant Vector"
            ),
        ));
    }
    let mut lookback = p
        .get("lookback_delta")
        .filter(|v| !v.is_empty())
        .map(|v| {
            parse_duration(v).map_err(|e| {
                ApiError::bad(format!(
                    "error parsing lookback delta duration: {}",
                    e.message
                ))
            })
        })
        .transpose()?
        .unwrap_or(300_000);
    if lookback <= 0 {
        lookback = 300_000;
    }
    let (a, b, selectors) = plan(&expr, start, end, lookback).map_err(ApiError::bad)?;
    let mut loaded = if selectors.is_empty() || !options.store_matches {
        Default::default()
    } else {
        app.store
            .load(a, b, &selectors)
            .await
            .map_err(ApiError::storage)?
    };
    if let Some(shard) = options.shard {
        loaded.series.retain(|s| shard.matches(&s.labels));
    }
    let replica_labels = options.replica_labels;
    let query_source = query.to_owned();
    let stats = p.get("stats").unwrap_or("").to_owned();
    let prepared = Instant::now();
    let analyze_warning = matches!(p.get("analyze"), Some("true" | "1")).then(|| {
        if p.get("engine") == Some("thanos") {
            "Query fallback to prometheus engine; not analyzable."
        } else {
            "Query not analyzable; change engine to 'thanos'."
        }
    });

    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let eval_start=Instant::now();
        let mut engine = Engine::new(&loaded.series, start, end);
        engine.source = query_source;
        engine.replica_labels = replica_labels;
        engine.lookback = lookback;
        engine.deadline = deadline;
        engine.cancelled = Some(cancelled);
        let value = if range {
            engine.range(&expr, step)
        } else {
            engine.evaluate(&expr, start)
        }
        .map_err(|e| {
            if e.to_string().contains("timed out") {
                ApiError::timeout()
            } else {
                ApiError::exec(e)
            }
        })?;
        let mut data = value.json(start);
        let elapsed=eval_start.elapsed().as_secs_f64();
        if !stats.is_empty() {
            data["stats"]=json!({"timings":{"evalTotalTime":elapsed,"execQueueTime":eval_start.duration_since(prepared).as_secs_f64(),"execTotalTime":preparation_time.elapsed().as_secs_f64(),"innerEvalTime":elapsed,"queryPreparationTime":prepared.duration_since(preparation_time).as_secs_f64(),"resultSortTime":0.0},"samples":{"totalQueryableSamples":engine.samples_read,"samplesRead":engine.samples_read,"peakSamples":engine.peak_samples}});
        }
        let mut warnings = engine.warnings.into_iter().collect::<Vec<_>>();
        warnings.extend(loaded.warnings);
        if let Some(warning) = analyze_warning {warnings.push(warning.into());}
        if !stats.is_empty() && stats!="all" && stats!="true" {warnings.push(format!("value {stats:?} for parameter \"stats\" is deprecated and will be rejected in the next major release, use \"true\" or \"all\""));}
        if limit > 0 && matches!(data["resultType"].as_str(), Some("vector" | "matrix")) {
            let values = data["result"].as_array_mut().unwrap();
            if values.len() > limit {
                values.truncate(limit);
                warnings.push("results truncated due to limit".into());
            }
        }
        let mut response = success(data);
        if !warnings.is_empty() {
            response["warnings"] = json!(cap_annotations(warnings, "warning"))
        }
        if !engine.infos.is_empty() {
            response["infos"] = json!(cap_annotations(engine.infos.into_iter().collect(), "info"))
        }
        Ok(response)
    })
    .await
    .map_err(ApiError::exec)?
}
async fn exemplar_api(app: &App, p: &Params) -> Result<JsonValue> {
    let start = p.time("start", 0)?;
    let end = p.time("end", 4_294_967_295_000)?;
    if end < start {
        return Err(ApiError::bad(
            "end timestamp must not be before start timestamp",
        ));
    }
    let query = p.get("query").unwrap_or("");
    let expr =
        Engine::parse(query).map_err(|e| ApiError::bad(parse_error(query, &e.to_string())))?;
    let (_, _, selectors) = plan(&expr, start, end, 300_000).map_err(ApiError::bad)?;
    if selectors.is_empty() {
        return Ok(json!({"status":"success"}));
    }
    let result = app
        .store
        .exemplars(start, end, &selectors)
        .await
        .map_err(ApiError::storage)?;
    Ok(success(json!(result)))
}
async fn discovery_api(app: &App, p: &Params, path: &str) -> Result<JsonValue> {
    let label = if !path.starts_with("label/") {
        None
    } else {
        let raw = &path[6..path.len() - 7];
        Some(unescape_label(
            &percent_encoding::percent_decode_str(raw)
                .decode_utf8()
                .map_err(ApiError::bad)?,
        ))
    };
    if label.as_ref().is_some_and(|name| name.is_empty()) {
        return Err(ApiError::bad("invalid label name: \"\""));
    }
    let start = p.time("start", 0)?;
    let end = p.time("end", 4_294_967_295_000)?;
    if end < start {
        return Err(ApiError::bad("end timestamp must not be before start time"));
    }
    let selectors = p.selectors(path == "series")?;
    let metadata_limit = |name: &str| -> Result<i64> {
        p.get(name).filter(|v| !v.is_empty()).map_or(Ok(-1), |v| {
            v.parse()
                .map_err(|_| ApiError::bad(format!("{name} must be a number")))
        })
    };
    let limit = if path == "metadata" { 0 } else { p.limit()? };
    let metric_limit = if path == "metadata" {
        metadata_limit("limit")?
    } else {
        -1
    };
    let per_metric = if path == "metadata" {
        metadata_limit("limit_per_metric")?
    } else {
        -1
    };
    let options = QueryOptions::parse(app, p)?;
    let mut loaded = if options.store_matches {
        app.store
            .discover(start, end, &selectors)
            .await
            .map_err(ApiError::storage)?
    } else {
        Default::default()
    };
    if !options.replica_labels.is_empty() {
        let mut seen = BTreeSet::new();
        for s in &mut loaded.series {
            for label in &options.replica_labels {
                s.labels.remove(label);
            }
        }
        loaded.series.retain(|s| seen.insert(s.labels.clone()));
    }
    let data = if path == "series" {
        json!(
            loaded
                .series
                .into_iter()
                .map(|s| s.labels)
                .collect::<Vec<_>>()
        )
    } else if path == "metadata" {
        let mut metadata = loaded.metadata;
        if let Some(name) = p.get("metric").filter(|v| !v.is_empty()) {
            metadata.retain(|k, _| k == name)
        }
        if metric_limit >= 0 {
            metadata = metadata.into_iter().take(metric_limit as usize).collect();
        }
        if per_metric > 0 {
            for values in metadata.values_mut() {
                values.truncate(per_metric as usize);
            }
        }
        return Ok(success(json!(metadata)));
    } else {
        let values = loaded
            .series
            .iter()
            .flat_map(|s| match &label {
                None => s.labels.keys().cloned().collect::<Vec<_>>(),
                Some(name) => s.labels.get(name).cloned().into_iter().collect(),
            })
            .collect::<BTreeSet<_>>();
        json!(values)
    };
    let mut response = success(data);
    if limit > 0 {
        let values = response["data"].as_array_mut().unwrap();
        if values.len() > limit {
            values.truncate(limit);
            response["warnings"] = json!(["results truncated due to limit"]);
        }
    }
    Ok(response)
}
fn parse_time(text: &str) -> Result<i64> {
    if let Ok(number) = text.parse::<f64>() {
        // Go's float-to-int conversion returns MinInt64 for NaN and overflow.
        // time.Unix and Prometheus timestamp conversion then wrap in int64.
        let whole = if !number.is_finite() || number >= i64::MAX as f64 || number < i64::MIN as f64
        {
            i64::MIN
        } else {
            number.trunc() as i64
        };
        let fraction = if number.is_finite() {
            (number.fract() * 1000.0).round() as i64
        } else {
            0
        };
        return Ok(whole.wrapping_mul(1000).wrapping_add(fraction));
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .map_err(|_| ApiError::bad(format!("cannot parse {text:?} to a valid timestamp")))
}
fn parse_duration_ns(text: &str) -> Result<i64> {
    if let Ok(number) = text.parse::<f64>() {
        let ns = number * 1e9;
        if ns > i64::MAX as f64 || ns < i64::MIN as f64 {
            return Err(ApiError::bad(format!(
                "cannot parse {text:?} to a valid duration. It overflows int64"
            )));
        }
        return Ok(if ns.is_nan() { i64::MIN } else { ns as i64 });
    }
    promql_parser::util::parse_duration(text)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .ok_or_else(|| ApiError::bad(format!("cannot parse {text:?} to a valid duration")))
}
fn parse_duration(text: &str) -> Result<i64> {
    Ok(parse_duration_ns(text)? / 1_000_000)
}
fn api_ast(mut value: JsonValue) -> JsonValue {
    if let Some(map) = value.as_object_mut() {
        for (key, v) in map.iter_mut() {
            *v = if matches!(key.as_str(), "rangeExpr" | "stepExpr" | "offsetExpr") {
                duration_ast(v.take())
            } else {
                api_ast(v.take())
            };
        }
        map.remove("experimental");
        map.remove("duration");
        let kind = map
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_owned();
        if kind == "subquery" {
            for key in ["rangeExpr", "offsetExpr", "stepExpr"] {
                map.entry(key).or_insert(JsonValue::Null);
            }
            if map.get("step").is_none_or(JsonValue::is_null) {
                map.insert("step".into(), json!(0));
            }
        }
        if kind == "binaryExpr" {
            if scalar_ast(&map["lhs"]) || scalar_ast(&map["rhs"]) {
                map.insert("matching".into(), JsonValue::Null);
            } else if map["matching"].is_null() {
                map.insert("matching".into(),json!({"card":"one-to-one","include":[],"labels":[],"on":false,"fillValues":{"lhs":null,"rhs":null}}));
            }
            if let Some(m) = map.get_mut("matching").and_then(JsonValue::as_object_mut)
                && let Some(card) = m["card"].as_object().cloned()
                && let Some((name, labels)) = card.into_iter().next()
            {
                m.insert("card".into(), json!(name));
                m.insert("include".into(), labels);
            }
        }
        if kind == "vectorSelector" || kind == "matrixSelector" {
            map.entry("anchored").or_insert(json!(false));
            map.entry("smoothed").or_insert(json!(false));
            map.entry("offsetExpr").or_insert(JsonValue::Null);
            if map.get("name").is_none_or(JsonValue::is_null) {
                map.insert("name".into(), json!(""));
            }
            if kind == "matrixSelector" {
                map.entry("rangeExpr").or_insert(JsonValue::Null);
            }
            if let Some(name) = map
                .get("name")
                .and_then(|n| n.as_str())
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                && let Some(matchers) = map.get_mut("matchers").and_then(JsonValue::as_array_mut)
            {
                matchers.push(json!({"name":"__name__","type":"=","value":name}));
            }
        }
    } else if let Some(array) = value.as_array_mut() {
        for v in array {
            *v = api_ast(v.take());
        }
    }
    value
}
fn scalar_ast(v: &JsonValue) -> bool {
    match v["type"].as_str().unwrap_or("") {
        "numberLiteral" => true,
        "call" => v["func"]["returnType"] == "scalar",
        "parenExpr" | "unaryExpr" => scalar_ast(&v["expr"]),
        "binaryExpr" => scalar_ast(&v["lhs"]) && scalar_ast(&v["rhs"]),
        _ => false,
    }
}
fn duration_ast(mut v: JsonValue) -> JsonValue {
    let kind = v["type"].as_str().unwrap_or("").to_owned();
    let (op, lhs, rhs, wrapped) = match kind.as_str() {
        "numberLiteral" => {
            v.as_object_mut()
                .unwrap()
                .entry("duration")
                .or_insert(json!(false));
            return v;
        }
        "binaryExpr" => (
            v["op"].clone(),
            duration_ast(v["lhs"].take()),
            duration_ast(v["rhs"].take()),
            false,
        ),
        "unaryExpr" => (
            v["op"].clone(),
            JsonValue::Null,
            duration_ast(v["expr"].take()),
            false,
        ),
        "call" => {
            let name = v["func"]["name"].clone();
            let a = duration_ast(
                v["args"]
                    .get_mut(0)
                    .map_or(JsonValue::Null, JsonValue::take),
            );
            let b = duration_ast(
                v["args"]
                    .get_mut(1)
                    .map_or(JsonValue::Null, JsonValue::take),
            );
            (name, a, b, false)
        }
        "parenExpr" => {
            let mut inner = duration_ast(v["expr"].take());
            if inner["type"] == "durationExpr" {
                inner["wrapped"] = json!(true);
                return inner;
            }
            (json!("+"), JsonValue::Null, inner, true)
        }
        _ => return v,
    };
    json!({"type":"durationExpr","op":op,"lhs":lhs,"rhs":rhs,"wrapped":wrapped})
}
fn parse_error(query: &str, message: &str) -> String {
    if query.trim().is_empty() {
        return "unknown position: parse error: no expression found in input".into();
    }
    let mut position = 0;
    let mut message = message.to_owned();
    if message.contains("unclosed") || message.contains("end of input") {
        position = query.len();
    }
    if message.starts_with("unknown function with name '") {
        message = message.replace("'", "\"");
    }
    if message.starts_with("expected type") {
        position = query.find('(').map_or(0, |p| p + 1);
        message = message
            .replace("'", "\"")
            .replace("type matrix", "type range vector")
            .replace("type vector", "type instant vector")
            .replace("got matrix", "got range vector")
            .replace("got vector", "got instant vector");
    }
    if message.contains("comparisons between scalars") {
        position = query.find(['=', '!', '<', '>']).unwrap_or(0);
    }
    let line = query[..position].bytes().filter(|b| *b == b'\n').count() + 1;
    let column = query[..position]
        .rfind('\n')
        .map_or(position + 1, |p| position - p);
    format!("{line}:{column}: parse error: {message}")
}

fn unescape_label(name: &str) -> String {
    let Some(encoded) = name.strip_prefix("U__") else {
        return name.into();
    };
    let mut out = String::new();
    let mut chars = encoded.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '_' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'_') {
            chars.next();
            out.push('_');
            continue;
        }
        let mut code = String::new();
        let mut closed = false;
        for c in chars.by_ref() {
            if c == '_' {
                closed = true;
                break;
            }
            code.push(c);
        }
        if !closed || code.is_empty() || code.len() > 5 {
            return name.into();
        }
        let Some(c) = u32::from_str_radix(&code, 16).ok().and_then(char::from_u32) else {
            return name.into();
        };
        out.push(c);
    }
    out
}
