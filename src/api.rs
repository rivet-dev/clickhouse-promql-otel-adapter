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
use tokio::sync::Semaphore;
use tower_http::{compression::CompressionLayer, cors::CorsLayer};

#[derive(Clone)]
pub struct App {
    pub store: Store,
    pub concurrency: Arc<Semaphore>,
}
impl App {
    pub fn new(store: Store) -> Self {
        Self {
            store,
            concurrency: Arc::new(Semaphore::new(16)),
        }
    }
}
pub fn router(app: App) -> Router {
    Router::new().route("/-/healthy",get(||async{"ClickHouse PromQL adapter is healthy\n"}))
        .route("/-/ready",get(ready)).route("/metrics",get(||async{"# HELP clickhouse_promql_build_info Build information.\n# TYPE clickhouse_promql_build_info gauge\nclickhouse_promql_build_info{version=\"0.1.0\"} 1\n"}))
        .route("/api/v1/{*path}",any(handle)).with_state(app).layer(CompressionLayer::new()).layer(CorsLayer::permissive())
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
    fn exec(message: impl ToString) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            kind: "execution",
            message: message.to_string(),
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
            message: "query timed out".into(),
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
type Result<T> = std::result::Result<T, ApiError>;
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
    fn required(&self, name: &str) -> Result<&str> {
        self.get(name)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| ApiError::bad(format!("missing required parameter '{name}'")))
    }
    fn limit(&self) -> Result<usize> {
        self.get("limit").map_or(Ok(0), |v| {
            v.parse::<usize>()
                .map_err(|_| ApiError::bad("limit must be a non-negative integer"))
        })
    }
    fn time(&self, name: &str, default: i64) -> Result<i64> {
        self.get(name).map_or(Ok(default), parse_time)
    }
    fn selectors(&self, required: bool) -> Result<Vec<promql_parser::parser::VectorSelector>> {
        let values = self.all("match[]");
        if required && values.is_empty() {
            return Err(ApiError::bad("no match[] parameter provided"));
        }
        values
            .into_iter()
            .map(|v| match Engine::parse(v).map_err(ApiError::bad)? {
                promql_parser::parser::Expr::VectorSelector(v) => Ok(v),
                _ => Err(ApiError::bad("match[] must be a vector selector")),
            })
            .collect()
    }
}
async fn handle(State(app): State<App>, request: Request) -> Response {
    match dispatch(app, request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    }
}
async fn dispatch(app: App, request: Request) -> Result<JsonValue> {
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
    if method == Method::POST {
        if !request
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("application/x-www-form-urlencoded")
        {
            return Err(ApiError::bad(
                "POST requires application/x-www-form-urlencoded",
            ));
        }
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
        let expr = Engine::parse(params.required("query")?).map_err(ApiError::bad)?;
        return Ok(success(if path == "format_query" {
            json!(expr.to_string())
        } else {
            serde_json::to_value(expr).map_err(ApiError::exec)?
        }));
    }
    let timeout = params
        .get("timeout")
        .map(parse_duration)
        .transpose()?
        .unwrap_or(120_000);
    if timeout <= 0 {
        return Err(ApiError::bad("timeout must be positive"));
    }
    let duration = Duration::from_millis(timeout.min(120_000) as u64);
    let deadline = Instant::now() + duration;
    tokio::time::timeout(duration, async {
        let _permit = app
            .concurrency
            .acquire()
            .await
            .map_err(ApiError::unavailable)?;
        match path.as_str() {
            "query" | "query_range" => {
                query_api(&app, &params, path == "query_range", deadline).await
            }
            "labels" | "series" | "metadata" => discovery_api(&app, &params, &path).await,
            _ if path.starts_with("label/") && path.ends_with("/values") => {
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
fn success(data: JsonValue) -> JsonValue {
    json!({"status":"success","data":data})
}
async fn query_api(app: &App, p: &Params, range: bool, deadline: Instant) -> Result<JsonValue> {
    let query = p.required("query")?;

    let now = chrono::Utc::now().timestamp_millis();
    let (start, end, step) = if range {
        let start = parse_time(p.required("start")?)?;
        let end = parse_time(p.required("end")?)?;
        let step = parse_duration(p.required("step")?)?;
        if end < start {
            return Err(ApiError::bad("end timestamp must not be before start time"));
        }
        if step <= 0 {
            return Err(ApiError::bad(
                "zero or negative query resolution step widths are not accepted",
            ));
        }
        if (end as i128 - start as i128) / step as i128 > 11000 {
            return Err(ApiError::bad(
                "exceeded maximum resolution of 11,000 points per timeseries",
            ));
        }
        (start, end, step)
    } else {
        let t = p.time("time", now)?;
        (t, t, 0)
    };
    let expr = Engine::parse_at(query, start, end, step).map_err(ApiError::bad)?;
    let lookback = p
        .get("lookback_delta")
        .map(parse_duration)
        .transpose()?
        .unwrap_or(300_000);
    if lookback <= 0 {
        return Err(ApiError::bad("lookback_delta must be positive"));
    }
    let limit = p.limit()?;
    let (a, b, selectors) = plan(&expr, start, end, lookback).map_err(ApiError::bad)?;
    let loaded = if selectors.is_empty() {
        Default::default()
    } else {
        app.store
            .load(a, b, &selectors)
            .await
            .map_err(ApiError::exec)?
    };
    let query_source = query.to_owned();

    tokio::task::spawn_blocking(move || {
        let mut engine = Engine::new(&loaded.series, start, end);
        engine.source = query_source;
        engine.lookback = lookback;
        engine.deadline = deadline;
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
        let mut warnings = engine.warnings.into_iter().collect::<Vec<_>>();
        warnings.extend(loaded.warnings);
        if limit > 0 && matches!(data["resultType"].as_str(), Some("vector" | "matrix")) {
            let values = data["result"].as_array_mut().unwrap();
            if values.len() > limit {
                values.truncate(limit);
                warnings.push("results truncated due to limit".into());
            }
        }
        let mut response = success(data);
        if !warnings.is_empty() {
            response["warnings"] = json!(warnings)
        }
        if !engine.infos.is_empty() {
            response["infos"] = json!(engine.infos)
        }
        Ok(response)
    })
    .await
    .map_err(ApiError::exec)?
}
async fn discovery_api(app: &App, p: &Params, path: &str) -> Result<JsonValue> {
    let start = p.time("start", 0)?;
    let end = p.time("end", 4_294_967_295_000)?;
    if end < start {
        return Err(ApiError::bad("end timestamp must not be before start time"));
    }
    let selectors = p.selectors(path == "series")?;
    let limit = p.limit()?;
    let loaded = app
        .store
        .discover(start, end, &selectors)
        .await
        .map_err(ApiError::exec)?;
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
        if let Some(name) = p.get("metric") {
            metadata.retain(|k, _| k == name)
        }
        if limit > 0 {
            metadata = metadata.into_iter().take(limit).collect();
        }
        return Ok(success(json!(metadata)));
    } else {
        let label = if path == "labels" {
            None
        } else {
            let raw = &path[6..path.len() - 7];
            Some(
                percent_encoding::percent_decode_str(raw)
                    .decode_utf8()
                    .map_err(ApiError::bad)?
                    .into_owned(),
            )
        };
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
        if number.is_finite()
            && number * 1000.0 > i64::MIN as f64
            && number * 1000.0 < i64::MAX as f64
        {
            return Ok((number * 1000.0).round() as i64);
        }
        return Err(ApiError::bad("invalid timestamp"));
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .map_err(|_| ApiError::bad(format!("cannot parse '{text}' to a valid timestamp")))
}
fn parse_duration(text: &str) -> Result<i64> {
    if let Ok(number) = text.parse::<f64>() {
        if number.is_finite() && number.abs() * 1000.0 < i64::MAX as f64 {
            return Ok((number * 1000.0) as i64);
        }
        return Err(ApiError::bad("invalid duration"));
    }
    promql_parser::util::parse_duration(text)
        .map_err(ApiError::bad)?
        .as_millis()
        .try_into()
        .map_err(ApiError::bad)
}
