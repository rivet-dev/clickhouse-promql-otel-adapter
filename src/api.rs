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
    if request.method() != Method::GET && request.method() != Method::POST {
        return (StatusCode::METHOD_NOT_ALLOWED, "Method Not Allowed\n").into_response();
    }
    match dispatch(app, request).await {
        Ok(value) if value.get("data").is_none() => StatusCode::NO_CONTENT.into_response(),
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
            json!(expr.to_string())
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
        let _permit = app
            .concurrency
            .acquire()
            .await
            .map_err(ApiError::unavailable)?;
        match path.as_str() {
            "query" | "query_range" => {
                query_api(&app, &params, path == "query_range", deadline).await
            }
            "query_exemplars" => exemplar_api(&app, &params).await,
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
    let query = p.get("query").unwrap_or("");
    let limit = p.limit()?;

    let now = chrono::Utc::now().timestamp_millis();
    let (start, end, step) = if range {
        let start = parse_time(p.get("start").unwrap_or(""))
            .map_err(|e| ApiError::parameter("start", e.message))?;
        let end = parse_time(p.get("end").unwrap_or(""))
            .map_err(|e| ApiError::parameter("end", e.message))?;
        let step_ns = parse_duration_ns(p.get("step").unwrap_or(""))
            .map_err(|e| ApiError::parameter("step", e.message))?;
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
    let loaded = if selectors.is_empty() {
        Default::default()
    } else {
        app.store
            .load(a, b, &selectors)
            .await
            .map_err(ApiError::exec)?
    };
    let query_source = query.to_owned();
    let stats = p.get("stats").unwrap_or("").to_owned();
    let preparation_time = Instant::now();

    tokio::task::spawn_blocking(move || {
        let eval_start=Instant::now();
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
        let elapsed=eval_start.elapsed().as_secs_f64();
        if !stats.is_empty() {
            data["stats"]=json!({"timings":{"evalTotalTime":elapsed,"execQueueTime":0.0,"execTotalTime":preparation_time.elapsed().as_secs_f64(),"innerEvalTime":elapsed,"queryPreparationTime":0.0,"resultSortTime":0.0},"samples":{"totalQueryableSamples":engine.samples_read,"samplesRead":engine.samples_read,"peakSamples":engine.peak_samples}});
        }
        let mut warnings = engine.warnings.into_iter().collect::<Vec<_>>();
        warnings.extend(loaded.warnings);
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
        .map_err(ApiError::exec)?;
    Ok(success(json!(result)))
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
