use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::Request,
    routing::post,
};
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

async fn backend(State(rows): State<Arc<String>>, sql: String) -> String {
    if sql.contains("otel_metrics_gauge") {
        rows.as_ref().clone()
    } else {
        String::new()
    }
}
async fn fixture() -> (App, tokio::task::JoinHandle<()>) {
    let rows = (0..2000).map(|i| json!({"MetricName":format!("metric_{i:04}"),"Attributes":{"label":format!("value_{i:04}")}}).to_string()).collect::<Vec<_>>().join("\n");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/", post(backend))
                .with_state(Arc::new(rows)),
        )
        .await
        .unwrap();
    });
    (
        App::new(Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap()),
        server,
    )
}
async fn search(app: App, query: &str) -> Vec<Value> {
    let request = Request::builder()
        .uri(format!("/api/v1/search/metric_names?{query}"))
        .body(Body::empty())
        .unwrap();
    let response = router(app).oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
    let bytes = to_bytes(response.into_body(), 100_000).await.unwrap();
    std::str::from_utf8(&bytes)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
#[tokio::test]
async fn bounded_search_keeps_best_ties_and_descending_alpha_prefix() {
    let (app, server) = fixture().await;
    let scored = search(
        app.clone(),
        "search[]=metric&sort_by=score&limit=3&include_score=true",
    )
    .await;
    assert_eq!(
        scored[0]["results"],
        json!([
            {"name":"metric_0000","score":1.0}, {"name":"metric_0001","score":1.0}, {"name":"metric_0002","score":1.0}
        ])
    );
    assert_eq!(scored.last().unwrap()["has_more"], true);
    let best = search(app.clone(), "search[]=metric_1&sort_by=score&limit=3").await;
    assert_eq!(
        best[0]["results"],
        json!([{"name":"metric_1000"},{"name":"metric_1001"},{"name":"metric_1002"}])
    );
    let descending = search(app.clone(), "sort_by=alpha&sort_dir=dsc&limit=2").await;
    assert_eq!(
        descending[0]["results"],
        json!([{"name":"metric_1999"},{"name":"metric_1998"}])
    );
    assert_eq!(descending.last().unwrap()["has_more"], true);
    let exact = search(app, "search[]=metric_1999&limit=2").await;
    assert_eq!(exact[0]["results"], json!([{"name":"metric_1999"}]));
    assert_eq!(exact.last().unwrap()["has_more"], false);
    server.abort();
}
#[tokio::test]
async fn discovery_limits_preserve_sorted_values_and_warning() {
    let (app, server) = fixture().await;
    let request = Request::builder()
        .uri("/api/v1/label/label/values?limit=2")
        .body(Body::empty())
        .unwrap();
    let response = router(app).oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await.unwrap()).unwrap();
    assert_eq!(body["data"], json!(["value_0000", "value_0001"]));
    assert_eq!(body["warnings"], json!(["results truncated due to limit"]));
    server.abort();
}
