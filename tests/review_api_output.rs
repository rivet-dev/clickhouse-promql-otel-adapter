mod common;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
use serde_json::{Value, json};
use tower::ServiceExt;
async fn call(app: App, query: &str, limit: Option<usize>) -> (u16, Value) {
    let body = {
        let mut params = url::form_urlencoded::Serializer::new(String::new());
        params
            .append_pair("query", query)
            .append_pair("time", "100");
        if let Some(limit) = limit {
            params.append_pair("limit", &limit.to_string());
        }
        params.finish()
    };
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap();
    let response = router(app).oneshot(request).await.unwrap();
    let status = response.status().as_u16();
    (
        status,
        serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await.unwrap()).unwrap(),
    )
}
#[tokio::test]
async fn tiny_cap_returns_complete_execution_error_and_releases_admission() {
    let mut app = App::new(
        Store::new(
            "http://127.0.0.1:1".into(),
            "otel".into(),
            "default".into(),
            "".into(),
        )
        .unwrap(),
    );
    app.max_response_bytes = 1;
    let (status, body) = call(app.clone(), "vector(1)", None).await;
    assert_eq!(status, 422);
    assert_eq!(body["status"], "error");
    assert_eq!(body["errorType"], "execution");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("response byte limit")
    );
    assert!(body.get("data").is_none());
    assert_eq!(app.concurrency.available_permits(), 16);
    app.max_response_bytes = 1024;
    assert_eq!(call(app, "vector(1)", None).await.0, 200);
}
#[tokio::test]
async fn label_amplification_is_bounded_but_explicit_series_limit_still_works() {
    common::empty_database_test(false,|store|async move {
        let rows=(0..50).map(|i|json!({"MetricName":"wide","TimeUnix":100,"Value":1,"Attributes":{"instance":i.to_string(),"payload":"x".repeat(512)}}).to_string()).collect::<Vec<_>>().join("\n");
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{rows}",store.database),&[]).await.unwrap();
        let mut app=App::new(store);app.max_response_bytes=8192;
        let query="label_join(wide,\"amplified\",\"\",\"payload\",\"payload\",\"payload\")";
        let (status,body)=call(app.clone(),query,None).await;
        assert_eq!(status,422,"{body}");assert!(body["error"].as_str().unwrap().contains("response byte limit"));
        let (status,body)=call(app,query,Some(1)).await;
        assert_eq!(status,200,"{body}");assert_eq!(body["data"]["result"].as_array().unwrap().len(),1);
        assert_eq!(body["warnings"],json!(["results truncated due to limit"]));
    }).await
}
