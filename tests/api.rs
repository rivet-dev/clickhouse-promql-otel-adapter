mod common;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
use serde_json::Value;
use tower::ServiceExt;
async fn request(store: Store, method: &str, path: &str, body: &str) -> (u16, Value) {
    let response = router(App::new(store))
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status().as_u16();
    let data = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (status, serde_json::from_slice(&data).unwrap())
}
#[tokio::test]
async fn get_and_post_query_agree() {
    common::database_test(|store| async move {
        let (a, x) = request(
            store.clone(),
            "GET",
            "/api/v1/query?query=temperature&time=1700000075",
            "",
        )
        .await;
        let (b, y) = request(
            store,
            "POST",
            "/api/v1/query",
            "query=temperature&time=1700000075",
        )
        .await;
        assert_eq!((a, b), (200, 200));
        assert_eq!(x, y);
        insta::assert_json_snapshot!("instant_temperature", x);
    })
    .await
}
#[tokio::test]
async fn range_and_discovery() {
    common::database_test(|store| async move {
        let (status, data) = request(
            store.clone(),
            "POST",
            "/api/v1/query_range",
            "query=temperature&start=1700000000&end=1700000075&step=15",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(
            data["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len(),
            6
        );
        let (status, data) =
            request(store.clone(), "GET", "/api/v1/label/__name__/values", "").await;
        assert_eq!(status, 200);
        assert!(
            data["data"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("requests_total"))
        );
        let (status, data) = request(
            store.clone(),
            "POST",
            "/api/v1/series",
            "match%5B%5D=temperature&match%5B%5D=requests_total",
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(data["data"].as_array().unwrap().len(), 2);
        let (status, data) =
            request(store, "GET", "/api/v1/metadata?metric=requests_total", "").await;
        assert_eq!(status, 200);
        assert_eq!(data["data"]["requests_total"][0]["type"], "counter");
    })
    .await
}
#[tokio::test]
async fn bad_parameters_do_not_query_storage() {
    let store = Store::new(
        "http://127.0.0.1:1".into(),
        "missing".into(),
        "default".into(),
        "".into(),
    )
    .unwrap();
    for path in [
        "/api/v1/query",
        "/api/v1/query?query=)",
        "/api/v1/query?query=1&time=NaN",
        "/api/v1/query_range?query=1&start=0&end=1&step=0",
        "/api/v1/series",
    ] {
        let (status, data) = request(store.clone(), "GET", path, "").await;
        assert_eq!(status, 400, "{path}: {data}");
        assert_eq!(data["errorType"], "bad_data");
    }
    let (status, data) = request(store, "GET", "/api/v1/query?query=1%2B2", "").await;
    assert_eq!(status, 200);
    assert_eq!(data["data"]["result"][1], "3");
}
#[tokio::test]
async fn form_parameters_take_precedence() {
    let store = Store::new(
        "http://127.0.0.1:1".into(),
        "missing".into(),
        "default".into(),
        "".into(),
    )
    .unwrap();
    let (status, data) = request(store, "POST", "/api/v1/query?query=1", "query=2&time=0").await;
    assert_eq!(status, 200);
    assert_eq!(data["data"]["result"][1], "2");
}
