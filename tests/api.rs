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
        "/api/v1/query?query=1&time=garbage",
        "/api/v1/query_range?query=1&start=0&end=1&step=0",
        "/api/v1/series",
        "/api/v1/label/U__/values",
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

#[tokio::test]
async fn thanos_ha_options_and_shards_on_clickhouse() {
    common::empty_database_test(false, |store| async move {
        let mut rows = Vec::new();
        for replica in ["a", "b"] {
            for i in 0..8 {
                rows.push(serde_json::json!({
                    "MetricName":"ha_total", "ScopeName":"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver",
                    "Attributes":{"replica":replica,"job":"ha"},"AggregationTemporality":2,"IsMonotonic":true,
                    "TimeUnix":1700000000+i*15,"StartTimeUnix":1700000000,"Value":i*10
                }).to_string());
            }
        }
        store.query(&format!("INSERT INTO {}.otel_metrics_sum FORMAT JSONEachRow\n{}", store.database, rows.join("\n")), &[]).await.unwrap();
        let mut app = App::new(store.clone());
        app.replica_labels = vec!["replica".into()];
        async fn call(app: App, path: &str, params: &str) -> (u16, Value) {
            let response = router(app).oneshot(Request::builder().method("POST").uri(path).header("content-type","application/x-www-form-urlencoded").body(Body::from(params.to_owned())).unwrap()).await.unwrap();
            let status=response.status().as_u16();
            (status,serde_json::from_slice(&to_bytes(response.into_body(),1_000_000).await.unwrap()).unwrap())
        }
        let base = "query=rate(ha_total%5B2m%5D)&time=1700000105";
        let (status, dedup) = call(app.clone(),"/api/v1/query",base).await;
        assert_eq!(status,200,"{dedup}");
        let result=dedup["data"]["result"].as_array().unwrap();
        assert_eq!(result.len(),1);
        assert!(result[0]["metric"].get("replica").is_none());
        assert!((result[0]["value"][1].as_str().unwrap().parse::<f64>().unwrap()-70.0/120.0).abs()<1e-12);
        let (_, raw) = call(app.clone(),"/api/v1/query",&format!("{base}&dedup=false")).await;
        assert_eq!(raw["data"]["result"].as_array().unwrap().len(),2);
        let (_, overridden) = call(app.clone(),"/api/v1/query",&format!("{base}&replicaLabels[]=different")).await;
        assert_eq!(overridden["data"]["result"].as_array().unwrap().len(),2);
        let (_, series) = call(app.clone(),"/api/v1/series","match[]=ha_total").await;
        assert_eq!(series["data"].as_array().unwrap().len(),1);
        let (_, excluded) = call(app.clone(),"/api/v1/query",&format!("{base}&storeMatch[]=%7Bregion%3D%22missing%22%7D")).await;
        assert_eq!(excluded["data"]["result"],serde_json::json!([]));
        let mut total=0;
        for i in 0..3 {
            let params=url::form_urlencoded::Serializer::new(String::new()).append_pair("query","ha_total").append_pair("time","1700000105").append_pair("dedup","false").append_pair("shard_info",&serde_json::json!({"shard_index":i,"total_shards":3}).to_string()).finish();
            let (status,data)=call(app.clone(),"/api/v1/query",&params).await;
            assert_eq!(status,200,"{data}");
            total+=data["data"]["result"].as_array().unwrap().len();
        }
        assert_eq!(total,2);
        for param in ["dedup=wrong","partial_response=wrong","engine=wrong","max_source_resolution=-1","shard_info=broken"] {
            let (status,data)=call(app.clone(),"/api/v1/query",&format!("{base}&{param}")).await;
            assert_eq!(status,400,"{param}: {data}");
        }
        let mut bounded=store;
        bounded.max_bytes=8;
        assert!(bounded.query("SELECT repeat('x', 100)",&[]).await.unwrap_err().to_string().contains("byte limit"));
    }).await;
}

#[tokio::test]
async fn metadata_limits_and_conflicts() {
    common::database_test(|store| async move {
        for limit in ["-1", "0", "1"] {
            let (status,data)=request(store.clone(),"GET",&format!("/api/v1/metadata?limit={limit}"),"").await;
            assert_eq!(status,200,"{data}");
            let count=data["data"].as_object().unwrap().len();
            if limit=="0" {assert_eq!(count,0);} else if limit=="1" {assert_eq!(count,1);} else {assert!(count>1);}
        }
        for name in ["limit","limit_per_metric"] {
            let (status,data)=request(store.clone(),"GET",&format!("/api/v1/metadata?{name}=bad"),"").await;
            assert_eq!(status,400); assert_eq!(data["error"],format!("{name} must be a number"));
        }
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge (MetricName,MetricDescription,TimeUnix,Value) VALUES ('conflict','first',1700000000,1),('conflict','second',1700000015,2)",store.database),&[]).await.unwrap();
        let (_,data)=request(store.clone(),"GET","/api/v1/metadata?metric=conflict","").await;
        assert_eq!(data["data"]["conflict"].as_array().unwrap().len(),2);
        let (_,data)=request(store,"GET","/api/v1/metadata?metric=conflict&limit_per_metric=1","").await;
        assert_eq!(data["data"]["conflict"].as_array().unwrap().len(),1);
    }).await;
}

#[tokio::test]
async fn utf8_label_paths_and_malformed_route() {
    common::empty_database_test(false,|store|async move {
        store.query(&format!("INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{}",store.database,serde_json::json!({"MetricName":"m","TimeUnix":1700000000,"Value":1,"ScopeName":"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver","Attributes":{"foo.bar":"yes"}})),&[]).await.unwrap();
        let (status,data)=request(store.clone(),"GET","/api/v1/label/U__foo_2e_bar/values","").await;assert_eq!(status,200);assert_eq!(data["data"],serde_json::json!(["yes"]));
        let (status,_)=request(store,"GET","/api/v1/label/values","").await;assert_eq!(status,404);
    }).await;
}

#[tokio::test]
async fn request_deadline_bounds_body_and_waiting_for_capacity() {
    use std::time::Duration;
    let store = Store::new(
        "http://127.0.0.1:1".into(),
        "test".into(),
        "default".into(),
        "".into(),
    )
    .unwrap();
    let mut app = App::new(store);
    app.request_timeout = Duration::from_millis(30);
    // A held capacity slot must not make a waiting request immortal.
    app.concurrency = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let held = app.concurrency.clone().acquire_owned().await.unwrap();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/query?query=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    drop(held);
    // Incoming bytes are bounded by the same request deadline.
    let stream = futures_util::stream::pending::<Result<axum::body::Bytes, std::io::Error>>();
    let response = router(app.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from_stream(stream))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_eq!(app.concurrency.available_permits(), 1);
    for preflight in [false, true] {
        let mut request = Request::builder().method("OPTIONS").uri("/api/v1/query");
        if preflight {
            request = request
                .header("origin", "https://promql-test.gameinc.io")
                .header("access-control-request-method", "POST");
        }
        let response = router(app.clone())
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 204);
    }
}

#[tokio::test]
async fn backend_failure_and_thanos_step_defaults() {
    let store = Store::new(
        "http://127.0.0.1:1".into(),
        "missing".into(),
        "default".into(),
        "".into(),
    )
    .unwrap();
    let (status, data) = request(store.clone(), "GET", "/api/v1/query?query=some_metric", "").await;
    assert_eq!(status, 500);
    assert_eq!(data["errorType"], "internal");
    let mut app = App::new(store);
    app.thanos_default_step = Some(std::time::Duration::from_secs(60));
    let response = router(app)
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=1&start=0&end=120")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let data: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 10000).await.unwrap()).unwrap();
    assert_eq!(
        data["data"]["result"][0]["values"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
}
