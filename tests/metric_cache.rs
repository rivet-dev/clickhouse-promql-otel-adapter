use axum::{Router, extract::State, http::StatusCode, routing::post};
use clickhouse_promql::storage::Store;
use promql_parser::parser::{self, Expr};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Default)]
struct Backend {
    requests: AtomicUsize,
    fail: AtomicBool,
}
async fn dictionary(State(state): State<Arc<Backend>>) -> (StatusCode, &'static str) {
    state.requests.fetch_add(1, Ordering::SeqCst);
    if state.fail.swap(false, Ordering::SeqCst) {
        return (StatusCode::SERVICE_UNAVAILABLE, "temporary failure");
    }
    (
        StatusCode::OK,
        "{\"MetricName\":\"existing\",\"MetricUnit\":\"\",\"ScopeName\":\"\",\"IsMonotonic\":false}\n",
    )
}

#[tokio::test]
async fn concurrent_panels_share_refresh_and_failed_refresh_is_retried() {
    let backend = Arc::new(Backend::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/", post(dictionary))
        .with_state(backend.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut store = Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap();
    store.metric_cache_ttl = Duration::from_secs(60);
    let Expr::VectorSelector(selector) = parser::parse("missing_metric").unwrap() else {
        panic!()
    };
    let mut tasks = Vec::new();
    for _ in 0..12 {
        let store = store.clone();
        let selector = selector.clone();
        tasks.push(tokio::spawn(async move {
            store.load(0, 1, &[selector]).await.unwrap()
        }));
    }
    for task in tasks {
        assert!(task.await.unwrap().series.is_empty());
    }
    assert_eq!(
        backend.requests.load(Ordering::SeqCst),
        5,
        "one scan per table across all panels"
    );

    // Expire all entries, then show a transient failure doesn't poison the cache.
    store.metric_cache_ttl = Duration::from_millis(1);
    tokio::time::sleep(Duration::from_millis(10)).await;
    backend.fail.store(true, Ordering::SeqCst);
    assert!(store.load(0, 1, &[selector.clone()]).await.is_err());
    store.load(0, 1, &[selector.clone()]).await.unwrap();
    assert_eq!(backend.requests.load(Ordering::SeqCst), 11);

    store.metric_cache_ttl = Duration::ZERO;
    store.load(0, 1, &[selector]).await.unwrap();
    assert_eq!(
        backend.requests.load(Ordering::SeqCst),
        16,
        "zero TTL disables reuse"
    );
    server.abort();
}
