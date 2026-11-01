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
/// The adapter's one-time `use_statistics` probe; mocks answer it without counting.
const PROBE: &str = "SELECT 1";

async fn dictionary(State(state): State<Arc<Backend>>, body: String) -> (StatusCode, &'static str) {
    if body == PROBE {
        return (StatusCode::OK, "1\n");
    }
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

    // Expired entries are served while one background scan per table
    // refreshes them, and a failed refresh is retried by the next query.
    store.metric_cache_ttl = Duration::from_millis(1);
    tokio::time::sleep(Duration::from_millis(10)).await;
    backend.fail.store(true, Ordering::SeqCst);
    store
        .load(0, 1, std::slice::from_ref(&selector))
        .await
        .unwrap();
    let requests_after = |count: usize| {
        let backend = backend.clone();
        async move {
            // The refresh runs on a spawned task; wait for it to reach the backend.
            for _ in 0..200 {
                if backend.requests.load(Ordering::SeqCst) >= count {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!("background refresh did not run");
        }
    };
    requests_after(10).await;
    assert!(!backend.fail.load(Ordering::SeqCst), "one refresh failed");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let before_retry = backend.requests.load(Ordering::SeqCst);
    store
        .load(0, 1, std::slice::from_ref(&selector))
        .await
        .unwrap();
    requests_after(before_retry + 5).await;

    store.metric_cache_ttl = Duration::ZERO;
    let before = backend.requests.load(Ordering::SeqCst);
    store.load(0, 1, &[selector]).await.unwrap();
    assert_eq!(
        backend.requests.load(Ordering::SeqCst) - before,
        5,
        "zero TTL disables reuse"
    );
    server.abort();
}

#[derive(Default)]
struct Recorder {
    ranges: std::sync::Mutex<Vec<(String, Option<String>)>>,
}
async fn recorded(
    State(state): State<Arc<Recorder>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: String,
) -> &'static str {
    if body == PROBE {
        return "1\n";
    }
    state.ranges.lock().unwrap().push((
        params.get("param_from").cloned().unwrap_or_default(),
        params.get("param_until").cloned(),
    ));
    ""
}

#[tokio::test]
async fn name_scans_cover_only_the_missing_time_range() {
    let backend = Arc::new(Recorder::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/", post(recorded))
        .with_state(backend.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut store = Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap();
    store.metric_cache_ttl = Duration::from_secs(60);
    let Expr::VectorSelector(selector) = parser::parse("missing_metric").unwrap() else {
        panic!()
    };
    let scans = |backend: &Recorder| backend.ranges.lock().unwrap().drain(..).collect::<Vec<_>>();

    store
        .load(1_000, 2_000, std::slice::from_ref(&selector))
        .await
        .unwrap();
    assert_eq!(
        scans(&backend),
        vec![("1000".into(), None); 5],
        "one bounded scan per table"
    );

    store
        .load(1_500, 2_000, std::slice::from_ref(&selector))
        .await
        .unwrap();
    assert!(scans(&backend).is_empty(), "a covered range reuses names");

    store
        .load(400, 2_000, std::slice::from_ref(&selector))
        .await
        .unwrap();
    assert_eq!(
        scans(&backend),
        vec![("400".into(), Some("1000".into())); 5],
        "an earlier start scans only the uncovered range"
    );
    server.abort();
}

#[derive(Default)]
struct OldServer {
    with_setting: AtomicUsize,
    queries: AtomicUsize,
}
async fn old_server(
    State(state): State<Arc<OldServer>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    body: String,
) -> (StatusCode, &'static str) {
    if params.contains_key("use_statistics") {
        state.with_setting.fetch_add(1, Ordering::SeqCst);
        return (
            StatusCode::NOT_FOUND,
            "Code: 115. DB::Exception: Unknown setting 'use_statistics'. (UNKNOWN_SETTING)",
        );
    }
    if body != PROBE {
        state.queries.fetch_add(1, Ordering::SeqCst);
    }
    (StatusCode::OK, "")
}

#[tokio::test]
async fn servers_without_use_statistics_get_queries_without_it() {
    let backend = Arc::new(OldServer::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/", post(old_server))
        .with_state(backend.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let store = Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap();
    let Expr::VectorSelector(selector) = parser::parse("missing_metric").unwrap() else {
        panic!()
    };
    store
        .load(0, 1, std::slice::from_ref(&selector))
        .await
        .unwrap();
    store.load(0, 1, &[selector]).await.unwrap();
    assert_eq!(
        backend.with_setting.load(Ordering::SeqCst),
        1,
        "probed once"
    );
    assert_eq!(
        backend.queries.load(Ordering::SeqCst),
        10,
        "every scan ran without it"
    );
    server.abort();
}
