mod common;
use axum::{Router, body::Bytes, extract::State, http::StatusCode, routing::post};
use clickhouse_promql::storage::Store;
use promql_parser::parser::{self, Expr, VectorSelector};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

const T: i64 = 1_700_000_000_000;
const PROM: &str =
    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/prometheusreceiver";

fn selector(s: &str) -> VectorSelector {
    match parser::parse(s).unwrap() {
        Expr::VectorSelector(s) => s,
        _ => panic!("selector"),
    }
}

/// Builds the RowBinaryWithNamesAndTypes payload of one gauge group.
fn rowbinary_group(value: f64) -> Vec<u8> {
    fn string(out: &mut Vec<u8>, text: &str) {
        out.push(u8::try_from(text.len()).unwrap());
        out.extend_from_slice(text.as_bytes());
    }
    let columns = [
        ("MetricName", "String"),
        ("ScopeName", "String"),
        ("Attributes", "Map(String, String)"),
        ("_samples", "Array(Tuple(Int64, Int64, UInt32, Float64))"),
    ];
    let mut out = vec![columns.len() as u8];
    for (name, _) in columns {
        string(&mut out, name);
    }
    for (_, ty) in columns {
        string(&mut out, ty);
    }
    string(&mut out, "requests");
    string(&mut out, PROM);
    out.push(1);
    string(&mut out, "tenant");
    string(&mut out, "a");
    out.push(1);
    out.extend_from_slice(&T.to_le_bytes());
    out.extend_from_slice(&0i64.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&value.to_le_bytes());
    out
}

#[derive(Default)]
struct Backend {
    sample_requests: AtomicUsize,
    discovery_requests: AtomicUsize,
    fail: AtomicBool,
}

async fn serve(State(state): State<Arc<Backend>>, sql: Bytes) -> (StatusCode, Vec<u8>) {
    let sql = String::from_utf8(sql.to_vec()).unwrap();
    if sql.starts_with("SELECT DISTINCT MetricName, MetricUnit") {
        // Only the gauge table stores the metric.
        let body = if sql.contains("otel_metrics_gauge") {
            format!(
                "{}\n",
                json!({"MetricName": "requests", "MetricUnit": "", "ScopeName": PROM})
            )
        } else {
            String::new()
        };
        return (StatusCode::OK, body.into_bytes());
    }
    if sql.contains("groupArray") {
        state.sample_requests.fetch_add(1, Ordering::SeqCst);
        // Let concurrent panels pile up behind one fetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
        if state.fail.swap(false, Ordering::SeqCst) {
            return (StatusCode::INTERNAL_SERVER_ERROR, b"boom".to_vec());
        }
        return (
            StatusCode::OK,
            rowbinary_group(state.sample_requests.load(Ordering::SeqCst) as f64),
        );
    }
    state.discovery_requests.fetch_add(1, Ordering::SeqCst);
    (StatusCode::OK, Vec::new())
}

async fn backend() -> (Arc<Backend>, Store, tokio::task::JoinHandle<()>) {
    let backend = Arc::new(Backend::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/", post(serve))
        .with_state(backend.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut store = Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap();
    store.metric_cache_ttl = Duration::from_secs(60);
    store.series_cache_ttl = Duration::from_secs(60);
    (backend, store, server)
}

#[tokio::test]
async fn identical_loads_in_flight_share_one_fetch_and_failures_are_retried() {
    let (backend, store, server) = backend().await;
    let load = |store: Store, query: &'static str| async move {
        store.load(T, T, &[selector(query)]).await
    };
    let mut tasks = Vec::new();
    for _ in 0..8 {
        tasks.push(tokio::spawn(load(store.clone(), "requests")));
    }
    for task in tasks {
        let loaded = task.await.unwrap().unwrap();
        assert_eq!(loaded.series.len(), 1);
        assert_eq!(loaded.series[0].points[0].value, 1.0);
    }
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 1);

    // A cached entry serves identical loads without a fetch; a different
    // selector, even over the same rows, is its own entry.
    load(store.clone(), "requests").await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 1);
    let other = load(store.clone(), "requests{tenant=~\"a.*\"}")
        .await
        .unwrap();
    assert_eq!(other.series[0].points[0].value, 2.0);
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 2);

    // Every waiter sees the leader's failure, and nothing is cached from it.
    backend.fail.store(true, Ordering::SeqCst);
    let mut tasks = Vec::new();
    for _ in 0..4 {
        tasks.push(tokio::spawn(load(store.clone(), "requests{tenant=\"a\"}")));
    }
    for task in tasks {
        let error = task.await.unwrap().err().unwrap().to_string();
        assert!(error.contains("500"), "{error}");
    }
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 3);
    load(store.clone(), "requests{tenant=\"a\"}").await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 4);

    // Discovery never reads or fills the cache.
    for _ in 0..2 {
        store.discover(T, T, &[selector("requests")]).await.unwrap();
    }
    assert_eq!(backend.discovery_requests.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn entries_expire_after_the_ttl_and_respect_the_byte_budget() {
    let (backend, mut store, server) = backend().await;
    let selectors = [selector("requests")];
    store.series_cache_ttl = Duration::from_millis(300);
    store.load(T, T, &selectors).await.unwrap();
    store.load(T, T, &selectors).await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(350)).await;
    store.load(T, T, &selectors).await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 2);

    // A different time range is a different statement.
    store.load(T, T + 1, &selectors).await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 3);

    store.series_cache_bytes = 1;
    store.load(T, T + 2, &selectors).await.unwrap();
    store.load(T, T + 2, &selectors).await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 5);

    store.series_cache_ttl = Duration::ZERO;
    store.series_cache_bytes = 1 << 20;
    store.load(T, T + 3, &selectors).await.unwrap();
    store.load(T, T + 3, &selectors).await.unwrap();
    assert_eq!(backend.sample_requests.load(Ordering::SeqCst), 7);
    server.abort();
}

#[tokio::test]
async fn cached_rows_serve_later_panels_until_the_ttl() {
    common::empty_database_test(false, |mut store| async move {
        let insert = |store: Store, second: i64| async move {
            let row = json!({"MetricName": "requests", "ScopeName": PROM, "Attributes": {"tenant": "a"}, "TimeUnix": T / 1000 + second, "Value": second});
            store
                .query(
                    &format!(
                        "INSERT INTO {}.otel_metrics_gauge FORMAT JSONEachRow\n{row}",
                        store.database
                    ),
                    &[],
                )
                .await
                .unwrap();
        };
        insert(store.clone(), 0).await;
        store.series_cache_ttl = Duration::from_secs(60);
        let points = |store: Store, query: &'static str| async move {
            let loaded = store.load(T, T + 10_000, &[selector(query)]).await.unwrap();
            loaded.series.iter().map(|s| s.points.len()).sum::<usize>()
        };
        assert_eq!(points(store.clone(), "requests").await, 1);
        insert(store.clone(), 1).await;
        // The same panel again sees the cached rows; a differently matched
        // or discovered read sees the new row.
        assert_eq!(points(store.clone(), "requests").await, 1);
        assert_eq!(points(store.clone(), "requests{tenant=\"a\"}").await, 2);
        assert_eq!(
            store
                .discover(T, T + 10_000, &[selector("requests")])
                .await
                .unwrap()
                .series
                .len(),
            1
        );
        store.series_cache_ttl = Duration::ZERO;
        assert_eq!(points(store.clone(), "requests").await, 2);
    })
    .await;
}

#[tokio::test]
async fn tables_sharing_a_metric_merge_into_one_series() {
    common::empty_database_test(false, |mut store| async move {
        let insert = |store: Store, kind: &'static str, second: i64, value: i64| async move {
            let row = json!({"MetricName": "shared", "ScopeName": PROM, "TimeUnix": T / 1000 + second, "Value": value});
            store
                .query(
                    &format!(
                        "INSERT INTO {}.otel_metrics_{kind} FORMAT JSONEachRow\n{row}",
                        store.database
                    ),
                    &[],
                )
                .await
                .unwrap();
        };
        insert(store.clone(), "gauge", 0, 1).await;
        insert(store.clone(), "gauge", 1, 2).await;
        insert(store.clone(), "sum", 2, 3).await;
        store.series_cache_ttl = Duration::from_secs(60);
        let loaded = store.load(T, T + 10_000, &[selector("shared")]).await.unwrap();
        assert_eq!(loaded.series.len(), 1);
        assert_eq!(
            loaded.series[0].points.iter().map(|p| p.value).collect::<Vec<_>>(),
            [1.0, 2.0, 3.0]
        );
        assert_eq!(loaded.metadata["shared"].len(), 1);
        // The sum table's row disagrees with the gauge table's at one timestamp.
        insert(store.clone(), "sum", 1, 9).await;
        let error = store
            .load(T, T + 10_000, &[selector("shared{job=\"\"}")])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("conflicting samples"), "{error}");
        // Budgets span every table, cached or not.
        store.max_rows = 3;
        let error = store
            .load(T, T + 10_000, &[selector("shared{job=~\"\"}")])
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("sample scan limit"), "{error}");
    })
    .await;
}
