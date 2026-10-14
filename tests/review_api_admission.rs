use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tower::ServiceExt;

fn app() -> App {
    let mut app = App::new(
        Store::new(
            "http://127.0.0.1:1".into(),
            "otel".into(),
            "default".into(),
            "".into(),
        )
        .unwrap(),
    );
    app.request_timeout = Duration::from_secs(2);
    app.concurrency = Arc::new(Semaphore::new(1));
    app.pending = Arc::new(Semaphore::new(1));
    app
}
fn query(timeout: &str) -> Request<Body> {
    Request::builder()
        .uri(format!("/api/v1/query?query=1&timeout={timeout}"))
        .body(Body::empty())
        .unwrap()
}
async fn error(response: axum::response::Response, kind: &str) {
    assert_eq!(response.status(), 503);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["errorType"], kind);
}
#[tokio::test]
async fn client_deadline_includes_queue_and_releases_waiter() {
    let app = app();
    let held = app.concurrency.clone().acquire_owned().await.unwrap();
    let response = tokio::time::timeout(
        Duration::from_millis(300),
        router(app.clone()).oneshot(query("20ms")),
    )
    .await
    .expect("client timeout must include admission")
    .unwrap();
    error(response, "timeout").await;
    assert_eq!(app.pending.available_permits(), 1);
    drop(held);
    assert_eq!(
        router(app).oneshot(query("1s")).await.unwrap().status(),
        200
    );
}
#[tokio::test]
async fn overload_and_cancelled_waiters_do_not_leak_permits() {
    let app = app();
    let held = app.concurrency.clone().acquire_owned().await.unwrap();
    let waiting = tokio::spawn(router(app.clone()).oneshot(query("1s")));
    tokio::time::timeout(Duration::from_millis(300), async {
        while app.pending.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    error(
        router(app.clone()).oneshot(query("1s")).await.unwrap(),
        "unavailable",
    )
    .await;
    // Operational liveness must remain independent of expensive request admission.
    let health = Request::builder()
        .uri("/-/healthy")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router(app.clone()).oneshot(health).await.unwrap().status(),
        200
    );
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(app.pending.available_permits(), 1);
    drop(held);
    assert_eq!(app.concurrency.available_permits(), 1);
}
#[test]
fn timed_out_blocking_job_retains_admission_until_it_actually_exits() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let app = app();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, entered) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
        entered.await.unwrap();
        let response = router(app.clone()).oneshot(query("20ms")).await.unwrap();
        // Release the blocker before assertions so test failures cannot strand shutdown.
        let held = app.concurrency.available_permits();
        release.send(()).unwrap();
        blocker.await.unwrap();
        error(response, "timeout").await;
        assert_eq!(
            held, 0,
            "queued blocking evaluation must own its execution permit"
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while app.concurrency.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}
#[tokio::test]
async fn stalled_body_times_out_and_releases_pending_slot() {
    let mut app = app();
    app.request_timeout = Duration::from_millis(20);
    let body = Body::from_stream(futures_util::stream::pending::<
        Result<String, std::io::Error>,
    >());
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/query")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(body)
        .unwrap();
    error(
        router(app.clone()).oneshot(request).await.unwrap(),
        "timeout",
    )
    .await;
    assert_eq!(app.pending.available_permits(), 1);
    assert_eq!(app.concurrency.available_permits(), 1);
}
#[tokio::test]
async fn configured_sample_budget_is_applied_to_evaluation() {
    let mut app = app();
    app.max_samples = 1;
    let request = Request::builder()
        .uri("/api/v1/query_range?query=vector(1)&start=0&end=10&step=1")
        .body(Body::empty())
        .unwrap();
    let response = router(app).oneshot(request).await.unwrap();
    assert_eq!(response.status(), 422);
}

#[tokio::test]
async fn excessive_parameter_count_is_rejected_before_backend_access() {
    let query = std::iter::repeat_n("match[]=up", 1025)
        .collect::<Vec<_>>()
        .join("&");
    let request = Request::builder()
        .uri(format!("/api/v1/series?{query}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(router(app()).oneshot(request).await.unwrap().status(), 400);
}

#[test]
fn cli_rejects_zero_resource_budgets() {
    for flag in [
        "--max-concurrency",
        "--max-pending-requests",
        "--request-timeout-seconds",
        "--max-rows",
        "--max-bytes",
        "--max-samples",
        "--max-response-bytes",
    ] {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_clickhouse-promql"))
            .args([flag, "0"])
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{flag}");
    }
}

#[tokio::test]
async fn readiness_probe_is_bounded_independently_of_query_admission() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/",
                axum::routing::post(|| async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    "1"
                }),
            ),
        )
        .await
        .unwrap();
    });
    let mut app = App::new(Store::new(url, "otel".into(), "reader".into(), "".into()).unwrap());
    app.request_timeout = Duration::from_millis(20);
    let request = Request::builder()
        .uri("/-/ready")
        .body(Body::empty())
        .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(300), router(app).oneshot(request))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), 503);
    server.abort();
}

#[tokio::test]
async fn helper_routes_ignore_client_query_timeout_options() {
    for path in ["status/buildinfo", "format_query", "parse_query"] {
        for timeout in ["invalid", "0"] {
            let request = Request::builder()
                .uri(format!("/api/v1/{path}?query=1&timeout={timeout}"))
                .body(Body::empty())
                .unwrap();
            assert_eq!(
                router(app()).oneshot(request).await.unwrap().status(),
                200,
                "{path} timeout={timeout}"
            );
        }
    }
}
