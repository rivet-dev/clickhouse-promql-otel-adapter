mod common;
use axum::{
    body::{Body, to_bytes},
    http::{HeaderName, Request},
};
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
use serde_json::Value;
use std::time::Duration;
use tower::ServiceExt;

const TABLES: [&str; 5] = [
    "gauge",
    "sum",
    "histogram",
    "summary",
    "exponential_histogram",
];

/// Adds the production `ProjectId` column and a reader user limited by the
/// production row policy, then returns a store connected as that reader.
async fn reader_store(admin: &Store) -> (Store, String) {
    let database = &admin.database;
    let reader = format!("reader_{database}");
    for table in TABLES {
        admin
            .query(
                &format!(
                    "ALTER TABLE {database}.otel_metrics_{table} ADD COLUMN ProjectId LowCardinality(String) MATERIALIZED ResourceAttributes['rivet.project']"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    admin
        .query(
            &format!(
                "CREATE USER {reader} IDENTIFIED WITH plaintext_password BY 'reader' SETTINGS readonly = 2"
            ),
            &[],
        )
        .await
        .unwrap();
    admin
        .query(&format!("GRANT SELECT ON {database}.* TO {reader}"), &[])
        .await
        .unwrap();
    for table in TABLES {
        admin
            .query(
                &format!(
                    "CREATE ROW POLICY tenant_{database} ON {database}.otel_metrics_{table} USING ProjectId = getSetting('SQL_project_id') TO {reader}"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    let rows = ["a", "b"]
        .iter()
        .zip([1.0, 2.0])
        .map(|(project, value)| {
            serde_json::json!({
                "ResourceAttributes": {"rivet.project": project, "service.name": "engine"},
                "ServiceName": "engine",
                "MetricName": "tenant_metric",
                "Attributes": {"kind": project},
                "TimeUnix": "2023-11-14 22:13:20",
                "Value": value,
            })
            .to_string()
        })
        .collect::<Vec<_>>()
        .join("\n");
    admin
        .query(
            &format!("INSERT INTO {database}.otel_metrics_gauge FORMAT JSONEachRow\n{rows}"),
            &[],
        )
        .await
        .unwrap();

    let mut store = Store::new(
        admin.url.clone(),
        database.clone(),
        reader.clone(),
        "reader".into(),
    )
    .unwrap();
    store.tenant_setting = Some("SQL_project_id".into());
    store.require_tenant = true;
    store.metric_cache_ttl = Duration::from_secs(60);
    store.series_cache_ttl = Duration::from_secs(60);
    store.promoted_resource_attributes = vec![("rivet.project".into(), "rivet_project".into())];
    store.label_columns = vec![("rivet_project".into(), "ProjectId".into())];
    (store, reader)
}

async fn cleanup(admin: &Store, reader: &str) {
    let database = &admin.database;
    for table in TABLES {
        admin
            .query(
                &format!(
                    "DROP ROW POLICY IF EXISTS tenant_{database} ON {database}.otel_metrics_{table}"
                ),
                &[],
            )
            .await
            .unwrap();
    }
    admin
        .query(&format!("DROP USER IF EXISTS {reader}"), &[])
        .await
        .unwrap();
}

async fn query(app: &App, tenant: Option<&str>, promql: &str) -> (u16, Value) {
    let mut request = Request::builder().uri(format!(
        "/api/v1/query?time=1700000000&query={}",
        url_encode(promql)
    ));
    if let Some(tenant) = tenant {
        request = request.header("x-rivet-project", tenant);
    }
    let response = router(app.clone())
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status().as_u16();
    let data = to_bytes(response.into_body(), 1_000_000).await.unwrap();
    (status, serde_json::from_slice(&data).unwrap())
}

fn url_encode(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

/// The `kind` label of every returned series.
fn kinds(data: &Value) -> Vec<String> {
    let mut kinds = data["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .map(|series| series["metric"]["kind"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    kinds.sort();
    kinds
}

#[tokio::test]
async fn tenants_read_only_their_rows() {
    common::empty_database_test(false, |admin| async move {
        let (store, reader) = reader_store(&admin).await;
        let mut app = App::new(store);
        app.tenant_header = Some(HeaderName::from_static("x-rivet-project"));

        let (status, body) = query(&app, None, "tenant_metric").await;
        assert_eq!(status, 400, "{body}");
        assert!(body["error"].as_str().unwrap().contains("x-rivet-project"));

        let (status, body) = query(&app, Some(""), "tenant_metric").await;
        assert_eq!(status, 400, "{body}");

        // Both caches are on: the second tenant must not get the first
        // tenant's cached names or rows.
        let (status, a) = query(&app, Some("a"), "tenant_metric").await;
        assert_eq!(status, 200, "{a}");
        assert_eq!(kinds(&a), ["a"]);
        let (status, b) = query(&app, Some("b"), "tenant_metric").await;
        assert_eq!(status, 200, "{b}");
        assert_eq!(kinds(&b), ["b"]);

        let (status, scoped) = query(&app, Some("a"), "tenant_metric{rivet_project=\"b\"}").await;
        assert_eq!(status, 200, "{scoped}");
        assert_eq!(kinds(&scoped), Vec::<String>::new());

        let (status, unknown) = query(&app, Some("c"), "tenant_metric").await;
        assert_eq!(status, 200, "{unknown}");
        assert_eq!(kinds(&unknown), Vec::<String>::new());

        cleanup(&admin, &reader).await;
    })
    .await
}

#[tokio::test]
async fn store_refuses_statements_without_tenant() {
    common::empty_database_test(false, |admin| async move {
        let (store, reader) = reader_store(&admin).await;
        let error = store.query("SELECT 1", &[]).await.unwrap_err();
        assert!(error.to_string().contains("without a tenant"), "{error}");
        store.ready().await.unwrap();
        cleanup(&admin, &reader).await;
    })
    .await
}
