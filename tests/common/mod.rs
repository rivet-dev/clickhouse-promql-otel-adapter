use clickhouse_promql::storage::Store;
use std::future::Future;
#[allow(dead_code)]
pub async fn database_test<F, Fut>(test: F)
where
    F: FnOnce(Store) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_database(true, false, test).await
}
#[allow(dead_code)]
pub async fn empty_database_test<F, Fut>(precise: bool, test: F)
where
    F: FnOnce(Store) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    run_database(false, precise, test).await
}
async fn run_database<F, Fut>(seed: bool, precise: bool, test: F)
where
    F: FnOnce(Store) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let database = format!("test_{}", uuid::Uuid::new_v4().simple());
    let store = Store::new(
        std::env::var("CLICKHOUSE_URL").unwrap_or("http://127.0.0.1:18123".into()),
        database.clone(),
        "default".into(),
        "local-test-only".into(),
    )
    .unwrap();
    store
        .query(&format!("CREATE DATABASE {database}"), &[])
        .await
        .expect("real ClickHouse required: docker compose up -d --wait");
    let task_store = store.clone();
    let task = tokio::spawn(async move {
        for ddl in include_str!("../snapshots/collector-schema.sql")
            .replace("{{database}}", &database)
            .split(';')
            .filter(|s| !s.trim().is_empty())
        {
            let ddl = if precise {
                ddl.replace("DateTime", "DateTime64(9)")
            } else {
                ddl.to_owned()
            };
            task_store.query(&ddl, &[]).await.unwrap();
        }
        if seed {
            let rows: serde_json::Value =
                serde_json::from_str(include_str!("../snapshots/collector-rows.json")).unwrap();
            for (table, rows) in rows.as_object().unwrap() {
                let body = rows
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| serde_json::to_string(r).unwrap())
                    .collect::<Vec<_>>()
                    .join("\n");
                task_store
                    .query(
                        &format!("INSERT INTO {database}.{table} FORMAT JSONEachRow\n{body}"),
                        &[],
                    )
                    .await
                    .unwrap();
            }
        }
        test(task_store).await;
    });
    let result = task.await;
    store
        .query(&format!("DROP DATABASE {} SYNC", store.database), &[])
        .await
        .expect("test database cleanup failed");
    if let Err(error) = result {
        std::panic::resume_unwind(error.into_panic());
    }
}
