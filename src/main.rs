use clap::Parser;
use clickhouse_promql::{
    api::{App, router},
    label_name,
    storage::{Store, validate_identifier},
};
#[derive(Parser)]
#[command(version)]
struct Config {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    #[arg(long, default_value_t = 9091)]
    port: u16,
    #[arg(long, env = "CLICKHOUSE_URL", default_value = "http://127.0.0.1:18123")]
    clickhouse_url: String,
    #[arg(long, env = "CLICKHOUSE_DATABASE", default_value = "otel")]
    database: String,
    #[arg(long, env = "CLICKHOUSE_USER", default_value = "default")]
    username: String,
    #[arg(long, env = "CLICKHOUSE_PASSWORD", default_value = "")]
    password: String,
    /// Maximum admitted queries/discovery requests, including blocking work.
    #[arg(long, default_value_t = 16, value_parser=clap::value_parser!(u32).range(1..))]
    max_concurrency: u32,
    /// Maximum request bodies/waiters before overload is rejected.
    #[arg(long, default_value_t = 128, value_parser=clap::value_parser!(u32).range(1..))]
    max_pending_requests: u32,
    #[arg(long, default_value_t = 120, value_parser=clap::value_parser!(u32).range(1..))]
    request_timeout_seconds: u32,
    #[arg(long, default_value_t = 1_000_000, value_parser=clap::value_parser!(u32).range(1..))]
    max_rows: u32,
    #[arg(long, default_value_t = 67_108_864, value_parser=clap::value_parser!(u32).range(1..))]
    max_bytes: u32,
    #[arg(long, default_value_t = 5_000_000, value_parser=clap::value_parser!(u32).range(1..))]
    max_samples: u32,
    /// Maximum uncompressed success-response size; oversized responses fail, never truncate.
    #[arg(long, default_value_t = 67_108_864, value_parser=clap::value_parser!(u32).range(1..))]
    max_response_bytes: u32,
    /// Metric-name metadata cache lifetime. New metric names appear within this interval.
    #[arg(long, default_value_t = 60)]
    metric_cache_ttl_seconds: u64,
    /// How long decoded sample rows serve identical requests from other
    /// panels of a dashboard refresh; 0 disables. A panel can see rows loaded
    /// this many seconds earlier, so samples ingested in between appear one
    /// refresh later.
    #[arg(long, default_value_t = 10)]
    series_cache_ttl_seconds: u64,
    /// Approximate bytes of decoded series kept for other panels.
    #[arg(long, default_value_t = 268_435_456)]
    series_cache_bytes: u64,
    /// Labels identifying HA replicas; may be repeated.
    #[arg(long = "replica-label")]
    replica_labels: Vec<String>,
    /// Copy a resource attribute onto every series as a label, as ATTRIBUTE or
    /// ATTRIBUTE=LABEL; may be repeated. Without a LABEL the attribute name is
    /// normalized like any other label.
    #[arg(long = "promote-resource-attribute", value_parser = parse_promotion)]
    promoted_resource_attributes: Vec<(String, String)>,
    /// Filter a label on a table column that mirrors it, as LABEL=COLUMN;
    /// may be repeated. Use it for labels copied into a sort-key column, such
    /// as a tenant ID, so selective queries skip other tenants. Positive
    /// matchers on the label are decided by the column alone, so every row's
    /// column must hold exactly its label's value.
    #[arg(long = "label-column", value_parser = parse_label_column)]
    label_columns: Vec<(String, String)>,
    /// Enable Thanos defaults for omitted range step (seconds, usually 60).
    #[arg(long, value_parser=clap::value_parser!(u64).range(1..))]
    thanos_default_step: Option<u64>,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let c = Config::parse();
    let mut store = Store::new(c.clickhouse_url, c.database, c.username, c.password)?;
    store.max_rows = c.max_rows as usize;
    store.max_samples = c.max_samples as usize;
    store.max_bytes = c.max_bytes as usize;
    store.metric_cache_ttl = std::time::Duration::from_secs(c.metric_cache_ttl_seconds);
    store.series_cache_ttl = std::time::Duration::from_secs(c.series_cache_ttl_seconds);
    store.series_cache_bytes = usize::try_from(c.series_cache_bytes)?;
    store.promoted_resource_attributes = c.promoted_resource_attributes;
    store.label_columns = c.label_columns;
    // Cover the default one-hour dashboard plus the five-minute lookback.
    let warm_start = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
        .saturating_sub(65 * 60 * 1000);
    let warm_store = store.clone();
    tokio::spawn(async move { warm_store.warm_names(warm_start).await });
    let address = format!("{}:{}", c.host, c.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address,"listening");
    let mut app = App::new(store);
    app.concurrency = std::sync::Arc::new(tokio::sync::Semaphore::new(c.max_concurrency as usize));
    app.pending = std::sync::Arc::new(tokio::sync::Semaphore::new(c.max_pending_requests as usize));
    app.max_samples = c.max_samples as usize;
    app.max_response_bytes = c.max_response_bytes as usize;
    app.request_timeout = std::time::Duration::from_secs(c.request_timeout_seconds as u64);
    app.replica_labels = c.replica_labels;
    app.thanos_default_step = c.thanos_default_step.map(std::time::Duration::from_secs);
    axum::serve(listener, router(app))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}

fn parse_promotion(value: &str) -> anyhow::Result<(String, String)> {
    let (attribute, label) = match value.split_once('=') {
        Some((attribute, label)) => (attribute, label.to_owned()),
        None => (value, label_name(value)?),
    };
    if attribute.is_empty() {
        anyhow::bail!("resource attribute name is empty");
    }
    if label_name(&label)? != label {
        anyhow::bail!("invalid label name {label:?}");
    }
    Ok((attribute.to_owned(), label))
}

fn parse_label_column(value: &str) -> anyhow::Result<(String, String)> {
    let (label, column) = value
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("expected LABEL=COLUMN"))?;
    if label_name(label)? != label {
        anyhow::bail!("invalid label name {label:?}");
    }
    validate_identifier(column)?;
    Ok((label.to_owned(), column.to_owned()))
}
