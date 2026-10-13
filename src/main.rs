use clap::Parser;
use clickhouse_promql::{
    api::{App, router},
    storage::Store,
};
#[derive(Parser)]
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
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let c = Config::parse();
    let store = Store::new(c.clickhouse_url, c.database, c.username, c.password)?;
    let address = format!("{}:{}", c.host, c.port);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(%address,"listening");
    axum::serve(listener, router(App::new(store)))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c().await.ok();
        })
        .await?;
    Ok(())
}
