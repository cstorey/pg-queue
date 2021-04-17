use std::{env, sync::Once};

use anyhow::{Context, Result};
use tokio_postgres::{self, Client, Config, NoTls};
use tracing::debug;

use pg_queue::{jobs, logs};
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

mod job_worker;
mod producer_consumer;
mod reusable_connections;

const DEFAULT_URL: &str = "postgres://postgres@localhost/";

fn load_pg_config(schema: &str) -> Result<Config> {
    let url = env::var("POSTGRES_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let mut config: Config = url.parse()?;
    debug!(?schema, "Use schema");

    config.options(&format!("-csearch_path={}", schema));

    Ok(config)
}

async fn connect(config: &Config) -> Result<Client> {
    let (client, conn) = config.connect(NoTls).await?;
    tokio::spawn(conn);
    Ok(client)
}

async fn setup_log(schema: &str) {
    let pg_config = load_pg_config(schema).expect("pg-config");

    let mut client = connect(&pg_config).await.expect("connect");

    create_fresh_schema(&mut client, schema)
        .await
        .expect("connect");
    logs::setup(&client).await.expect("setup");
}

async fn setup_jobs(schema: &str) {
    let pg_config = load_pg_config(schema).expect("pg-config");

    let mut client = connect(&pg_config).await.expect("connect");

    create_fresh_schema(&mut client, schema)
        .await
        .expect("connect");
    jobs::setup(&client).await.expect("setup");
}

async fn create_fresh_schema(client: &mut Client, schema: &str) -> Result<()> {
    let t = client.transaction().await.expect("BEGIN");
    t.execute(
        &*format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema),
        &[],
    )
    .await
    .context("drop schema")
    .expect("execute");
    t.execute(&*format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .context("create schema")
        .expect("execute");
    t.commit().await.expect("commit");

    Ok(())
}

pub(crate) fn setup_logging() {
    static LOGGING: Once = Once::new();

    LOGGING.call_once(|| {
        LogTracer::init().expect("log-tracer init");
        tracing::subscriber::set_global_default(
            tracing_subscriber::FmtSubscriber::builder()
                .with_env_filter(EnvFilter::from_default_env())
                .with_ansi(false)
                .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc3339())
                .with_thread_names(true)
                .with_thread_ids(true)
                .finish(),
        )
        .expect("set global tracer");
    });
}
