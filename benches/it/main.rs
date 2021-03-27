use std::{env, sync::Once};

use anyhow::{Context, Result};
use criterion::{criterion_group, criterion_main};
use pg_queue::logs::setup;
use tokio_postgres::{Client, Config, NoTls};
use tracing::debug;
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

mod pipelined_inserts;
mod seq_inserts;

fn load_pg_config(schema: &str) -> Result<Config> {
    let url = env::var("POSTGRES_URL").expect("$POSTGRES_URL");
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

async fn setup_db(schema: &str) -> Result<()> {
    let pg_config = load_pg_config(schema).context("pg-config")?;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await.context("BEGIN")?;
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

    setup(&client).await.context("setup")?;

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

criterion_group!(
    benches,
    seq_inserts::batch_seq_insert,
    pipelined_inserts::batch_pipeline_insert,
);
criterion_main!(benches);
