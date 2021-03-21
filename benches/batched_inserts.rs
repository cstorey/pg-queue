use std::{env, sync::Once};

use anyhow::{Context, Result};
use bencher::{benchmark_group, benchmark_main, black_box, Bencher};
use futures::{stream, StreamExt, TryStreamExt};
use pg_queue::logs::{setup, Batch};
use tokio_postgres::{Client, Config, NoTls};
use tracing::debug;
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

fn insert_seq_0000(b: &mut Bencher) {
    batch_seq_insert_n(b, 0);
}
fn insert_seq_0001(b: &mut Bencher) {
    batch_seq_insert_n(b, 1);
}
fn insert_seq_0016(b: &mut Bencher) {
    batch_seq_insert_n(b, 16);
}
fn insert_seq_0256(b: &mut Bencher) {
    batch_seq_insert_n(b, 256);
}
fn insert_seq_4096(b: &mut Bencher) {
    batch_seq_insert_n(b, 4096);
}

fn insert_pipeline_0000(b: &mut Bencher) {
    batch_pipeline_insert_n(b, 0);
}
fn insert_pipeline_0001(b: &mut Bencher) {
    batch_pipeline_insert_n(b, 1);
}
fn insert_pipeline_0016(b: &mut Bencher) {
    batch_pipeline_insert_n(b, 16);
}
fn insert_pipeline_0256(b: &mut Bencher) {
    batch_pipeline_insert_n(b, 256);
}
fn insert_pipeline_4096(b: &mut Bencher) {
    batch_pipeline_insert_n(b, 4096);
}

fn batch_seq_insert_n(b: &mut Bencher, nitems: usize) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "batch_seq_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");
    runtime.block_on(setup_db(schema)).expect("setup db");
    let mut client = runtime.block_on(connect(&pg_config)).expect("connect");
    let bodies = (0..nitems).map(|i| format!("{}", i)).collect::<Vec<_>>();
    b.iter(|| {
        runtime.block_on(async {
            let batch = Batch::begin(&mut client).await.expect("batch");

            for b in bodies.iter() {
                batch.produce(b"a", b.as_bytes()).await.expect("produce");
            }

            batch.commit().await.expect("commit");
        })
    })
}

fn batch_pipeline_insert_n(b: &mut Bencher, nitems: usize) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "bench_pipelined_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");
    runtime.block_on(setup_db(schema)).expect("setup db");
    let mut client = runtime.block_on(connect(&pg_config)).expect("connect");
    let bodies = (0..nitems).map(|i| format!("{}", i)).collect::<Vec<_>>();
    b.iter(|| {
        runtime.block_on(async {
            let batch = Batch::begin(&mut client).await.expect("batch");

            stream::iter(bodies.iter())
                .map(|it| batch.produce(b"test", it.as_bytes()))
                .buffered(nitems)
                .try_for_each(|v| async move {
                    black_box(v);
                    Ok(())
                })
                .await
                .expect("insert");

            batch.commit().await.expect("commit");
        })
    })
}

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

benchmark_group!(
    benches,
    insert_seq_0000,
    insert_seq_0001,
    insert_seq_0016,
    insert_seq_0256,
    insert_seq_4096,
    insert_pipeline_0000,
    insert_pipeline_0001,
    insert_pipeline_0016,
    insert_pipeline_0256,
    insert_pipeline_4096,
);
benchmark_main!(benches);
