use std::{env, time::Duration};

use anyhow::{Context, Result};
use futures::FutureExt;
use pg_queue::logs::{Cursor, Entry};
use rand::{
    distributions::Uniform,
    prelude::{Distribution, StdRng},
    RngCore, SeedableRng,
};
use tokio::time::sleep;
use tokio_postgres::{Client, Config, NoTls};
use tracing::{debug, instrument};
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
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
    .context("tracing subscriber")?;

    let url = env::var("POSTGRES_URL").context("$POSTGRES_URL")?;
    let pg: Config = url.parse()?;

    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    tokio::spawn(conn);

    pg_queue::logs::setup(&client).await.context("setup db")?;

    let a = tokio::spawn(run_pingpong(
        pg.clone(),
        StdRng::from_entropy(),
        "ping",
        "pong",
    ))
    .map(|res| res?);
    let b = tokio::spawn(run_pingpong(pg, StdRng::from_entropy(), "pong", "ping")).map(|res| res?);

    {
        let version = pg_queue::logs::produce(&mut client, b"ping", 1usize.to_string().as_bytes())
            .await
            .context("produce")?;
        debug!(?version, "Produced seed");
    }

    let ((), ()) = tokio::try_join!(a, b).context("join")?;

    Ok(())
}

#[instrument(skip(pg, rng))]
async fn run_pingpong<R: RngCore>(pg: Config, mut rng: R, from: &str, to: &str) -> Result<()> {
    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    tokio::spawn(conn);
    let jitter_dist = Uniform::from(1f64..1.5);

    let backoff = Duration::from_millis(1000);

    loop {
        let mut cursor = Cursor::load(&client, from).await.context("load cursor")?;
        if let Some(it) = cursor.poll(&mut client).await.context("cursor poll")? {
            handle_item(&mut client, from, to, &it)
                .await
                .context("handle")?;

            cursor
                .commit_upto(&mut client, &it)
                .await
                .context("commit")?;
        } else {
            let jitter: f64 = jitter_dist.sample(&mut rng);
            let pause = backoff.mul_f64(jitter);
            debug!(?pause, "Backoff");
            sleep(pause).await;
        }
    }
}

#[instrument(
    skip(client,from,to,it),
    fields(
    version=%it.version,
    key=?String::from_utf8_lossy(&it.key),
))]
async fn handle_item(client: &mut Client, from: &str, to: &str, it: &Entry) -> Result<()> {
    debug!("Found item");

    if from.as_bytes() == it.key {
        let value = std::str::from_utf8(&it.data).context("decode utf8")?;
        let num: u64 = value.parse().context("parse body")?;
        debug!("Found number: {}", num);

        let next = num + 1;

        let version = pg_queue::logs::produce(client, to.as_bytes(), next.to_string().as_bytes())
            .await
            .context("produce")?;
        debug!(%version, "Produced");
    } else {
        debug!("Ignoring item");
    }

    Ok(())
}
