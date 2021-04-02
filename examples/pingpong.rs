use std::{env, sync::Arc};

use anyhow::{Context, Result};
use futures::{future::try_join_all, FutureExt};
use pg_queue::{
    connection::notify_on_notification,
    logs::{Cursor, Entry},
};

use tokio::sync::Notify;
use tokio_postgres::{Config, GenericClient, NoTls};
use tracing::{debug, field, instrument, Instrument};
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

    let mut tasks = Vec::new();

    let instances = ["a", "b", "c", "d", "e"];
    for (x, y) in instances.iter().zip(instances.iter().cycle().skip(1)) {
        let task = tokio::spawn(run_pingpong(pg.clone(), x, y)).map({
            let id = (x.to_string(), y.to_string());
            |res| {
                res.with_context({
                    let id = id.clone();
                    move || format!("outer error: {}->{}", id.0, id.1)
                })?
                .with_context(move || format!("inner error: {}->{}", id.0, id.1))
            }
        });
        tasks.push(task);
    }

    {
        let key = instances.get(0).expect("first item");
        let version =
            pg_queue::logs::produce(&mut client, key.as_bytes(), 1usize.to_string().as_bytes())
                .await
                .context("produce")?;
        debug!(?version, ?key, "Produced seed");
    }

    let _: Vec<()> = try_join_all(tasks).await?;

    Ok(())
}

#[instrument(skip(pg))]
async fn run_pingpong(pg: Config, from: &str, to: &str) -> Result<()> {
    let notify = Arc::new(Notify::new());

    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    let span = tracing::error_span!("connection", from = field::Empty, to = field::Empty);
    span.record("from", &from);
    span.record("to", &to);
    tokio::spawn(notify_on_notification(conn, notify.clone()).instrument(span));

    pg_queue::logs::listen(&client).await.context("LISTEN")?;

    loop {
        let mut t = client.transaction().await?;
        let mut cursor = Cursor::load(&t, from).await.context("load cursor")?;
        if let Some(it) = cursor.poll(&mut t).await.context("cursor poll")? {
            handle_item(&mut t, from, to, &it).await.context("handle")?;

            cursor.commit_upto(&mut t, &it).await.context("commit")?;
            t.commit().await?;
        } else {
            debug!("Await notification");
            notify.notified().await;
            debug!("Notification received");
        }
    }
}

#[instrument(
    skip(client,from,to,it),
    fields(
    version=%it.version,
    key=?String::from_utf8_lossy(&it.key),
))]
async fn handle_item<C: GenericClient>(
    client: &mut C,
    from: &str,
    to: &str,
    it: &Entry,
) -> Result<()> {
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
