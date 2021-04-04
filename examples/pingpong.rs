use std::{
    env,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures::{
    channel::mpsc::UnboundedSender,
    future::{self, select, try_join_all, Either},
    FutureExt, SinkExt, StreamExt,
};
use pg_queue::{
    connection::NotificationStream,
    logs::{Cursor, Entry},
};

use rand::{
    distributions::Uniform,
    prelude::{Distribution, StdRng},
    RngCore, SeedableRng,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    time::timeout,
};
use tokio_postgres::{Config, Connection, GenericClient, NoTls};
use tracing::{debug, field, instrument, trace, warn, Instrument};
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

const MIN_PAUSE: Duration = Duration::from_millis(10);
const MAX_PAUSE: Duration = Duration::from_secs(30);

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
        let rng = StdRng::from_entropy();
        let task = tokio::spawn(run_pingpong(pg.clone(), rng, x, y)).map({
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
        let version = pg_queue::logs::produce(&mut client, key, 1usize.to_string().as_bytes())
            .await
            .context("produce")?;
        debug!(?version, ?key, "Produced seed");
    }

    let _: Vec<()> = try_join_all(tasks).await?;

    Ok(())
}

#[instrument(skip(pg, rng))]
async fn run_pingpong<R: RngCore>(pg: Config, mut rng: R, from: &str, to: &str) -> Result<()> {
    // We use an unbounded channel here as the same process both mediates SQL
    // commands, and feeding us notifications, so if the channel is full, and
    // we're still waiting on a response from the server, then we'll deadlock.
    let (tx, mut rx) = futures::channel::mpsc::unbounded();

    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    let span = tracing::error_span!("connection", from = field::Empty, to = field::Empty);
    span.record("from", &from);
    span.record("to", &to);
    tokio::spawn(siphon_notifications(conn, tx).instrument(span));

    pg_queue::logs::listen(&client).await.context("LISTEN")?;

    let mut base_pause = MIN_PAUSE;
    let jitter_factor = Uniform::from(1f64..1.5);

    let mut cursor = Cursor::load(&client, from).await.context("load cursor")?;
    loop {
        let mut t = client.transaction().await?;

        while let Some(it) = cursor.poll(&mut t).await.context("cursor poll")? {
            if from.as_bytes() == it.key {
                handle_item(&mut t, to, &it).await.context("handle")?;
            } else {
                debug!(version=%it.version, "Ignoring item");
            }

            cursor.commit_upto(&mut t, &it).await.context("commit")?;
        }

        // Close the transaction, as postgres will only deliver notifications
        // that happened-before the start of the transaction (I think).
        t.commit().await?;

        let pause = base_pause
            .mul_f64(jitter_factor.sample(&mut rng))
            .min(MAX_PAUSE);

        debug!(timeout=?pause, "Await notification");
        let started = Instant::now();
        match timeout(base_pause, rx.next()).await {
            Ok(Some(notification)) => {
                debug!(after=?started.elapsed(), ?notification, "Notification received");
                base_pause = MIN_PAUSE;

                'read_unread: loop {
                    match select(rx.next(), future::ready(())).await {
                        Either::Left((Some(notification), _)) => {
                            debug!(after=?started.elapsed(), ?notification, "Another notification received");
                        }
                        _ => {
                            break 'read_unread;
                        }
                    };
                }
            }
            Ok(None) => {
                // We should get a proper error at some point.
                warn!(after=?started.elapsed(), "Connection exited?");
            }
            Err(_) => {
                trace!(?pause, after=?started.elapsed(), "Not notified within timeout, retrying with backoff");
                base_pause = base_pause.mul_f64(1.5).min(MAX_PAUSE);
            }
        }
    }
}

#[instrument(
    skip(client,to,it),
    fields(
    version=%it.version,
    key=?String::from_utf8_lossy(&it.key),
))]
async fn handle_item<C: GenericClient>(client: &mut C, to: &str, it: &Entry) -> Result<()> {
    debug!("Found item");

    let value = std::str::from_utf8(&it.data).context("decode utf8")?;
    let num: u64 = value.parse().context("parse body")?;
    debug!("Found number: {}", num);

    let next = num + 1;

    let version = pg_queue::logs::produce(client, to, next.to_string().as_bytes())
        .await
        .context("produce")?;
    debug!(%version, "Produced");

    Ok(())
}

pub async fn siphon_notifications<
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
>(
    conn: Connection<S, T>,
    mut tx: UnboundedSender<tokio_postgres::Notification>,
) -> Result<()> {
    debug!("Listening for notifies on connection");

    let mut notifies = NotificationStream::new(conn);

    while let Some(notification) = notifies.next().await.transpose()? {
        debug!(?notification, "Received notification");
        tx.send(notification).await?;
        trace!("Notified");
    }

    Ok(())
}
