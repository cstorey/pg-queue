use std::{env, sync::Arc, task::Poll};

use anyhow::{Context, Result};
use futures::{ready, FutureExt, Stream, StreamExt};
use pg_queue::logs::{Cursor, Entry};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
};
use tokio_postgres::{AsyncMessage, Client, Config, Connection, NoTls, Notification};
use tracing::{debug, field, info, instrument, trace, Instrument};
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

    let a = tokio::spawn(run_pingpong(pg.clone(), "ping", "pong")).map(|res| res?);
    let b = tokio::spawn(run_pingpong(pg, "pong", "ping")).map(|res| res?);

    {
        let version = pg_queue::logs::produce(&mut client, b"ping", 1usize.to_string().as_bytes())
            .await
            .context("produce")?;
        debug!(?version, "Produced seed");
    }

    let ((), ()) = tokio::try_join!(a, b).context("join")?;

    Ok(())
}

#[instrument(skip(pg))]
async fn run_pingpong(pg: Config, from: &str, to: &str) -> Result<()> {
    let notify = Arc::new(Notify::new());

    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    let span = tracing::error_span!("connection", from = field::Empty, to = field::Empty);
    span.record("from", &from);
    span.record("to", &to);
    tokio::spawn(run_connection(conn, notify.clone()).instrument(span));

    pg_queue::logs::listen(&client).await.context("LISTEN")?;

    let mut cursor = Cursor::load(&client, from).await.context("load cursor")?;
    loop {
        if let Some(it) = cursor.poll(&mut client).await.context("cursor poll")? {
            handle_item(&mut client, from, to, &it)
                .await
                .context("handle")?;

            cursor
                .commit_upto(&mut client, &it)
                .await
                .context("commit")?;
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

#[instrument(skip(conn, notify))]
async fn run_connection<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin>(
    conn: Connection<S, T>,
    notify: Arc<Notify>,
) -> Result<()> {
    debug!("Listening for notifies on connection");

    let mut notifies = NotificationStream::new(conn);

    while let Some(notification) = notifies.next().await.transpose()? {
        debug!(?notification, "Received notification");
        notify.notify_one();
    }

    Ok(())
}

struct NotificationStream<S, T> {
    conn: Connection<S, T>,
}

impl<S, T> NotificationStream<S, T> {
    fn new(conn: Connection<S, T>) -> Self {
        Self { conn }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin, T: AsyncRead + AsyncWrite + Unpin> Stream
    for NotificationStream<S, T>
{
    type Item = Result<Notification, tokio_postgres::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let maybe_message = ready!(self.conn.poll_message(cx));
        let message = match maybe_message {
            Some(Ok(item)) => item,
            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
            None => return Poll::Ready(None),
        };

        match message {
            AsyncMessage::Notice(notice) => {
                info!(?notice, "Db notice");
                Poll::Pending
            }
            AsyncMessage::Notification(notification) => Poll::Ready(Some(Ok(notification))),
            message => {
                trace!(?message, "Other message received");
                Poll::Pending
            }
        }
    }
}
