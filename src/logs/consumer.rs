use std::{collections::BTreeMap, fmt, sync::Arc, time};

use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    time::{sleep, Duration},
};
use tokio_postgres::{self, tls::MakeTlsConnect, Client, Config, Connection, Socket};
use tracing::{debug, trace};

use crate::{
    connection::NotificationStream,
    logs::{Cursor, Error, Result, Version},
};

static LISTEN: &str = "LISTEN logs";
static IS_VISIBLE: &str = "\
     WITH snapshot as (
         select txid_snapshot_xmin(txid_current_snapshot()) as xmin,
         txid_current() as current
        )
        SELECT $1 < xmin as lt_xmin, xmin, current FROM snapshot";
static LIST_CONSUMERS: &str =
    "SELECT name, epoch, tx_position, position FROM log_consumer_positions";
static DISCARD_ENTRIES: &str = "DELETE FROM logs WHERE (epoch, tx_id, id) <= ($1, $2, $3)";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
    pub version: Version,
    pub written_at: DateTime<Utc>,
    pub key: Vec<u8>,
    pub meta: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

pub struct Consumer {
    client: Client,
    notify: Arc<Notify>,
    cursor: Cursor,
}

impl Consumer {
    pub async fn connect<T>(config: &Config, tls: T, name: &str) -> Result<Self>
    where
        T: MakeTlsConnect<Socket>,
        T::Stream: Send + 'static,
    {
        let (client, conn) = config.connect(tls).await?;

        let notify = Arc::new(Notify::new());
        tokio::spawn(Self::run_connection(conn, notify.clone()));

        listen(&client).await?;

        Self::new(notify, client, name).await
    }

    async fn new(notify: Arc<Notify>, client: Client, name: &str) -> Result<Self> {
        let cursor = Cursor::load(&client, name).await?;

        let consumer = Consumer {
            client,
            notify,
            cursor,
        };

        Ok(consumer)
    }

    async fn run_connection<
        S: AsyncRead + AsyncWrite + Unpin,
        T: AsyncRead + AsyncWrite + Unpin,
    >(
        conn: Connection<S, T>,
        notify: Arc<Notify>,
    ) -> Result<()> {
        debug!("Listening for notifies on connection");

        let mut notifies = NotificationStream::new(conn);

        while let Some(notification) = notifies.next().await.transpose()? {
            debug!(?notification, "Received notification");
            notify.notify_waiters();
        }

        Ok(())
    }

    pub async fn poll(&mut self) -> Result<Option<Entry>> {
        self.poll_item().await
    }

    async fn poll_item(&mut self) -> Result<Option<Entry>> {
        return self.cursor.poll(&mut self.client).await;
    }

    pub async fn wait_next(&mut self) -> Result<Entry> {
        loop {
            if let Some(entry) = self.poll_item().await? {
                return Ok(entry);
            }
            trace!("Awaiting notifications");
            self.notify.notified().await;
        }
    }

    pub async fn wait_until_visible(
        &self,
        version: Version,
        timeout: time::Duration,
    ) -> Result<()> {
        wait_until_visible(&self.client, version, timeout).await
    }

    pub async fn commit_upto(&mut self, entry: &Entry) -> Result<()> {
        self.cursor.commit_upto(&mut self.client, entry).await
    }

    pub async fn discard_upto(&mut self, limit: Version) -> Result<()> {
        debug!(?limit, "Discarding entries upto");
        self.client
            .execute(DISCARD_ENTRIES, &[&limit.epoch, &limit.tx_id, &limit.seq])
            .await?;
        Ok(())
    }

    pub async fn discard_consumed(&mut self) -> Result<()> {
        let rows = self.client.query(LIST_CONSUMERS, &[]).await?;
        if let Some(min_version) = rows.into_iter().map(|r| Version::from_row(&r)).min() {
            debug!(?min_version, "Discarding consumed upto");
            self.discard_upto(min_version).await?;
        }
        Ok(())
    }

    pub async fn consumers(client: &mut Client) -> Result<BTreeMap<String, Version>> {
        let t = client.transaction().await?;
        let rows = t.query(LIST_CONSUMERS, &[]).await?;

        let consumers = rows
            .into_iter()
            .map(|r| (r.get("name"), Version::from_row(&r)))
            .collect();
        t.commit().await?;

        Ok(consumers)
    }

    pub async fn clear_offset(&mut self) -> Result<()> {
        self.cursor.clear_offset(&mut self.client).await
    }
}

pub async fn wait_until_visible(
    client: &Client,
    version: Version,
    timeout: time::Duration,
) -> Result<()> {
    let deadline = time::Instant::now() + timeout;
    client.execute(LISTEN, &[]).await?;
    for backoff in 0..64 {
        trace!(?version, "Checking for visibility");
        let (is_visible, txmin, tx_current) = client
            .query(IS_VISIBLE, &[&version.tx_id])
            .await?
            .into_iter()
            .next()
            .map(|r| {
                (
                    r.get::<_, bool>("lt_xmin"),
                    r.get::<_, i64>("xmin"),
                    r.get::<_, i64>("current"),
                )
            })
            .ok_or(Error::NoRowsFromVisibilityCheck)?;
        trace!(?is_visible, ?txmin, ?tx_current, "Visibility check",);

        let now = time::Instant::now();

        if is_visible {
            break;
        }

        if now > deadline {
            return Err(Error::VisibilityTimeout(version));
        }

        let remaining = deadline - now;
        let backoff = Duration::from_millis((2u64).pow(backoff));
        let pause = std::cmp::min(remaining, backoff);
        trace!(?remaining, ?pause, "Pause before retry",);
        sleep(pause).await;
    }

    Ok(())
}

pub async fn listen(client: &Client) -> Result<()> {
    client.execute(LISTEN, &[]).await?;
    Ok(())
}

impl fmt::Debug for Consumer {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Consumer")
            .field("cursor", &self.cursor)
            .finish()
    }
}
