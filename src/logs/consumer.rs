use std::{collections::BTreeMap, fmt, sync::Arc, time};

use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    time::{sleep, Duration},
};
use tokio_postgres::{self, tls::MakeTlsConnect, AsyncMessage, Client, Config, Connection, Socket};
use tracing::{debug, info, trace};

use crate::logs::{Cursor, Error, Result, Version};

static LISTEN: &str = "LISTEN logs";
static IS_VISIBLE: &str = "\
     WITH snapshot as (
         select txid_snapshot_xmin(txid_current_snapshot()) as xmin,
         txid_current() as current
        )
        SELECT $1 < xmin as lt_xmin, xmin, current FROM snapshot";
static LIST_CONSUMERS: &str =
    "SELECT name, epoch, tx_position, position FROM log_consumer_positions";
static DISCARD_ENTRIES: &str = "DELETE FROM logs WHERE (tx_id, id) <= ($1, $2)";

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

        Self::new(notify, client, name).await
    }

    async fn new(notify: Arc<Notify>, mut client: Client, name: &str) -> Result<Self> {
        let cursor = Cursor::load(&mut client, name).await?;

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
        mut conn: Connection<S, T>,
        notify: Arc<Notify>,
    ) -> Result<()> {
        debug!("Listening for notifies on connection");
        let mut messages = futures::stream::poll_fn(|cx| conn.poll_message(cx));
        while let Some(item) = messages.next().await.transpose()? {
            match item {
                AsyncMessage::Notice(err) => info!("Db notice: {}", err),
                AsyncMessage::Notification(n) => {
                    trace!("Received notification: {:?}", n);
                    notify.notify_one();
                }
                _ => trace!("Other message received"),
            }
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
        self.client.execute(LISTEN, &[]).await?;
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
        let t = self.client.transaction().await?;
        t.execute(DISCARD_ENTRIES, &[&limit.tx_id, &limit.seq])
            .await?;
        t.commit().await?;
        Ok(())
    }

    pub async fn discard_consumed(&mut self) -> Result<()> {
        let t = self.client.transaction().await?;
        let rows = t.query(LIST_CONSUMERS, &[]).await?;
        if let Some(min_version) = rows.into_iter().map(|r| Version::from_row(&r)).min() {
            t.execute(DISCARD_ENTRIES, &[&min_version.tx_id, &min_version.seq])
                .await?;
            t.commit().await?;
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
        trace!("Checking for visibility of: {:?}", version,);
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
        trace!(
            "Visibility check: is_visible:{:?}; xmin:{:?}; current: {:?}",
            is_visible,
            txmin,
            tx_current
        );

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
        trace!(
            "remaining: {:?}; backoff: {:?}; Pause for: {:?}",
            remaining,
            backoff,
            pause
        );
        sleep(pause).await;
    }

    Ok(())
}

impl fmt::Debug for Consumer {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Consumer")
            .field("cursor", &self.cursor)
            .finish()
    }
}
