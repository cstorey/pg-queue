use std::{fmt, sync::Arc, unimplemented};

#[macro_use]
extern crate log;

use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use std::collections::{BTreeMap, VecDeque};
use std::time;
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Notify,
    time::{delay_for, Duration},
};
use tokio_postgres::{
    self, tls::MakeTlsConnect, AsyncMessage, Client, Config, Connection, Socket, Transaction,
};

const LIMIT_BUFFER: i64 = 1024;

// In our SQL, the "current epoch" is defined as either:
// 1 if is_empty(log)
// log_head.epoch if log_head.epoch.tx_id < txid_current()
// log_head.epoch otherwise

static INSERT_ROW_SQL: &str =
    "INSERT INTO logs (epoch, key, meta, body) values($1, $2, $3, $4) RETURNING epoch, tx_id as tx_position, id as position";
static SAMPLE_HEAD: &str = "\
    SELECT epoch, tx_id, txid_current() as current_tx_id
    FROM logs ORDER BY (epoch, tx_id) desc
    LIMIT 1";
static CONSUMER_EPOCHS: &str =
    "SELECT epoch, tx_position as tx_id, txid_current() as current_tx_id FROM log_consumer_positions ORDER BY epoch DESC LIMIT 1";
static SEND_NOTIFY_SQL: &str = "SELECT pg_notify('logs', '')";

static FETCH_NEXT_ROW: &str = "\
    SELECT epoch, tx_id as tx_position, id as position, key, meta, body, written_at
    FROM logs
    WHERE (epoch, tx_id, id) > ($1, $2, $3)
    AND (epoch, tx_id) < ($4, txid_snapshot_xmin(txid_current_snapshot()))
    ORDER BY (epoch, tx_id, id)
    LIMIT $5";
static IS_VISIBLE: &str = "\
    WITH snapshot as (
        select txid_snapshot_xmin(txid_current_snapshot()) as xmin,
        txid_current() as current
    )
    SELECT $1 < xmin as lt_xmin, xmin, current FROM snapshot";
static DISCARD_ENTRIES: &str = "DELETE FROM logs WHERE (tx_id, id) <= ($1, $2)";

static UPSERT_CONSUMER_OFFSET: &str = "\
     INSERT INTO log_consumer_positions (name, epoch, tx_position, position) \
     values ($1, $2, $3, $4) \
     ON CONFLICT (name) DO \
     UPDATE SET tx_position = EXCLUDED.tx_position, position = EXCLUDED.position";
static FETCH_CONSUMER_POSITION: &str =
    "SELECT epoch, tx_position, position FROM log_consumer_positions WHERE \
     name = $1";
static LIST_CONSUMERS: &str =
    "SELECT name, epoch, tx_position, position FROM log_consumer_positions";
static DISCARD_CONSUMER: &str = "DELETE FROM log_consumer_positions WHERE name = $1";
static CREATE_TABLE_SQL: &str = include_str!("schema.sql");

static LISTEN: &str = "LISTEN logs";

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Version {
    pub epoch: i64,
    pub tx_id: i64,
    pub seq: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
    pub version: Version,
    pub written_at: DateTime<Utc>,
    pub key: Vec<u8>,
    pub meta: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("insert returned no rows?")]
    NoRowsFromInsert,
    #[error("txn version comparison returned no rows?")]
    NoRowsFromVisibilityCheck,
    #[error("Item was not visible before deadline: {0:?}")]
    VisibilityTimeout(Version),
    #[error("Connection exited unexpectedly")]
    ConnectionExited,
}

pub async fn setup(conn: &Client) -> Result<()> {
    debug!("Running setup SQL");
    for chunk in CREATE_TABLE_SQL.split("\n\n") {
        conn.batch_execute(chunk.trim()).await?;
    }
    debug!("Ran setup SQL ok");
    Ok(())
}

pub struct Batch<'a> {
    transaction: tokio_postgres::Transaction<'a>,
    insert: tokio_postgres::Statement,
    epoch: i64,
}

pub async fn produce(client: &mut Client, key: &[u8], body: &[u8]) -> Result<Version> {
    let batch = batch(client).await?;
    let version = batch.produce(key, body).await?;
    batch.commit().await?;
    Ok(version)
}

pub async fn produce_meta(
    client: &mut Client,
    key: &[u8],
    meta: Option<&[u8]>,
    body: &[u8],
) -> Result<Version> {
    let batch = batch(client).await?;
    let version = batch.produce_meta(key, meta, body).await?;
    batch.commit().await?;
    Ok(version)
}

pub async fn batch(client: &mut Client) -> Result<Batch<'_>> {
    let t = client.transaction().await?;
    let epoch = current_epoch(&t).await?;
    let insert = t.prepare(INSERT_ROW_SQL).await?;
    Ok(Batch {
        transaction: t,
        insert,
        epoch,
    })
}

async fn current_epoch(t: &Transaction<'_>) -> Result<i64> {
    if let Some(epoch_row) = t.query_opt(SAMPLE_HEAD, &[]).await? {
        let head_epoch: i64 = epoch_row.get("epoch");
        let head_tx_id: i64 = epoch_row.get("tx_id");
        let current_tx_id: i64 = epoch_row.get("current_tx_id");
        if current_tx_id >= head_tx_id {
            return Ok(head_epoch);
        } else {
            warn!(
                "Running behind in epoch {:?}? {:?} > {:?}",
                head_epoch, current_tx_id, head_tx_id
            );
            return Ok(head_epoch + 1);
        }
    };

    if let Some(row) = t.query_opt(CONSUMER_EPOCHS, &[]).await? {
        let epoch = row.get("epoch");
        let tx_id: i64 = row.get("tx_id");
        let current_tx_id: i64 = row.get("current_tx_id");
        if current_tx_id >= tx_id {
            return Ok(epoch);
        } else {
            warn!(
                "Running behind in epoch {:?}? {:?} > {:?}",
                epoch, current_tx_id, tx_id
            );
            return Ok(epoch + 1);
        }
    };

    Ok(1)
}

impl<'a> Batch<'a> {
    pub async fn produce(&self, key: &[u8], body: &[u8]) -> Result<Version> {
        self.produce_meta(key, None, body).await
    }
    pub async fn produce_meta(
        &self,
        key: &[u8],
        meta: Option<&[u8]>,
        body: &[u8],
    ) -> Result<Version> {
        let rows = self
            .transaction
            .query(&self.insert, &[&self.epoch, &key, &meta, &body])
            .await?;
        let id = rows
            .iter()
            .map(|r| Version::from_row(r))
            .next()
            .ok_or(Error::NoRowsFromInsert)?;

        trace!("id: {:?}", id);
        Ok(id)
    }

    pub async fn commit(self) -> Result<()> {
        // It looks like postgres 9x will:
        // * Take a database scoped exclusive lock when appending notifications to the queue on commit
        // * Continue holding that lock until the transaction overall commits.
        // This means that WAL flushes get serialized, we can't take advantage of group commit,
        // and write throughput tanks.
        // However, because it's _really_ awkward_ to get a reference to both
        // a client and it's transaction, we do this inside the transaction
        // and hope for the best.
        let Batch { transaction, .. } = self;
        transaction.query(SEND_NOTIFY_SQL, &[]).await?;
        trace!("Sent notify");
        transaction.commit().await?;
        trace!("Committed");

        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        let Batch { transaction, .. } = self;
        transaction.rollback().await?;
        trace!("Rolled back");
        Ok(())
    }
}

pub struct Consumer {
    client: Client,
    name: String,
    last_seen_offset: Version,
    buf: VecDeque<Entry>,
    notify: Arc<Notify>,
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
        let position = Self::fetch_consumer_pos(&mut client, name).await?;

        trace!("Loaded position for consumer {:?}: {:?}", name, position);

        let consumer = Consumer {
            client,
            notify,
            name: name.to_string(),
            last_seen_offset: position,
            buf: VecDeque::new(),
        };

        Ok(consumer)
    }

    async fn fetch_consumer_pos(client: &mut Client, name: &str) -> Result<Version> {
        let t = client.transaction().await?;
        let rows = t.query(FETCH_CONSUMER_POSITION, &[&name]).await?;
        t.commit().await?;

        trace!("next rows:{:?}", rows.len());
        let position = rows
            .into_iter()
            .next()
            .map(|r| Version::from_row(&r))
            .unwrap_or_else(Version::default);

        Ok(position)
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
                    notify.notify();
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
        if let Some(entry) = self.buf.pop_front() {
            trace!("returning (from buffer): {:?}", entry);
            self.last_seen_offset = entry.version;
            return Ok(Some(entry));
        }

        let t = self.client.transaction().await?;
        let epoch = current_epoch(&t).await?;

        let rows = t
            .query(
                FETCH_NEXT_ROW,
                &[
                    &self.last_seen_offset.epoch,
                    &self.last_seen_offset.tx_id,
                    &self.last_seen_offset.seq,
                    &epoch,
                    &LIMIT_BUFFER,
                ],
            )
            .await?;
        trace!("next rows:{:?}", rows.len());
        for r in rows.into_iter() {
            let version = Version::from_row(&r);
            let key: Vec<u8> = r.get("key");
            let meta: Option<Vec<u8>> = r.get("meta");
            let data: Vec<u8> = r.get("body");
            let written_at = r.get("written_at");
            trace!("buffering id: {:?}", version);
            self.buf.push_back(Entry {
                version,
                written_at,
                meta,
                key,
                data,
            })
        }
        t.commit().await?;

        if let Some(res) = self.buf.pop_front() {
            self.last_seen_offset = res.version;
            trace!("returning (from db): {:?}", res);
            Ok(Some(res))
        } else {
            trace!("nothing yet");
            Ok(None)
        }
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
        let deadline = time::Instant::now() + timeout;
        self.client.execute(LISTEN, &[]).await?;
        for backoff in 0..64 {
            trace!("Checking for visibility of: {:?}", version,);
            let (is_visible, txmin, tx_current) = self
                .client
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
            delay_for(pause).await;
        }

        Ok(())
    }

    pub async fn commit_upto(&mut self, entry: &Entry) -> Result<()> {
        let t = self.client.transaction().await?;
        t.execute(
            UPSERT_CONSUMER_OFFSET,
            &[
                &self.name,
                &entry.version.epoch,
                &entry.version.tx_id,
                &entry.version.seq,
            ],
        )
        .await?;
        t.commit().await?;
        trace!(
            "Persisted position for consumer {:?}: {:?}",
            self.name,
            entry.version
        );
        Ok(())
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
        let t = self.client.transaction().await?;
        t.execute(DISCARD_CONSUMER, &[&self.name]).await?;
        t.commit().await?;
        Ok(())
    }
}

impl fmt::Debug for Consumer {
    fn fmt(&self, _fmt: &mut fmt::Formatter) -> fmt::Result {
        unimplemented!()
    }
}

impl Version {
    fn from_row(row: &tokio_postgres::Row) -> Self {
        Version {
            epoch: row.get("epoch"),
            tx_id: row.get("tx_position"),
            seq: row.get("position"),
        }
    }
}
