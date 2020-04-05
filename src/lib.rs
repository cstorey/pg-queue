use std::{fmt, sync::Arc};

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
use tokio_postgres::{self, AsyncMessage, Client, Connection};

const LIMIT_BUFFER: i64 = 1024;

static INSERT_ROW_SQL: &'static str =
    "INSERT INTO logs (key, body) values($1, $2) RETURNING tx_id as tx_position, id as position";
static SEND_NOTIFY_SQL: &'static str = "SELECT pg_notify('logs', $1 :: text)";
static FETCH_NEXT_ROW: &'static str = "\
    SELECT tx_id as tx_position, id as position, key, body, written_at
    FROM logs
    WHERE (tx_id, id) > ($1, $2)
    AND tx_id < txid_snapshot_xmin(txid_current_snapshot())
    LIMIT $3";
static IS_VISIBLE: &'static str = "\
    WITH snapshot as (
        select txid_snapshot_xmin(txid_current_snapshot()) as xmin,
        txid_current() as current
    )
    SELECT $1 < xmin as lt_xmin, xmin, current FROM snapshot";
static DISCARD_ENTRIES: &'static str = "DELETE FROM logs WHERE (tx_id, id) <= ($1, $2)";

static UPSERT_CONSUMER_OFFSET: &'static str = "INSERT INTO log_consumer_positions (name, \
     tx_position, position) values ($1, $2, $3) ON CONFLICT (name) DO \
     UPDATE SET tx_position = EXCLUDED.tx_position, position = EXCLUDED.position";
static FETCH_CONSUMER_POSITION: &'static str =
    "SELECT tx_position, position FROM log_consumer_positions WHERE \
     name = $1";
static LIST_CONSUMERS: &'static str =
    "SELECT name, tx_position, position FROM log_consumer_positions";
static DISCARD_CONSUMER: &'static str = "DELETE FROM log_consumer_positions WHERE name = $1";
static CREATE_TABLE_SQL: &'static str = include_str!("schema.sql");

static LISTEN: &'static str = "LISTEN logs";

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Version {
    pub tx_id: i64,
    pub seq: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
    pub version: Version,
    pub written_at: DateTime<Utc>,
    pub key: Vec<u8>,
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
    conn.batch_execute(CREATE_TABLE_SQL).await?;
    debug!("Ran setup SQL ok");
    Ok(())
}

pub struct Batch<'a> {
    transaction: tokio_postgres::Transaction<'a>,
    last_version: Option<Version>,
}

pub async fn produce(client: &mut Client, key: &[u8], body: &[u8]) -> Result<Version> {
    let mut batch = batch(client).await?;
    let version = batch.produce(key, body).await?;
    batch.commit().await?;
    Ok(version)
}

pub async fn batch(client: &mut Client) -> Result<Batch<'_>> {
    let t = client.transaction().await?;
    Ok(Batch {
        transaction: t,
        last_version: None,
    })
}

impl<'a> Batch<'a> {
    pub async fn produce(&mut self, key: &[u8], body: &[u8]) -> Result<Version> {
        let rows = self
            .transaction
            .query(INSERT_ROW_SQL, &[&key, &body])
            .await?;
        let id = rows
            .iter()
            .map(|r| Version::from_row(r))
            .next()
            .ok_or_else(|| Error::NoRowsFromInsert)?;

        debug!("id: {:?}", id);
        self.last_version = Some(id);
        debug!("Wrote: {:?}", body.len());
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
        let Batch {
            transaction,
            last_version,
        } = self;
        if let Some(id) = last_version {
            // We never actually parse the version, it's just a nice to have.
            let vers = format!("{}", id.seq);
            transaction.query(SEND_NOTIFY_SQL, &[&vers]).await?;
            debug!("Sent notify for id: {:?}", id);
        }
        transaction.commit().await?;
        debug!("Committed");

        Ok(())
    }

    pub async fn rollback(self) -> Result<()> {
        let Batch { transaction, .. } = self;
        transaction.rollback().await?;
        debug!("Rolled back");
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
    // We will also need to spawn a process that:
    // a: spawns a connection into a process that will siphon messages into a queue

    pub async fn new<
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
        T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    >(
        mut conn: Connection<S, T>,
        mut client: Client,
        name: &str,
    ) -> Result<Self> {
        let rows_f = async {
            let t = client.transaction().await?;
            let rows = t.query(FETCH_CONSUMER_POSITION, &[&name]).await?;
            t.commit().await?;
            Ok::<_, tokio_postgres::Error>(rows)
        };

        let rows = tokio::select! {
            res = (&mut conn) => {
                let () = res?;
                return Err(Error::ConnectionExited.into());
            },
            rows = rows_f => { rows? },
        };
        debug!("next rows:{:?}", rows.len());
        let position = rows
            .into_iter()
            .next()
            .map(|r| Version::from_row(&r))
            .unwrap_or_else(Version::default);

        trace!("Loaded position for consumer {:?}: {:?}", name, position);

        let notify = Arc::new(Notify::new());
        tokio::spawn(Self::run_connection(conn, notify.clone()));

        let consumer = Consumer {
            client,
            notify,
            name: name.to_string(),
            last_seen_offset: position,
            buf: VecDeque::new(),
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
                    debug!("Received notification: {:?}", n);
                    notify.notify();
                }
                _ => debug!("Other message received"),
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
        let rows = t
            .query(
                FETCH_NEXT_ROW,
                &[
                    &self.last_seen_offset.tx_id,
                    &self.last_seen_offset.seq,
                    &LIMIT_BUFFER,
                ],
            )
            .await?;
        debug!("next rows:{:?}", rows.len());
        for r in rows.into_iter() {
            let version = Version::from_row(&r);
            let key: Vec<u8> = r.get("key");
            let data: Vec<u8> = r.get("body");
            let written_at = r.get("written_at");
            debug!("buffering id: {:?}", version);
            self.buf.push_back(Entry {
                version,
                written_at,
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
            debug!("Awaiting notifications");
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
                .ok_or_else(|| Error::NoRowsFromVisibilityCheck)?;
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
            let sleep = std::cmp::min(remaining, backoff);
            trace!(
                "remaining: {:?}; backoff: {:?}; Pause for: {:?}",
                remaining,
                sleep,
                sleep
            );
            delay_for(sleep).await;
        }

        Ok(())
    }

    pub async fn commit_upto(&mut self, entry: &Entry) -> Result<()> {
        let t = self.client.transaction().await?;
        t.execute(
            UPSERT_CONSUMER_OFFSET,
            &[&self.name, &entry.version.tx_id, &entry.version.seq],
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

    pub async fn consumers(&mut self) -> Result<BTreeMap<String, Version>> {
        let t = self.client.transaction().await?;
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
            tx_id: row.get("tx_position"),
            seq: row.get("position"),
        }
    }
}
