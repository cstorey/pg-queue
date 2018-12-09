#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
extern crate fallible_iterator;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;

use failure::Error;
use fallible_iterator::FallibleIterator;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;
use std::collections::{BTreeMap, VecDeque};
use std::time;

const LIMIT_BUFFER: i64 = 1024;

static INSERT_ROW_SQL: &'static str = "INSERT INTO logs (body) values($1) RETURNING tx_id, id";
static SEND_NOTIFY_SQL: &'static str = "SELECT pg_notify('logs', $1 :: text)";
static FETCH_NEXT_ROW: &'static str = "\
    SELECT tx_id, id, body
    FROM logs
    WHERE (tx_id, id) > ($1, $2)
    AND tx_id < txid_snapshot_xmin(txid_current_snapshot())
    LIMIT $3";
static IS_VISIBLE: &'static str = "\
    WITH snapshot as (
        select txid_snapshot_xmin(txid_current_snapshot()) as xmin,
        txid_current() as current
    )
    SELECT $1 < xmin, xmin, current FROM snapshot";
static DISCARD_ENTRIES: &'static str = "DELETE FROM logs WHERE (tx_id, id) <= ($1, $2)";

static UPSERT_CONSUMER_OFFSET: &'static str =
    "INSERT INTO log_consumer_positions (name, \
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

type Result<T> = ::std::result::Result<T, Error>;
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Version {
    tx_id: i64,
    seq: i64,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
    pub version: Version,
    pub data: Vec<u8>,
}

pub fn setup<C: postgres::GenericConnection>(conn: &C) -> Result<()> {
    try!(conn.batch_execute(CREATE_TABLE_SQL));
    Ok(())
}

#[derive(Debug)]
pub struct Producer {
    conn: r2d2::PooledConnection<PostgresConnectionManager>,
}

pub struct Batch<'a> {
    transaction: postgres::transaction::Transaction<'a>,
    conn: &'a postgres::Connection,
    last_version: Option<Version>,
}

impl Producer {
    pub fn new(pool: Pool<PostgresConnectionManager>) -> Result<Self> {
        let conn = try!(pool.get());
        Ok(Producer { conn: conn })
    }

    pub fn produce(&mut self, body: &[u8]) -> Result<Version> {
        let mut batch = self.batch()?;
        let version = batch.produce(body)?;
        batch.commit()?;
        Ok(version)
    }

    pub fn batch(&mut self) -> Result<Batch> {
        let t = try!(self.conn.transaction());
        Ok(Batch {
            conn: &self.conn,
            transaction: t,
            last_version: None,
        })
    }
}

impl<'a> Batch<'a> {
    pub fn produce(&mut self, body: &[u8]) -> Result<Version> {
        let rows = try!(self.transaction.query(INSERT_ROW_SQL, &[&body]));
        let id = rows
            .iter()
            .map(|r| Version {
                tx_id: r.get::<_, i64>(0),
                seq: r.get::<_, i64>(1),
            }).next()
            .ok_or_else(|| failure::err_msg("insert returned no rows?"))?;

        debug!("id: {:?}", id);
        self.last_version = Some(id);
        debug!("Wrote: {:?}", body.len());
        Ok(id)
    }

    pub fn commit(self) -> Result<()> {
        let Batch {
            transaction,
            conn,
            last_version,
        } = self;
        try!(transaction.commit());
        debug!("Committed");
        // It looks like postgres will:
        // * Take a database scoped exclusive lock when appending notifications to the queue on commit
        // * Continue holding that lock until the transaction overall commits.
        // This means that WAL flushes get serialized, we can't take advantage of group commit,
        // and write throughput tanks.
        if let Some(id) = last_version {
            // We never actually parse the version, it's just a nice to have.
            let vers = format!("{}", id.seq);
            try!(conn.query(SEND_NOTIFY_SQL, &[&vers]));
            debug!("Sent notify for id: {:?}", id);
        }
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        let Batch { transaction, .. } = self;
        transaction.set_rollback();
        try!(transaction.finish());
        debug!("Rolled back");
        Ok(())
    }
}

#[derive(Debug)]
pub struct Consumer {
    pool: Pool<PostgresConnectionManager>,
    name: String,
    last_seen_offset: Version,
    buf: VecDeque<Entry>,
}

impl Consumer {
    pub fn new(pool: Pool<PostgresConnectionManager>, name: &str) -> Result<Self> {
        let conn = try!(pool.get());
        let t = try!(conn.transaction());
        let stmt = try!(t.prepare_cached(FETCH_CONSUMER_POSITION));
        let rows = try!(stmt.query(&[&name]));
        debug!("next rows:{:?}", rows.len());
        let position = rows
            .iter()
            .next()
            .map(|r| Version {
                tx_id: r.get(0),
                seq: r.get(1),
            }).unwrap_or(Version {
                tx_id: 0,
                seq: -1i64,
            });
        trace!("Loaded position for consumer {:?}: {:?}", name, position);
        Ok(Consumer {
            pool: pool,
            name: name.to_string(),
            last_seen_offset: position,
            buf: VecDeque::new(),
        })
    }

    pub fn poll(&mut self) -> Result<Option<Entry>> {
        let conn = try!(self.pool.get());
        self.poll_item(&conn)
    }

    fn poll_item(&mut self, conn: &postgres::Connection) -> Result<Option<Entry>> {
        if let Some(entry) = self.buf.pop_front() {
            trace!("returning (from buffer): {:?}", entry);
            self.last_seen_offset = entry.version;
            return Ok(Some(entry));
        }

        let t = try!(conn.transaction());
        let next_row = try!(t.prepare_cached(FETCH_NEXT_ROW));
        let rows = try!(next_row.query(&[
            &self.last_seen_offset.tx_id,
            &self.last_seen_offset.seq,
            &LIMIT_BUFFER
        ]));
        debug!("next rows:{:?}", rows.len());
        for r in rows.iter() {
            let id = Version {
                tx_id: r.get(0),
                seq: r.get(1),
            };
            let body: Vec<u8> = r.get(2);
            debug!("buffering id: {:?}", id);
            self.buf.push_back(Entry {
                version: id,
                data: body,
            })
        }
        t.commit()?;

        if let Some(res) = self.buf.pop_front() {
            self.last_seen_offset = res.version;
            trace!("returning (from db): {:?}", res);
            Ok(Some(res))
        } else {
            trace!("nothing yet");
            Ok(None)
        }
    }

    pub fn wait_next(&mut self) -> Result<Entry> {
        let conn = try!(self.pool.get());
        let listen = try!(conn.prepare_cached(LISTEN));
        try!(listen.execute(&[]));
        loop {
            let notifications = conn.notifications();
            while let Some(n) = notifications.iter().next()? {
                debug!("Saw previous notification: {:?}", n);
            }
            if let Some(entry) = try!(self.poll_item(&conn)) {
                return Ok(entry);
            }
            debug!("Awaiting notifications");
            if let Some(n) = notifications.blocking_iter().next()? {
                debug!("Saw new notification:{:?}", n);
            }
        }
    }

    pub fn wait_until_visible(&self, version: Version, timeout: time::Duration) -> Result<()> {
        let deadline = time::Instant::now() + timeout;
        let conn = self.pool.get()?;
        let listen = conn.prepare_cached(LISTEN)?;
        listen.execute(&[])?;
        for backoff in 0..64 {
            trace!("Checking for visibility of: {:?}", version,);
            let (is_visible, txmin, tx_current) = conn
                .query(IS_VISIBLE, &[&version.tx_id])?
                .into_iter()
                .next()
                .map(|r| (r.get::<_, bool>(0), r.get::<_, i64>(1), r.get::<_, i64>(2)))
                .ok_or_else(|| failure::err_msg("txn version comparison returned no rows?"))?;
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
                bail!("Item was not visible before deadline: {:?}", version);
            }

            let remaining = deadline - now;
            let backoff = time::Duration::from_millis((2u64).pow(backoff));
            let sleep = std::cmp::min(remaining, backoff);
            trace!(
                "remaining: {:?}; backoff: {:?}; Pause for: {:?}",
                remaining,
                sleep,
                sleep
            );
            std::thread::sleep(sleep);
        }

        Ok(())
    }

    pub fn commit_upto(&self, entry: &Entry) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let upsert = try!(t.prepare_cached(UPSERT_CONSUMER_OFFSET));
        try!(upsert.execute(&[&self.name, &entry.version.tx_id, &entry.version.seq]));
        try!(t.commit());
        trace!(
            "Persisted position for consumer {:?}: {:?}",
            self.name,
            entry.version
        );
        Ok(())
    }

    pub fn discard_upto(&self, limit: Version) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let discard = try!(t.prepare_cached(DISCARD_ENTRIES));
        try!(discard.execute(&[&limit.tx_id, &limit.seq]));
        try!(t.commit());
        Ok(())
    }
    pub fn consumers(&self) -> Result<BTreeMap<String, Version>> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let list = try!(t.prepare_cached(LIST_CONSUMERS));
        let rows = try!(list.query(&[]));

        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get(0),
                    Version {
                        tx_id: r.get(1),
                        seq: r.get(2),
                    },
                )
            }).collect())
    }
    pub fn clear_offset(&mut self) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let discard = try!(t.prepare_cached(DISCARD_CONSUMER));

        try!(discard.execute(&[&self.name]));
        try!(t.commit());
        Ok(())
    }
}
