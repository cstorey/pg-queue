#[macro_use]
extern crate log;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;

#[macro_use]
extern crate error_chain;

use std::collections::BTreeMap;
use r2d2::Pool;
use r2d2_postgres::PostgresConnectionManager;

static INSERT_ROW_SQL: &'static str = "INSERT INTO logs (body) values($1) RETURNING id";
static SEND_NOTIFY_SQL: &'static str = "SELECT pg_notify('logs', $1 :: bigint :: text)";
static FETCH_NEXT_ROW: &'static str = "SELECT id, body FROM logs WHERE id > $1 LIMIT 1";
static DISCARD_ENTRIES: &'static str = "DELETE FROM logs WHERE id <= $1";

static UPSERT_CONSUMER_OFFSET: &'static str = "INSERT INTO log_consumer_positions (name, \
                                               position) values ($1, $2) ON CONFLICT (name) DO \
                                               UPDATE SET position = EXCLUDED.position";
static FETCH_CONSUMER_POSITION: &'static str = "SELECT position FROM log_consumer_positions WHERE \
                                                name = $1";
static LIST_CONSUMERS: &'static str = "SELECT name, position FROM log_consumer_positions";
static DISCARD_CONSUMER: &'static str = "DELETE FROM log_consumer_positions WHERE name = $1";
static CREATE_TABLE_SQL: &'static str = "
        CREATE TABLE IF NOT EXISTS logs ( 
            id BIGSERIAL PRIMARY KEY,
            written_at TIMESTAMPTZ NOT NULL DEFAULT now() ,
            body bytea NOT NULL
        );
        CREATE TABLE IF NOT EXISTS log_consumer_positions ( 
            name TEXT PRIMARY KEY,
            position BIGINT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS logs_timestamp_idx ON logs (written_at);
        ";

static LISTEN: &'static str = "LISTEN logs";

error_chain!{
    foreign_links {
        postgres::error::Error, Pg;
        r2d2::GetTimeout, PoolTimeout;
    }
}

#[derive(Debug,Clone,Eq,PartialEq)]
pub struct Entry {
    pub offset: i64,
    pub data: Vec<u8>,
}

pub fn setup<C: postgres::GenericConnection>(conn: &C) -> Result<()> {
    try!(conn.batch_execute(CREATE_TABLE_SQL));
    Ok(())
}

pub struct Producer {
    pool: Pool<PostgresConnectionManager>,
}

impl Producer {
    pub fn new(pool: Pool<PostgresConnectionManager>) -> Result<Self> {
        Ok(Producer { pool: pool })
    }
    pub fn produce(&mut self, body: &[u8]) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        {
            let rows = try!(t.query(INSERT_ROW_SQL, &[&body]));
            for r in rows.iter() {
                let id: i64 = r.get(0);
                debug!("id: {}", id);
                try!(t.query(SEND_NOTIFY_SQL, &[&id]));
            }
        }
        try!(t.commit());

        debug!("Wrote: {:?}", body.len());
        Ok(())
    }
}

pub struct Consumer {
    pool: Pool<PostgresConnectionManager>,
    name: String,
    last_seen_offset: i64,
}

impl Consumer {
    pub fn new(pool: Pool<PostgresConnectionManager>, name: &str) -> Result<Self> {
        let conn = try!(pool.get());
        let t = try!(conn.transaction());
        let stmt = try!(t.prepare_cached(FETCH_CONSUMER_POSITION));
        let rows = try!(stmt.query(&[&name]));
        debug!("next rows:{:?}", rows.len());
        let position = rows.iter().next().map(|r| r.get(0)).unwrap_or(-1i64);
        Ok(Consumer {
            pool: pool,
            name: name.to_string(),
            last_seen_offset: position,
        })
    }

    pub fn poll(&mut self) -> Result<Option<Entry>> {
        let conn = try!(self.pool.get());
        self.poll_item(&conn)

    }

    fn poll_item(&mut self, conn: &postgres::Connection) -> Result<Option<Entry>> {
        let t = try!(conn.transaction());
        let next_row = try!(t.prepare_cached(FETCH_NEXT_ROW));
        let rows = try!(next_row.query(&[&self.last_seen_offset]));
        debug!("next rows:{:?}", rows.len());
        let res = if let Some(r) = rows.iter().next() {
            let id: i64 = r.get(0);
            let body: Vec<u8> = r.get(1);
            debug!("id: {}", id);
            self.last_seen_offset = id;
            Some(Entry {
                offset: id,
                data: body,
            })
        } else {
            None
        };
        trace!("returning: {:?}", res);
        Ok(res)
    }

    pub fn wait_next(&mut self) -> Result<Entry> {
        let conn = try!(self.pool.get());
        let listen = try!(conn.prepare_cached(LISTEN));
        try!(listen.execute(&[]));
        loop {
            let notifications = conn.notifications();
            while let Some(n) = notifications.iter().next() {
                debug!("Saw previous notification: {:?}", n);
            }
            if let Some(entry) = try!(self.poll_item(&conn)) {
                return Ok(entry);
            }
            debug!("Awaiting notifications");
            if let Some(n) = notifications.blocking_iter().next() {
                debug!("Saw new notification:{:?}", n);
            }
        }
    }

    pub fn commit_upto(&self, entry: &Entry) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let upsert = try!(t.prepare_cached(UPSERT_CONSUMER_OFFSET));
        try!(upsert.execute(&[&self.name, &entry.offset]));
        try!(t.commit());
        Ok(())
    }

    pub fn discard_upto(&self, limit: i64) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let discard = try!(t.prepare_cached(DISCARD_ENTRIES));
        try!(discard.execute(&[&limit]));
        try!(t.commit());
        Ok(())
    }
    pub fn consumers(&self) -> Result<BTreeMap<String, i64>> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let list = try!(t.prepare_cached(LIST_CONSUMERS));
        let rows = try!(list.query(&[]));

        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
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
