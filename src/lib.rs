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

pub fn setup(conn: &postgres::Connection) -> Result<()> {
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
        let rows = try!(t.query(FETCH_CONSUMER_POSITION, &[&name]));
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
        let t = try!(conn.transaction());
        let rows = try!(t.query(FETCH_NEXT_ROW, &[&self.last_seen_offset]));
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

    pub fn commit_upto(&self, entry: &Entry) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        try!(t.execute(UPSERT_CONSUMER_OFFSET, &[&self.name, &entry.offset]));
        try!(t.commit());
        Ok(())
    }

    pub fn discard_upto(&self, limit: i64) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        try!(t.execute(DISCARD_ENTRIES, &[&limit]));
        try!(t.commit());
        Ok(())
    }
    pub fn consumers(&self) -> Result<BTreeMap<String, i64>> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());
        let rows = try!(t.query(LIST_CONSUMERS, &[]));

        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }
    pub fn clear_offset(&mut self) -> Result<()> {
        let conn = try!(self.pool.get());
        let t = try!(conn.transaction());

        try!(t.execute(DISCARD_CONSUMER, &[&self.name]));
        try!(t.commit());
        Ok(())
    }
}
