use std::{collections::VecDeque, fmt};

use tokio_postgres::{Client, GenericClient};
use tracing::{debug, trace};

use crate::logs::{Entry, Result, Version};

static FETCH_CONSUMER_POSITION: &str =
    "SELECT epoch, tx_position, position FROM log_consumer_positions WHERE \
     name = $1";
/// For previous epochs, we want to see entries with any transaction id, but
/// for the current one, we only care about entries with a tranasction id
// before the current one.
/// The `head` table expression allows us to find the current epoch.
/// That means we can query for entries where:
/// l.epoch < head.epoch => l.tx_id is in [1, +Inf)
/// l.epoch = head.epoch => l.tx_id is in [1, head.tx_id)
static FETCH_NEXT_ROW: &str = "\
    WITH head as (
        SELECT l.epoch
        FROM logs as l
        ORDER BY l.epoch desc
        LIMIT 1
    )
     SELECT l.epoch, l.tx_id as tx_position, l.id as position, l.key, l.meta, l.body, l.written_at
     FROM logs as l, head as h
     WHERE (l.epoch, l.tx_id, l.id) > ($1, $2, $3)
     AND (l.epoch, l.tx_id) < (h.epoch, txid_snapshot_xmin(txid_current_snapshot()))
     ORDER BY l.epoch asc, l.tx_id asc, l.id asc
     LIMIT $4";
static UPSERT_CONSUMER_OFFSET: &str = "\
     INSERT INTO log_consumer_positions (name, epoch, tx_position, position) \
     values ($1, $2, $3, $4) \
     ON CONFLICT (name) DO \
     UPDATE SET epoch = EXCLUDED.epoch, tx_position = EXCLUDED.tx_position, position = EXCLUDED.position";
static DISCARD_CONSUMER: &str = "DELETE FROM log_consumer_positions WHERE name = $1";

const LIMIT_BUFFER: i64 = 1024;

pub struct Cursor {
    name: String,
    last_seen_offset: Version,
    buf: VecDeque<Entry>,
}

impl Cursor {
    pub async fn load<C: GenericClient>(client: &C, name: &str) -> Result<Self> {
        let position = Self::fetch_consumer_pos(client, name).await?;
        debug!(?name, ?position, "Loaded offset");
        let consumer = Cursor {
            name: name.to_string(),
            last_seen_offset: position,
            buf: VecDeque::new(),
        };

        Ok(consumer)
    }

    pub async fn poll<C: GenericClient>(&mut self, client: &mut C) -> Result<Option<Entry>> {
        if let Some(entry) = self.buf.pop_front() {
            trace!(version=?entry.version, "returning (from buffer)");
            self.last_seen_offset = entry.version;
            return Ok(Some(entry));
        }

        let rows = client
            .query(
                FETCH_NEXT_ROW,
                &[
                    &self.last_seen_offset.epoch,
                    &self.last_seen_offset.tx_id,
                    &self.last_seen_offset.seq,
                    &LIMIT_BUFFER,
                ],
            )
            .await?;
        trace!(rows=?rows.len(), after=%self.last_seen_offset, "next rows");
        for r in rows.into_iter() {
            let version = Version::from_row(&r);
            let key: Vec<u8> = r.get("key");
            let meta: Option<Vec<u8>> = r.get("meta");
            let data: Vec<u8> = r.get("body");
            let written_at = r.get("written_at");
            trace!(?version, "buffering");
            self.buf.push_back(Entry {
                version,
                written_at,
                meta,
                key,
                data,
            })
        }

        if let Some(res) = self.buf.pop_front() {
            self.last_seen_offset = res.version;
            trace!(version=?res.version, "returning (from db)");
            Ok(Some(res))
        } else {
            trace!("nothing yet");
            Ok(None)
        }
    }

    async fn fetch_consumer_pos<C: GenericClient>(client: &C, name: &str) -> Result<Version> {
        let rows = client.query(FETCH_CONSUMER_POSITION, &[&name]).await?;

        trace!(rows=?rows.len(), "next rows");
        let position = rows
            .into_iter()
            .next()
            .map(|r| Version::from_row(&r))
            .unwrap_or_else(Version::default);

        Ok(position)
    }

    pub async fn commit_upto<C: GenericClient>(
        &mut self,
        client: &mut C,
        entry: &Entry,
    ) -> Result<()> {
        client
            .execute(
                UPSERT_CONSUMER_OFFSET,
                &[
                    &self.name,
                    &entry.version.epoch,
                    &entry.version.tx_id,
                    &entry.version.seq,
                ],
            )
            .await?;
        trace!(
            name = ?self.name,
            version = ?entry.version,
            "Persisted position for consumer",
        );
        Ok(())
    }

    pub async fn clear_offset(&mut self, client: &mut Client) -> Result<()> {
        client.execute(DISCARD_CONSUMER, &[&self.name]).await?;
        Ok(())
    }
}

impl fmt::Debug for Cursor {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Consumer")
            .field("name", &self.name)
            .field("last_seen_offset", &self.last_seen_offset)
            .finish()
    }
}
