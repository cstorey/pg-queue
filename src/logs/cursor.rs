use std::{collections::VecDeque, fmt};

use tokio_postgres::Client;
use tracing::trace;

use crate::logs::{current_epoch, Entry, Result, Version};

static FETCH_CONSUMER_POSITION: &str =
    "SELECT epoch, tx_position, position FROM log_consumer_positions WHERE \
     name = $1";
static FETCH_NEXT_ROW: &str = "\
     SELECT epoch, tx_id as tx_position, id as position, key, meta, body, written_at
     FROM logs
     WHERE (epoch, tx_id, id) > ($1, $2, $3)
     AND (epoch, tx_id) < ($4, txid_snapshot_xmin(txid_current_snapshot()))
     ORDER BY epoch asc, tx_id asc, id asc
     LIMIT $5";
static UPSERT_CONSUMER_OFFSET: &str = "\
     INSERT INTO log_consumer_positions (name, epoch, tx_position, position) \
     values ($1, $2, $3, $4) \
     ON CONFLICT (name) DO \
     UPDATE SET tx_position = EXCLUDED.tx_position, position = EXCLUDED.position";
static DISCARD_CONSUMER: &str = "DELETE FROM log_consumer_positions WHERE name = $1";

const LIMIT_BUFFER: i64 = 1024;

pub struct Cursor {
    name: String,
    last_seen_offset: Version,
    buf: VecDeque<Entry>,
}

impl Cursor {
    pub async fn load(client: &mut Client, name: &str) -> Result<Self> {
        let position = Self::fetch_consumer_pos(client, name).await?;
        let consumer = Cursor {
            name: name.to_string(),
            last_seen_offset: position,
            buf: VecDeque::new(),
        };

        Ok(consumer)
    }

    pub(super) async fn poll(&mut self, client: &mut Client) -> Result<Option<Entry>> {
        if let Some(entry) = self.buf.pop_front() {
            trace!(version=?entry.version, "returning (from buffer)");
            self.last_seen_offset = entry.version;
            return Ok(Some(entry));
        }

        let t = client.transaction().await?;
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
        trace!(rows=?rows.len(), "next rows");
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
        t.commit().await?;

        if let Some(res) = self.buf.pop_front() {
            self.last_seen_offset = res.version;
            trace!(version=?res.version, "returning (from db)");
            Ok(Some(res))
        } else {
            trace!("nothing yet");
            Ok(None)
        }
    }

    async fn fetch_consumer_pos(client: &mut Client, name: &str) -> Result<Version> {
        let t = client.transaction().await?;
        let rows = t.query(FETCH_CONSUMER_POSITION, &[&name]).await?;
        t.commit().await?;

        trace!(rows=?rows.len(), "next rows");
        let position = rows
            .into_iter()
            .next()
            .map(|r| Version::from_row(&r))
            .unwrap_or_else(Version::default);

        Ok(position)
    }

    pub async fn commit_upto(&mut self, client: &mut Client, entry: &Entry) -> Result<()> {
        let t = client.transaction().await?;
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
            name = ?self.name,
            version = ?entry.version,
            "Persisted position for consumer",
        );
        Ok(())
    }

    pub async fn clear_offset(&mut self, client: &mut Client) -> Result<()> {
        let t = client.transaction().await?;
        t.execute(DISCARD_CONSUMER, &[&self.name]).await?;
        t.commit().await?;
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
