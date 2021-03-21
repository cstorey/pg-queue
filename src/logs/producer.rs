use tokio_postgres::{self, Client};
use tracing::trace;

use crate::logs::{Error, Result, Version};

/// This query is a little odd, as while PostgreSQL distribute the set of
/// backwards index scans over a set of UNION ALL'ed subqueries if they all
/// use an index, adding a CTE can will result in it materializing each of the
/// UNION'ed table expressions, then sorting and taking the last item. The
/// downside is that it's comparatively slow to plan.
static INSERT_ROW_SQL: &str = "\
WITH observed_epoch AS (
    SELECT epoch, tx_id, txid_current() AS current_tx_id from logs
    UNION ALL
    SELECT epoch, tx_position AS tx_id, txid_current() AS current_tx_id
    FROM log_consumer_positions
    ORDER BY epoch DESC, tx_id DESC
    LIMIT 1
),
fallback AS (
    SELECT 1 AS epoch
),
insertion_epoch AS (
    SELECT CASE
        WHEN o.current_tx_id >= o.tx_id THEN o.epoch
        ELSE o.epoch + 1
    END AS epoch
    FROM observed_epoch AS o
    UNION ALL
    SELECT epoch FROM fallback
    ORDER BY epoch desc
    LIMIT 1
)
INSERT INTO logs (epoch, key, meta, body)
SELECT e.epoch, $1, $2, $3
FROM insertion_epoch AS e
RETURNING epoch, tx_id as tx_position, id as position";
static SEND_NOTIFY_SQL: &str = "SELECT pg_notify('logs', '')";

pub struct Batch<'a> {
    transaction: tokio_postgres::Transaction<'a>,
    insert: tokio_postgres::Statement,
}

pub async fn produce(client: &mut Client, key: &[u8], body: &[u8]) -> Result<Version> {
    let batch = Batch::begin(client).await?;
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
    let batch = Batch::begin(client).await?;
    let version = batch.produce_meta(key, meta, body).await?;
    batch.commit().await?;
    Ok(version)
}

impl<'a> Batch<'a> {
    pub async fn begin(client: &mut Client) -> Result<Batch<'_>> {
        let t = client.transaction().await?;
        let insert = t.prepare(INSERT_ROW_SQL).await?;
        let batch = Batch {
            transaction: t,
            insert,
        };
        Ok(batch)
    }

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
            .query(&self.insert, &[&key, &meta, &body])
            .await?;
        let id = rows
            .iter()
            .map(|r| Version::from_row(r))
            .next()
            .ok_or(Error::NoRowsFromInsert)?;

        trace!(?id, "Produced");
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
