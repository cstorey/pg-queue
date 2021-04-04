use std::sync::Mutex;

use tokio_postgres::{self, Client, GenericClient};
use tracing::trace;

use crate::logs::{current_epoch, Error, Result, Version};

static INSERT_ROW_SQL: &str =
    "INSERT INTO logs (epoch, key_text, meta, body) values($1, $2, $3, $4) RETURNING epoch, tx_id as tx_position, id as position";
static SEND_NOTIFY_SQL: &str = "SELECT pg_notify('logs', $1)";

pub struct Batch<'a> {
    transaction: tokio_postgres::Transaction<'a>,
    insert: tokio_postgres::Statement,
    last_produced_version: Mutex<Option<Version>>,
    epoch: i64,
}

pub async fn produce<C: GenericClient>(client: &mut C, key: &str, body: &[u8]) -> Result<Version> {
    let batch = Batch::begin(client).await?;
    let version = batch.produce(key, body).await?;
    batch.commit().await?;
    Ok(version)
}

pub async fn produce_meta(
    client: &mut Client,
    key: &str,
    meta: Option<&[u8]>,
    body: &[u8],
) -> Result<Version> {
    let batch = Batch::begin(client).await?;
    let version = batch.produce_meta(key, meta, body).await?;
    batch.commit().await?;
    Ok(version)
}

impl<'a> Batch<'a> {
    pub async fn begin<C: GenericClient>(client: &mut C) -> Result<Batch<'_>> {
        let t = client.transaction().await?;
        let epoch = current_epoch(&t).await?;
        let insert = t.prepare(INSERT_ROW_SQL).await?;
        let last_produced_version = Mutex::new(None);
        let batch = Batch {
            transaction: t,
            insert,
            epoch,
            last_produced_version,
        };
        Ok(batch)
    }

    pub async fn produce(&self, key: &str, body: &[u8]) -> Result<Version> {
        self.produce_meta(key, None, body).await
    }

    pub async fn produce_meta(
        &self,
        key: &str,
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
        *self.last_produced_version.lock().expect("lock") = Some(id);
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
        let Batch {
            transaction,
            last_produced_version,
            ..
        } = self;
        let last_version = last_produced_version.lock().expect("lock").take();
        if let Some(version) = last_version {
            transaction
                .execute(SEND_NOTIFY_SQL, &[&version.to_string()])
                .await?;
        }
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
