use thiserror::Error;
use tokio_postgres::{self, Client, Transaction};
use tracing::{debug, warn};

mod consumer;
mod cursor;
mod producer;

pub use self::{consumer::*, cursor::*, producer::*};

static SAMPLE_HEAD: &str = "\
    SELECT epoch, tx_id, txid_current() as current_tx_id
    FROM logs ORDER BY epoch desc, tx_id desc
    LIMIT 1";
static CONSUMER_EPOCHS: &str =
    "SELECT epoch, tx_position as tx_id, txid_current() as current_tx_id FROM log_consumer_positions ORDER BY epoch DESC LIMIT 1";
static CREATE_TABLE_SQL: &str = include_str!("schema.sql");

#[derive(Debug, Default, Copy, Clone, Eq, PartialEq, PartialOrd, Ord)]
pub struct Version {
    pub epoch: i64,
    pub tx_id: i64,
    pub seq: i64,
}

pub(crate) type Result<T> = ::std::result::Result<T, Error>;

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

async fn current_epoch(t: &Transaction<'_>) -> Result<i64> {
    if let Some(epoch_row) = t.query_opt(SAMPLE_HEAD, &[]).await? {
        let head_epoch: i64 = epoch_row.get("epoch");
        let head_tx_id: i64 = epoch_row.get("tx_id");
        let current_tx_id: i64 = epoch_row.get("current_tx_id");
        if current_tx_id >= head_tx_id {
            return Ok(head_epoch);
        } else {
            warn!(
                ?head_epoch,
                ?current_tx_id,
                ?head_tx_id,
                "Running behind in epoch, incrementing",
            );
            return Ok(head_epoch + 1);
        }
    };

    if let Some(row) = t.query_opt(CONSUMER_EPOCHS, &[]).await? {
        let head_epoch = row.get("epoch");
        let head_tx_id: i64 = row.get("tx_id");
        let current_tx_id: i64 = row.get("current_tx_id");
        if current_tx_id >= head_tx_id {
            return Ok(head_epoch);
        } else {
            warn!(
                ?head_epoch,
                ?current_tx_id,
                ?head_tx_id,
                "Running behind in epoch, incrementing",
            );
            return Ok(head_epoch + 1);
        }
    };

    Ok(1)
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
