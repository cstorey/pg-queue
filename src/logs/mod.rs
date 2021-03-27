use thiserror::Error;
use tokio_postgres::{self, Client, GenericClient};
use tracing::debug;

mod consumer;
mod cursor;
mod producer;

pub use self::{consumer::*, cursor::*, producer::*};

static CURRENT_EPOCH: &str = "\
WITH observed AS (
    SELECT epoch, tx_id, txid_current() AS current_tx_id from logs
    UNION ALL
    SELECT epoch, tx_position AS tx_id, txid_current() AS current_tx_id
    FROM log_consumer_positions
    ORDER BY epoch DESC, tx_id DESC
    LIMIT 1
)
SELECT CASE
    WHEN observed.current_tx_id >= observed.tx_id THEN observed.epoch
    ELSE observed.epoch + 1
END as epoch
FROM observed
";

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

async fn current_epoch<C: GenericClient>(t: &C) -> Result<i64> {
    if let Some(row) = t.query_opt(CURRENT_EPOCH, &[]).await? {
        return Ok(row.get("epoch"));
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
