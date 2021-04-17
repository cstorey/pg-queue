use tokio_postgres::Client;
use tracing::debug;

use crate::jobs::Result;

static CREATE_TABLE_SQL: &str = include_str!("schema.sql");

pub async fn setup(conn: &Client) -> Result<()> {
    debug!("Running setup SQL");

    assert!(!CREATE_TABLE_SQL.contains("logs_epoch_offset_idx"));

    for chunk in CREATE_TABLE_SQL.split("-- Split\n") {
        conn.batch_execute(chunk.trim()).await?;
    }
    debug!("Ran setup SQL ok");
    Ok(())
}
