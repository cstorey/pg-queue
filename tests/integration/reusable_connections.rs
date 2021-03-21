use std::time::Duration;

use anyhow::{Context, Result};

use pg_queue::logs::{produce, wait_until_visible, Cursor};

use crate::{connect, load_pg_config, setup_db, setup_logging};

#[tokio::test]
async fn example_over_connection() -> Result<()> {
    setup_logging();
    let schema = "example_over_connection";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_db(schema).await;

    let mut pg = connect(&pg_config).await.context("connect")?;

    let produced_version = produce(&mut pg, b"test", b"test")
        .await
        .context("produce")?;

    wait_until_visible(&pg, produced_version, Duration::from_secs(1))
        .await
        .context("wait visible")?;

    let mut cursor = Cursor::load(&pg, "florble").await.context("load cursor")?;

    let res = cursor.poll(&mut pg).await?;

    assert_eq!(res.map(|it| it.version), Some(produced_version));

    Ok(())
}

#[tokio::test]
async fn example_over_transaction() -> Result<()> {
    setup_logging();
    let schema = "example_over_transaction";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_db(schema).await;

    let mut pg = connect(&pg_config).await.context("connect")?;

    let produced_version = produce(&mut pg, b"test", b"test")
        .await
        .context("produce")?;

    wait_until_visible(&pg, produced_version, Duration::from_secs(1))
        .await
        .context("wait visible")?;

    let mut t = pg.transaction().await.context("BEGIN")?;
    let mut cursor = Cursor::load(&t, "florble").await.context("load cursor")?;

    let res = cursor.poll(&mut t).await?;

    assert_eq!(res.map(|it| it.version), Some(produced_version));

    Ok(())
}
