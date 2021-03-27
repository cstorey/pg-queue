use std::time::Duration;

use anyhow::{Context, Result};

use pg_queue::logs::{produce, wait_until_visible, Batch, Cursor};
use tracing::debug;

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

    let mut t = pg.transaction().await.context("BEGIN")?;

    let produced_version = produce(&mut t, b"test", b"test").await.context("produce")?;

    t.commit().await?;
    let mut t = pg.transaction().await.context("BEGIN")?;
    let mut cursor = Cursor::load(&t, "florble").await.context("load cursor")?;

    let res = cursor.poll(&mut t).await?;

    t.commit().await?;

    assert_eq!(res.map(|it| it.version), Some(produced_version));

    Ok(())
}

#[tokio::test]
async fn example_consuming_in_chunks() -> Result<()> {
    setup_logging();
    let schema = "example_consuming_in_chunks";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_db(schema).await;

    let mut pg = connect(&pg_config).await.context("connect")?;

    let mut produced = Vec::new();

    let mut t = pg.transaction().await.context("BEGIN")?;
    let b = Batch::begin(&mut t).await.context("batch start")?;

    for i in 0usize..16 {
        produced.push(b.produce(b"test", i.to_string().as_bytes()).await?);
    }

    b.commit().await.context("finish batch")?;
    t.commit().await.context("commit")?;

    wait_until_visible(
        &pg,
        produced.last().cloned().unwrap(),
        Duration::from_secs(1),
    )
    .await
    .context("await visible")?;

    let mut observed = Vec::new();

    'chunk_loop: loop {
        let mut t = pg.transaction().await.context("BEGIN")?;
        let mut cursor = Cursor::load(&t, "consumer").await.context("load cursor")?;
        debug!(?cursor, "Loaded cursor");

        for _ in 0usize..4 {
            if let Some(it) = cursor.poll(&mut t).await? {
                debug!(version=?it.version, "Found item");
                observed.push(it.version);
                cursor
                    .commit_upto(&mut t, &it)
                    .await
                    .context("commit_upto")?;
            } else {
                debug!("Ran out of items");
                break 'chunk_loop;
            }
        }

        t.commit().await?;
    }

    assert_eq!(observed, produced);

    Ok(())
}
