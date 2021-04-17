use anyhow::{Context, Result};

use bytes::Bytes;

use pg_queue::jobs::{complete, consume_one, produce};

use crate::{connect, load_pg_config, setup_jobs, setup_logging};

#[tokio::test]
async fn can_produce_one() -> Result<()> {
    setup_logging();
    let schema = "jobs_can_produce_one";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let orig = produce(&t, "42".as_bytes().into())
        .await
        .context("produce")?;

    let job = consume_one(&t).await?;

    assert_eq!(
        job.as_ref().map(|j| (j.id, j.body.clone())),
        Some((orig.id, Bytes::from("42")))
    );

    t.commit().await?;

    Ok(())
}

#[tokio::test]
async fn marking_as_complete_removes_from_available_jobs() -> Result<()> {
    setup_logging();
    let schema = "jobs_marking_as_complete_removes_from_available_jobs";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let orig = produce(&t, "42".as_bytes().into())
        .await
        .context("produce")?;

    complete(&t, &orig).await.context("complete")?;

    let job = consume_one(&t).await?;

    assert!(job.is_none(), "Job should have been completed");

    t.commit().await?;

    Ok(())
}

#[tokio::test]
#[ignore]
async fn marking_one_complete_means_others_remain_available() {}
