use std::collections::BTreeSet;

use anyhow::{Context, Result};
use bytes::Bytes;
use maplit::btreeset;

use pg_queue::jobs::{complete, consume_one, produce, retry_later, Error};

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
async fn marking_as_complete_twice_fails() -> Result<()> {
    setup_logging();
    let schema = "jobs_marking_as_complete_twice_fails";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let orig = produce(&t, "42".as_bytes().into())
        .await
        .context("produce")?;

    complete(&t, &orig).await.context("complete")?;

    let e = complete(&t, &orig)
        .await
        .expect_err("Completing job twice should fail");
    match e {
        Error::JobNotFound => {}
        _ => panic!("Expected failure with Error::JobNotFound, saw: {:?}", e),
    }

    t.commit().await?;

    Ok(())
}

#[tokio::test]
async fn marking_one_complete_means_others_remain_available() -> Result<()> {
    setup_logging();
    let schema = "jobs_marking_one_complete_means_others_remain_available";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let a = produce(&t, "a".as_bytes().into())
        .await
        .context("produce")?;
    let b = produce(&t, "b".as_bytes().into())
        .await
        .context("produce")?;
    let c = produce(&t, "c".as_bytes().into())
        .await
        .context("produce")?;

    complete(&t, &a).await.context("complete")?;
    complete(&t, &c).await.context("complete")?;

    let mut ids = BTreeSet::new();
    while let Some(job) = consume_one(&t).await.context("consume_one")? {
        ids.insert(job.id);
        complete(&t, &job).await.context("complete")?;
    }

    t.commit().await?;

    assert_eq!(ids, btreeset! {b.id});

    Ok(())
}

#[tokio::test]
async fn can_mark_job_to_retry_later() -> Result<()> {
    setup_logging();
    let schema = "jobs_can_mark_job_to_retry_later";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let a = produce(&t, "a".as_bytes().into())
        .await
        .context("produce")?;
    let b = produce(&t, "b".as_bytes().into())
        .await
        .context("produce")?;

    retry_later(&t, &a).await.context("retry_later")?;

    let mut ids = BTreeSet::new();
    while let Some(job) = consume_one(&t).await.context("consume_one")? {
        ids.insert(job.id);
        complete(&t, &job).await.context("produce")?;
    }

    t.commit().await?;
    assert_eq!(ids, btreeset! {b.id});

    let t = client.transaction().await?;
    let mut later = BTreeSet::new();
    while let Some(job) = consume_one(&t).await.context("consume_one")? {
        later.insert(job.id);
        complete(&t, &job).await.context("complete")?;
    }
    t.commit().await?;
    assert_eq!(later, btreeset! {a.id});

    Ok(())
}

#[tokio::test]
async fn retry_later_on_complete_job_fails() -> Result<()> {
    setup_logging();
    let schema = "jobs_retry_later_on_complete_job_fails";
    let pg_config = load_pg_config(schema).context("pg-config")?;
    setup_jobs(schema).await;

    let mut client = connect(&pg_config).await.context("connect")?;

    let t = client.transaction().await?;

    let orig = produce(&t, "42".as_bytes().into())
        .await
        .context("produce")?;

    complete(&t, &orig).await.context("complete")?;

    let e = retry_later(&t, &orig)
        .await
        .expect_err("Retrying job after completion should fail");
    match e {
        Error::JobNotFound => {}
        _ => panic!("Expected failure with Error::JobNotFound, saw: {:?}", e),
    }

    t.commit().await?;

    Ok(())
}
