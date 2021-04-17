use std::fmt;

use bytes::Bytes;
use tokio_postgres::{Row, Transaction};

use crate::jobs::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId {}
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub body: Bytes,
}

pub async fn produce(t: &Transaction<'_>, body: Bytes) -> Result<Job> {
    t.execute("INSERT INTO pg_queue_jobs (body) VALUES ($1)", &[&&*body])
        .await?;

    let id = JobId {};

    Ok(Job { id, body })
}

pub async fn consume_one(t: &Transaction<'_>) -> Result<Option<Job>> {
    if let Some(row) = t
        .query_opt("SELECT id, body FROM pg_queue_jobs", &[])
        .await?
    {
        let id = JobId::of(&row);
        let body: Vec<u8> = row.get("body");
        let body = Bytes::from(body);

        let job = Job { id, body };

        Ok(Some(job))
    } else {
        Ok(None)
    }
}

pub async fn complete(t: &Transaction<'_>, _job: &Job) -> Result<()> {
    t.execute("DELETE FROM pg_queue_jobs", &[]).await?;

    Ok(())
}

impl JobId {
    fn of(_row: &Row) -> Self {
        Self {}
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
