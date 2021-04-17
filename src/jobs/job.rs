use std::fmt;

use bytes::Bytes;
use tokio_postgres::{types::FromSql, Transaction};

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
        let id: JobId = row.get("id");
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

impl<'a> FromSql<'a> for JobId {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        _: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(JobId {})
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        i64::accepts(ty)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
