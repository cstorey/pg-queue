use std::fmt;

use bytes::Bytes;
use tokio_postgres::{
    types::{FromSql, ToSql},
    Transaction,
};

use crate::jobs::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId {
    inner: i64,
}
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub retried_count: i64,
    pub body: Bytes,
}

const PRODUCE_JOB_SQL: &str =
    "INSERT INTO pg_queue_jobs (body) VALUES ($1) RETURNING id, retried_count";
const CONSUME_JOB_SQL: &str = "\
SELECT id, retried_count, body FROM pg_queue_jobs \
WHERE retry_at <= CURRENT_TIMESTAMP \
FOR UPDATE SKIP LOCKED \
LIMIT 1";
const COMPLETE_JOB_SQL: &str = "DELETE FROM pg_queue_jobs WHERE id = $1";
const RETRY_LATER_SQL: &str = "UPDATE pg_queue_jobs
    SET retry_at = current_timestamp + interval '0.01 second',
        retried_count = retried_count + 1
    WHERE id = $1";

pub async fn produce(t: &Transaction<'_>, body: Bytes) -> Result<Job> {
    let row = t.query_one(PRODUCE_JOB_SQL, &[&&*body]).await?;

    let id = row.get("id");
    let retried_count = row.get("retried_count");

    Ok(Job {
        id,
        retried_count,
        body,
    })
}

pub async fn consume_one(t: &Transaction<'_>) -> Result<Option<Job>> {
    if let Some(row) = t.query_opt(CONSUME_JOB_SQL, &[]).await? {
        let id: JobId = row.get("id");
        let retried_count = row.get("retried_count");
        let body: Vec<u8> = row.get("body");
        let body = Bytes::from(body);

        let job = Job {
            id,
            retried_count,
            body,
        };

        Ok(Some(job))
    } else {
        Ok(None)
    }
}

pub async fn complete(t: &Transaction<'_>, job: &Job) -> Result<()> {
    let nrows = t.execute(COMPLETE_JOB_SQL, &[&job.id]).await?;

    if nrows == 1 {
        Ok(())
    } else {
        Err(Error::JobNotFound)
    }
}

pub async fn retry_later(t: &Transaction<'_>, job: &Job) -> Result<()> {
    let nrows = t.execute(RETRY_LATER_SQL, &[&job.id]).await?;
    if nrows == 1 {
        Ok(())
    } else {
        Err(Error::JobNotFound)
    }
}

impl<'a> FromSql<'a> for JobId {
    fn from_sql(
        ty: &tokio_postgres::types::Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        let inner = i64::from_sql(ty, raw)?;
        Ok(JobId { inner })
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <i64 as FromSql>::accepts(ty)
    }
}
impl ToSql for JobId {
    fn to_sql(
        &self,
        ty: &tokio_postgres::types::Type,
        out: &mut bytes::BytesMut,
    ) -> std::result::Result<tokio_postgres::types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    {
        self.inner.to_sql(ty, out)
    }

    fn accepts(ty: &tokio_postgres::types::Type) -> bool {
        <i64 as ToSql>::accepts(ty)
    }

    tokio_postgres::types::to_sql_checked!();
}

impl fmt::Display for JobId {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "J{}", self.inner)
    }
}
