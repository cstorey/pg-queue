use std::fmt;

use bytes::Bytes;
use tokio_postgres::{
    types::{FromSql, ToSql},
    Transaction,
};

use crate::jobs::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId {
    inner: i64,
}
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub body: Bytes,
}

pub async fn produce(t: &Transaction<'_>, body: Bytes) -> Result<Job> {
    let row = t
        .query_one(
            "INSERT INTO pg_queue_jobs (body) VALUES ($1) RETURNING (id)",
            &[&&*body],
        )
        .await?;

    let id = row.get("id");

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

pub async fn complete(t: &Transaction<'_>, job: &Job) -> Result<()> {
    t.execute("DELETE FROM pg_queue_jobs WHERE id = $1", &[&job.id])
        .await?;

    Ok(())
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
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
