use std::{fmt, time::Duration};

use bytes::Bytes;
use tokio_postgres::{
    types::{FromSql, ToSql},
    Transaction,
};
use tracing::debug;

use crate::jobs::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId {
    inner: i64,
}
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub retried_count: i32,
    pub body: Bytes,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub base_backoff: Duration,
    pub backoff_exponent: f64,
}
#[derive(Debug, Clone)]
pub struct ConfigBuilder {
    pub base_backoff: Duration,
    pub backoff_exponent: f64,
}

const PRODUCE_JOB_SQL: &str =
    "INSERT INTO pg_queue_jobs (body) VALUES ($1) RETURNING id, retried_count";
const CONSUME_JOB_SQL: &str = "\
SELECT id, retried_count, body FROM pg_queue_jobs \
WHERE retry_at <= CURRENT_TIMESTAMP \
FOR UPDATE SKIP LOCKED \
LIMIT 1";
const COMPLETE_JOB_SQL: &str = "DELETE FROM pg_queue_jobs WHERE id = $1";
const RETRY_LATER_SQL: &str = "UPDATE pg_queue_jobs \
    SET retry_at = current_timestamp + interval '1 second' * $2, \
        retried_count = retried_count + 1 \
    WHERE id = $1 \
    RETURNING retry_at";

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

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub async fn consume_one(&self, t: &Transaction<'_>) -> Result<Option<Job>> {
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

    pub async fn complete(&self, t: &Transaction<'_>, job: &Job) -> Result<()> {
        let nrows = t.execute(COMPLETE_JOB_SQL, &[&job.id]).await?;

        if nrows == 1 {
            Ok(())
        } else {
            Err(Error::JobNotFound)
        }
    }

    pub async fn retry_later(&self, t: &Transaction<'_>, job: &Job) -> Result<()> {
        let backoff_duration = self
            .base_backoff
            .mul_f64(self.backoff_exponent.powi(job.retried_count));
        debug!(?job.retried_count, ?backoff_duration, "Calculated backoff time");
        let rows = t
            .query_opt(RETRY_LATER_SQL, &[&job.id, &backoff_duration.as_secs_f64()])
            .await?;
        if let Some(row) = rows {
            let retry_at: chrono::DateTime<chrono::Utc> = row.get("retry_at");
            debug!(%job.id, ?retry_at, "Job");
            Ok(())
        } else {
            Err(Error::JobNotFound)
        }
    }
}

impl ConfigBuilder {
    pub fn build(&self) -> Config {
        Config {
            base_backoff: self.base_backoff,
            backoff_exponent: self.backoff_exponent,
        }
    }

    pub fn base_backoff(&mut self, dur: Duration) -> &mut Self {
        self.base_backoff = dur;
        self
    }
    pub fn backoff_exponent(&mut self, exp: f64) -> &mut Self {
        self.backoff_exponent = exp;
        self
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

impl Default for Config {
    fn default() -> Self {
        Self::builder().build()
    }
}
impl Default for ConfigBuilder {
    fn default() -> Self {
        Self {
            base_backoff: Duration::from_secs(10),
            backoff_exponent: 2.0,
        }
    }
}
