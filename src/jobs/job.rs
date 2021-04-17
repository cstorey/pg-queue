use std::fmt;

use bytes::Bytes;
use tokio_postgres::Transaction;

use crate::jobs::Result;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId {}
#[derive(Debug)]
pub struct Job {
    pub id: JobId,
    pub kind: String,
    pub data: Bytes,
}

impl fmt::Display for JobId {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}
pub async fn consume_one(_t: &Transaction<'_>, _key: &str) -> Result<Job> {
    todo!()
}

pub async fn produce(_t: &Transaction<'_>, _key: &str, _data: Bytes) -> Result<Job> {
    todo!()
}

pub async fn complete(_t: &Transaction<'_>, _job: &Job) -> Result<Job> {
    todo!()
}
