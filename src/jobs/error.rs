use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("Job not found")]
    JobNotFound,
}

pub(crate) type Result<T> = ::std::result::Result<T, Error>;
