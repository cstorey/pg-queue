use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Postgres")]
    Postgres(#[from] tokio_postgres::Error),
}

pub(crate) type Result<T> = ::std::result::Result<T, Error>;
