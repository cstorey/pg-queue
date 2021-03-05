use tokio_postgres::Client;

pub mod logs;

#[deprecated]
pub type Consumer = logs::Consumer;
#[deprecated]
pub type Version = logs::Version;

#[deprecated]
pub async fn batch(client: &mut Client) -> logs::Result<logs::Batch<'_>> {
    logs::batch(client).await
}
#[deprecated]
pub async fn produce(client: &mut Client, key: &[u8], body: &[u8]) -> logs::Result<logs::Version> {
    logs::produce(client, key, body).await
}
#[deprecated]
pub async fn produce_meta(
    client: &mut Client,
    key: &[u8],
    meta: Option<&[u8]>,
    body: &[u8],
) -> logs::Result<logs::Version> {
    logs::produce_meta(client, key, meta, body).await
}

#[deprecated]
pub async fn setup(client: &Client) -> logs::Result<()> {
    logs::setup(client).await
}
