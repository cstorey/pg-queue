use std::env;

use anyhow::{Context, Result};
use log::debug;
use tokio_postgres::{self, Client, Config, NoTls};

use pg_queue::logs::setup;

mod original_interface;
mod producer_consumer;

const DEFAULT_URL: &str = "postgres://postgres@localhost/";

fn load_pg_config(schema: &str) -> Result<Config> {
    let url = env::var("POSTGRES_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    let mut config: Config = url.parse()?;
    debug!("Use schema name: {}", schema);

    config.options(&format!("-csearch_path={}", schema));

    Ok(config)
}

async fn connect(config: &Config) -> Result<Client> {
    let (client, conn) = config.connect(NoTls).await?;
    tokio::spawn(conn);
    Ok(client)
}

async fn setup_db(schema: &str) {
    let pg_config = load_pg_config(schema).expect("pg-config");

    let mut client = connect(&pg_config).await.expect("connect");

    let t = client.transaction().await.expect("BEGIN");
    t.execute(
        &*format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema),
        &[],
    )
    .await
    .context("drop schema")
    .expect("execute");
    t.execute(&*format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .context("create schema")
        .expect("execute");
    t.commit().await.expect("commit");

    setup(&client).await.expect("setup");
}
