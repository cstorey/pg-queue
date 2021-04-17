use std::env;

use anyhow::{Context, Result};
use jobs::Job;
use pg_queue::jobs;

use tokio_postgres::{Config, NoTls};
use tracing::debug;
use tracing_log::LogTracer;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LogTracer::init().expect("log-tracer init");
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(EnvFilter::from_default_env())
            .with_ansi(false)
            .with_timer(tracing_subscriber::fmt::time::ChronoUtc::rfc3339())
            .with_thread_names(true)
            .with_thread_ids(true)
            .finish(),
    )
    .context("tracing subscriber")?;

    let url = env::var("POSTGRES_URL").context("$POSTGRES_URL")?;
    let pg: Config = url.parse()?;

    let (mut client, conn) = pg.connect(NoTls).await.context("connect pg")?;
    tokio::spawn(conn);

    jobs::setup(&client).await.context("setup db")?;

    let config = jobs::Config::default();
    {
        let t = client.transaction().await?;
        let blob = serde_json::to_vec(&0u64)?;
        let job0: Job = jobs::produce(&t, blob.into()).await.context("create")?;
        debug!(job_id=%job0.id, "Created job");
        t.commit().await?;
    };

    {
        let t = client.transaction().await?;
        let job: Job = config.consume_one(&t).await?.expect("just produced job");
        debug!(job_id=%job.id, "processing");
        // do stuff
        let val: u64 = serde_json::from_slice(&job.body)?;
        let newval = val + 1;
        let blob = serde_json::to_vec(&newval)?;
        let job1: Job = jobs::produce(&t, blob.into()).await.context("create 1")?;
        debug!(job_id=%job1.id, "Produced new job");

        config.complete(&t, &job).await?;
        t.commit().await?;
    }

    {
        let t = client.transaction().await?;
        let job: Job = config.consume_one(&t).await?.expect("just produced job");
        debug!(job_id=%job.id, "processing");
        // do stuff
        let val: u64 = serde_json::from_slice(&job.body)?;
        debug!(job_id=%job.id, value=?val, "Process job");
        config.complete(&t, &job).await?;
        t.commit().await?;
    }

    Ok(())
}
