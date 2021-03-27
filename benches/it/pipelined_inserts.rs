use criterion::{black_box, Criterion};
use futures::{stream, StreamExt, TryStreamExt};
use pg_queue::logs::Batch;

use crate::{connect, load_pg_config, setup_db, setup_logging};

pub(crate) fn insert_pipeline_0000(c: &mut Criterion) {
    batch_pipeline_insert_n(c, 0);
}
pub(crate) fn insert_pipeline_0001(c: &mut Criterion) {
    batch_pipeline_insert_n(c, 1);
}
pub(crate) fn insert_pipeline_0016(c: &mut Criterion) {
    batch_pipeline_insert_n(c, 16);
}
pub(crate) fn insert_pipeline_0256(c: &mut Criterion) {
    batch_pipeline_insert_n(c, 256);
}
pub(crate) fn insert_pipeline_4096(c: &mut Criterion) {
    batch_pipeline_insert_n(c, 4096);
}

fn batch_pipeline_insert_n(c: &mut Criterion, nitems: usize) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "bench_pipelined_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");
    c.bench_function(&format!("batch_pipeline_insert:{}", nitems), |b| {
        runtime.block_on(setup_db(schema)).expect("setup db");
        let mut client = runtime.block_on(connect(&pg_config)).expect("connect");
        let bodies = (0..nitems).map(|i| format!("{}", i)).collect::<Vec<_>>();
        b.iter(|| {
            runtime.block_on(async {
                let batch = Batch::begin(&mut client).await.expect("batch");

                stream::iter(bodies.iter())
                    .map(|it| batch.produce(b"test", it.as_bytes()))
                    .buffered(nitems + 1)
                    .try_for_each(|v| async move {
                        black_box(v);
                        Ok(())
                    })
                    .await
                    .expect("insert");

                batch.commit().await.expect("commit");
            })
        })
    });
}
