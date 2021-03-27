use criterion::Criterion;
use pg_queue::logs::Batch;

use crate::{connect, load_pg_config, setup_db, setup_logging};

pub(crate) fn insert_seq_0000(c: &mut Criterion) {
    batch_seq_insert_n(c, 0);
}
pub(crate) fn insert_seq_0001(c: &mut Criterion) {
    batch_seq_insert_n(c, 1);
}
pub(crate) fn insert_seq_0016(c: &mut Criterion) {
    batch_seq_insert_n(c, 16);
}
pub(crate) fn insert_seq_0256(c: &mut Criterion) {
    batch_seq_insert_n(c, 256);
}
pub(crate) fn insert_seq_4096(c: &mut Criterion) {
    batch_seq_insert_n(c, 4096);
}

fn batch_seq_insert_n(c: &mut Criterion, nitems: usize) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "batch_seq_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");
    c.bench_function(&format!("batch_seq_insert_n:{}", nitems), |b| {
        runtime.block_on(setup_db(schema)).expect("setup db");
        let mut client = runtime.block_on(connect(&pg_config)).expect("connect");
        let bodies = (0..nitems).map(|i| format!("{}", i)).collect::<Vec<_>>();
        b.iter(|| {
            runtime.block_on(async {
                let batch = Batch::begin(&mut client).await.expect("batch");

                for b in bodies.iter() {
                    batch.produce(b"a", b.as_bytes()).await.expect("produce");
                }

                batch.commit().await.expect("commit");
            })
        })
    });
}
