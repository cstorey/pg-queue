use bencher::Bencher;
use pg_queue::logs::Batch;

use crate::{connect, load_pg_config, setup_db, setup_logging};

pub(crate) fn insert_seq_0000(b: &mut Bencher) {
    batch_seq_insert_n(b, 0);
}
pub(crate) fn insert_seq_0001(b: &mut Bencher) {
    batch_seq_insert_n(b, 1);
}
pub(crate) fn insert_seq_0016(b: &mut Bencher) {
    batch_seq_insert_n(b, 16);
}
pub(crate) fn insert_seq_0256(b: &mut Bencher) {
    batch_seq_insert_n(b, 256);
}
pub(crate) fn insert_seq_4096(b: &mut Bencher) {
    batch_seq_insert_n(b, 4096);
}

fn batch_seq_insert_n(b: &mut Bencher, nitems: usize) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "batch_seq_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");
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
}
