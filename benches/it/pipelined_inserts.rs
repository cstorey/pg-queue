use std::convert::TryInto;

use criterion::{black_box, BatchSize, BenchmarkId, Criterion, Throughput};
use futures::{stream, StreamExt, TryStreamExt};
use pg_queue::logs::Batch;

use crate::{connect, load_pg_config, setup_db, setup_logging};

pub(crate) fn batch_pipeline_insert(c: &mut Criterion) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "bench_pipelined_insert";
    let pg_config = load_pg_config(schema).expect("pg-config");

    let mut group = c.benchmark_group("batch_pipeline_insert");
    for nitems in [0u64, 1, 16, 256, 4096].iter() {
        group.throughput(Throughput::Elements(*nitems));
        group.bench_with_input(BenchmarkId::from_parameter(nitems), nitems, |b, &nitems| {
            runtime.block_on(setup_db(schema)).expect("setup db");
            let bodies = (0..nitems).map(|i| format!("{}", i)).collect::<Vec<_>>();
            b.iter_batched_ref(
                || runtime.block_on(connect(&pg_config)).expect("connect"),
                |client| {
                    runtime.block_on(async {
                        let batch = Batch::begin(client).await.expect("batch");

                        stream::iter(bodies.iter())
                            .map(|it| batch.produce("test", it.as_bytes()))
                            .buffered((nitems + 1).try_into().unwrap())
                            .try_for_each(|v| async move {
                                black_box(v);
                                Ok(())
                            })
                            .await
                            .expect("insert");

                        batch.commit().await.expect("commit");
                    })
                },
                BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}
