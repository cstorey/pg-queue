use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput};
use pg_queue::logs::{Batch, Consumer};
use tokio_postgres::NoTls;

use crate::{connect, load_pg_config, setup_db, setup_logging};

pub(crate) fn consume(c: &mut Criterion) {
    setup_logging();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let schema = "bench_consumer";
    let pg_config = load_pg_config(schema).expect("pg-config");
    let mut group = c.benchmark_group("consumer");
    for nitems in [1, 4, 16, 64, 256, 1024, 4096].iter() {
        group.throughput(Throughput::Elements(*nitems));
        group.bench_with_input(BenchmarkId::from_parameter(nitems), nitems, |b, &nitems| {
            runtime.block_on(setup_db(schema)).expect("setup db");
            let mut client = runtime.block_on(connect(&pg_config)).expect("connect");

            runtime.block_on(async {
                let batch = Batch::begin(&mut client).await.expect("batch");
                let mut ver = None;

                for i in 0..nitems {
                    let b = format!("{}", i);
                    ver = Some(batch.produce("a", b.as_bytes()).await.expect("produce"));
                }

                batch.commit().await.expect("commit");

                if let Some(ver) = ver {
                    let cons = Consumer::connect(&pg_config, NoTls, "default")
                        .await
                        .expect("consumer");

                    cons.wait_until_visible(ver, Duration::from_secs(1))
                        .await
                        .expect("wait for version");
                }
            });

            b.iter_batched_ref(
                || {
                    runtime
                        .block_on(Consumer::connect(&pg_config, NoTls, "default"))
                        .expect("consumer")
                },
                |cons| {
                    runtime.block_on(async {
                        for i in 0..nitems {
                            let it = cons.poll().await.expect("item");
                            assert!(it.is_some(), "Consume on iter: {:?}", i);
                        }
                    })
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();
}
