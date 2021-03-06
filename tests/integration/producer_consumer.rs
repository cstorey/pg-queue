use std::{cmp, collections::BTreeMap, thread, time};

use futures::{
    stream::{self, StreamExt, TryStreamExt},
    FutureExt,
};

use tokio_postgres::{self, NoTls};
use tracing::{debug, info};

use pg_queue::logs::{produce, produce_meta, Batch, Consumer, Version};

use crate::{connect, load_pg_config, setup_db, setup_logging};

#[tokio::test]
async fn can_produce_none() {
    setup_logging();
    let schema = "producer_consumer_can_produce_none";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    debug!("setup consumer");
    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("Consumer");
    debug!("built consumer");
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None);
}

#[tokio::test]
async fn can_produce_one() {
    setup_logging();
    let schema = "producer_consumer_can_produce_one";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let v = produce(&mut client, "foo", b"42").await.expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    let it = cons.poll().await.expect("poll");
    assert_eq!(
        it.map(|e| (e.key.clone(), String::from_utf8_lossy(&e.data).to_string())),
        Some(("foo".to_string(), "42".to_string()))
    );
    assert_eq!(
        cons.poll()
            .await
            .expect("poll")
            .map(|e| String::from_utf8_lossy(&e.data).into_owned()),
        None
    )
}

#[tokio::test]
async fn can_produce_with_metadata() {
    setup_logging();
    let schema = "producer_consumer_can_produce_with_metadata";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let v = produce_meta(&mut client, "foo", Some(b"42-meta".as_ref()), b"42")
        .await
        .expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    let it = cons.poll().await.expect("poll");
    assert_eq!(
        it.map(|e| (e.key.clone(), String::from_utf8_lossy(&e.data).to_string())),
        Some(("foo".to_string(), "42".to_string()))
    );
    assert_eq!(
        cons.poll()
            .await
            .expect("poll")
            .map(|e| String::from_utf8_lossy(&e.data).into_owned()),
        None
    )
}

#[tokio::test]
async fn can_produce_several() {
    setup_logging();
    let schema = "producer_consumer_can_produce_several";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "a", b"0").await.expect("produce");
    produce(&mut client, "b", b"1").await.expect("produce");
    let v = produce(&mut client, "c", b"2").await.expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some(("a".to_string(), b"0".to_vec()))
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some(("b".to_string(), b"1".to_vec()))
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some(("c".to_string(), b"2".to_vec()))
    );
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None)
}

#[tokio::test]
async fn can_produce_ordered() {
    setup_logging();
    let schema = "producer_consumer_can_produce_ordered";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let v0 = produce(&mut client, "a", b"0").await.expect("produce");
    let v1 = produce(&mut client, "a", b"1").await.expect("produce");
    let v2 = produce(&mut client, "a", b"2").await.expect("produce");

    assert!(v0 < v1, "{:?} < {:?}", v0, v1);
    assert!(v1 < v2, "{:?} < {:?}", v1, v2);
}

#[tokio::test]
async fn can_produce_in_batches() {
    setup_logging();
    let schema = "producer_consumer_can_produce_in_batches";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let v = {
        let batch = Batch::begin(&mut client).await.expect("batch");
        batch.produce("a", b"0").await.expect("produce");
        batch.produce("a", b"1").await.expect("produce");
        let v = batch.produce("a", b"2").await.expect("produce");
        batch.commit().await.expect("commit");
        v
    };

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| e.data),
        Some(b"0".to_vec())
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| e.data),
        Some(b"1".to_vec())
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| e.data),
        Some(b"2".to_vec())
    );
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None)
}

#[tokio::test]
async fn can_produce_in_batches_with_metadata() {
    setup_logging();
    let schema = "producer_consumer_can_produce_in_batches_with_metadata";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let v = {
        let batch = Batch::begin(&mut client).await.expect("batch");
        let v = batch
            .produce_meta("a", Some(b"one-meta".as_ref()), b"one")
            .await
            .expect("produce");
        batch.commit().await.expect("commit");
        v
    };

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    let item = cons.poll().await.expect("poll ok");
    assert_eq!(item.and_then(|e| e.meta), Some(b"one-meta".to_vec()));
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None)
}

#[tokio::test]
async fn can_rollback_batches() {
    setup_logging();
    let schema = "producer_consumer_can_rollback_batches";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let v = {
        let batch = Batch::begin(&mut client).await.expect("batch");
        batch.produce("a", b"0").await.expect("produce");
        batch.produce("a", b"1").await.expect("produce");
        let v = batch.produce("key", b"2").await.expect("produce");
        batch.rollback().await.expect("rollback");
        v
    };
    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None,);
}

#[tokio::test]
async fn can_produce_incrementally() {
    setup_logging();
    let schema = "producer_consumer_can_produce_incrementally";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "a", b"0").await.expect("produce");
    produce(&mut client, "a", b"1").await.expect("produce");
    let v = produce(&mut client, "a", b"2").await.expect("produce");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");
    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    let mut observations = Vec::new();
    while let Some(e) = cons.poll().await.expect("poll") {
        observations.push(String::from_utf8_lossy(&e.data).into_owned())
    }

    assert_eq!(&observations, &["0", "1", "2"])
}

#[tokio::test]
async fn can_consume_incrementally() {
    setup_logging();
    let schema = "producer_consumer_can_consume_incrementally";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");
    produce(&mut client, "key", b"2").await.expect("produce");
    produce(&mut client, "key", b"3").await.expect("produce");
    let v = produce(&mut client, "key", b"4").await.expect("produce");

    let mut observations = Vec::new();
    let expected = &["0", "1", "2", "3", "4"];
    for i in 0..expected.len() {
        debug!("Creating consumer iteration {}", i);
        let mut cons = {
            Consumer::connect(&pg_config, NoTls, "default")
                .await
                .expect("consumer")
        };
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        debug!("Consuming on iteration {}", i);
        let entry = cons.poll().await.expect("poll");
        if let Some(ref e) = entry {
            debug!("Got item: {:?}", e);
            cons.commit_upto(&e).await.expect("commit");
            observations.push(String::from_utf8_lossy(&e.data).into_owned());
        }
    }
    assert_eq!(&observations, expected);
}

#[tokio::test]
async fn can_restart_consume_at_commit_point() {
    setup_logging();
    let schema = "producer_consumer_can_restart_consume_at_commit_point";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");
    let v = produce(&mut client, "key", b"2").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll");
        cons.commit_upto(entry.as_ref().unwrap())
            .await
            .expect("commit");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[tokio::test]
async fn can_progress_without_commit() {
    setup_logging();
    let schema = "producer_consumer_can_progress_without_commit";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");
    let v = produce(&mut client, "key", b"2").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[tokio::test]
async fn can_consume_multiply() {
    setup_logging();
    let schema = "producer_consumer_can_consume_multiply";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "key", b"0").await.expect("produce");
    let v = produce(&mut client, "key", b"1").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).await.expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).await.expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[tokio::test]
async fn producing_concurrently_should_never_leave_holes() {
    setup_logging();
    let schema = "producer_consumer_producing_concurrently_should_never_leave_holes";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client1 = connect(&pg_config).await.expect("connect");

    let b1 = Batch::begin(&mut client1).await.expect("batch b1");
    let v = b1.produce("key", b"first").await.expect("produce 1");

    {
        let mut client2 = connect(&pg_config).await.expect("connect");

        let b2 = Batch::begin(&mut client2).await.expect("batch b2");
        b2.produce("key", b"second").await.expect("produce 2");
        b2.commit().await.expect("commit b1");
    }

    let observations_a = {
        let mut observations = Vec::new();
        let mut cons = Consumer::connect(&pg_config, NoTls, "a")
            .await
            .expect("consumer");
        while let Some(entry) = cons.poll().await.expect("poll") {
            observations.push(String::from_utf8_lossy(&entry.data).into_owned());
        }
        observations
    };

    b1.commit().await.expect("commit b1");

    let observations_b = {
        let mut observations = Vec::new();
        let mut cons = Consumer::connect(&pg_config, NoTls, "b")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        while let Some(entry) = cons.poll().await.expect("poll") {
            observations.push(String::from_utf8_lossy(&entry.data).into_owned());
        }
        observations
    };

    println!("observations_a: {:?}", observations_a);
    println!("observations_b: {:?}", observations_b);

    // assert observations_a is a prefix of observations_b
    let prefix_b = observations_b
        .iter()
        .cloned()
        .take(observations_a.len())
        .collect::<Vec<_>>();
    assert_eq!(
        observations_a, prefix_b,
        "{:?} is prefix of {:?} ({:?})",
        observations_a, observations_b, prefix_b
    );
}

#[tokio::test]
async fn can_list_zero_consumer_offsets() {
    setup_logging();
    let schema = "producer_consumer_can_list_zero_consumer_offsets";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");

    let mut client = connect(&pg_config).await.expect("connect");

    let offsets = Consumer::consumers(&mut client).await.expect("iter");
    assert!(offsets.is_empty());
}

#[tokio::test]
async fn can_list_consumer_offset() {
    setup_logging();
    let schema = "producer_consumer_can_list_consumer_offset";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    let v = produce(&mut client, "key", b"1").await.expect("produce");

    let entry;
    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
    }

    let mut client = connect(&pg_config).await.expect("connect");
    let offsets = Consumer::consumers(&mut client).await.expect("iter");
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets.get("one"), Some(&entry.version));
}

#[tokio::test]
async fn can_list_consumer_offsets() {
    setup_logging();
    let schema = "producer_consumer_can_list_consumer_offsets";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    let v = produce(&mut client, "key", b"1").await.expect("produce");

    let one = {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
        entry
    };

    let two = {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");
        let _ = cons.poll().await.expect("poll").expect("some entry");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
        entry
    };

    let mut expected = BTreeMap::new();
    expected.insert("one".to_string(), one.version);
    expected.insert("two".to_string(), two.version);

    let mut client = connect(&pg_config).await.expect("connect");
    let offsets = Consumer::consumers(&mut client).await.expect("consumers");
    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn can_discard_entries() {
    setup_logging();
    let schema = "producer_consumer_can_discard_entries";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    let v = produce(&mut client, "key", b"1").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
    }

    {
        let mut client = connect(&pg_config).await.expect("connect");
        let one_off = Consumer::consumers(&mut client).await.expect("consumers")["one"];
        let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
            .await
            .expect("consumer");
        cons.discard_upto(one_off).await.expect("discard");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(&entry.data, b"1");
    }
}

#[tokio::test]
async fn can_discard_on_empty() {
    setup_logging();
    let schema = "producer_consumer_can_discard_on_empty";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;
    let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
        .await
        .expect("consumer");
    cons.discard_upto(Version::default())
        .await
        .expect("discard");
}

#[tokio::test]
async fn can_discard_consumed() {
    setup_logging();
    let schema = "producer_consumer_can_discard_consumed";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    let v = produce(&mut client, "key", b"1").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(String::from_utf8_lossy(&entry.data), "0");
        cons.commit_upto(&entry).await.expect("commit");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
            .await
            .expect("consumer");
        cons.discard_consumed().await.expect("discard");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(String::from_utf8_lossy(&entry.data), "1");
    }
}

#[tokio::test]
async fn can_discard_consumed_on_empty() {
    setup_logging();
    let schema = "producer_consumer_can_discard_consumed_on_empty";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;
    let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
        .await
        .expect("consumer");
    cons.discard_consumed().await.expect("discard");
}

#[tokio::test]
async fn can_discard_after_written() {
    setup_logging();
    let schema = "producer_consumer_can_discard_after_written";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    let v = produce(&mut client, "key", b"0").await.expect("produce");
    let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
        .await
        .expect("consumer");
    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    cons.discard_upto(Version::default())
        .await
        .expect("discard");
}

#[tokio::test]
async fn can_discard_consumed_without_losing_entries() {
    setup_logging();
    let schema = "producer_consumer_can_discard_consumed_without_losing_entries";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    let _ = produce(&mut client, "key", b"0").await.expect("produce");
    let v1 = produce(&mut client, "key", b"1").await.expect("produce");
    let v2 = produce(&mut client, "key", b"2").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v1, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let _ = cons.poll().await.expect("poll").expect("some entry");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        debug_assert_eq!(v1, entry.version);
        cons.commit_upto(&entry).await.expect("commit");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "two")
            .await
            .expect("consumer");

        cons.wait_until_visible(v2, time::Duration::from_secs(1))
            .await
            .expect("wait for version");

        let _ = cons.poll().await.expect("poll").expect("some entry");
        let _ = cons.poll().await.expect("poll").expect("some entry");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        debug_assert_eq!(v2, entry.version);
        cons.commit_upto(&entry).await.expect("commit");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "cleaner")
            .await
            .expect("consumer");

        cons.discard_consumed().await.expect("discard consumed");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "one")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(v2, entry.version);
    }
}

#[tokio::test]
async fn can_remove_consumer_offset() {
    setup_logging();
    let schema = "producer_consumer_can_remove_consumer_offset";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");
    let v = produce(&mut client, "key", b"2").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll");
        cons.commit_upto(entry.as_ref().unwrap())
            .await
            .expect("commit");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        let _ = cons.clear_offset().await.expect("clear_offset");
    }
    {
        let mut client = connect(&pg_config).await.expect("connect");
        let consumers = Consumer::consumers(&mut client).await.expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[tokio::test]
async fn removing_non_consumer_is_noop() {
    setup_logging();
    let schema = "producer_consumer_removing_non_consumer_is_noop";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"0").await.expect("produce");
    produce(&mut client, "key", b"1").await.expect("produce");
    let v = produce(&mut client, "key", b"2").await.expect("produce");

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let _entry = cons.poll().await.expect("poll");
    }

    {
        let mut cons = Consumer::connect(&pg_config, NoTls, "default")
            .await
            .expect("consumer");
        cons.clear_offset().await.expect("clear_offset");
    }
    {
        let mut client = connect(&pg_config).await.expect("connect");
        let consumers = Consumer::consumers(&mut client).await.expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[tokio::test]
async fn can_produce_consume_with_wait() {
    setup_logging();
    let schema = "producer_consumer_can_produce_consume_with_wait";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;
    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let waiter = tokio::spawn(async move {
        debug!("Awaiting");
        cons.wait_next().await.expect("wait")
    });

    thread::sleep(time::Duration::from_millis(5));
    debug!("Producing");
    let mut client = connect(&pg_config).await.expect("connect");
    produce(&mut client, "key", b"42").await.expect("produce");

    assert_eq!(
        waiter
            .await
            .map(|e| String::from_utf8_lossy(&e.data).to_string())
            .expect("join"),
        "42".to_string()
    );
}

#[tokio::test]
async fn can_read_timestamp() {
    setup_logging();
    let start = chrono::Utc::now();
    let schema = "producer_consumer_can_read_timestamp";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;
    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");
    let v = produce(&mut client, "foo", b"42").await.expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    let it = cons
        .poll()
        .await
        .expect("poll no error")
        .expect("has some record");

    let error = chrono::Duration::minutes(1);
    let lower = start - error;
    let upper = start + error;
    assert!(
        it.written_at >= lower && it.written_at < upper,
        "Entry.written_at:{} is start time:{} ± {}",
        it.written_at,
        start,
        error
    );
}

#[tokio::test]
async fn can_batch_produce_pipelined() {
    setup_logging();
    let schema = "producer_consumer_can_batch_produce_pipelined";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;
    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut client = connect(&pg_config).await.expect("connect");

    let batch = Batch::begin(&mut client).await.expect("batch");

    let nitems: usize = 1024;
    let items = (0..nitems).map(|i| i.to_string()).collect::<Vec<_>>();

    let versions = stream::iter(items.iter())
        .map(|it| {
            batch
                .produce("test", it.as_bytes())
                .inspect(move |res| println!("Produced From: {:?} → {:?}", it, res))
        })
        .buffered(nitems)
        .try_collect::<Vec<_>>()
        .await
        .expect("versions");

    batch.commit().await.expect("commit");

    println!("Versions: {:?}", versions);

    let max_ver = versions.iter().cloned().max().expect("some version");

    cons.wait_until_visible(max_ver, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    let mut observed = Vec::new();

    while let Some(item) = cons.poll().await.expect("item") {
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(items, observed);
}

#[tokio::test]
async fn can_batch_produce_with_transaction_then_insert_order() {
    setup_logging();
    let schema = "producer_consumer_can_batch_produce_with_transaction_then_insert_order";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client1 = connect(&pg_config).await.expect("connect");
    let mut client2 = connect(&pg_config).await.expect("connect");

    let batch1 = Batch::begin(&mut client1).await.expect("batch");
    let batch2 = Batch::begin(&mut client2).await.expect("batch");

    batch1.produce("a", b"a-1").await.expect("produce");
    batch2.produce("b", b"b-1").await.expect("produce");
    let v1 = batch1.produce("a", b"a-2").await.expect("produce");
    let v2 = batch2.produce("b", b"b-2").await.expect("produce");

    batch1.commit().await.expect("commit");
    batch2.commit().await.expect("commit");

    println!("Versions: {:?} / {:?}", v1, v2);
    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    cons.wait_until_visible(cmp::max(v1, v2), time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    let mut observed = Vec::new();

    while let Some(item) = cons.poll().await.expect("item") {
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(vec!["a-1", "a-2", "b-1", "b-2"], observed);
}

#[tokio::test]
async fn can_recover_from_restore_without_without_resetting_epoch() {
    setup_logging();
    let schema = "producer_consumer_can_recover_from_restore_without_without_resetting_epoch";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1i64;
    let original_epoch = 23i64;

    tx.execute(
        "INSERT INTO log_consumer_positions (name, epoch, tx_position, position) VALUES ($1, $2, $3, $4)",
        &[&"default", &original_epoch, &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    let consumers = Consumer::consumers(&mut client).await.expect("consumers");
    info!("Consumer positions: {:?}", consumers);
    let default_pos = consumers["default"];

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let ver = batch.produce("_", b"second").await.expect("produce");
    batch.commit().await.expect("commit");
    debug!("appended: {:?}", ver);

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id > backup_tx_id,
        "Current transaction ID {:?} should be ahead of backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    assert!(
        ver > default_pos,
        "Written item version ({:?}) should happen after default consumer position ({:?})",
        ver,
        default_pos,
    );
    assert!(ver.epoch == original_epoch);
}

#[tokio::test]
async fn can_recover_from_transaction_id_reset_with_only_consumers() {
    setup_logging();
    let schema = "producer_consumer_can_recover_from_transaction_id_reset_with_only_consumers";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    let original_epoch = 23i64;

    tx.execute(
        "INSERT INTO log_consumer_positions (name, epoch, tx_position, position) VALUES ($1, $2, $3, $4)",
        &[&"default", &original_epoch, &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    let consumers = Consumer::consumers(&mut client).await.expect("consumers");
    info!("Consumer positions: {:?}", consumers);
    let default_pos = consumers["default"];

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let ver = batch.produce("_", b"second").await.expect("produce");
    batch.commit().await.expect("commit");
    debug!("appended: {:?}", ver);

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id < backup_tx_id,
        "Current transaction ID {:?} should be behind backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    assert!(
        ver > default_pos,
        "Written item version ({:?}) should happen after default consumer position ({:?})",
        ver,
        default_pos,
    )
}

#[tokio::test]
async fn can_recover_from_transaction_id_reset_with_entries() {
    setup_logging();
    let schema = "producer_consumer_can_recover_from_transaction_id_reset_with_entries";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    tx.execute(
        "INSERT INTO logs (epoch, tx_id, id, key_text, body) VALUES ($1, $2, $3, $4, $5)",
        &[
            &23i64,
            &(backup_tx_id + 1),
            &10i64,
            &"_",
            &(b"first" as &[u8]),
        ],
    )
    .await
    .expect("insert log");

    tx.execute(
        "INSERT INTO log_consumer_positions (epoch, name, position, tx_position) VALUES ($1, $2, $3, $4)",
        &[&23i64, &"default", &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let ver = batch.produce("_", b"second").await.expect("produce");
    batch.commit().await.expect("commit");
    debug!("appended: {:?}", ver);

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id < backup_tx_id,
        "Current transaction ID {:?} should be behind backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    info!("Reconnect");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    cons.wait_until_visible(ver, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    let mut observed = Vec::new();

    while let Some(item) = cons.poll().await.expect("item") {
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(vec!["first", "second"], observed);
}

#[tokio::test]
async fn can_recover_from_transaction_id_reset_when_committing_offsets() {
    setup_logging();
    let schema = "can_recover_from_transaction_id_reset_when_committing_offsets";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    tx.execute(
        "INSERT INTO logs (epoch, tx_id, id, key_text, body) VALUES ($1, $2, $3, $4, $5)",
        &[
            &23i64,
            &(backup_tx_id + 1),
            &10i64,
            &"_",
            &(b"first" as &[u8]),
        ],
    )
    .await
    .expect("insert log");

    tx.execute(
        "INSERT INTO log_consumer_positions (epoch, name, position, tx_position) VALUES ($1, $2, $3, $4)",
        &[&23i64, &"default", &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let second = batch.produce("_", b"second").await.expect("produce");
    debug!("second: {:?}", second);
    let third = batch.produce("_", b"third").await.expect("produce");
    debug!("third: {:?}", third);
    batch.commit().await.expect("commit");

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id < backup_tx_id,
        "Current transaction ID {:?} should be behind backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    info!("Reconnect");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    cons.wait_until_visible(third, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    while let Some(item) = cons.poll().await.expect("item") {
        if item.version <= second {
            debug!(version=?item.version, "committing offset");
            cons.commit_upto(&item).await.expect("commit upto");
        } else {
            break;
        }
    }

    drop(cons);

    info!("Reconnect");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    let mut observed = Vec::new();

    while let Some(item) = cons.poll().await.expect("item") {
        debug!(version=?item.version, data=?String::from_utf8_lossy(&item.data), "Found");
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(vec!["third"], observed);
}

#[tokio::test]
async fn can_recover_from_transaction_id_reset_with_discard_upto() {
    setup_logging();
    let schema = "can_recover_from_transaction_id_reset_with_discard_upto";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    tx.execute(
        "INSERT INTO logs (epoch, tx_id, id, key_text, body) VALUES ($1, $2, $3, $4, $5)",
        &[
            &23i64,
            &(backup_tx_id + 1),
            &10i64,
            &"_",
            &(b"first" as &[u8]),
        ],
    )
    .await
    .expect("insert log");

    tx.execute(
        "INSERT INTO log_consumer_positions (epoch, name, position, tx_position) VALUES ($1, $2, $3, $4)",
        &[&23i64, &"default", &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let second = batch.produce("_", b"second").await.expect("produce");
    debug!("second: {:?}", second);
    let third = batch.produce("_", b"third").await.expect("produce");
    debug!("third: {:?}", third);
    batch.commit().await.expect("commit");

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id < backup_tx_id,
        "Current transaction ID {:?} should be behind backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    info!("Reconnect");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    cons.wait_until_visible(second, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    debug!(?second, "Discard upto second");
    cons.discard_upto(second).await.expect("discard_upto");

    let mut observed = Vec::new();

    while let Some(item) = cons.poll().await.expect("item") {
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(vec!["third"], observed);
}

#[tokio::test]
async fn can_recover_from_transaction_id_reset_with_discard_consumed() {
    setup_logging();
    let schema = "can_recover_from_transaction_id_reset_with_discard_consumed";
    let pg_config = load_pg_config(schema).expect("pg-config");
    setup_db(schema).await;

    let mut client = connect(&pg_config).await.expect("connect");

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    tx.execute(
        "INSERT INTO logs (epoch, tx_id, id, key_text, body) VALUES ($1, $2, $3, $4, $5)",
        &[
            &23i64,
            &(backup_tx_id + 1),
            &10i64,
            &"_",
            &(b"first" as &[u8]),
        ],
    )
    .await
    .expect("insert log");

    tx.execute(
        "INSERT INTO log_consumer_positions (epoch, name, position, tx_position) VALUES ($1, $2, $3, $4)",
        &[&23i64, &"default", &backup_tx_id, &5i64]
    ).await.expect("insert log");

    tx.commit().await.expect("COMMIT");

    info!("Append new entries");
    let batch = Batch::begin(&mut client).await.expect("batch start");
    let second = batch.produce("_", b"second").await.expect("produce");
    debug!("second: {:?}", second);
    let third = batch.produce("_", b"third").await.expect("produce");
    debug!("third: {:?}", third);
    batch.commit().await.expect("commit");

    let row = client
        .query_one("SELECT txid_current() as tx_id", &[])
        .await
        .expect("read current transction ID");
    let tx_id: i64 = row.get("tx_id");
    assert!(
        tx_id < backup_tx_id,
        "Current transaction ID {:?} should be behind backup: {:?}",
        tx_id,
        backup_tx_id
    );
    drop(client);

    info!("Reconnect");

    let mut cons = Consumer::connect(&pg_config, NoTls, "default")
        .await
        .expect("consumer");

    cons.wait_until_visible(second, time::Duration::from_secs(1))
        .await
        .expect("wait for version");

    while let Some(item) = cons.poll().await.expect("item") {
        if item.version <= second {
            cons.commit_upto(&item).await.expect("commit upto");
            debug!(version=?item.version, "commiting offset");
        } else {
            break;
        }
    }
    debug!("Discard upto consumed");
    cons.discard_consumed().await.expect("discard_upto");
    drop(cons);

    let mut other_consumer = Consumer::connect(&pg_config, NoTls, "other")
        .await
        .expect("consumer");

    let mut observed = Vec::new();

    while let Some(item) = other_consumer.poll().await.expect("item") {
        observed.push(String::from_utf8(item.data).expect("from utf8"));
    }

    assert_eq!(vec!["third"], observed);
}
