#[macro_use]
extern crate log;

use futures::{
    pin_mut,
    stream::{self, StreamExt, TryStreamExt},
    FutureExt,
};
use tokio_postgres::{
    self, binary_copy::BinaryCopyInWriter, types::Type, Client, Connection, NoTls,
};

use anyhow::{Error, Result};
use std::env;
use std::thread;
use std::time;
use std::{cmp, collections::BTreeMap};

const DEFAULT_URL: &str = "postgres://postgres@localhost/";

async fn configure_schema(client: &mut Client, schema: &str) -> Result<()> {
    loop {
        let t = client.transaction().await?;
        let nschemas: i64 = {
            let rows = t
                .query(
                    "SELECT count(*) from pg_catalog.pg_namespace n where n.nspname = $1",
                    &[&schema],
                )
                .await?;
            let row = rows.get(0).expect("row 0");
            row.get(0)
        };
        debug!("Number of {} schemas:{}", schema, nschemas);
        if nschemas == 0 {
            match t
                .execute(&*format!("CREATE SCHEMA \"{}\"", schema), &[])
                .await
            {
                Ok(_) => {
                    t.commit().await?;
                    break;
                }
                Err(e) => warn!("Error creating schema:{:?}: {:?}", schema, e),
            }
        } else {
            break;
        }
    }
    client
        .execute(&*format!("SET search_path TO \"{}\"", schema), &[])
        .await?;
    Ok(())
}

async fn connect(
    schema: &str,
) -> Result<(
    Client,
    Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
)> {
    let url = env::var("POSTGRES_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    debug!("Use schema name: {}", schema);
    let (mut client, mut conn) = tokio_postgres::connect(&*url, NoTls).await?;

    tokio::select! {
        res = (&mut conn) => panic!("Connection exited? {:?}", res),
        res = configure_schema(&mut client, schema) => res.expect("configure schema"),
    }

    tokio::select! {
        res = (&mut conn) => panic!("Connection exited? {:?}", res),

        rowsp = client.query("select pg_backend_pid()", &[]) => {
            let rows = rowsp.expect("rows");
            let pid : i32 = rows.get(0).expect("some row").get(0);
            info!("Connected to backend:{:?}", pid);
        }
    }

    Ok((client, conn))
}

async fn setup(schema: &str) {
    let (mut client, mut conn) = connect(schema).await.expect("connect");
    let setup_f = async {
        let t = client.transaction().await?;
        for table in &["logs", "log_consumer_positions"] {
            t.execute(&*format!("DROP TABLE IF EXISTS {}", table), &[])
                .await?;
        }
        t.commit().await?;

        pg_queue::setup(&client).await?;
        Ok::<_, Error>(())
    };

    tokio::select! {
        res = (&mut conn) => panic!("Connection exited? {:?}", res),
        res = setup_f => res.expect("cleanup tables"),
    };
}

#[tokio::test]
async fn can_produce_none() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_none";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    debug!("setup consumer");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("Consumer");
    debug!("built consumer");
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None);
}

#[tokio::test]
async fn can_produce_one() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_one";
    setup(schema).await;
    let (client, conn) = connect(schema).await.expect("connect");

    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let v = pg_queue::produce(&mut client, b"foo", b"42")
        .await
        .expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    let it = cons.poll().await.expect("poll");
    assert_eq!(
        it.map(|e| (
            String::from_utf8_lossy(&e.key).to_string(),
            String::from_utf8_lossy(&e.data).to_string()
        )),
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_several";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"a", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"b", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"c", b"2")
        .await
        .expect("produce");

    cons.wait_until_visible(v, time::Duration::from_secs(1))
        .await
        .expect("wait for version");
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some((b"a".to_vec(), b"0".to_vec()))
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some((b"b".to_vec(), b"1".to_vec()))
    );
    assert_eq!(
        cons.poll().await.expect("poll").map(|e| (e.key, e.data)),
        Some((b"c".to_vec(), b"2".to_vec()))
    );
    assert_eq!(cons.poll().await.expect("poll").map(|e| e.data), None)
}

#[tokio::test]
async fn can_produce_ordered() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_ordered";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let v0 = pg_queue::produce(&mut client, b"a", b"0")
        .await
        .expect("produce");
    let v1 = pg_queue::produce(&mut client, b"a", b"1")
        .await
        .expect("produce");
    let v2 = pg_queue::produce(&mut client, b"a", b"2")
        .await
        .expect("produce");

    assert!(v0 < v1, "{:?} < {:?}", v0, v1);
    assert!(v1 < v2, "{:?} < {:?}", v1, v2);
}

#[tokio::test]
async fn can_produce_in_batches() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_in_batches";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let v = {
        let batch = pg_queue::batch(&mut client).await.expect("batch");
        batch.produce(b"a", b"0").await.expect("produce");
        batch.produce(b"a", b"1").await.expect("produce");
        let v = batch.produce(b"a", b"2").await.expect("produce");
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
async fn can_rollback_batches() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_rollback_batches";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let v = {
        let batch = pg_queue::batch(&mut client).await.expect("batch");
        batch.produce(b"a", b"0").await.expect("produce");
        batch.produce(b"a", b"1").await.expect("produce");
        let v = batch.produce(b"key", b"2").await.expect("produce");
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_incrementally";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"a", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"a", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"a", b"2")
        .await
        .expect("produce");

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_consume_incrementally";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"3")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"4")
        .await
        .expect("produce");

    let mut observations = Vec::new();
    let expected = &["0", "1", "2", "3", "4"];
    for i in 0..expected.len() {
        debug!("Creating consumer iteration {}", i);
        let mut cons = {
            let (client, conn) = connect(schema).await.expect("connect");
            pg_queue::Consumer::new(conn, client, "default")
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_restart_consume_at_commit_point";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[tokio::test]
async fn can_progress_without_commit() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_progress_without_commit";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_consume_multiply";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).await.expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[tokio::test]
async fn producing_concurrently_should_never_leave_holes() {
    env_logger::try_init().unwrap_or(());
    let schema = "producing_concurrently_should_never_leave_holes";
    setup(schema).await;

    let (mut client1, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let b1 = pg_queue::batch(&mut client1).await.expect("batch b1");
    let v = b1.produce(b"key", b"first").await.expect("produce 1");

    {
        let (mut client2, conn) = connect(schema).await.expect("connect");
        tokio::spawn(conn);

        let b2 = pg_queue::batch(&mut client2).await.expect("batch b2");
        b2.produce(b"key", b"second").await.expect("produce 2");
        b2.commit().await.expect("commit b1");
    }

    let observations_a = {
        let mut observations = Vec::new();
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "a")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "b")
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_list_zero_consumer_offsets";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    let (client, conn) = connect(schema).await.expect("connect");

    let mut cons = pg_queue::Consumer::new(conn, client, "one")
        .await
        .expect("consumer");
    let offsets = cons.consumers().await.expect("iter");
    assert!(offsets.is_empty());
}

#[tokio::test]
async fn can_list_consumer_offset() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_list_consumer_offset";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    let entry;
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
    }

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "one")
        .await
        .expect("consumer");
    let offsets = cons.consumers().await.expect("iter");
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets.get("one"), Some(&entry.version));
}

#[tokio::test]
async fn can_list_consumer_offsets() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_list_consumer_offsets";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    let one = {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
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

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "two")
        .await
        .expect("consumer");
    let offsets = cons.consumers().await.expect("consumers");
    assert_eq!(offsets, expected);
}

#[tokio::test]
async fn can_discard_entries() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_entries";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        cons.commit_upto(&entry).await.expect("commit");
    }

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");
        let one_off = cons.consumers().await.expect("consumers")["one"];
        cons.discard_upto(one_off).await.expect("discard");
    }

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(&entry.data, b"1");
    }
}

#[tokio::test]
async fn can_discard_on_empty() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_on_empty";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    {
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");
        cons.discard_upto(pg_queue::Version::default())
            .await
            .expect("discard");
    }
}

#[tokio::test]
async fn can_discard_consumed() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_consumed";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");
        cons.discard_consumed().await.expect("discard");

        println!(
            "Consumers: {:#?}",
            cons.consumers().await.expect("consumers")
        );
    }

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(String::from_utf8_lossy(&entry.data), "1");
    }
}

#[tokio::test]
async fn can_discard_consumed_on_empty() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_consumed_on_empty";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    {
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");
        cons.discard_consumed().await.expect("discard");
    }
}

#[tokio::test]
async fn can_discard_after_written() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_after_written";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    let v = pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        cons.discard_upto(pg_queue::Version::default())
            .await
            .expect("discard");
    }
}

#[tokio::test]
async fn can_discard_consumed_without_losing_entries() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_discard_consumed_without_losing_entries";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    let _ = pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    let v1 = pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    let v2 = pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "two")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "cleaner")
            .await
            .expect("consumer");

        cons.discard_consumed().await.expect("discard consumed");
    }

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "one")
            .await
            .expect("consumer");
        let entry = cons.poll().await.expect("poll").expect("some entry");
        assert_eq!(v2, entry.version);
    }
}

#[tokio::test]
async fn can_remove_consumer_offset() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_remove_consumer_offset";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        let _ = cons.clear_offset().await.expect("clear_offset");
    }
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        let consumers = cons.consumers().await.expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[tokio::test]
async fn removing_non_consumer_is_noop() {
    env_logger::try_init().unwrap_or(());
    let schema = "removing_non_consumer_is_noop";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"0")
        .await
        .expect("produce");
    pg_queue::produce(&mut client, b"key", b"1")
        .await
        .expect("produce");
    let v = pg_queue::produce(&mut client, b"key", b"2")
        .await
        .expect("produce");

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        cons.wait_until_visible(v, time::Duration::from_secs(1))
            .await
            .expect("wait for version");
        let _entry = cons.poll().await.expect("poll");
    }

    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        cons.clear_offset().await.expect("clear_offset");
    }
    {
        let (client, conn) = connect(schema).await.expect("connect");
        let mut cons = pg_queue::Consumer::new(conn, client, "default")
            .await
            .expect("consumer");
        let consumers = cons.consumers().await.expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[tokio::test]
async fn can_produce_consume_with_wait() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_produce_consume_with_wait";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let waiter = tokio::spawn(async move {
        debug!("Awaiting");
        cons.wait_next().await.expect("wait")
    });

    thread::sleep(time::Duration::from_millis(5));
    debug!("Producing");
    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    pg_queue::produce(&mut client, b"key", b"42")
        .await
        .expect("produce");

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
    env_logger::try_init().unwrap_or(());
    let start = chrono::Utc::now();
    let schema = "can_read_timestamp";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);
    let v = pg_queue::produce(&mut client, b"foo", b"42")
        .await
        .expect("produce");

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
    env_logger::try_init().unwrap_or(());
    let schema = "can_batch_produce_pipelined";
    setup(schema).await;

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
        .await
        .expect("consumer");

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let batch = pg_queue::batch(&mut client).await.expect("batch");

    let nitems: usize = 1024;
    let items = (0..nitems).map(|i| i.to_string()).collect::<Vec<_>>();

    let versions = stream::iter(items.iter())
        .map(|it| {
            batch
                .produce(b"test", it.as_bytes())
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
    env_logger::try_init().unwrap_or(());
    let schema = "can_batch_produce_with_transaction_then_insert_order";
    setup(schema).await;

    let (mut client1, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let (mut client2, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let batch1 = pg_queue::batch(&mut client1).await.expect("batch");
    let batch2 = pg_queue::batch(&mut client2).await.expect("batch");

    batch1.produce(b"a", b"a-1").await.expect("produce");
    batch2.produce(b"b", b"b-1").await.expect("produce");
    let v1 = batch1.produce(b"a", b"a-2").await.expect("produce");
    let v2 = batch2.produce(b"b", b"b-2").await.expect("produce");

    batch1.commit().await.expect("commit");
    batch2.commit().await.expect("commit");

    println!("Versions: {:?} / {:?}", v1, v2);

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
async fn can_recover_from_transaction_id_reset() {
    env_logger::try_init().unwrap_or(());
    let schema = "can_recover_from_transaction_id_reset";
    setup(schema).await;

    let (mut client, conn) = connect(schema).await.expect("connect");
    tokio::spawn(conn);

    let tx = client.transaction().await.expect("BEGIN");

    info!("Restoring from backup");
    // Assume the backup had advanced to an absurdly high transaction ID.
    let backup_tx_id = 1_000_000_000_000_000_000i64;
    let sink = tx
        .copy_in("COPY logs (epoch, tx_id, id, key, body) FROM STDIN BINARY")
        .await
        .expect("copy in logs");
    let writer = BinaryCopyInWriter::new(
        sink,
        &[Type::INT8, Type::INT8, Type::INT8, Type::BYTEA, Type::BYTEA],
    );
    pin_mut!(writer);
    writer
        .as_mut()
        .write(&[
            &23i64,
            &(backup_tx_id + 1),
            &10i64,
            &(b"_" as &[u8]),
            &(b"first" as &[u8]),
        ])
        .await
        .expect("write row");
    writer.finish().await.expect("expect finish");

    let sink = tx
        .copy_in(
            "COPY log_consumer_positions (epoch, name, position, tx_position) FROM STDIN BINARY",
        )
        .await
        .expect("copy in logs");
    let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::TEXT, Type::INT8, Type::INT8]);
    pin_mut!(writer);
    writer
        .as_mut()
        .write(&[&23i64, &"default", &backup_tx_id, &5i64])
        .await
        .expect("write row");
    writer.finish().await.expect("expect finish");

    tx.commit().await.expect("COMMIT");

    info!("Append new entries");
    let batch = pg_queue::batch(&mut client).await.expect("batch start");
    let ver = batch.produce(b"_", b"second").await.expect("produce");
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

    let (client, conn) = connect(schema).await.expect("connect");
    let mut cons = pg_queue::Consumer::new(conn, client, "default")
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
