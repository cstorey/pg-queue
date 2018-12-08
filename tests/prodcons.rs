extern crate pg_queue;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate postgres;
extern crate r2d2;
extern crate r2d2_postgres;

use r2d2::Pool;
use r2d2_postgres::{PostgresConnectionManager, TlsMode};
use std::env;
use std::thread;
use std::time;

const DEFAULT_URL: &'static str = "postgres://postgres@localhost/";

#[derive(Debug)]
struct UseTempSchema(String);

impl r2d2::CustomizeConnection<postgres::Connection, postgres::Error> for UseTempSchema {
    fn on_acquire(&self, conn: &mut postgres::Connection) -> Result<(), postgres::Error> {
        loop {
            let t = try!(conn.transaction().map_err(|e| e));
            let nschemas: i64 = {
                let rows = try!(
                    t.query(
                        "SELECT count(*) from pg_catalog.pg_namespace n where n.nspname = $1",
                        &[&self.0]
                    ).map_err(|e| e)
                );
                let row = rows.get(0);
                row.get(0)
            };
            debug!("Number of {} schemas:{}", self.0, nschemas);
            if nschemas == 0 {
                match t
                    .execute(&format!("CREATE SCHEMA \"{}\"", self.0), &[])
                    .map_err(|e| e)
                {
                    Ok(_) => {
                        try!(t.commit().map_err(|e| e));
                        break;
                    }
                    Err(e) => warn!("Error creating schema:{:?}: {:?}", self.0, e),
                }
            } else {
                break;
            }
        }
        try!(
            conn.execute(&format!("SET search_path TO \"{}\"", self.0), &[])
                .map_err(|e| e)
        );
        Ok(())
    }
}

fn pool(schema: &str) -> Pool<PostgresConnectionManager> {
    let url = env::var("POSTGRES_URL").unwrap_or_else(|_| DEFAULT_URL.to_string());
    debug!("Use schema name: {}", schema);
    let manager = PostgresConnectionManager::new(&*url, TlsMode::None).expect("postgres");
    let pool = r2d2::Pool::builder()
        .max_size(2)
        .connection_customizer(Box::new(UseTempSchema(schema.to_string())))
        .build(manager)
        .expect("pool");
    let conn = pool.get().expect("temp connection");
    let t = conn.transaction().expect("begin");
    for table in &["logs", "log_consumer_positions"] {
        conn.execute(&format!("DROP TABLE IF EXISTS {}", table), &[])
            .expect("drop table");
    }
    pg_queue::setup(&t).expect("setup");
    t.commit().expect("commit");
    pool
}

#[test]
fn can_produce_none() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_none");
    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_one() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_one");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
    prod.produce(b"42").expect("produce");

    assert_eq!(
        cons.poll()
            .expect("poll")
            .map(|e| String::from_utf8_lossy(&e.data).to_string()),
        Some("42".to_string())
    );
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_several() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_several");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"0".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"1".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"2".to_vec())
    );
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_in_batches() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_in_batches");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
    {
        let mut batch = prod.batch().expect("batch");
        batch.produce(b"0").expect("produce");
        batch.produce(b"1").expect("produce");
        batch.produce(b"2").expect("produce");
        batch.commit().expect("commit");
    }

    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"0".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"1".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"2".to_vec())
    );
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_produce_incrementally() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_incrementally");
    pg_queue::Producer::new(pool.clone())
        .expect("producer")
        .produce(b"0")
        .expect("produce");
    pg_queue::Producer::new(pool.clone())
        .expect("producer")
        .produce(b"1")
        .expect("produce");
    pg_queue::Producer::new(pool.clone())
        .expect("producer")
        .produce(b"2")
        .expect("produce");

    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"0".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"1".to_vec())
    );
    assert_eq!(
        cons.poll().expect("poll").map(|e| e.data),
        Some(b"2".to_vec())
    );
    assert_eq!(cons.poll().expect("poll").map(|e| e.data), None)
}

#[test]
fn can_consume_incrementally() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_consume_incrementally");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    for expected in &[Some(b"0"), Some(b"1"), Some(b"2"), None] {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), expected.map(|v| v.to_vec()));
    }
}

#[test]
fn can_restart_consume_at_commit_point() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_restart_consume_at_commit_point");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        cons.commit_upto(entry.as_ref().unwrap()).expect("commit");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_progress_without_commit() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_progress_without_commit");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn can_consume_multiply() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_consume_multiply");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        let entry = cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "two").expect("consumer");
        let entry = cons.poll().expect("poll");
        if let Some(ref e) = entry {
            cons.commit_upto(&e).expect("commit");
        }
        assert_eq!(entry.map(|e| e.data), Some(b"0".to_vec()));
    }
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "two").expect("consumer");
        let entry = cons.poll().expect("poll");
        assert_eq!(entry.map(|e| e.data), Some(b"1".to_vec()));
    }
}

#[test]
fn producing_concurrently_should_never_leave_holes() {
    env_logger::init().unwrap_or(());

    let pool = pool("producing_concurrently_should_never_leave_holes");
    let mut prod1 = pg_queue::Producer::new(pool.clone()).expect("producer");
    let mut b1 = prod1.batch().expect("batch b1");
    b1.produce(b"first").expect("produce 1");

    {
        let mut prod2 = pg_queue::Producer::new(pool.clone()).expect("producer");
        let mut b2 = prod2.batch().expect("batch b2");
        b2.produce(b"second").expect("produce 2");
        b2.commit().expect("commit b1");
    }

    let observations_a = {
        let mut observations = Vec::new();
        let mut cons = pg_queue::Consumer::new(pool.clone(), "a").expect("consumer");
        while let Some(entry) = cons.poll().expect("poll") {
            observations.push(String::from_utf8_lossy(&entry.data).into_owned());
        }
        observations
    };

    b1.commit().expect("commit b1");

    let observations_b = {
        let mut observations = Vec::new();
        let mut cons = pg_queue::Consumer::new(pool.clone(), "b").expect("consumer");
        while let Some(entry) = cons.poll().expect("poll") {
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

#[test]
fn can_list_zero_consumer_offsets() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_list_zero_consumer_offsets");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    let cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert!(offsets.is_empty());
}

#[test]
fn can_list_consumer_offset() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_list_consumer_offset");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    let entry;
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        entry = cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }

    let cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert_eq!(offsets.len(), 1);
    assert_eq!(offsets.get("one"), Some(&entry.offset));
}

#[test]
fn can_list_consumer_offsets() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_list_consumer_offsets");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    let one;
    let two;
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        let entry = cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }
    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        let entry = cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
        one = entry;
    }

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "two").expect("consumer");
        let entry = cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
        two = entry;
    }

    let cons = pg_queue::Consumer::new(pool.clone(), "two").expect("consumer");
    let offsets = cons.consumers().expect("iter");
    assert_eq!(offsets.len(), 2);
    assert_eq!(offsets.get("one"), Some(&one.offset));
    assert_eq!(offsets.get("two"), Some(&two.offset));
}

#[test]
fn can_discard_entries() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_discard_entries");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "one").expect("consumer");
        let entry = cons.poll().expect("poll").expect("some entry");
        cons.commit_upto(&entry).expect("commit");
    }

    {
        let cons = pg_queue::Consumer::new(pool.clone(), "cleaner").expect("consumer");
        let one_off = cons.consumers().expect("consumers")["one"];
        cons.discard_upto(one_off).expect("discard");
    }

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "two").expect("consumer");
        let entry = cons.poll().expect("poll").expect("some entry");
        assert_eq!(&entry.data, b"1");
    }
}

#[test]
fn can_discard_on_empty() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_discard_on_empty");
    {
        let cons = pg_queue::Consumer::new(pool.clone(), "cleaner").expect("consumer");
        cons.discard_upto(42).expect("discard");
    }
}

#[test]
fn can_discard_after_written() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_discard_after_written");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");

    {
        let cons = pg_queue::Consumer::new(pool.clone(), "cleaner").expect("consumer");
        cons.discard_upto(42).expect("discard");
    }
}

#[test]
fn can_remove_consumer_offset() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_remove_consumer_offset");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let entry = cons.poll().expect("poll");
        cons.commit_upto(entry.as_ref().unwrap()).expect("commit");
    }

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let _ = cons.clear_offset().expect("clear_offset");
    }
    {
        let cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let consumers = cons.consumers().expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[test]
fn removing_non_consumer_is_noop() {
    env_logger::init().unwrap_or(());
    let pool = pool("removing_non_consumer_is_noop");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    prod.produce(b"0").expect("produce");
    prod.produce(b"1").expect("produce");
    prod.produce(b"2").expect("produce");

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let _entry = cons.poll().expect("poll");
    }

    {
        let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        cons.clear_offset().expect("clear_offset");
    }
    {
        let cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");
        let consumers = cons.consumers().expect("consumers");
        assert_eq!(consumers.get("default"), None);
    }
}

#[test]
fn can_produce_consume_with_wait() {
    env_logger::init().unwrap_or(());
    let pool = pool("can_produce_consume_with_wait");
    let mut prod = pg_queue::Producer::new(pool.clone()).expect("producer");
    let mut cons = pg_queue::Consumer::new(pool.clone(), "default").expect("consumer");

    let waiter = thread::spawn(move || {
        debug!("Awaiting");
        cons.wait_next().expect("wait")
    });

    thread::sleep(time::Duration::from_millis(5));
    debug!("Producing");
    prod.produce(b"42").expect("produce");

    assert_eq!(
        waiter
            .join()
            .map(|e| String::from_utf8_lossy(&e.data).to_string())
            .expect("join"),
        "42".to_string()
    );
}
