//! A stock Kafka client follows a failover (#751).
//!
//! Three candidate gateways, every one listing all three as brokers, over one
//! range. librdkafka produces at `acks=all` with idempotence from a bootstrap
//! list of all three. The leader is killed with `kill -9` semantics, mid
//! stream; the lease record still names it until a candidate is granted the
//! range at the next epoch and installs its bridge. The client is given no
//! restart and no configuration change: what it needs it reads from Metadata.
//!
//! What must hold, and what each assertion is for:
//!
//! - every record the client was told was acknowledged is in the log, once, in
//!   the order it was sent, at the offset the acknowledgement named: nothing
//!   acknowledged is lost, nothing is duplicated by the client's retries;
//! - records were appended under both epochs, so the client reached the new
//!   leader and the test is not passing on the old one's tail;
//! - the log's fence, not the gateway's belief, is what refuses a deposed
//!   leader (the other test file), so this one needs no stalled node: the
//!   leader it kills is gone.

mod common;

use common::*;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const RECORDS: usize = 300;
const KILL_AFTER: usize = 100;

fn value(index: usize) -> String {
    format!("record-{index:05}")
}

#[derive(Default)]
struct Outcomes {
    delivered: Mutex<Vec<(usize, i64)>>,
    failed: Mutex<Vec<(usize, String)>>,
}

struct Context(Arc<Outcomes>);

impl ClientContext for Context {}

impl ProducerContext for Context {
    type DeliveryOpaque = Box<usize>;

    fn delivery(&self, result: &DeliveryResult<'_>, index: Box<usize>) {
        match result {
            Ok(message) => self
                .0
                .delivered
                .lock()
                .unwrap()
                .push((*index, message.offset())),
            Err((error, _)) => self
                .0
                .failed
                .lock()
                .unwrap()
                .push((*index, error.to_string())),
        }
    }
}

fn producer(bootstrap: &str, outcomes: &Arc<Outcomes>) -> BaseProducer<Context> {
    ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("acks", "all")
        .set("enable.idempotence", "true")
        .set("linger.ms", "0")
        // A failover is the only thing slowing this client down, so a retry
        // is quick and a stuck one is reported rather than waited out.
        .set("retry.backoff.ms", "50")
        .set("retry.backoff.max.ms", "200")
        .set("reconnect.backoff.ms", "50")
        .set("reconnect.backoff.max.ms", "200")
        .set("socket.connection.setup.timeout.ms", "2000")
        .set("message.timeout.ms", "40000")
        .set("broker.address.family", "v4")
        .create_with_context(Context(Arc::clone(outcomes)))
        .expect("a librdkafka producer")
}

/// Send `RECORDS` records one at a time, a few milliseconds apart, so the
/// stream is still going when `during` runs. Returns when every record has a
/// delivery report, or panics at the deadline.
fn produce_all(
    producer: &BaseProducer<Context>,
    outcomes: &Arc<Outcomes>,
    mut during: impl FnMut(usize),
) {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut next = 0;
    loop {
        let reported =
            outcomes.delivered.lock().unwrap().len() + outcomes.failed.lock().unwrap().len();
        if reported >= RECORDS {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{reported} of {RECORDS} records had a delivery report at the deadline; \
             {next} were sent; failures: {:?}",
            outcomes.failed.lock().unwrap()
        );
        during(outcomes.delivered.lock().unwrap().len());
        if next < RECORDS {
            let payload = value(next);
            let key = format!("k{next}");
            match producer.send(
                BaseRecord::with_opaque_to(TOPIC, Box::new(next))
                    .payload(&payload)
                    .key(&key)
                    .partition(0),
            ) {
                Ok(()) => next += 1,
                Err((error, _)) => {
                    // The local queue is full of records waiting out a
                    // failover; poll and try this one again.
                    assert!(
                        error.to_string().contains("Queue full"),
                        "send of record {next} failed: {error}"
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(4));
        }
        producer.poll(Duration::from_millis(1));
    }
}

/// Every acknowledged record is in the log exactly once, in send order, at the
/// offset it was acknowledged at.
fn assert_nothing_acknowledged_was_lost(shared: &Shared, outcomes: &Outcomes) {
    assert_eq!(
        *outcomes.failed.lock().unwrap(),
        Vec::<(usize, String)>::new(),
        "a delivery failed"
    );
    let log = log_values(shared);
    let delivered = outcomes.delivered.lock().unwrap();
    assert_eq!(
        delivered.len(),
        RECORDS,
        "every record has exactly one delivery report"
    );
    for (index, offset) in delivered.iter() {
        assert_eq!(
            log.get(usize::try_from(*offset).unwrap()),
            Some(&value(*index)),
            "record {index} was acknowledged at offset {offset}, and the log holds something else there"
        );
    }
    assert_eq!(
        log,
        (0..RECORDS).map(value).collect::<Vec<_>>(),
        "the log is every record, once, in the order they were sent"
    );
}

#[test]
fn a_producing_client_follows_a_killed_leader_to_the_next_one() {
    let cluster = Arc::new(Cluster::start(3));
    assert_eq!(cluster.promote(0), 1);
    let outcomes = Arc::new(Outcomes::default());
    let producer = producer(&cluster.bootstrap(), &outcomes);

    let mut killed = None;
    produce_all(&producer, &outcomes, |delivered| {
        if killed.is_none() && delivered >= KILL_AFTER {
            // kill -9 of the leader, under the stream. Its lease is still the
            // metadata plane's record, so for a while the survivors name a
            // dead holder; then the next candidate is granted the range.
            cluster.kill(0);
            let cluster = Arc::clone(&cluster);
            killed = Some(std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(400));
                cluster.promote(1)
            }));
        }
    });
    let epoch = killed
        .expect("the stream outlasted the kill point")
        .join()
        .unwrap();
    assert_eq!(epoch, 2);
    producer.flush(Duration::from_secs(10)).unwrap();

    assert_nothing_acknowledged_was_lost(&cluster.shared, &outcomes);
    let by_epoch = cluster.shared.appended_by_epoch.lock().unwrap().clone();
    assert!(
        by_epoch.get(&1).copied().unwrap_or(0) >= KILL_AFTER as u64,
        "the first leader took what was acknowledged before the kill: {by_epoch:?}"
    );
    assert!(
        by_epoch.get(&2).copied().unwrap_or(0) > 0,
        "the client never reached the new leader: {by_epoch:?}"
    );
    assert_eq!(
        by_epoch.values().sum::<u64>(),
        RECORDS as u64,
        "appended more than it was sent: {by_epoch:?}"
    );
}

#[test]
fn a_client_started_after_the_failover_with_the_dead_broker_first_still_produces() {
    let cluster = Cluster::start(3);
    cluster.promote(0);
    cluster.kill(0);
    cluster.promote(1);
    let bootstrap = cluster.bootstrap(); // node 0, the dead one, is listed first
    assert!(bootstrap.starts_with(&cluster.nodes[0].addr.to_string()));

    let outcomes = Arc::new(Outcomes::default());
    let producer = producer(&bootstrap, &outcomes);
    produce_all(&producer, &outcomes, |_| {});
    producer.flush(Duration::from_secs(10)).unwrap();

    assert_nothing_acknowledged_was_lost(&cluster.shared, &outcomes);
    let by_epoch = cluster.shared.appended_by_epoch.lock().unwrap().clone();
    assert_eq!(
        by_epoch.get(&2).copied(),
        Some(RECORDS as u64),
        "{by_epoch:?}"
    );
}
