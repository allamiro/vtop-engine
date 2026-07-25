//! End-to-end native-broker memory budgets (#187).
//!
//! Producer flood, slow-consumer fetch queues, and oversized records must fail
//! closed with explicit overload / invalid responses — never silent drops.

use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;
use uuid::Uuid;
use vtop_broker::memory_budget::{
    BudgetRejectReason, MemoryBudgetConfig, MemoryBudgetPool, OverloadAction,
};
use vtop_broker::{LocalBroker, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, Message, ProduceRecord, ProduceRequest, RangeIdentity,
    Role, WireFrame,
};

fn range_identity() -> RangeIdentity {
    RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: Uuid::from_u128(0xD1),
        range_generation: 0,
    }
}

fn open_broker(dir: &TempDir, budget: Arc<MemoryBudgetPool>) -> (LocalBroker, RangeIdentity) {
    let range = range_identity();
    let descriptor = SegmentDescriptor {
        segment_id: Uuid::from_u128(0xD2),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id: range.range_id,
            generation: range.range_generation,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let segment = ActiveSegment::create(
        dir.path().join("range.active"),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let epochs = ProducerEpochJournal::open(dir.path().join("epochs")).unwrap();
    let broker = LocalBroker::new(segment, epochs, range.clone(), 1)
        .unwrap()
        .with_memory_budget(budget);
    (broker, range)
}

fn tiny_budget() -> Arc<MemoryBudgetPool> {
    MemoryBudgetPool::new(MemoryBudgetConfig {
        per_producer_conn_bytes: 2_048,
        per_consumer_conn_bytes: 8_192,
        per_shard_bytes: 4_096,
        process_ceiling_bytes: 4_096,
        per_follower_bytes: 2_048,
        catch_up_bytes: 1_024,
        fetch_response_queue_bytes: 2_048,
        max_record_bytes: 2_048,
        overload_block_timeout: std::time::Duration::from_millis(5),
    })
    .unwrap()
}

fn produce_frame(
    range: RangeIdentity,
    producer: Uuid,
    sequence: u64,
    request_id: u64,
    value: Vec<u8>,
) -> WireFrame {
    WireFrame {
        request_id,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range,
            fencing_epoch: 1,
            producer_id: producer,
            producer_epoch: 1,
            first_sequence: sequence,
            durability: WireDurability::LocalFsync,
            records: vec![ProduceRecord {
                timestamp_millis: 1_000,
                key: b"k".to_vec(),
                value,
            }],
        }),
    }
}

#[test]
fn producer_flood_stays_bounded_and_rejects_retryably() {
    let dir = tempfile::tempdir().unwrap();
    let budget = tiny_budget();
    let (broker, range) = open_broker(&dir, Arc::clone(&budget));
    let broker = Arc::new(broker);
    let barrier = Arc::new(Barrier::new(9));
    let mut handles = Vec::new();
    for i in 0..8 {
        let broker = Arc::clone(&broker);
        let range = range.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let producer = Uuid::from_u128(0xF0 + u128::from(i));
            let value = vec![b'x'; 800];
            broker.handle(
                Role::Producer,
                produce_frame(range, producer, 0, i + 1, value),
            )
        }));
    }
    barrier.wait();
    let mut overloaded = 0u32;
    let mut acked = 0u32;
    for handle in handles {
        match handle.join().unwrap().message {
            Message::ProduceResponse(_) => acked += 1,
            Message::Error(error) => {
                assert_eq!(error.code, ErrorCode::Overloaded);
                assert!(error.retryable);
                overloaded += 1;
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(acked >= 1, "at least one produce should succeed");
    assert!(overloaded >= 1, "flood must reject with Overloaded");
    let metrics = budget.metrics();
    assert!(metrics.rejections_total() >= 1);
    assert!(
        metrics.shard_used_bytes() <= budget.config().per_shard_bytes,
        "shard used {} exceeds ceiling {}",
        metrics.shard_used_bytes(),
        budget.config().per_shard_bytes
    );
    assert!(
        metrics.process_used_bytes() <= budget.config().process_ceiling_bytes,
        "process used {} exceeds ceiling",
        metrics.process_used_bytes()
    );
    // After all waiters return, reservations must be released.
    assert_eq!(metrics.shard_used_bytes(), 0);
    assert_eq!(metrics.process_used_bytes(), 0);
}

#[test]
fn oversized_record_rejected_before_admission() {
    let dir = tempfile::tempdir().unwrap();
    let budget = tiny_budget();
    let (broker, range) = open_broker(&dir, Arc::clone(&budget));
    let producer = Uuid::from_u128(0xE2);
    let response = broker.handle(
        Role::Producer,
        produce_frame(range, producer, 0, 1, vec![b'y'; 3_000]),
    );
    match response.message {
        Message::Error(error) => {
            assert_eq!(error.code, ErrorCode::InvalidRequest);
            assert!(!error.retryable);
            assert!(error.message.contains("max_record_bytes"));
        }
        other => panic!("expected InvalidRequest, got {other:?}"),
    }
    assert_eq!(
        budget
            .metrics()
            .rejections(BudgetRejectReason::OversizedRecord),
        1
    );
    assert_eq!(budget.metrics().shard_used_bytes(), 0);
}

#[test]
fn slow_consumer_fetch_queue_budget_rejects_retryably() {
    let dir = tempfile::tempdir().unwrap();
    let budget = tiny_budget();
    let (broker, range) = open_broker(&dir, Arc::clone(&budget));
    let producer = Uuid::from_u128(0xE3);
    // Seed one small durable record so fetch has something to return.
    let ack = broker.handle(
        Role::Producer,
        produce_frame(range.clone(), producer, 0, 1, b"ok".to_vec()),
    );
    assert!(matches!(ack.message, Message::ProduceResponse(_)));

    let consumer = budget.open_consumer_connection();
    // Exhaust the fetch-queue ceiling with an outstanding reservation.
    let _hold = budget
        .try_reserve_fetch(budget.config().fetch_response_queue_bytes, &consumer)
        .unwrap();
    let err = budget
        .try_reserve_fetch(1, &consumer)
        .expect_err("fetch queue must reject when full");
    assert_eq!(err, BudgetRejectReason::FetchQueue);
    assert_eq!(
        BudgetRejectReason::FetchQueue.default_action(),
        OverloadAction::RejectRetryable
    );
    assert_eq!(
        budget.metrics().rejections(BudgetRejectReason::FetchQueue),
        1
    );
    assert!(budget.metrics().fetch_queue_used_bytes() > 0);
}

#[test]
fn producer_connection_budget_is_enforced() {
    let dir = tempfile::tempdir().unwrap();
    let budget = tiny_budget();
    let (broker, range) = open_broker(&dir, Arc::clone(&budget));
    let producer = Uuid::from_u128(0xE4);
    let conn = budget.open_producer_connection();
    let value = vec![b'z'; 1_500];
    // First request under the connection ceiling succeeds.
    let ok = broker.handle_with_connection(
        Role::Producer,
        produce_frame(range.clone(), producer, 0, 1, value.clone()),
        Some(&conn),
    );
    assert!(matches!(ok.message, Message::ProduceResponse(_)));
    // Hold the full connection budget so the next produce is rejected.
    let _hold = budget
        .try_reserve_produce(budget.config().per_producer_conn_bytes, Some(&conn))
        .unwrap();
    let rejected = broker.handle_with_connection(
        Role::Producer,
        produce_frame(range, producer, 1, 2, value),
        Some(&conn),
    );
    match rejected.message {
        Message::Error(error) => {
            assert_eq!(error.code, ErrorCode::Overloaded);
            assert!(error.retryable);
        }
        other => panic!("expected Overloaded, got {other:?}"),
    }
    assert!(
        budget
            .metrics()
            .rejections(BudgetRejectReason::ProducerConn)
            >= 1
    );
}

#[test]
fn follower_budget_bounds_slow_replica_inflight() {
    let budget = tiny_budget();
    let follower = Arc::new(budget.open_follower());
    let a = follower.try_reserve_inflight(1_500).unwrap();
    assert!(follower.try_reserve_inflight(1_000).is_err());
    assert_eq!(
        budget
            .metrics()
            .rejections(BudgetRejectReason::ReplicaFollower),
        1
    );
    drop(a);
    let catch = follower.try_reserve_catch_up(1_000).unwrap();
    assert_eq!(
        follower.try_reserve_catch_up(100).unwrap_err(),
        BudgetRejectReason::ReplicaCatchUp
    );
    drop(catch);
    assert_eq!(follower.inflight_used_bytes(), 0);
    assert_eq!(follower.catch_up_used_bytes(), 0);
}
