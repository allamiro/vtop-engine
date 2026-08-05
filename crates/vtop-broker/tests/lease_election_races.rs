//! Lease-election races, decided deterministically (#223).
//!
//! `meta_lease_fencing.rs` proves that a broker holding a stale epoch is
//! refused. This file covers the step before that: the election itself, and
//! specifically the races that a timing-based design would get wrong.
//!
//! Every case here runs against the real metadata state machine with explicit
//! `issued_at_ms` values, so nothing depends on wall-clock timing or on tests
//! racing each other. Two candidates "at the same instant" means two commands
//! carrying the same timestamp applied in log order — which is exactly what
//! Raft delivers to every replica.
//!
//! The claim under test is the one the whole design rests on:
//!
//!   Expiry is liveness. The fencing epoch is safety.
//!
//! So a skewed, slow, or duplicated candidate may be *disruptive* — it can take
//! a range earlier than an operator would like — but no interleaving can
//! produce two holders that each believe they may write.

use uuid::Uuid;
use vtop_broker::{LocalBroker, MetaFencingEpoch, ProducerEpochJournal};
use vtop_log::{ActiveSegment, KeyRange, RangeLineage, SegmentConfig, SegmentDescriptor};
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaStateMachine, MetaValue, MetadataCommand, MetadataResponse,
};
use vtop_protocol::{
    Durability as WireDurability, ErrorCode, ErrorResponse, Message, ProduceRecord, ProduceRequest,
    RangeIdentity, Role, WireFrame,
};

const NODE_A: Uuid = Uuid::from_u128(0x10);
const NODE_B: Uuid = Uuid::from_u128(0x11);
const TOPIC: Uuid = Uuid::from_u128(0x20);
const RANGE: Uuid = Uuid::from_u128(0x21);

struct Cluster {
    machine: MetaStateMachine,
    index: u64,
    request: u128,
}

impl Cluster {
    /// A metadata state machine with two active nodes and one root range.
    fn new() -> Self {
        let mut cluster = Self {
            machine: MetaStateMachine::new(),
            index: 0,
            request: 0,
        };
        for (node, addr) in [(NODE_A, "a:9200"), (NODE_B, "b:9200")] {
            let env = cluster.envelope(0);
            cluster.apply(MetadataCommand::RegisterNode {
                env,
                node_uuid: node,
                addr: addr.to_owned(),
                expected_generation: None,
            });
        }
        let env = cluster.envelope(0);
        cluster.apply(MetadataCommand::CreateTopic {
            env,
            name: "events.v1".to_owned(),
            topic_uuid: TOPIC,
            root_range_uuid: RANGE,
        });
        cluster
    }

    fn envelope(&mut self, issued_at_ms: i64) -> CommandEnvelope {
        self.request += 1;
        CommandEnvelope {
            request_id: Uuid::from_u128(0xbeef_0000_0000 + self.request),
            issued_at_ms,
        }
    }

    fn apply(&mut self, command: MetadataCommand) -> MetadataResponse {
        self.index += 1;
        self.machine.apply(self.index, &command)
    }

    fn acquire(&mut self, holder: Uuid, now_ms: i64, duration_ms: u64) -> MetadataResponse {
        let generation = self.range_generation();
        let env = self.envelope(now_ms);
        self.apply(MetadataCommand::AcquireRangeLease {
            env,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            holder_node_uuid: holder,
            expected_range_generation: generation,
            lease_duration_ms: duration_ms,
        })
    }

    /// Acquire with a deliberately stale CAS token, as a candidate that read
    /// the range before someone else changed it would.
    fn acquire_with_generation(
        &mut self,
        holder: Uuid,
        now_ms: i64,
        duration_ms: u64,
        generation: u64,
    ) -> MetadataResponse {
        let env = self.envelope(now_ms);
        self.apply(MetadataCommand::AcquireRangeLease {
            env,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            holder_node_uuid: holder,
            expected_range_generation: generation,
            lease_duration_ms: duration_ms,
        })
    }

    fn renew(
        &mut self,
        holder: Uuid,
        epoch: u64,
        now_ms: i64,
        duration_ms: u64,
    ) -> MetadataResponse {
        let env = self.envelope(now_ms);
        self.apply(MetadataCommand::RenewRangeLease {
            env,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            holder_node_uuid: holder,
            expected_fencing_epoch: epoch,
            lease_duration_ms: duration_ms,
        })
    }

    fn range_generation(&self) -> u64 {
        self.range().generation
    }

    fn range(&self) -> vtop_meta::RangeRecord {
        let Some(MetaValue::Range(range)) = self.machine.record(&MetaKey::Range {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
        }) else {
            panic!("range record missing")
        };
        range.clone()
    }

    fn holder(&self) -> Option<Uuid> {
        self.range().lease.map(|lease| lease.holder_node_uuid)
    }
}

fn granted(response: &MetadataResponse) -> Option<u64> {
    match response {
        MetadataResponse::LeaseGranted { fencing_epoch } => Some(*fencing_epoch),
        _ => None,
    }
}

/// Two candidates racing for a free range: exactly one wins.
///
/// Not a timing property — Raft serialises the two proposals, and the loser's
/// CAS token is stale by the time its command applies.
#[test]
fn only_one_of_two_simultaneous_candidates_wins() {
    let mut cluster = Cluster::new();
    let generation = cluster.range_generation();

    // Both read the same generation, then both propose. This is precisely the
    // interleaving an election produces when two followers notice an expiry in
    // the same instant.
    let first = cluster.acquire_with_generation(NODE_A, 1_000, 10_000, generation);
    let second = cluster.acquire_with_generation(NODE_B, 1_000, 10_000, generation);

    let winners = [granted(&first), granted(&second)]
        .into_iter()
        .flatten()
        .count();
    assert_eq!(
        winners, 1,
        "exactly one candidate may win a round; got {first:?} and {second:?}"
    );
    assert_eq!(
        cluster.holder(),
        Some(NODE_A),
        "the first command in log order wins; the second's token is stale"
    );
}

/// The safety claim, stated as a test: however early a skewed candidate
/// acquires, the epoch it mints fences the previous holder.
///
/// A design that relied on clocks agreeing would fail here. This one does not
/// need them to agree — only to be monotonic in the log.
#[test]
fn a_clock_skewed_candidate_is_disruptive_but_never_produces_two_writers() {
    let mut cluster = Cluster::new();
    let held = granted(&cluster.acquire(NODE_A, 1_000, 60_000)).expect("first grant");

    // B's clock is an hour fast, so it believes A's lease lapsed long ago.
    // It is wrong, and it wins anyway — that is the disruption.
    let stolen = granted(&cluster.acquire(NODE_B, 3_601_000, 60_000))
        .expect("a skewed candidate can take the range early");

    assert!(
        stolen > held,
        "the steal must mint a higher epoch ({stolen} <= {held})"
    );
    assert_eq!(cluster.holder(), Some(NODE_B));

    // And now the safety half: A's broker, still holding the old epoch, is
    // refused. Two holders existed in metadata for zero instants, and two
    // *writers* can never exist because the epoch gates the data path.
    let dir = tempfile::tempdir().unwrap();
    let fencing = MetaFencingEpoch::new(held);
    let (broker, range) = open_broker(dir.path(), held, fencing.clone());
    fencing.set(stolen);

    let response = broker.handle(Role::Producer, produce(range, held));
    assert!(
        matches!(
            response.message,
            Message::Error(ErrorResponse {
                code: ErrorCode::Fenced,
                ..
            })
        ),
        "the displaced holder must be fenced, got {:?}",
        response.message
    );
}

/// A renewal that arrives after a steal must not resurrect the old holder.
///
/// This is the interleaving a partitioned leader creates: it kept renewing
/// into a network that was dropping its packets, and the renewals land after
/// the range has already moved.
#[test]
fn a_late_renewal_from_a_displaced_holder_is_refused() {
    let mut cluster = Cluster::new();
    let held = granted(&cluster.acquire(NODE_A, 1_000, 5_000)).expect("first grant");
    let stolen = granted(&cluster.acquire(NODE_B, 6_001, 5_000)).expect("takeover after expiry");

    let late = cluster.renew(NODE_A, held, 6_500, 5_000);
    assert!(
        matches!(late, MetadataResponse::Rejected { .. }),
        "a renewal naming a superseded epoch must be refused: {late:?}"
    );
    assert_eq!(
        cluster.holder(),
        Some(NODE_B),
        "the range must stay with the new holder"
    );
    assert_eq!(cluster.range().fencing_epoch, stolen);
}

/// A holder that keeps renewing keeps the range, however many rounds pass.
///
/// The liveness counterpart: without this the design would be safe and useless.
#[test]
fn a_renewing_holder_is_never_displaced() {
    let mut cluster = Cluster::new();
    let held = granted(&cluster.acquire(NODE_A, 1_000, 10_000)).expect("first grant");

    let mut now = 1_000;
    for _ in 0..10 {
        now += 3_000;
        assert!(
            granted(&cluster.renew(NODE_A, held, now, 10_000)).is_some(),
            "a timely renewal must succeed at {now}"
        );
        let challenge = cluster.acquire(NODE_B, now, 10_000);
        assert!(
            matches!(challenge, MetadataResponse::Rejected { .. }),
            "a live lease must not be stealable at {now}: {challenge:?}"
        );
    }
    assert_eq!(cluster.holder(), Some(NODE_A));
    assert_eq!(
        cluster.range().lease.unwrap().fencing_epoch,
        held,
        "renewals must not churn the epoch; a producer's in-flight requests \
         would be fenced by their own leader"
    );
}

/// Duplicate acquisitions from the same node must not ratchet the epoch on
/// every retry: a retrying agent would otherwise fence its own broker
/// repeatedly, each time invalidating the epoch it had just adopted.
#[test]
fn a_holder_reacquiring_still_mints_exactly_one_epoch_per_round() {
    let mut cluster = Cluster::new();
    let first = granted(&cluster.acquire(NODE_A, 1_000, 10_000)).expect("first grant");
    // Re-acquisition by the SAME holder is permitted — it is how an agent that
    // lost track of its epoch recovers — but it is not free.
    let second = granted(&cluster.acquire(NODE_A, 2_000, 10_000)).expect("self-reacquire");
    assert_eq!(
        second,
        first + 1,
        "each acquisition mints exactly one epoch, so an agent can tell whether \
         it is the current holder"
    );
}

fn open_broker(
    dir: &std::path::Path,
    held_epoch: u64,
    meta_epoch: MetaFencingEpoch,
) -> (LocalBroker, RangeIdentity) {
    let range = RangeIdentity {
        topic: "events.v1".to_owned(),
        topic_epoch: 1,
        range_id: RANGE,
        range_generation: 0,
    };
    let descriptor = SegmentDescriptor {
        segment_id: Uuid::from_u128(0x30),
        topic: range.topic.clone(),
        topic_epoch: range.topic_epoch,
        lineage: RangeLineage {
            range_id: RANGE,
            generation: 0,
            key_range: KeyRange::full(),
            parents: Vec::new(),
        },
        base_offset: 0,
    };
    let segment = ActiveSegment::create(
        dir.join(format!("seg-{held_epoch}.active")),
        descriptor,
        SegmentConfig::default(),
    )
    .unwrap();
    let epochs = ProducerEpochJournal::open(dir.join(format!("epochs-{held_epoch}"))).unwrap();
    let broker = LocalBroker::with_meta_fencing_epoch(
        segment,
        epochs,
        range.clone(),
        held_epoch,
        meta_epoch,
    )
    .unwrap();
    (broker, range)
}

fn produce(range: RangeIdentity, fencing_epoch: u64) -> WireFrame {
    WireFrame {
        request_id: 1,
        stream_id: 1,
        message: Message::ProduceRequest(ProduceRequest {
            range,
            fencing_epoch,
            producer_id: Uuid::from_u128(0x99),
            producer_epoch: 1,
            first_sequence: 0,
            durability: WireDurability::LocalFsync,
            records: vec![ProduceRecord {
                timestamp_millis: 1,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }],
        }),
    }
}
