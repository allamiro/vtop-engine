//! Full command matrix for the deterministic metadata state machine: every
//! command's success path and every deterministic rejection, fencing-epoch
//! monotonicity across grant/release interleavings, dedup semantics across
//! snapshot/restore and FIFO eviction, and byte-identical snapshots from
//! independently driven instances.

use uuid::Uuid;
use vtop_meta::{
    CommandEnvelope, MetaKey, MetaStateMachine, MetaValue, MetadataCommand, MetadataError,
    MetadataResponse, NodeState, RangeAssignment, SegmentState, DEDUP_CAPACITY,
    MAX_NODE_ADDR_BYTES, MAX_TOPIC_NAME_BYTES,
};

const NODE: Uuid = Uuid::from_u128(0x10);
const NODE_B: Uuid = Uuid::from_u128(0x11);
const TOPIC: Uuid = Uuid::from_u128(0x20);
const RANGE: Uuid = Uuid::from_u128(0x21);
const SEGMENT: Uuid = Uuid::from_u128(0x30);
const KEY: Uuid = Uuid::from_u128(0x40);
const GROUP: Uuid = Uuid::from_u128(0x50);
const MEMBER: Uuid = Uuid::from_u128(0x51);
const MEMBER_B: Uuid = Uuid::from_u128(0x52);

/// Deterministic unique request ids for commands whose dedup identity is
/// irrelevant to the test at hand.
struct Requests(u128);

impl Requests {
    fn next(&mut self) -> CommandEnvelope {
        self.0 += 1;
        CommandEnvelope {
            request_id: Uuid::from_u128(0xdead_0000_0000 + self.0),
            issued_at_ms: 1_750_000_000_000,
        }
    }
}

fn register_node(requests: &mut Requests, node: Uuid) -> MetadataCommand {
    MetadataCommand::RegisterNode {
        env: requests.next(),
        node_uuid: node,
        addr: "10.0.0.1:9200".to_owned(),
        expected_generation: None,
    }
}

fn create_topic(requests: &mut Requests) -> MetadataCommand {
    MetadataCommand::CreateTopic {
        env: requests.next(),
        name: "events.v1".to_owned(),
        topic_uuid: TOPIC,
        root_range_uuid: RANGE,
    }
}

fn grant(requests: &mut Requests, holder: Uuid, expected_range_generation: u64) -> MetadataCommand {
    MetadataCommand::GrantRangeLease {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        holder_node_uuid: holder,
        expected_range_generation,
    }
}

fn release(requests: &mut Requests, expected_fencing_epoch: u64) -> MetadataCommand {
    MetadataCommand::ReleaseRangeLease {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        expected_fencing_epoch,
    }
}

fn machine_with_topic_and_node(requests: &mut Requests) -> MetaStateMachine {
    let mut machine = MetaStateMachine::new();
    assert_eq!(
        machine.apply(1, &register_node(requests, NODE)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(2, &create_topic(requests)),
        MetadataResponse::TopicCreated {
            topic_uuid: TOPIC,
            topic_epoch: 1,
            root_range_uuid: RANGE,
        }
    );
    machine
}

fn rejected(error: MetadataError) -> MetadataResponse {
    MetadataResponse::Rejected(error)
}

#[test]
fn register_node_covers_create_reregister_and_every_rejection() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();

    // Absent + CAS expectation: nothing to CAS against.
    assert_eq!(
        machine.apply(
            1,
            &MetadataCommand::RegisterNode {
                env: requests.next(),
                node_uuid: NODE,
                addr: "a:1".to_owned(),
                expected_generation: Some(0),
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // First registration.
    assert_eq!(
        machine.apply(2, &register_node(&mut requests, NODE)),
        MetadataResponse::Ack { generation: 0 }
    );
    // Present + no expectation: uuid collision.
    assert_eq!(
        machine.apply(3, &register_node(&mut requests, NODE)),
        rejected(MetadataError::AlreadyExists)
    );
    // CAS with the wrong generation.
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::RegisterNode {
                env: requests.next(),
                node_uuid: NODE,
                addr: "b:2".to_owned(),
                expected_generation: Some(7),
            }
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 7,
            actual: 0,
        })
    );
    // CAS re-registration succeeds, resets the state to Active, bumps the
    // generation, and rewrites the address.
    assert_eq!(
        machine.apply(
            5,
            &MetadataCommand::SetNodeState {
                env: requests.next(),
                node_uuid: NODE,
                state: NodeState::Draining,
                expected_generation: 0,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            6,
            &MetadataCommand::RegisterNode {
                env: requests.next(),
                node_uuid: NODE,
                addr: "10.0.0.9:9200".to_owned(),
                expected_generation: Some(1),
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    let Some(MetaValue::Node(node)) = machine.record(&MetaKey::Node { node_uuid: NODE }) else {
        panic!("node record must exist");
    };
    assert_eq!(node.addr, "10.0.0.9:9200");
    assert_eq!(node.state, NodeState::Active);
    assert_eq!(node.generation, 2);

    // Address bounds are re-checked in apply, not just the codec.
    assert_eq!(
        machine.apply(
            7,
            &MetadataCommand::RegisterNode {
                env: requests.next(),
                node_uuid: NODE_B,
                addr: "x".repeat(MAX_NODE_ADDR_BYTES + 1),
                expected_generation: None,
            }
        ),
        rejected(MetadataError::limit(
            "node address must be 1..=256 bytes, got 257"
        ))
    );
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::RegisterNode {
                env: requests.next(),
                node_uuid: NODE_B,
                addr: String::new(),
                expected_generation: None,
            }
        ),
        rejected(MetadataError::limit(
            "node address must be 1..=256 bytes, got 0"
        ))
    );
}

#[test]
fn set_node_state_enforces_guarded_transitions_cas_and_existence() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    assert_eq!(
        machine.apply(
            1,
            &MetadataCommand::SetNodeState {
                env: requests.next(),
                node_uuid: NODE,
                state: NodeState::Draining,
                expected_generation: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    machine.apply(2, &register_node(&mut requests, NODE));

    let mut set_state = |machine: &mut MetaStateMachine, state, expected_generation| {
        machine.apply(
            3,
            &MetadataCommand::SetNodeState {
                env: requests.next(),
                node_uuid: NODE,
                state,
                expected_generation,
            },
        )
    };

    // CAS mismatch reports the authoritative generation.
    assert_eq!(
        set_state(&mut machine, NodeState::Draining, 9),
        rejected(MetadataError::GenerationMismatch {
            expected: 9,
            actual: 0,
        })
    );
    // Same-state writes are invalid transitions, not silent no-ops.
    assert!(matches!(
        set_state(&mut machine, NodeState::Active, 0),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));
    // Legal walk: Active -> Draining -> Active -> Draining -> Dead.
    assert_eq!(
        set_state(&mut machine, NodeState::Draining, 0),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        set_state(&mut machine, NodeState::Active, 1),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        set_state(&mut machine, NodeState::Draining, 2),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        set_state(&mut machine, NodeState::Dead, 3),
        MetadataResponse::Ack { generation: 4 }
    );
    // Dead is terminal for SetNodeState.
    for target in [NodeState::Active, NodeState::Draining, NodeState::Dead] {
        assert!(matches!(
            set_state(&mut machine, target, 4),
            MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
        ));
    }
}

#[test]
fn create_topic_allocates_epochs_creates_the_root_range_and_rejects_collisions() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    assert_eq!(
        machine.apply(1, &create_topic(&mut requests)),
        MetadataResponse::TopicCreated {
            topic_uuid: TOPIC,
            topic_epoch: 1,
            root_range_uuid: RANGE,
        }
    );
    let Some(MetaValue::Range(range)) = machine.record(&MetaKey::Range {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
    }) else {
        panic!("root range must exist");
    };
    assert_eq!(
        (
            range.generation,
            range.key_prefix,
            range.key_prefix_bits,
            range.fencing_epoch
        ),
        (0, 0, 0, 0),
        "the root range covers the full key interval at generation 0"
    );
    assert!(range.lease.is_none());

    // Proposer-supplied uuid collision.
    assert_eq!(
        machine.apply(2, &create_topic(&mut requests)),
        rejected(MetadataError::AlreadyExists)
    );
    // Name bounds are enforced in apply.
    assert!(matches!(
        machine.apply(
            3,
            &MetadataCommand::CreateTopic {
                env: requests.next(),
                name: "n".repeat(MAX_TOPIC_NAME_BYTES + 1),
                topic_uuid: Uuid::from_u128(0x99),
                root_range_uuid: Uuid::from_u128(0x9a),
            }
        ),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
    assert!(matches!(
        machine.apply(
            4,
            &MetadataCommand::CreateTopic {
                env: requests.next(),
                name: String::new(),
                topic_uuid: Uuid::from_u128(0x99),
                root_range_uuid: Uuid::from_u128(0x9a),
            }
        ),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
}

#[test]
fn recreating_a_topic_name_bumps_the_topic_epoch_and_rebinds_the_name() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    machine.apply(1, &create_topic(&mut requests));

    let second_uuid = Uuid::from_u128(0x22);
    let second_range = Uuid::from_u128(0x23);
    assert_eq!(
        machine.apply(
            2,
            &MetadataCommand::CreateTopic {
                env: requests.next(),
                name: "events.v1".to_owned(),
                topic_uuid: second_uuid,
                root_range_uuid: second_range,
            }
        ),
        MetadataResponse::TopicCreated {
            topic_uuid: second_uuid,
            topic_epoch: 2,
            root_range_uuid: second_range,
        }
    );
    let Some(MetaValue::TopicName(name)) = machine.record(&MetaKey::TopicByName {
        name: "events.v1".to_owned(),
    }) else {
        panic!("name record must exist");
    };
    assert_eq!(name.topic_uuid, second_uuid);
    assert_eq!(name.latest_epoch, 2);

    // A third incarnation keeps climbing; epochs never repeat for a name.
    assert_eq!(
        machine.apply(
            3,
            &MetadataCommand::CreateTopic {
                env: requests.next(),
                name: "events.v1".to_owned(),
                topic_uuid: Uuid::from_u128(0x24),
                root_range_uuid: Uuid::from_u128(0x25),
            }
        ),
        MetadataResponse::TopicCreated {
            topic_uuid: Uuid::from_u128(0x24),
            topic_epoch: 3,
            root_range_uuid: Uuid::from_u128(0x25),
        }
    );
}

#[test]
fn grant_and_release_cover_success_cas_epoch_and_holder_rejections() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);

    // Holder must exist.
    assert_eq!(
        machine.apply(3, &grant(&mut requests, NODE_B, 0)),
        rejected(MetadataError::NotFound)
    );
    // Range must exist.
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::GrantRangeLease {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: Uuid::from_u128(0xff),
                holder_node_uuid: NODE,
                expected_range_generation: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // CAS mismatch.
    assert_eq!(
        machine.apply(5, &grant(&mut requests, NODE, 3)),
        rejected(MetadataError::GenerationMismatch {
            expected: 3,
            actual: 0,
        })
    );
    // Success mints epoch 1 and records the apply index.
    assert_eq!(
        machine.apply(6, &grant(&mut requests, NODE, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    let Some(MetaValue::Range(range)) = machine.record(&MetaKey::Range {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
    }) else {
        panic!("range must exist");
    };
    let lease = range.lease.clone().expect("lease must be recorded");
    assert_eq!(lease.holder_node_uuid, NODE);
    assert_eq!(lease.fencing_epoch, 1);
    assert_eq!(lease.granted_apply_index, 6);

    // A non-active holder cannot take a lease.
    machine.apply(
        7,
        &MetadataCommand::SetNodeState {
            env: requests.next(),
            node_uuid: NODE,
            state: NodeState::Draining,
            expected_generation: 0,
        },
    );
    assert!(matches!(
        machine.apply(8, &grant(&mut requests, NODE, 1)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));

    // Release: epoch mismatch, then success, then no-lease rejection.
    assert_eq!(
        machine.apply(9, &release(&mut requests, 5)),
        rejected(MetadataError::EpochMismatch {
            expected: 5,
            actual: 1,
        })
    );
    assert_eq!(
        machine.apply(10, &release(&mut requests, 1)),
        MetadataResponse::Ack { generation: 2 }
    );
    assert!(matches!(
        machine.apply(11, &release(&mut requests, 1)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::ReleaseRangeLease {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: Uuid::from_u128(0xff),
                expected_fencing_epoch: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
}

#[test]
fn fencing_epochs_are_strictly_monotonic_across_grant_release_interleavings() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);
    machine.apply(3, &register_node(&mut requests, NODE_B));

    let mut apply_index = 4;
    let mut range_generation = 0_u64;
    let mut last_epoch = 0_u64;
    // Deterministic interleaving: grants (alternating holders, including
    // steals from a live holder) and releases in several patterns.
    for round in 0_u64..64 {
        let holder = if round % 2 == 0 { NODE } else { NODE_B };
        let response = machine.apply(apply_index, &grant(&mut requests, holder, range_generation));
        apply_index += 1;
        let MetadataResponse::LeaseGranted { fencing_epoch } = response else {
            panic!("grant {round} must succeed, got {response:?}");
        };
        assert!(
            fencing_epoch > last_epoch,
            "grant {round} minted epoch {fencing_epoch} <= {last_epoch}"
        );
        last_epoch = fencing_epoch;
        range_generation += 1;
        // Release on every third round; the epoch must never move.
        if round % 3 == 0 {
            let response = machine.apply(apply_index, &release(&mut requests, last_epoch));
            apply_index += 1;
            assert_eq!(
                response,
                MetadataResponse::Ack {
                    generation: range_generation + 1
                }
            );
            range_generation += 1;
            let Some(MetaValue::Range(range)) = machine.record(&MetaKey::Range {
                topic_uuid: TOPIC,
                range_uuid: RANGE,
            }) else {
                panic!("range must exist");
            };
            assert_eq!(
                range.fencing_epoch, last_epoch,
                "release must not rewind the fencing epoch"
            );
        }
    }
    assert_eq!(last_epoch, 64);
}

#[test]
fn sealed_segment_registration_and_verification_cover_every_rejection() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);
    machine.apply(3, &grant(&mut requests, NODE, 0));

    let seal = |requests: &mut Requests, sealed_by_epoch, expected_range_generation| {
        MetadataCommand::RegisterSealedSegment {
            env: requests.next(),
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
            segment_generation: 0,
            base_offset: 0,
            next_offset: 128,
            content_root: [7; 32],
            sealed_by_epoch,
            expected_range_generation,
        }
    };

    // Missing range.
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: Uuid::from_u128(0xff),
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // Stale sealer epoch is fenced even with a fresh CAS token.
    assert_eq!(
        machine.apply(5, &seal(&mut requests, 0, 1)),
        rejected(MetadataError::EpochMismatch {
            expected: 0,
            actual: 1,
        })
    );
    // CAS mismatch.
    assert_eq!(
        machine.apply(6, &seal(&mut requests, 1, 9)),
        rejected(MetadataError::GenerationMismatch {
            expected: 9,
            actual: 1,
        })
    );
    // Regressing offsets.
    assert!(matches!(
        machine.apply(
            7,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 128,
                next_offset: 0,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));
    // Success bumps the range generation.
    assert_eq!(
        machine.apply(8, &seal(&mut requests, 1, 1)),
        MetadataResponse::Ack { generation: 2 }
    );
    // Segment uuid collision.
    assert_eq!(
        machine.apply(9, &seal(&mut requests, 1, 2)),
        rejected(MetadataError::AlreadyExists)
    );

    let verify = |requests: &mut Requests, content_root, expected_generation| {
        MetadataCommand::MarkSegmentVerified {
            env: requests.next(),
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
            content_root,
            expected_generation,
        }
    };

    // Missing segment.
    assert_eq!(
        machine.apply(
            10,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: Uuid::from_u128(0xfe),
                content_root: [7; 32],
                expected_generation: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // CAS mismatch against the segment generation.
    assert_eq!(
        machine.apply(11, &verify(&mut requests, [7; 32], 4)),
        rejected(MetadataError::GenerationMismatch {
            expected: 4,
            actual: 0,
        })
    );
    // Verifier disagreeing about content is an invalid transition.
    assert!(matches!(
        machine.apply(12, &verify(&mut requests, [8; 32], 0)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));
    // Success flips the state and bumps the segment generation.
    assert_eq!(
        machine.apply(13, &verify(&mut requests, [7; 32], 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::Verified);
    // Double verification is rejected.
    assert!(matches!(
        machine.apply(14, &verify(&mut requests, [7; 32], 1)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));
}

#[test]
fn sealing_requires_a_live_lease_before_any_grant_and_after_release() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);

    let seal =
        |requests: &mut Requests, segment: u128, sealed_by_epoch, expected_range_generation| {
            MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: Uuid::from_u128(segment),
                segment_generation: 0,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch,
                expected_range_generation,
            }
        };

    // A fresh range sits at epoch 0 with generation 0, so a forged
    // registration carrying exactly those default values must still be
    // rejected: no lease was ever granted, so nobody holds the authority.
    assert!(matches!(
        machine.apply(3, &seal(&mut requests, 0x31, 0, 0)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));

    // Grant (epoch 1, generation 1), then release (generation 2): the
    // epoch still "matches" after release, but the lease is gone and the
    // authority with it.
    machine.apply(4, &grant(&mut requests, NODE, 0));
    assert_eq!(
        machine.apply(5, &release(&mut requests, 1)),
        MetadataResponse::Ack { generation: 2 }
    );
    assert!(matches!(
        machine.apply(6, &seal(&mut requests, 0x32, 1, 2)),
        MetadataResponse::Rejected(MetadataError::InvalidTransition(_))
    ));

    // A re-grant restores authority under the freshly minted epoch.
    machine.apply(7, &grant(&mut requests, NODE, 2));
    assert_eq!(
        machine.apply(8, &seal(&mut requests, 0x33, 2, 3)),
        MetadataResponse::Ack { generation: 4 }
    );
}

#[test]
fn verifying_a_segment_at_the_generation_ceiling_is_rejected_not_wrapped() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);
    machine.apply(3, &grant(&mut requests, NODE, 0));

    // Registration accepts any proposer-supplied generation, including the
    // ceiling; the increment on verification must reject deterministically
    // rather than wrap (or panic every replica in checked builds).
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: u64::MAX,
                base_offset: 0,
                next_offset: 128,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert!(matches!(
        machine.apply(
            5,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: [7; 32],
                expected_generation: u64::MAX,
            }
        ),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
    // The rejection left the segment untouched: still unverified, still at
    // the ceiling generation.
    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::SealedUnverified);
    assert_eq!(segment.segment_generation, u64::MAX);
}

#[test]
fn put_key_record_creates_immutable_records_and_rejects_collisions() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    let put = |requests: &mut Requests| MetadataCommand::PutKeyRecord {
        env: requests.next(),
        key_uuid: KEY,
        scheme: 1,
        public_material_digest: [9; 32],
    };
    assert_eq!(
        machine.apply(1, &put(&mut requests)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(2, &put(&mut requests)),
        rejected(MetadataError::AlreadyExists)
    );
}

#[test]
fn duplicate_request_ids_return_the_stored_original_response_even_across_restore() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    let create = create_topic(&mut requests);
    let original = machine.apply(1, &create);
    assert!(matches!(original, MetadataResponse::TopicCreated { .. }));

    // Replay: state would now answer AlreadyExists, but dedup preserves the
    // original success.
    assert_eq!(machine.apply(2, &create), original);

    // Rejections are deduplicated too: the client must mint a new request.
    let bad_release = release(&mut requests, 42);
    let rejection = machine.apply(3, &bad_release);
    assert!(matches!(rejection, MetadataResponse::Rejected(_)));
    assert_eq!(machine.apply(4, &bad_release), rejection);

    // The dedup table travels inside the snapshot.
    let restored_bytes = machine.encode_snapshot().unwrap();
    let mut restored = MetaStateMachine::decode_snapshot(&restored_bytes).unwrap();
    assert_eq!(restored.apply(5, &create), original);
    assert_eq!(restored.apply(6, &bad_release), rejection);
}

#[test]
fn dedup_table_evicts_in_fifo_apply_order_at_exactly_its_capacity() {
    let mut machine = MetaStateMachine::new();
    let first = MetadataCommand::PutKeyRecord {
        env: CommandEnvelope {
            request_id: Uuid::from_u128(1),
            issued_at_ms: 0,
        },
        key_uuid: Uuid::from_u128(0x1000),
        scheme: 1,
        public_material_digest: [1; 32],
    };
    assert_eq!(
        machine.apply(1, &first),
        MetadataResponse::Ack { generation: 0 }
    );
    // Fill the table to capacity with distinct requests.
    for extra in 0..DEDUP_CAPACITY - 1 {
        let command = MetadataCommand::ReleaseRangeLease {
            env: CommandEnvelope {
                request_id: Uuid::from_u128(0x2000 + extra as u128),
                issued_at_ms: 0,
            },
            topic_uuid: Uuid::from_u128(0x77),
            range_uuid: Uuid::from_u128(0x78),
            expected_fencing_epoch: 0,
        };
        machine.apply(2 + extra as u64, &command);
    }
    assert_eq!(machine.dedup_len(), DEDUP_CAPACITY);
    // Still deduplicated at exact capacity.
    assert_eq!(
        machine.apply(70_000, &first),
        MetadataResponse::Ack { generation: 0 }
    );
    // One more unique request evicts the oldest entry (the first command,
    // whose replay above did not refresh its FIFO position)...
    let overflow = MetadataCommand::ReleaseRangeLease {
        env: CommandEnvelope {
            request_id: Uuid::from_u128(0x9_0000_0000),
            issued_at_ms: 0,
        },
        topic_uuid: Uuid::from_u128(0x77),
        range_uuid: Uuid::from_u128(0x78),
        expected_fencing_epoch: 0,
    };
    machine.apply(70_001, &overflow);
    assert_eq!(machine.dedup_len(), DEDUP_CAPACITY);
    // ...so replaying the first request now re-executes it and hits the
    // uuid collision instead of the stored ack: the entry was evicted.
    assert_eq!(
        machine.apply(70_002, &first),
        MetadataResponse::Rejected(MetadataError::AlreadyExists)
    );
}

#[test]
fn two_instances_applying_the_same_sequence_produce_byte_identical_snapshots() {
    let build = || {
        let mut requests = Requests(0);
        let mut machine = machine_with_topic_and_node(&mut requests);
        machine.apply(3, &register_node(&mut requests, NODE_B));
        machine.apply(4, &grant(&mut requests, NODE, 0));
        machine.apply(
            5,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 64,
                content_root: [3; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            },
        );
        machine.apply(6, &release(&mut requests, 1));
        machine.apply(
            7,
            &MetadataCommand::PutKeyRecord {
                env: requests.next(),
                key_uuid: KEY,
                scheme: 2,
                public_material_digest: [5; 32],
            },
        );
        // Include a rejection so dedup entries with errors are covered.
        machine.apply(8, &release(&mut requests, 99));
        machine
    };
    let first = build().encode_snapshot().unwrap();
    let second = build().encode_snapshot().unwrap();
    assert_eq!(first, second);
    assert_eq!(
        MetaStateMachine::decode_snapshot(&first)
            .unwrap()
            .encode_snapshot()
            .unwrap(),
        first,
        "decode/encode must be a fixed point"
    );
}

fn create_group(requests: &mut Requests) -> MetadataCommand {
    MetadataCommand::CreateConsumerGroup {
        env: requests.next(),
        name: "audit.consumers".to_owned(),
        group_uuid: GROUP,
    }
}

fn join_member(
    requests: &mut Requests,
    member: Uuid,
    expected_group_generation: u64,
) -> MetadataCommand {
    MetadataCommand::JoinConsumerGroup {
        env: requests.next(),
        group_uuid: GROUP,
        member_uuid: member,
        expected_group_generation,
    }
}

fn assign_member(
    requests: &mut Requests,
    member: Uuid,
    expected_member_generation: u64,
) -> MetadataCommand {
    MetadataCommand::AssignMemberRanges {
        env: requests.next(),
        group_uuid: GROUP,
        member_uuid: member,
        ranges: vec![RangeAssignment {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
        }],
        expected_member_generation,
    }
}

fn machine_with_group(requests: &mut Requests) -> MetaStateMachine {
    let mut machine = machine_with_topic_and_node(requests);
    assert_eq!(
        machine.apply(3, &create_group(requests)),
        MetadataResponse::GroupCreated {
            group_uuid: GROUP,
            generation: 0,
        }
    );
    assert_eq!(
        machine.apply(4, &join_member(requests, MEMBER, 0)),
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: 1,
        }
    );
    assert_eq!(
        machine.apply(5, &assign_member(requests, MEMBER, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    machine
}

#[test]
fn consumer_group_join_leave_assign_and_rejections() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);

    assert_eq!(
        machine.apply(3, &create_group(&mut requests)),
        MetadataResponse::GroupCreated {
            group_uuid: GROUP,
            generation: 0,
        }
    );
    assert_eq!(
        machine.apply(4, &create_group(&mut requests)),
        rejected(MetadataError::AlreadyExists)
    );
    assert_eq!(
        machine.apply(
            5,
            &MetadataCommand::CreateConsumerGroup {
                env: requests.next(),
                name: "audit.consumers".to_owned(),
                group_uuid: Uuid::from_u128(0x99),
            }
        ),
        rejected(MetadataError::AlreadyExists)
    );
    assert_eq!(
        machine.apply(6, &join_member(&mut requests, MEMBER, 7)),
        rejected(MetadataError::GenerationMismatch {
            expected: 7,
            actual: 0,
        })
    );
    assert_eq!(
        machine.apply(7, &join_member(&mut requests, MEMBER, 0)),
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: 1,
        }
    );
    assert_eq!(
        machine.apply(8, &join_member(&mut requests, MEMBER, 1)),
        rejected(MetadataError::AlreadyExists)
    );
    assert_eq!(
        machine.apply(9, &assign_member(&mut requests, MEMBER, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            10,
            &MetadataCommand::LeaveConsumerGroup {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                expected_member_generation: 0,
            }
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(
        machine.apply(
            11,
            &MetadataCommand::LeaveConsumerGroup {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                expected_member_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert!(machine
        .record(&MetaKey::GroupMember {
            group_uuid: GROUP,
            member_uuid: MEMBER,
        })
        .is_none());
    assert_eq!(
        machine.apply(12, &join_member(&mut requests, MEMBER_B, 3)),
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: 4,
        }
    );
}

#[test]
fn lineage_aware_cursor_commit_cas_monotonicity_and_lineage_guards() {
    let mut requests = Requests(0);
    let mut machine = machine_with_group(&mut requests);
    let root = [9u8; 32];

    assert_eq!(
        machine.apply(6, &grant(&mut requests, NODE, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    assert_eq!(
        machine.apply(
            7,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 100,
                content_root: root,
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );

    // Unregistered segment identity is rejected fail-closed.
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: Uuid::from_u128(0x999),
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        rejected(MetadataError::NotFound)
    );

    // Wrong topic epoch is rejected before any durable write.
    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 99,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        rejected(MetadataError::EpochMismatch {
            expected: 99,
            actual: 1,
        })
    );

    // First commit creates checkpoint generation 0.
    assert_eq!(
        machine.apply(
            10,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 0,
        }
    );
    assert_eq!(
        machine.apply(
            11,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        rejected(MetadataError::AlreadyExists)
    );

    // Stale CAS generation is rejected.
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 20,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: Some(7),
            }
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 7,
            actual: 0,
        })
    );

    // Backward move within the same segment is rejected.
    assert_eq!(
        machine.apply(
            13,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 5,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: Some(0),
            }
        ),
        rejected(MetadataError::invalid_transition(
            "cursor moved backward within the same segment"
        ))
    );

    // Forward CAS succeeds and bumps checkpoint generation.
    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 20,
                record_index: 1,
                lineage_transition_id: Some(Uuid::from_u128(0x60)),
                expected_checkpoint_generation: Some(0),
            }
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 1,
        }
    );

    let MetaValue::GroupCursor(cursor) = machine
        .record(&MetaKey::GroupCursor {
            group_uuid: GROUP,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
        })
        .cloned()
        .unwrap()
    else {
        panic!("expected cursor record");
    };
    assert_eq!(cursor.record_offset, 20);
    assert_eq!(cursor.checkpoint_generation, 1);
    assert_eq!(cursor.committed_by_member, MEMBER);
    assert_eq!(cursor.lineage_transition_id, Some(Uuid::from_u128(0x60)));

    // Unassigned member cannot commit. Group generation is 2 after join+assign.
    assert_eq!(
        machine.apply(15, &join_member(&mut requests, MEMBER_B, 2)),
        MetadataResponse::MemberJoined {
            member_generation: 0,
            group_generation: 3,
        }
    );
    assert_eq!(
        machine.apply(
            16,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER_B,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 30,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: Some(1),
            }
        ),
        rejected(MetadataError::invalid_transition(
            "member is not assigned the cursor topic/range"
        ))
    );

    // Exclusive assignment: MEMBER still holds RANGE, so MEMBER_B cannot steal it.
    assert_eq!(
        machine.apply(17, &assign_member(&mut requests, MEMBER_B, 0)),
        rejected(MetadataError::invalid_transition(
            "range is already assigned to another group member"
        ))
    );

    // Snapshot round-trip preserves group/cursor records.
    let encoded = machine.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&encoded).unwrap();
    assert_eq!(restored.encode_snapshot().unwrap(), encoded);
}

#[test]
fn member_heartbeat_and_stale_expiry_keep_cursors() {
    let mut requests = Requests(0);
    let mut machine = machine_with_group(&mut requests);
    let root = [2u8; 32];

    assert_eq!(
        machine.apply(6, &grant(&mut requests, NODE, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    assert_eq!(
        machine.apply(
            7,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 50,
                content_root: root,
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 0,
        }
    );

    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::HeartbeatMember {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    let MetaValue::GroupMember(member) = machine
        .record(&MetaKey::GroupMember {
            group_uuid: GROUP,
            member_uuid: MEMBER,
        })
        .cloned()
        .unwrap()
    else {
        panic!("expected member");
    };
    assert_eq!(member.last_heartbeat_apply_index, 9);

    // Still live relative to heartbeat 9.
    assert_eq!(
        machine.apply(
            10,
            &MetadataCommand::ExpireStaleMember {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                stale_before_apply_index: 9,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "member heartbeat is still within the live window"
        ))
    );

    assert_eq!(
        machine.apply(
            11,
            &MetadataCommand::ExpireStaleMember {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                stale_before_apply_index: 10,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert!(machine
        .record(&MetaKey::GroupMember {
            group_uuid: GROUP,
            member_uuid: MEMBER,
        })
        .is_none());
    // Durable cursor survives member expiry.
    assert!(matches!(
        machine.record(&MetaKey::GroupCursor {
            group_uuid: GROUP,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
        }),
        Some(MetaValue::GroupCursor(_))
    ));
}

#[test]
fn sealed_segment_lineage_is_checked_on_cursor_commit() {
    let mut requests = Requests(0);
    let mut machine = machine_with_group(&mut requests);
    let root = [4u8; 32];

    assert_eq!(
        machine.apply(6, &grant(&mut requests, NODE, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    assert_eq!(
        machine.apply(
            7,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 100,
                content_root: root,
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );

    // Outside sealed bounds.
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 101,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "record offset 101 is outside sealed segment [0, 100]"
        ))
    );

    // Root mismatch.
    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: [1; 32],
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "segment root does not match the registered segment"
        ))
    );

    assert_eq!(
        machine.apply(
            10,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                topic_epoch: 1,
                range_generation: 2,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                segment_root: root,
                record_offset: 10,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: None,
            }
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 0,
        }
    );
}
