//! Full command matrix for the deterministic metadata state machine: every
//! command's success path and every deterministic rejection, fencing-epoch
//! monotonicity across grant/release interleavings, dedup semantics across
//! snapshot/restore and FIFO eviction, and byte-identical snapshots from
//! independently driven instances.

use uuid::Uuid;
use vtop_meta::{
    select_replicas, CommandEnvelope, MetaKey, MetaStateMachine, MetaValue, MetadataCommand,
    MetadataError, MetadataResponse, NodeState, PlacementCandidate, RangeAssignment, SegmentState,
    DEDUP_CAPACITY, MAX_NODE_ADDR_BYTES, MAX_TIER_OBJECT_URI_BYTES, MAX_TOPIC_NAME_BYTES,
};

const NODE: Uuid = Uuid::from_u128(0x10);
const NODE_B: Uuid = Uuid::from_u128(0x11);
const NODE_C: Uuid = Uuid::from_u128(0x12);
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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

    // Lineage is decoupled from the CAS token: the range's metadata
    // generation is 2 after grant + segment registration, but no lineage
    // transition ever happened, so a cursor pinning that CAS value fails
    // the lineage check — not a checkpoint-generation conflict.
    assert_eq!(
        machine.apply(
            18,
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
                record_offset: 30,
                record_index: 0,
                lineage_transition_id: None,
                expected_checkpoint_generation: Some(1),
            }
        ),
        rejected(MetadataError::LineageMismatch {
            expected: 2,
            actual: 0,
        })
    );

    // Snapshot round-trip preserves group/cursor records.
    let encoded = machine.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&encoded).unwrap();
    assert_eq!(restored.encode_snapshot().unwrap(), encoded);
}

#[test]
fn legacy_cursor_cas_generation_is_normalized_to_lineage_generation_once() {
    let mut requests = Requests(0);
    let mut machine = machine_with_group(&mut requests);
    let root = [0x44; 32];
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
    let commit = |requests: &mut Requests,
                  range_generation: u64,
                  record_offset: u64,
                  expected_checkpoint_generation: Option<u64>| {
        MetadataCommand::CommitGroupCursor {
            env: requests.next(),
            group_uuid: GROUP,
            member_uuid: MEMBER,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            topic_epoch: 1,
            range_generation,
            segment_uuid: SEGMENT,
            segment_generation: 0,
            segment_root: root,
            record_offset,
            record_index: 0,
            lineage_transition_id: None,
            expected_checkpoint_generation,
        }
    };
    assert_eq!(
        machine.apply(8, &commit(&mut requests, 0, 10, None)),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 0,
        }
    );

    // Pre-lineage snapshots stored the range metadata CAS generation (2)
    // where current records store lineage generation (0).
    let legacy_snapshot = rewrite_snapshot_value(
        &machine.encode_snapshot().unwrap(),
        &MetaKey::GroupCursor {
            group_uuid: GROUP,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
        },
        |value| value[9..17].copy_from_slice(&2_u64.to_be_bytes()),
    );
    let mut restored = MetaStateMachine::decode_snapshot(&legacy_snapshot).unwrap();

    assert_eq!(
        restored.apply(9, &commit(&mut requests, 2, 20, Some(0))),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 1,
        }
    );
    let Some(MetaValue::GroupCursor(cursor)) = restored.record(&MetaKey::GroupCursor {
        group_uuid: GROUP,
        topic_uuid: TOPIC,
        range_uuid: RANGE,
    }) else {
        panic!("cursor must exist");
    };
    assert_eq!(cursor.range_generation, 0);

    // Once rewritten, the legacy CAS value is no longer accepted.
    assert_eq!(
        restored.apply(10, &commit(&mut requests, 2, 30, Some(1))),
        rejected(MetadataError::LineageMismatch {
            expected: 2,
            actual: 0,
        })
    );
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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
                range_generation: 0,
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

fn set_placement_attrs(
    requests: &mut Requests,
    node: Uuid,
    domain: &str,
    weight: u32,
    expected_generation: u64,
) -> MetadataCommand {
    MetadataCommand::SetNodePlacementAttrs {
        env: requests.next(),
        node_uuid: node,
        failure_domain: domain.to_owned(),
        placement_weight: weight,
        expected_generation,
    }
}

fn verified_segment_machine(requests: &mut Requests) -> (MetaStateMachine, [u8; 32], u64) {
    let mut machine = machine_with_topic_and_node(requests);
    assert_eq!(
        machine.apply(3, &grant(requests, NODE, 0)),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    let root = [0xab; 32];
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: 0,
                base_offset: 0,
                next_offset: 64,
                content_root: root,
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(
            5,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: 0,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    (machine, root, 1)
}

#[test]
fn node_placement_attrs_cas_and_bounds() {
    let mut requests = Requests(0);
    let mut machine = MetaStateMachine::new();
    assert_eq!(
        machine.apply(1, &register_node(&mut requests, NODE)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            2,
            &set_placement_attrs(&mut requests, NODE, "rack-a", 100, 0)
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    let Some(MetaValue::Node(node)) = machine.record(&MetaKey::Node { node_uuid: NODE }) else {
        panic!("node must exist");
    };
    assert_eq!(node.failure_domain, "rack-a");
    assert_eq!(node.placement_weight, 100);
    assert_eq!(node.generation, 1);

    assert_eq!(
        machine.apply(
            3,
            &set_placement_attrs(&mut requests, NODE, "rack-b", 50, 0)
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 0,
            actual: 1,
        })
    );
    assert_eq!(
        machine.apply(
            4,
            &MetadataCommand::SetNodePlacementAttrs {
                env: requests.next(),
                node_uuid: NODE,
                failure_domain: "x".repeat(65),
                placement_weight: 100,
                expected_generation: 1,
            }
        ),
        rejected(MetadataError::limit(
            "failure domain must be 0..=64 bytes, got 65"
        ))
    );
    assert_eq!(
        machine.apply(
            5,
            &set_placement_attrs(&mut requests, NODE_B, "rack-a", 100, 0)
        ),
        rejected(MetadataError::NotFound)
    );
}

#[test]
fn segment_placement_commits_only_deterministic_verified_sets() {
    let mut requests = Requests(0);
    let (mut machine, _root, segment_generation) = verified_segment_machine(&mut requests);

    // Register two more Active nodes in distinct domains.
    for (node, domain, gen_idx) in [(NODE_B, "rack-b", 6_u64), (NODE_C, "rack-c", 8)] {
        assert_eq!(
            machine.apply(gen_idx, &register_node(&mut requests, node)),
            MetadataResponse::Ack { generation: 0 }
        );
        assert_eq!(
            machine.apply(
                gen_idx + 1,
                &set_placement_attrs(&mut requests, node, domain, 100, 0)
            ),
            MetadataResponse::Ack { generation: 1 }
        );
    }
    assert_eq!(
        machine.apply(
            10,
            &set_placement_attrs(&mut requests, NODE, "rack-a", 100, 0)
        ),
        MetadataResponse::Ack { generation: 1 }
    );

    let expected = select_replicas(
        SEGMENT,
        &[
            PlacementCandidate {
                node_uuid: NODE,
                failure_domain: "rack-a".to_owned(),
                weight: 100,
            },
            PlacementCandidate {
                node_uuid: NODE_B,
                failure_domain: "rack-b".to_owned(),
                weight: 100,
            },
            PlacementCandidate {
                node_uuid: NODE_C,
                failure_domain: "rack-c".to_owned(),
                weight: 100,
            },
        ],
        3,
        true,
    )
    .unwrap();

    // Wrong ordering / set is rejected.
    let mut wrong = expected.clone();
    wrong.reverse();
    if wrong == expected {
        wrong.swap(0, 1);
    }
    assert_eq!(
        machine.apply(
            11,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 3,
                replica_nodes: wrong,
                expected_segment_generation: segment_generation,
                expected_placement_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "proposed replica set does not match deterministic placement"
        ))
    );

    // Independent RF must match the proposed list length.
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 3,
                replica_nodes: expected[..1].to_vec(),
                expected_segment_generation: segment_generation,
                expected_placement_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "replica set length 1 does not match replication_factor 3"
        ))
    );

    assert_eq!(
        machine.apply(
            13,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 3,
                replica_nodes: expected.clone(),
                expected_segment_generation: segment_generation,
                expected_placement_generation: None,
            }
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    let Some(MetaValue::SegmentPlacement(placement)) = machine.record(&MetaKey::SegmentPlacement {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("placement record must exist");
    };
    assert_eq!(placement.replica_nodes, expected);
    assert_eq!(placement.declared_replication_factor, 3);
    assert_eq!(placement.generation, 0);
    assert_eq!(placement.committed_apply_index, 13);

    // Unverified segment cannot be placed: seal a second segment and try.
    let unverified = Uuid::from_u128(0x31);
    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: unverified,
                segment_generation: 0,
                base_offset: 64,
                next_offset: 128,
                content_root: [0xcd; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(
            15,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: unverified,
                replication_factor: 3,
                replica_nodes: expected,
                expected_segment_generation: 0,
                expected_placement_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "placement requires a verified segment"
        ))
    );
}

#[test]
fn segment_placement_rejects_same_domain_when_replicas_require_distinctness() {
    let mut requests = Requests(0);
    let (mut machine, _root, segment_generation) = verified_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(6, &register_node(&mut requests, NODE_B)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            7,
            &set_placement_attrs(&mut requests, NODE, "rack-a", 100, 0)
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            8,
            &set_placement_attrs(&mut requests, NODE_B, "rack-a", 100, 0)
        ),
        MetadataResponse::Ack { generation: 1 }
    );

    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 2,
                replica_nodes: vec![NODE, NODE_B],
                expected_segment_generation: segment_generation,
                expected_placement_generation: None,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "need 2 eligible replica(s), only 1 available"
        ))
    );
}

#[test]
fn replacement_proof_gates_replica_retirement() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, _spare) =
        placed_segment_machine(&mut requests);
    let source = replicas[0];
    let destination = replicas[1];

    // Retirement without a proof is rejected.
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "replica retirement requires a committed replacement proof"
        ))
    );

    // Stale fencing epoch is rejected on proof commit.
    assert_eq!(
        machine.apply(
            13,
            &MetadataCommand::CommitReplacementProof {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                expected_segment_generation: segment_generation,
                content_root: root,
                expected_length_bytes: 4096,
                source_node_uuid: source,
                destination_node_uuid: destination,
                fencing_epoch: 99,
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: destination,
                verified_term: 1,
            }
        ),
        rejected(MetadataError::EpochMismatch {
            expected: 99,
            actual: 1,
        })
    );

    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::CommitReplacementProof {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                expected_segment_generation: segment_generation,
                content_root: root,
                expected_length_bytes: 4096,
                source_node_uuid: source,
                destination_node_uuid: destination,
                fencing_epoch: 1,
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: destination,
                verified_term: 1,
            }
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // Wrong content root rejected before planning.
    assert_eq!(
        machine.apply(
            15,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: destination,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "cannot retire the verified replacement destination"
        ))
    );

    assert_eq!(
        machine.apply(
            16,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );

    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::RetirePlanned);
    assert_eq!(segment.segment_generation, 2);

    // A rejected confirmation must mutate nothing (apply is all-or-nothing):
    // after this rejection the segment must still be RETIRE_PLANNED at
    // generation 2, or the successful confirmation below could not succeed.
    assert_eq!(
        machine.apply(
            17,
            &MetadataCommand::ConfirmReplicaRetired {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: 999,
            }
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 999,
            actual: 2,
        })
    );
    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::RetirePlanned);
    assert_eq!(segment.segment_generation, 2);

    assert_eq!(
        machine.apply(
            18,
            &MetadataCommand::ConfirmReplicaRetired {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::Verified);
    assert_eq!(placement_record(&machine).committed_apply_index, 18);
    assert!(machine
        .record(&MetaKey::SegmentReplacementProof {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        })
        .is_none());

    // Confirmation returned the surviving placement to Verified; it is not a
    // second route through the one-time verification transition.
    assert_eq!(
        machine.apply(
            19,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: 3,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "verification requires SEALED_UNVERIFIED"
        ))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );
}

#[test]
fn retired_segment_from_an_older_snapshot_cannot_be_resurrected() {
    let mut requests = Requests(0);
    let (machine, root, segment_generation) = verified_segment_machine(&mut requests);
    let retired_snapshot = rewrite_snapshot_value(
        &machine.encode_snapshot().unwrap(),
        &MetaKey::Segment {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        },
        |value| value[57] = 5,
    );
    let mut restored = MetaStateMachine::decode_snapshot(&retired_snapshot).unwrap();
    assert_eq!(
        restored.apply(
            6,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: segment_generation,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "verification requires SEALED_UNVERIFIED"
        ))
    );
    assert_eq!(
        segment_record(&restored, SEGMENT).state,
        SegmentState::Retired
    );
}

/// A verified segment with a committed RF-2 placement over three Active
/// nodes in distinct racks. Returns the machine, the sealed content root,
/// the segment generation, the committed replica list, and the one Active
/// node the deterministic selection left out (the natural rebalance target).
fn placed_segment_machine(
    requests: &mut Requests,
) -> (MetaStateMachine, [u8; 32], u64, Vec<Uuid>, Uuid) {
    let (mut machine, root, segment_generation) = verified_segment_machine(requests);
    for (node, domain, at) in [(NODE_B, "rack-b", 6_u64), (NODE_C, "rack-c", 8)] {
        assert_eq!(
            machine.apply(at, &register_node(requests, node)),
            MetadataResponse::Ack { generation: 0 }
        );
        assert_eq!(
            machine.apply(at + 1, &set_placement_attrs(requests, node, domain, 100, 0)),
            MetadataResponse::Ack { generation: 1 }
        );
    }
    assert_eq!(
        machine.apply(10, &set_placement_attrs(requests, NODE, "rack-a", 100, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    let replicas = select_replicas(
        SEGMENT,
        &[
            PlacementCandidate {
                node_uuid: NODE,
                failure_domain: "rack-a".to_owned(),
                weight: 100,
            },
            PlacementCandidate {
                node_uuid: NODE_B,
                failure_domain: "rack-b".to_owned(),
                weight: 100,
            },
            PlacementCandidate {
                node_uuid: NODE_C,
                failure_domain: "rack-c".to_owned(),
                weight: 100,
            },
        ],
        2,
        true,
    )
    .unwrap();
    assert_eq!(
        machine.apply(
            11,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 2,
                replica_nodes: replicas.clone(),
                expected_segment_generation: segment_generation,
                expected_placement_generation: None,
            }
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    let spare = [NODE, NODE_B, NODE_C]
        .into_iter()
        .find(|node| !replicas.contains(node))
        .expect("RF 2 over three nodes always leaves one out");
    (machine, root, segment_generation, replicas, spare)
}

fn propose_rebalance(
    requests: &mut Requests,
    from: Uuid,
    to: Uuid,
    expected_placement_generation: u64,
) -> MetadataCommand {
    MetadataCommand::ProposeRebalance {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
        from_node_uuid: from,
        to_node_uuid: to,
        expected_placement_generation,
    }
}

fn cancel_rebalance(
    requests: &mut Requests,
    expected_placement_generation: u64,
) -> MetadataCommand {
    MetadataCommand::CancelRebalance {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
        expected_placement_generation,
    }
}

fn commit_proof(
    requests: &mut Requests,
    root: [u8; 32],
    source: Uuid,
    destination: Uuid,
    expected_segment_generation: u64,
) -> MetadataCommand {
    MetadataCommand::CommitReplacementProof {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
        expected_segment_generation,
        content_root: root,
        expected_length_bytes: 4096,
        source_node_uuid: source,
        destination_node_uuid: destination,
        fencing_epoch: 1,
        verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
        verifier_node_uuid: destination,
        verified_term: 1,
    }
}

#[test]
fn retirement_plan_requires_the_verified_destination_in_current_placement() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let source = replicas[0];

    // An active node is not deletion authority by itself: the verified
    // replacement must already be committed into this segment's placement.
    assert_eq!(
        machine.apply(
            12,
            &commit_proof(&mut requests, root, source, spare, segment_generation)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            13,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "placement does not contain the verified replacement destination"
        ))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );
    assert_eq!(placement_record(&machine).replica_nodes, replicas);
}

#[test]
fn replacement_proof_can_be_refreshed_after_lease_epoch_turnover() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, _spare) =
        placed_segment_machine(&mut requests);
    let source = replicas[0];
    let destination = replicas[1];

    assert_eq!(
        machine.apply(
            12,
            &commit_proof(&mut requests, root, source, destination, segment_generation,)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(13, &release(&mut requests, 1)),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(14, &grant(&mut requests, NODE, 3)),
        MetadataResponse::LeaseGranted { fencing_epoch: 2 }
    );

    let refreshed = |requests: &mut Requests, expected_length_bytes: u64| {
        MetadataCommand::CommitReplacementProof {
            env: requests.next(),
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
            expected_segment_generation: segment_generation,
            content_root: root,
            expected_length_bytes,
            source_node_uuid: source,
            destination_node_uuid: destination,
            fencing_epoch: 2,
            verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
            verifier_node_uuid: destination,
            verified_term: 2,
        }
    };

    // Lease turnover cannot rewrite the proof to cover different bytes.
    assert_eq!(
        machine.apply(15, &refreshed(&mut requests, 8192)),
        rejected(MetadataError::invalid_transition(
            "stale replacement proof identity does not match the new proof"
        ))
    );
    let Some(MetaValue::ReplacementProof(stale)) =
        machine.record(&MetaKey::SegmentReplacementProof {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        })
    else {
        panic!("stale replacement proof must remain after rejection");
    };
    assert_eq!(stale.generation, 0);
    assert_eq!(stale.fencing_epoch, 1);
    // Re-verifying the same immutable replacement under the live epoch
    // supersedes the stale proof instead of wedging recovery forever.
    assert_eq!(
        machine.apply(16, &refreshed(&mut requests, 4096)),
        MetadataResponse::Ack { generation: 1 }
    );
    let Some(MetaValue::ReplacementProof(proof)) =
        machine.record(&MetaKey::SegmentReplacementProof {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        })
    else {
        panic!("replacement proof must exist");
    };
    assert_eq!(proof.generation, 1);
    assert_eq!(proof.fencing_epoch, 2);

    assert_eq!(
        machine.apply(
            17,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: source,
                expected_segment_generation: segment_generation,
                fencing_epoch: 2,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
}

fn placement_record(machine: &MetaStateMachine) -> vtop_meta::SegmentPlacementRecord {
    let Some(MetaValue::SegmentPlacement(placement)) = machine.record(&MetaKey::SegmentPlacement {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("placement record must exist");
    };
    placement.clone()
}

fn intent_record(machine: &MetaStateMachine) -> Option<vtop_meta::RebalanceIntentRecord> {
    match machine.record(&MetaKey::SegmentRebalanceIntent {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) {
        Some(MetaValue::RebalanceIntent(intent)) => Some(intent.clone()),
        None => None,
        Some(_) => panic!("intent keys only hold intent records"),
    }
}

/// Asserts the core rebalance invariant at every step: the replica set never
/// holds fewer nodes than the declared replication factor, and the declared
/// factor itself is never rewritten.
fn assert_at_or_above_declared_rf(machine: &MetaStateMachine, declared: u8) {
    let placement = placement_record(machine);
    assert_eq!(placement.declared_replication_factor, declared);
    assert!(
        placement.replica_nodes.len() >= usize::from(declared),
        "replica set {} dropped below declared RF {declared}",
        placement.replica_nodes.len()
    );
}

#[test]
fn rebalance_lifecycle_completes_via_verified_retirement() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];
    let other = replicas[1];
    assert_at_or_above_declared_rf(&machine, 2);

    // Tier evidence committed before the move must remain usable afterward:
    // replica lifecycle CAS churn does not change sealed identity.
    assert_eq!(
        machine.apply(
            12,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // Propose: intent recorded, destination added, declared RF untouched.
    assert_eq!(
        machine.apply(13, &propose_rebalance(&mut requests, from, spare, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    let placement = placement_record(&machine);
    assert_eq!(placement.replica_nodes, vec![from, other, spare]);
    assert_eq!(placement.declared_replication_factor, 2);
    assert_eq!(placement.generation, 1);
    assert_eq!(placement.committed_apply_index, 13);
    assert_at_or_above_declared_rf(&machine, 2);
    let intent = intent_record(&machine).expect("intent must exist after propose");
    assert_eq!(intent.from_node_uuid, from);
    assert_eq!(intent.to_node_uuid, spare);
    assert_eq!(intent.proposed_at_apply_index, 13);
    assert_eq!(intent.placement_generation_at_proposal, 0);

    // A second in-flight rebalance is blocked outright.
    assert_eq!(
        machine.apply(14, &propose_rebalance(&mut requests, other, spare, 1)),
        rejected(MetadataError::AlreadyExists)
    );

    // During the intent, retirement may only ever target the intent source.
    assert_eq!(
        machine.apply(
            15,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: other,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "segment has an active rebalance intent for a different node"
        ))
    );

    // A proof that is not the intended (from -> to) move is rejected.
    assert_eq!(
        machine.apply(
            16,
            &commit_proof(&mut requests, root, other, spare, segment_generation)
        ),
        rejected(MetadataError::invalid_transition(
            "replacement proof does not match the active rebalance intent"
        ))
    );

    // The matching proof, plan, and confirmation complete the move.
    assert_eq!(
        machine.apply(
            17,
            &commit_proof(&mut requests, root, from, spare, segment_generation)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_at_or_above_declared_rf(&machine, 2);
    assert_eq!(
        machine.apply(
            18,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: from,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_at_or_above_declared_rf(&machine, 2);
    assert_eq!(placement_record(&machine).replica_nodes.len(), 3);
    assert_eq!(
        machine.apply(
            19,
            &MetadataCommand::ConfirmReplicaRetired {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: from,
                expected_segment_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );

    // Completed: destination replaced the source at exactly declared RF, and
    // the intent + consumed proof went with the confirmation atomically.
    let placement = placement_record(&machine);
    assert_eq!(placement.replica_nodes, vec![other, spare]);
    assert_eq!(placement.declared_replication_factor, 2);
    assert_eq!(placement.generation, 2);
    assert_eq!(placement.committed_apply_index, 19);
    assert_at_or_above_declared_rf(&machine, 2);
    assert_eq!(intent_record(&machine), None);
    assert!(machine
        .record(&MetaKey::SegmentReplacementProof {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        })
        .is_none());
    let Some(MetaValue::Segment(segment)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: SEGMENT,
    }) else {
        panic!("segment must exist");
    };
    assert_eq!(segment.state, SegmentState::Verified);

    // The consumed proof no longer blocks retention, and the pre-move tier
    // evidence still matches the immutable root despite generation 1 -> 3.
    assert_eq!(
        machine.apply(20, &plan_retention(&mut requests, SEGMENT, 3, 1)),
        MetadataResponse::Ack { generation: 4 }
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::RetentionPlanned
    );
}

#[test]
fn cancel_rebalance_before_proof_restores_placement() {
    let mut requests = Requests(0);
    let (mut machine, _root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];

    assert_eq!(
        machine.apply(12, &propose_rebalance(&mut requests, from, spare, 0)),
        MetadataResponse::Ack { generation: 1 }
    );

    // Stale CAS token is rejected without touching state.
    assert_eq!(
        machine.apply(13, &cancel_rebalance(&mut requests, 0)),
        rejected(MetadataError::GenerationMismatch {
            expected: 0,
            actual: 1,
        })
    );
    assert!(intent_record(&machine).is_some());

    // The placement itself is locked while the intent is in flight.
    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::CommitSegmentPlacement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                replication_factor: 2,
                replica_nodes: replicas.clone(),
                expected_segment_generation: segment_generation,
                expected_placement_generation: Some(1),
            }
        ),
        rejected(MetadataError::invalid_transition(
            "placement is locked by an active rebalance intent"
        ))
    );

    // Cancel restores the original replica set and drops the intent.
    assert_eq!(
        machine.apply(15, &cancel_rebalance(&mut requests, 1)),
        MetadataResponse::Ack { generation: 2 }
    );
    let placement = placement_record(&machine);
    assert_eq!(placement.replica_nodes, replicas);
    assert_eq!(placement.declared_replication_factor, 2);
    assert_eq!(placement.generation, 2);
    assert_eq!(placement.committed_apply_index, 15);
    assert_eq!(intent_record(&machine), None);

    // Nothing left to cancel.
    assert_eq!(
        machine.apply(16, &cancel_rebalance(&mut requests, 2)),
        rejected(MetadataError::NotFound)
    );

    // A fresh proposal is allowed again after the cancel.
    assert_eq!(
        machine.apply(17, &propose_rebalance(&mut requests, from, spare, 2)),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(placement_record(&machine).committed_apply_index, 17);
}

#[test]
fn cancel_rebalance_after_matching_proof_is_rejected() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];

    assert_eq!(
        machine.apply(12, &propose_rebalance(&mut requests, from, spare, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            13,
            &commit_proof(&mut requests, root, from, spare, segment_generation)
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // The verified copy is committed evidence; the move can only complete.
    assert_eq!(
        machine.apply(14, &cancel_rebalance(&mut requests, 1)),
        rejected(MetadataError::invalid_transition(
            "cannot cancel a rebalance whose replacement proof is committed"
        ))
    );
    assert!(intent_record(&machine).is_some());
    assert_eq!(placement_record(&machine).replica_nodes.len(), 3);
}

#[test]
fn confirm_replica_retired_without_intent_preserves_declared_rf() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let (retiring, surviving) = (replicas[0], replicas[1]);

    // Plain repair-style retirement: proof between two placed replicas.
    assert_eq!(
        machine.apply(
            12,
            &commit_proof(&mut requests, root, retiring, surviving, segment_generation)
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // A rebalance cannot start once a replacement proof already exists.
    assert_eq!(
        machine.apply(13, &propose_rebalance(&mut requests, retiring, spare, 0)),
        rejected(MetadataError::invalid_transition(
            "segment already has a committed replacement proof"
        ))
    );

    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::PlanReplicaRetirement {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: retiring,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );

    // A rebalance also cannot start on a segment that is no longer Verified.
    assert_eq!(
        machine.apply(15, &propose_rebalance(&mut requests, surviving, spare, 0)),
        rejected(MetadataError::invalid_transition(
            "rebalance requires a verified segment"
        ))
    );

    assert_eq!(
        machine.apply(
            16,
            &MetadataCommand::ConfirmReplicaRetired {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                retiring_node_uuid: retiring,
                expected_segment_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );

    // Regression: the declared factor survives the shrink instead of being
    // rewritten to the post-retirement list length.
    let placement = placement_record(&machine);
    assert_eq!(placement.replica_nodes, vec![surviving]);
    assert_eq!(placement.declared_replication_factor, 2);
    assert_eq!(placement.generation, 1);
    assert_eq!(placement.committed_apply_index, 16);
}

#[test]
fn propose_rebalance_rejects_every_bad_shape_deterministically() {
    let mut requests = Requests(0);
    let (mut machine, _root, _segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];
    const NODE_D: Uuid = Uuid::from_u128(0x13);

    // Unknown segment, then unknown destination node.
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::ProposeRebalance {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: Uuid::from_u128(0x99),
                from_node_uuid: from,
                to_node_uuid: spare,
                expected_placement_generation: 0,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    assert_eq!(
        machine.apply(13, &propose_rebalance(&mut requests, from, NODE_D, 0)),
        rejected(MetadataError::NotFound)
    );

    // Stale placement CAS.
    assert_eq!(
        machine.apply(14, &propose_rebalance(&mut requests, from, spare, 5)),
        rejected(MetadataError::GenerationMismatch {
            expected: 5,
            actual: 0,
        })
    );

    // Source and destination must differ; source must be placed; the
    // destination must not already be placed.
    assert_eq!(
        machine.apply(15, &propose_rebalance(&mut requests, from, from, 0)),
        rejected(MetadataError::invalid_transition(
            "rebalance source and destination must differ"
        ))
    );
    assert_eq!(
        machine.apply(16, &register_node(&mut requests, NODE_D)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(17, &propose_rebalance(&mut requests, spare, NODE_D, 0)),
        rejected(MetadataError::invalid_transition(
            "rebalance source is not in the current placement"
        ))
    );
    assert_eq!(
        machine.apply(18, &propose_rebalance(&mut requests, from, replicas[1], 0)),
        rejected(MetadataError::invalid_transition(
            "rebalance destination is already in the placement"
        ))
    );

    // A draining destination cannot receive a rebalance.
    assert_eq!(
        machine.apply(
            19,
            &MetadataCommand::SetNodeState {
                env: requests.next(),
                node_uuid: spare,
                state: NodeState::Draining,
                expected_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(20, &propose_rebalance(&mut requests, from, spare, 0)),
        rejected(MetadataError::invalid_transition(format!(
            "rebalance destination {spare} is draining, not active"
        )))
    );

    // Nothing above mutated the placement or created an intent.
    let placement = placement_record(&machine);
    assert_eq!(placement.replica_nodes, replicas);
    assert_eq!(placement.generation, 0);
    assert_eq!(intent_record(&machine), None);
}

#[test]
fn propose_rebalance_preserves_distinct_failure_domains() {
    let mut requests = Requests(0);
    let (mut machine, _root, _segment_generation, replicas, _spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];
    let survivor = replicas[1];
    let domain_of = |node: Uuid| -> &'static str {
        if node == NODE {
            "rack-a"
        } else if node == NODE_B {
            "rack-b"
        } else {
            "rack-c"
        }
    };
    const NODE_D: Uuid = Uuid::from_u128(0x14);
    assert_eq!(
        machine.apply(12, &register_node(&mut requests, NODE_D)),
        MetadataResponse::Ack { generation: 0 }
    );

    // Destination sharing the SURVIVOR's failure domain is rejected: the
    // completed move would put two replicas in one domain, silently
    // bypassing the distinct-domain durability constraint the deterministic
    // placement enforced.
    assert_eq!(
        machine.apply(
            13,
            &set_placement_attrs(&mut requests, NODE_D, domain_of(survivor), 100, 0)
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(14, &propose_rebalance(&mut requests, from, NODE_D, 0)),
        rejected(MetadataError::invalid_transition(format!(
            "rebalance destination shares failure domain {:?} with surviving replica {survivor}",
            domain_of(survivor)
        )))
    );

    // Reusing the SOURCE's domain is allowed: the source is leaving, so its
    // domain frees up and the completed move stays domain-distinct.
    assert_eq!(
        machine.apply(
            15,
            &set_placement_attrs(&mut requests, NODE_D, domain_of(from), 100, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(16, &propose_rebalance(&mut requests, from, NODE_D, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
}

#[test]
fn snapshots_round_trip_active_rebalance_intents_byte_exactly() {
    fn drive(requests: &mut Requests) -> MetaStateMachine {
        let (mut machine, _root, _segment_generation, replicas, spare) =
            placed_segment_machine(requests);
        assert_eq!(
            machine.apply(12, &propose_rebalance(requests, replicas[0], spare, 0)),
            MetadataResponse::Ack { generation: 1 }
        );
        machine
    }

    let machine_a = drive(&mut Requests(0));
    let machine_b = drive(&mut Requests(0));

    // Independently driven instances agree byte-for-byte with an intent live.
    let encoded_a = machine_a.encode_snapshot().unwrap();
    let encoded_b = machine_b.encode_snapshot().unwrap();
    assert_eq!(encoded_a, encoded_b);

    // Restore preserves the intent, the RF + 1 placement, and the dedup FIFO,
    // and re-encodes to the identical byte string.
    let decoded = MetaStateMachine::decode_snapshot(&encoded_a).unwrap();
    assert_eq!(decoded.encode_snapshot().unwrap(), encoded_a);
    assert_eq!(decoded, machine_a);
    let intent = intent_record(&decoded).expect("intent must survive restore");
    assert_eq!(intent.proposed_at_apply_index, 12);
    assert_eq!(intent.placement_generation_at_proposal, 0);
    let placement = placement_record(&decoded);
    assert_eq!(placement.replica_nodes.len(), 3);
    assert_eq!(placement.declared_replication_factor, 2);
}

// ---------------------------------------------------------------------------
// Stage-8b: retention and object-store tiering metadata.
// ---------------------------------------------------------------------------

const SEGMENT_B: Uuid = Uuid::from_u128(0x31);

fn commit_tier_evidence(
    requests: &mut Requests,
    segment: Uuid,
    root: [u8; 32],
    expected_segment_generation: u64,
    fencing_epoch: u64,
    verifier: Uuid,
) -> MetadataCommand {
    MetadataCommand::CommitTierEvidence {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
        expected_segment_generation,
        content_root: root,
        byte_length: 4096,
        backend_id: "s3-native".to_owned(),
        object_uri: "s3://tier/native/events.v1/segment-30.segment".to_owned(),
        object_version_id: Some("4sL4kqCJo05qOWBhBqpfOFAdT4dRJVvW".to_owned()),
        manifest_version_id: Some("3sL4kqCJo05qOWBhBqpfOFAdT4dRJVvV".to_owned()),
        manifest_core_digest: [0x5d; 32],
        verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
        verifier_node_uuid: verifier,
        fencing_epoch,
        verified_term: 5,
    }
}

fn set_retention_policy(
    requests: &mut Requests,
    allowed: bool,
    expected_generation: Option<u64>,
) -> MetadataCommand {
    MetadataCommand::SetTopicRetentionPolicy {
        env: requests.next(),
        topic_uuid: TOPIC,
        unarchived_deletion_allowed: allowed,
        expected_generation,
    }
}

fn plan_retention(
    requests: &mut Requests,
    segment: Uuid,
    expected_segment_generation: u64,
    fencing_epoch: u64,
) -> MetadataCommand {
    MetadataCommand::PlanRetention {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
        expected_segment_generation,
        fencing_epoch,
    }
}

fn confirm_retention_expired(
    requests: &mut Requests,
    segment: Uuid,
    expected_segment_generation: u64,
) -> MetadataCommand {
    MetadataCommand::ConfirmRetentionExpired {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
        expected_segment_generation,
    }
}

fn cancel_retention(
    requests: &mut Requests,
    segment: Uuid,
    expected_segment_generation: u64,
) -> MetadataCommand {
    MetadataCommand::CancelRetention {
        env: requests.next(),
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
        expected_segment_generation,
    }
}

fn segment_record(machine: &MetaStateMachine, segment: Uuid) -> vtop_meta::SegmentRecord {
    let Some(MetaValue::Segment(record)) = machine.record(&MetaKey::Segment {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
    }) else {
        panic!("segment record must exist");
    };
    record.clone()
}

fn tier_record(machine: &MetaStateMachine, segment: Uuid) -> Option<vtop_meta::TierCopyRecord> {
    match machine.record(&MetaKey::SegmentTierCopy {
        topic_uuid: TOPIC,
        range_uuid: RANGE,
        segment_uuid: segment,
    }) {
        Some(MetaValue::TierCopy(tier)) => Some(tier.clone()),
        None => None,
        Some(_) => panic!("tier-copy keys only hold tier-copy records"),
    }
}

/// Rewrite one record's value bytes inside an encoded snapshot, fixing up the
/// value length. Lets tests reach defensive rejection branches (a mismatched
/// evidence root, a placement generation at the ceiling) that no command
/// sequence can produce, without exposing state internals.
fn rewrite_snapshot_value(
    snapshot: &[u8],
    target: &MetaKey,
    mut mutate: impl FnMut(&mut Vec<u8>),
) -> Vec<u8> {
    let record_count = u32::from_be_bytes(snapshot[2..6].try_into().unwrap());
    let target_key = target.encode();
    let mut out = snapshot[..6].to_vec();
    let mut at = 6_usize;
    let mut found = false;
    for _ in 0..record_count {
        let key_len = u16::from_be_bytes(snapshot[at..at + 2].try_into().unwrap()) as usize;
        let key = &snapshot[at + 2..at + 2 + key_len];
        let value_len_at = at + 2 + key_len;
        let value_len =
            u32::from_be_bytes(snapshot[value_len_at..value_len_at + 4].try_into().unwrap())
                as usize;
        let mut value = snapshot[value_len_at + 4..value_len_at + 4 + value_len].to_vec();
        if key == target_key.as_slice() {
            mutate(&mut value);
            found = true;
        }
        out.extend_from_slice(&snapshot[at..at + 2 + key_len]);
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(&value);
        at = value_len_at + 4 + value_len;
    }
    assert!(found, "target key not present in snapshot");
    out.extend_from_slice(&snapshot[at..]);
    out
}

#[test]
fn commit_tier_evidence_records_verified_facts_and_rejects_every_bad_shape() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);

    // Bounds are re-checked in apply, not just the codec, so a
    // hand-constructed command cannot bypass them.
    let bounded =
        |byte_length: u64, backend_id: &str, object_uri: &str, version: Option<String>| {
            MetadataCommand::CommitTierEvidence {
                env: CommandEnvelope {
                    request_id: Uuid::new_v4(),
                    issued_at_ms: 0,
                },
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                expected_segment_generation: segment_generation,
                content_root: root,
                byte_length,
                backend_id: backend_id.to_owned(),
                object_uri: object_uri.to_owned(),
                object_version_id: None,
                manifest_version_id: version,
                manifest_core_digest: [0x5d; 32],
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: NODE,
                fencing_epoch: 1,
                verified_term: 5,
            }
        };
    for (at, command) in [
        bounded(0, "s3-native", "s3://tier/object", None),
        bounded(4096, "", "s3://tier/object", None),
        bounded(4096, &"b".repeat(65), "s3://tier/object", None),
        bounded(4096, "s3-native", "", None),
        bounded(
            4096,
            "s3-native",
            &"u".repeat(MAX_TIER_OBJECT_URI_BYTES + 1),
            None,
        ),
        bounded(4096, "s3-native", "s3://tier/object", Some("v".repeat(129))),
    ]
    .into_iter()
    .enumerate()
    {
        assert!(
            matches!(
                machine.apply(6 + at as u64, &command),
                MetadataResponse::Rejected(MetadataError::Limit(_))
            ),
            "bound violation {at} must reject with Limit"
        );
    }

    // Unknown range.
    assert_eq!(
        machine.apply(
            12,
            &MetadataCommand::CommitTierEvidence {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: Uuid::from_u128(0xff),
                segment_uuid: SEGMENT,
                expected_segment_generation: segment_generation,
                content_root: root,
                byte_length: 4096,
                backend_id: "s3-native".to_owned(),
                object_uri: "s3://tier/object".to_owned(),
                object_version_id: None,
                manifest_version_id: None,
                manifest_core_digest: [0x5d; 32],
                verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
                verifier_node_uuid: NODE,
                fencing_epoch: 1,
                verified_term: 5,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // A stale actor cannot commit evidence: epoch fencing.
    assert_eq!(
        machine.apply(
            13,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 99, NODE)
        ),
        rejected(MetadataError::EpochMismatch {
            expected: 99,
            actual: 1,
        })
    );
    // Unknown segment, then a stale CAS token.
    assert_eq!(
        machine.apply(
            14,
            &commit_tier_evidence(&mut requests, Uuid::from_u128(0xfe), root, 0, 1, NODE)
        ),
        rejected(MetadataError::NotFound)
    );
    assert_eq!(
        machine.apply(
            15,
            &commit_tier_evidence(&mut requests, SEGMENT, root, 9, 1, NODE)
        ),
        rejected(MetadataError::GenerationMismatch {
            expected: 9,
            actual: 1,
        })
    );
    // A sealed-but-unverified segment can never carry tier evidence.
    assert_eq!(
        machine.apply(
            16,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT_B,
                segment_generation: 0,
                base_offset: 64,
                next_offset: 128,
                content_root: [0xcd; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(
            17,
            &commit_tier_evidence(&mut requests, SEGMENT_B, [0xcd; 32], 0, 1, NODE)
        ),
        rejected(MetadataError::invalid_transition(
            "tier evidence requires a verified segment"
        ))
    );
    // Wrong content root.
    assert_eq!(
        machine.apply(
            18,
            &commit_tier_evidence(&mut requests, SEGMENT, [8; 32], segment_generation, 1, NODE)
        ),
        rejected(MetadataError::invalid_transition(
            "tier evidence content root does not match the sealed segment"
        ))
    );
    // Verifier must be a registered, non-dead node.
    assert_eq!(
        machine.apply(
            19,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE_C)
        ),
        rejected(MetadataError::NotFound)
    );
    machine.apply(20, &register_node(&mut requests, NODE_B));
    assert_eq!(
        machine.apply(
            21,
            &MetadataCommand::SetNodeState {
                env: requests.next(),
                node_uuid: NODE_B,
                state: NodeState::Dead,
                expected_generation: 0,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            22,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE_B)
        ),
        rejected(MetadataError::invalid_transition(
            "tier evidence verifier node is dead"
        ))
    );

    // Nothing above created a record.
    assert_eq!(tier_record(&machine, SEGMENT), None);

    // Success records verified facts only and does NOT mutate the segment.
    assert_eq!(
        machine.apply(
            23,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    let tier = tier_record(&machine, SEGMENT).expect("tier record must exist");
    assert_eq!(tier.generation, 0);
    assert_eq!(tier.segment_generation, segment_generation);
    assert_eq!(tier.content_root, root);
    assert_eq!(tier.byte_length, 4096);
    assert_eq!(tier.backend_id, "s3-native");
    assert_eq!(
        tier.object_uri,
        "s3://tier/native/events.v1/segment-30.segment"
    );
    assert_eq!(
        tier.object_version_id.as_deref(),
        Some("4sL4kqCJo05qOWBhBqpfOFAdT4dRJVvW")
    );
    assert_eq!(
        tier.manifest_version_id.as_deref(),
        Some("3sL4kqCJo05qOWBhBqpfOFAdT4dRJVvV")
    );
    assert_eq!(tier.manifest_core_digest, [0x5d; 32]);
    assert_eq!(
        tier.verification_method,
        vtop_meta::VerificationMethod::AuthenticatedContentRoot
    );
    assert_eq!(tier.verifier_node_uuid, NODE);
    assert_eq!(tier.verified_at_apply_index, 23);
    assert_eq!(tier.verified_term, 5);
    assert_eq!(tier.fencing_epoch, 1);
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::Verified);
    assert_eq!(segment.segment_generation, segment_generation);

    // One tier copy per segment in this slice.
    assert_eq!(
        machine.apply(
            24,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        rejected(MetadataError::AlreadyExists)
    );

    // Without a live lease there is no authority to commit evidence at all.
    assert_eq!(
        machine.apply(25, &release(&mut requests, 1)),
        MetadataResponse::Ack { generation: 4 }
    );
    assert_eq!(
        machine.apply(
            26,
            &commit_tier_evidence(&mut requests, SEGMENT_B, [0xcd; 32], 0, 1, NODE)
        ),
        rejected(MetadataError::invalid_transition(
            "range holds no active lease for tier evidence"
        ))
    );
}

#[test]
fn set_topic_retention_policy_follows_the_register_node_cas_pattern() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);

    // The topic must exist.
    assert_eq!(
        machine.apply(
            3,
            &MetadataCommand::SetTopicRetentionPolicy {
                env: requests.next(),
                topic_uuid: Uuid::from_u128(0xff),
                unarchived_deletion_allowed: true,
                expected_generation: None,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    // Absent + CAS expectation: nothing to CAS against.
    assert_eq!(
        machine.apply(4, &set_retention_policy(&mut requests, true, Some(0))),
        rejected(MetadataError::NotFound)
    );
    // Creation at generation 0.
    assert_eq!(
        machine.apply(5, &set_retention_policy(&mut requests, true, None)),
        MetadataResponse::Ack { generation: 0 }
    );
    // Present + no expectation: collision.
    assert_eq!(
        machine.apply(6, &set_retention_policy(&mut requests, true, None)),
        rejected(MetadataError::AlreadyExists)
    );
    // CAS with the wrong generation.
    assert_eq!(
        machine.apply(7, &set_retention_policy(&mut requests, false, Some(5))),
        rejected(MetadataError::GenerationMismatch {
            expected: 5,
            actual: 0,
        })
    );
    // CAS update flips the flag and bumps the generation.
    assert_eq!(
        machine.apply(8, &set_retention_policy(&mut requests, false, Some(0))),
        MetadataResponse::Ack { generation: 1 }
    );
    let Some(MetaValue::TopicRetentionPolicy(policy)) =
        machine.record(&MetaKey::TopicRetentionPolicy { topic_uuid: TOPIC })
    else {
        panic!("policy record must exist");
    };
    assert_eq!(policy.generation, 1);
    assert!(!policy.unarchived_deletion_allowed);
}

#[test]
fn plan_retention_requires_tier_evidence_or_an_explicit_policy() {
    // No evidence and no policy: fail-closed.
    let mut requests = Requests(0);
    let (mut machine, _root, segment_generation) = verified_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(
            6,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "retention requires pinned tier evidence or an explicit unarchived-deletion policy"
        ))
    );
    // An explicit policy that does NOT allow unarchived deletion still fails.
    assert_eq!(
        machine.apply(7, &set_retention_policy(&mut requests, false, None)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            8,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "retention requires pinned tier evidence or an explicit unarchived-deletion policy"
        ))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );
    // The committed opt-out unlocks the plan.
    assert_eq!(
        machine.apply(9, &set_retention_policy(&mut requests, true, Some(0))),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            10,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::RetentionPlanned);
    assert_eq!(segment.segment_generation, 2);
}

#[test]
fn retention_plan_cannot_be_cancelled_after_deletion_is_authorized() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(
            6,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    // The evidence path authorizes the plan...
    assert_eq!(
        machine.apply(
            7,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    // PlanRetention is durable deletion authority. Without a separate proof
    // that no worker deleted bytes, cancellation must fail closed.
    assert_eq!(
        machine.apply(8, &cancel_retention(&mut requests, SEGMENT, 2)),
        rejected(MetadataError::invalid_transition(
            "retention cannot be cancelled after deletion is authorized"
        ))
    );
    let planned = segment_record(&machine, SEGMENT);
    assert_eq!(planned.state, SegmentState::RetentionPlanned);
    assert_eq!(planned.segment_generation, 2);
    assert_eq!(
        tier_record(&machine, SEGMENT).unwrap().segment_generation,
        1
    );

    // Verification also cannot resurrect the deletion-authorized record.
    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: 2,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "verification requires SEALED_UNVERIFIED"
        ))
    );
}

#[test]
fn plan_retention_rejects_a_mismatched_evidence_root_fail_closed() {
    // No command sequence can produce evidence whose root disagrees with the
    // segment (CommitTierEvidence validates the match and nothing rewrites a
    // sealed root), so reach the defensive branch through snapshot surgery:
    // flip the pinned root inside the tier record, then inside the segment
    // record, and require the fail-closed rejection both ways even though a
    // maximally permissive policy is committed.
    let build = || {
        let mut requests = Requests(0);
        let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);
        assert_eq!(
            machine.apply(
                6,
                &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
            ),
            MetadataResponse::Ack { generation: 0 }
        );
        assert_eq!(
            machine.apply(7, &set_retention_policy(&mut requests, true, None)),
            MetadataResponse::Ack { generation: 0 }
        );
        (machine, requests)
    };

    // Tier record layout: tag 1 + generation 8 + segment generation 8, then
    // the 32-byte content root.
    let (machine, mut requests) = build();
    let snapshot = machine.encode_snapshot().unwrap();
    let evidence_flipped = rewrite_snapshot_value(
        &snapshot,
        &MetaKey::SegmentTierCopy {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        },
        |value| value[17] ^= 0xff,
    );
    let mut mutated = MetaStateMachine::decode_snapshot(&evidence_flipped).unwrap();
    assert_eq!(
        mutated.apply(8, &plan_retention(&mut requests, SEGMENT, 1, 1)),
        rejected(MetadataError::invalid_transition(
            "tier evidence content root does not match the sealed segment"
        ))
    );

    // Segment record layout: tag 1 + generation 8 + base 8 + next 8, then
    // the 32-byte content root.
    let (machine, mut requests) = build();
    let snapshot = machine.encode_snapshot().unwrap();
    let segment_flipped = rewrite_snapshot_value(
        &snapshot,
        &MetaKey::Segment {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        },
        |value| value[25] ^= 0xff,
    );
    let mut mutated = MetaStateMachine::decode_snapshot(&segment_flipped).unwrap();
    assert_eq!(
        mutated.apply(8, &plan_retention(&mut requests, SEGMENT, 1, 1)),
        rejected(MetadataError::invalid_transition(
            "tier evidence content root does not match the sealed segment"
        ))
    );
}

#[test]
fn plan_retention_rejects_unpinned_tier_evidence_fail_closed() {
    // The CLI refuses to commit unpinned evidence under --require-versioning,
    // but the state machine is the trust boundary: a hand-constructed admin
    // proposal (or a legacy snapshot record) can carry
    // `object_version_id: None`. The commit is accepted — the record stays
    // as the audit anchor — and the retention gate must refuse deletion
    // authority on it.
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);
    let unpinned = |requests: &mut Requests| {
        let mut command =
            commit_tier_evidence(requests, SEGMENT, root, segment_generation, 1, NODE);
        let MetadataCommand::CommitTierEvidence {
            object_version_id, ..
        } = &mut command
        else {
            unreachable!("helper builds CommitTierEvidence");
        };
        *object_version_id = None;
        command
    };
    assert_eq!(
        machine.apply(6, &unpinned(&mut requests)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        tier_record(&machine, SEGMENT).unwrap().object_version_id,
        None
    );

    // Archival-required plan: rejected, and the segment is untouched.
    assert_eq!(
        machine.apply(
            7,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "retention requires pinned tier evidence or an explicit unarchived-deletion policy"
        ))
    );
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::Verified);
    assert_eq!(segment.segment_generation, segment_generation);

    // An explicit policy that does NOT allow unarchived deletion still fails:
    // unpinned evidence counts as no archive at all.
    assert_eq!(
        machine.apply(8, &set_retention_policy(&mut requests, false, None)),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            9,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "retention requires pinned tier evidence or an explicit unarchived-deletion policy"
        ))
    );

    // The explicit unarchived-deletion policy is the operator escape hatch:
    // deletion without archive is already expressible, so the plan succeeds.
    assert_eq!(
        machine.apply(10, &set_retention_policy(&mut requests, true, Some(0))),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            11,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::RetentionPlanned
    );

    // Pinned evidence authorizes the plan with no policy at all; covered by
    // retention_plan_cannot_be_cancelled_after_deletion_is_authorized and
    // plan_retention_is_blocked_by_durable_group_cursors_below_the_segment_end,
    // which both commit evidence through the pinned helper and plan
    // successfully.
}

#[test]
fn plan_retention_is_blocked_by_durable_group_cursors_below_the_segment_end() {
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
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: 0,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            9,
            &commit_tier_evidence(&mut requests, SEGMENT, root, 1, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // A durable committed cursor below the segment end blocks the plan.
    let cursor = |requests: &mut Requests,
                  segment: Uuid,
                  segment_generation: u64,
                  segment_root: [u8; 32],
                  record_offset: u64,
                  expected: Option<u64>| {
        MetadataCommand::CommitGroupCursor {
            env: requests.next(),
            group_uuid: GROUP,
            member_uuid: MEMBER,
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            topic_epoch: 1,
            range_generation: 0,
            segment_uuid: segment,
            segment_generation,
            segment_root,
            record_offset,
            record_index: 0,
            lineage_transition_id: None,
            expected_checkpoint_generation: expected,
        }
    };
    assert_eq!(
        machine.apply(10, &cursor(&mut requests, SEGMENT, 1, root, 10, None)),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 0,
        }
    );
    assert_eq!(
        machine.apply(11, &plan_retention(&mut requests, SEGMENT, 1, 1)),
        rejected(MetadataError::invalid_transition(format!(
            "group cursor {GROUP} at offset 10 is below segment end 100"
        )))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );

    // A cursor at exactly next_offset has fully consumed the segment and
    // does not block: advancing unblocks the same plan.
    assert_eq!(
        machine.apply(12, &cursor(&mut requests, SEGMENT, 1, root, 100, Some(0))),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 1,
        }
    );
    assert_eq!(
        machine.apply(13, &plan_retention(&mut requests, SEGMENT, 1, 1)),
        MetadataResponse::Ack { generation: 2 }
    );

    // A cursor sitting in an EARLIER segment of the range still protects a
    // later one: offsets are monotonic within the range's lineage stream.
    let root_b = [0xcd; 32];
    assert_eq!(
        machine.apply(
            14,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT_B,
                segment_generation: 0,
                base_offset: 100,
                next_offset: 200,
                content_root: root_b,
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(
            15,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT_B,
                content_root: root_b,
                expected_generation: 0,
            }
        ),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            16,
            &commit_tier_evidence(&mut requests, SEGMENT_B, root_b, 1, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(17, &plan_retention(&mut requests, SEGMENT_B, 1, 1)),
        rejected(MetadataError::invalid_transition(format!(
            "group cursor {GROUP} at offset 100 is below segment end 200"
        )))
    );

    // A cursor on a different topic/range holds no claim over this one, and
    // once the range's own cursor advances past the segment end the plan
    // succeeds.
    let other_topic = Uuid::from_u128(0x60);
    let other_range = Uuid::from_u128(0x61);
    assert_eq!(
        machine.apply(
            18,
            &MetadataCommand::CreateTopic {
                env: requests.next(),
                name: "other.v1".to_owned(),
                topic_uuid: other_topic,
                root_range_uuid: other_range,
            }
        ),
        MetadataResponse::TopicCreated {
            topic_uuid: other_topic,
            topic_epoch: 1,
            root_range_uuid: other_range,
        }
    );
    assert_eq!(
        machine.apply(
            19,
            &MetadataCommand::GrantRangeLease {
                env: requests.next(),
                topic_uuid: other_topic,
                range_uuid: other_range,
                holder_node_uuid: NODE,
                expected_range_generation: 0,
            }
        ),
        MetadataResponse::LeaseGranted { fencing_epoch: 1 }
    );
    let other_root = [0x11; 32];
    assert_eq!(
        machine.apply(
            20,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: other_topic,
                range_uuid: other_range,
                segment_uuid: Uuid::from_u128(0x32),
                segment_generation: 0,
                base_offset: 0,
                next_offset: 50,
                content_root: other_root,
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(
            21,
            &MetadataCommand::AssignMemberRanges {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                ranges: vec![
                    RangeAssignment {
                        topic_uuid: TOPIC,
                        range_uuid: RANGE,
                    },
                    RangeAssignment {
                        topic_uuid: other_topic,
                        range_uuid: other_range,
                    },
                ],
                expected_member_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(
            22,
            &MetadataCommand::CommitGroupCursor {
                env: requests.next(),
                group_uuid: GROUP,
                member_uuid: MEMBER,
                topic_uuid: other_topic,
                range_uuid: other_range,
                topic_epoch: 1,
                range_generation: 0,
                segment_uuid: Uuid::from_u128(0x32),
                segment_generation: 0,
                segment_root: other_root,
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
            23,
            &cursor(&mut requests, SEGMENT_B, 1, root_b, 200, Some(1))
        ),
        MetadataResponse::CursorCommitted {
            checkpoint_generation: 2,
        }
    );
    assert_eq!(
        machine.apply(24, &plan_retention(&mut requests, SEGMENT_B, 1, 1)),
        MetadataResponse::Ack { generation: 2 }
    );
}

#[test]
fn plan_retention_is_mutually_exclusive_with_rebalance_and_repair() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];

    assert_eq!(
        machine.apply(
            12,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );

    // A live rebalance intent blocks retention planning outright.
    assert_eq!(
        machine.apply(13, &propose_rebalance(&mut requests, from, spare, 0)),
        MetadataResponse::Ack { generation: 1 }
    );
    assert_eq!(
        machine.apply(
            14,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "segment has an active rebalance intent"
        ))
    );
    assert_eq!(
        machine.apply(15, &cancel_rebalance(&mut requests, 1)),
        MetadataResponse::Ack { generation: 2 }
    );

    // A committed replacement proof (an in-flight verified move) also blocks.
    assert_eq!(
        machine.apply(
            16,
            &commit_proof(&mut requests, root, from, spare, segment_generation)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            17,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "segment has a committed replacement proof; complete or resolve the replica retirement first"
        ))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );
}

#[test]
fn retention_states_block_placement_rebalance_and_repair_conversely() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, spare) =
        placed_segment_machine(&mut requests);
    let from = replicas[0];

    assert_eq!(
        machine.apply(
            12,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            13,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );

    // The existing Verified guards pin the reverse direction for
    // RetentionPlanned...
    let assert_blocked = |machine: &mut MetaStateMachine,
                          requests: &mut Requests,
                          at: u64,
                          expected_segment_generation: u64,
                          expected_placement_generation: u64| {
        assert_eq!(
            machine.apply(
                at,
                &propose_rebalance(requests, from, spare, expected_placement_generation)
            ),
            rejected(MetadataError::invalid_transition(
                "rebalance requires a verified segment"
            ))
        );
        assert_eq!(
            machine.apply(
                at + 1,
                &MetadataCommand::CommitSegmentPlacement {
                    env: requests.next(),
                    topic_uuid: TOPIC,
                    range_uuid: RANGE,
                    segment_uuid: SEGMENT,
                    replication_factor: 2,
                    replica_nodes: replicas.clone(),
                    expected_segment_generation,
                    expected_placement_generation: Some(0),
                }
            ),
            rejected(MetadataError::invalid_transition(
                "placement requires a verified segment"
            ))
        );
        assert_eq!(
            machine.apply(
                at + 2,
                &commit_proof(requests, root, from, spare, expected_segment_generation)
            ),
            rejected(MetadataError::invalid_transition(
                "replacement proof requires a verified or repairing segment"
            ))
        );
    };
    assert_blocked(&mut machine, &mut requests, 14, 2, 0);

    // ...and for RetentionExpired (whose confirmation bumped the placement
    // generation while emptying the replica set).
    assert_eq!(
        machine.apply(17, &confirm_retention_expired(&mut requests, SEGMENT, 2)),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_blocked(&mut machine, &mut requests, 18, 3, 1);
}

#[test]
fn confirm_retention_expired_empties_placement_and_preserves_declared_rf() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, replicas, _spare) =
        placed_segment_machine(&mut requests);

    assert_eq!(
        machine.apply(
            12,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    // Confirmation without a plan is rejected.
    assert_eq!(
        machine.apply(
            13,
            &confirm_retention_expired(&mut requests, SEGMENT, segment_generation)
        ),
        rejected(MetadataError::invalid_transition(
            "retention confirmation requires RETENTION_PLANNED"
        ))
    );
    assert_eq!(
        machine.apply(
            14,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    // A rejected confirmation mutates nothing (apply is all-or-nothing).
    assert_eq!(
        machine.apply(15, &confirm_retention_expired(&mut requests, SEGMENT, 999)),
        rejected(MetadataError::GenerationMismatch {
            expected: 999,
            actual: 2,
        })
    );
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::RetentionPlanned);
    assert_eq!(segment.segment_generation, 2);
    assert_eq!(placement_record(&machine).replica_nodes, replicas);

    // The single-apply effect: segment state + generation, and the placement
    // empties while its declared factor survives verbatim — the durable
    // audit reads "target RF 2, zero local replicas, tier evidence present".
    assert_eq!(
        machine.apply(16, &confirm_retention_expired(&mut requests, SEGMENT, 2)),
        MetadataResponse::Ack { generation: 3 }
    );
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::RetentionExpired);
    assert_eq!(segment.segment_generation, 3);
    assert_eq!(segment.content_root, root);
    let placement = placement_record(&machine);
    assert!(placement.replica_nodes.is_empty());
    assert_eq!(placement.declared_replication_factor, 2);
    assert_eq!(placement.generation, 1);
    assert_eq!(placement.committed_apply_index, 16);
    // The segment and tier-copy records are retained forever.
    assert!(tier_record(&machine, SEGMENT).is_some());

    // Terminal: neither confirmation nor cancellation applies again.
    assert_eq!(
        machine.apply(17, &confirm_retention_expired(&mut requests, SEGMENT, 3)),
        rejected(MetadataError::invalid_transition(
            "retention confirmation requires RETENTION_PLANNED"
        ))
    );
    assert_eq!(
        machine.apply(18, &cancel_retention(&mut requests, SEGMENT, 3)),
        rejected(MetadataError::invalid_transition(
            "retention cancellation requires RETENTION_PLANNED"
        ))
    );
    assert_eq!(
        machine.apply(
            19,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: root,
                expected_generation: 3,
            }
        ),
        rejected(MetadataError::invalid_transition(
            "verification requires SEALED_UNVERIFIED"
        ))
    );
}

#[test]
fn cancel_retention_fails_closed_and_leaves_the_plan_byte_exactly_unchanged() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(
            6,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            7,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    // A cancel with a stale CAS token changes nothing.
    assert_eq!(
        machine.apply(8, &cancel_retention(&mut requests, SEGMENT, 7)),
        rejected(MetadataError::GenerationMismatch {
            expected: 7,
            actual: 2,
        })
    );
    assert_eq!(
        machine.apply(9, &cancel_retention(&mut requests, SEGMENT, 2)),
        rejected(MetadataError::invalid_transition(
            "retention cannot be cancelled after deletion is authorized"
        ))
    );
    let after = segment_record(&machine, SEGMENT);
    assert_eq!(after.state, SegmentState::RetentionPlanned);
    assert_eq!(after.segment_generation, 2);
    assert_eq!(after.content_root, root);
    // Repeating the cancellation remains a fail-closed rejection.
    assert_eq!(
        machine.apply(10, &cancel_retention(&mut requests, SEGMENT, 2)),
        rejected(MetadataError::invalid_transition(
            "retention cannot be cancelled after deletion is authorized"
        ))
    );
    // Unknown segment.
    assert_eq!(
        machine.apply(
            11,
            &cancel_retention(&mut requests, Uuid::from_u128(0xfe), 0)
        ),
        rejected(MetadataError::NotFound)
    );
}

#[test]
fn retention_planning_rejects_without_lease_and_with_stale_epoch() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation) = verified_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(
            6,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    // Unknown range, stale epoch, unknown segment, stale CAS.
    assert_eq!(
        machine.apply(
            7,
            &MetadataCommand::PlanRetention {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: Uuid::from_u128(0xff),
                segment_uuid: SEGMENT,
                expected_segment_generation: segment_generation,
                fencing_epoch: 1,
            }
        ),
        rejected(MetadataError::NotFound)
    );
    assert_eq!(
        machine.apply(
            8,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 99)
        ),
        rejected(MetadataError::EpochMismatch {
            expected: 99,
            actual: 1,
        })
    );
    assert_eq!(
        machine.apply(
            9,
            &plan_retention(&mut requests, Uuid::from_u128(0xfe), 0, 1)
        ),
        rejected(MetadataError::NotFound)
    );
    assert_eq!(
        machine.apply(10, &plan_retention(&mut requests, SEGMENT, 9, 1)),
        rejected(MetadataError::GenerationMismatch {
            expected: 9,
            actual: 1,
        })
    );
    // Releasing the lease removes retention authority entirely: deletion is
    // an act of current leaseholder authority.
    assert_eq!(
        machine.apply(11, &release(&mut requests, 1)),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(
            12,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        rejected(MetadataError::invalid_transition(
            "range holds no active lease for retention planning"
        ))
    );
    assert_eq!(
        segment_record(&machine, SEGMENT).state,
        SegmentState::Verified
    );
}

#[test]
fn retention_generation_ceilings_reject_deterministically_without_mutation() {
    let mut requests = Requests(0);
    let mut machine = machine_with_topic_and_node(&mut requests);
    machine.apply(3, &grant(&mut requests, NODE, 0));
    assert_eq!(
        machine.apply(4, &set_retention_policy(&mut requests, true, None)),
        MetadataResponse::Ack { generation: 0 }
    );

    // PlanRetention at the segment-generation ceiling.
    assert_eq!(
        machine.apply(
            5,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                segment_generation: u64::MAX - 1,
                base_offset: 0,
                next_offset: 64,
                content_root: [7; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 1,
            }
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    assert_eq!(
        machine.apply(
            6,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT,
                content_root: [7; 32],
                expected_generation: u64::MAX - 1,
            }
        ),
        MetadataResponse::Ack {
            generation: u64::MAX
        }
    );
    assert!(matches!(
        machine.apply(7, &plan_retention(&mut requests, SEGMENT, u64::MAX, 1)),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
    let segment = segment_record(&machine, SEGMENT);
    assert_eq!(segment.state, SegmentState::Verified);
    assert_eq!(segment.segment_generation, u64::MAX);

    // Confirm and cancel at the ceiling: plan a second segment into
    // RetentionPlanned at u64::MAX, then both follow-ups must reject with
    // Limit and leave it untouched.
    assert_eq!(
        machine.apply(
            8,
            &MetadataCommand::RegisterSealedSegment {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT_B,
                segment_generation: u64::MAX - 2,
                base_offset: 64,
                next_offset: 128,
                content_root: [8; 32],
                sealed_by_epoch: 1,
                expected_range_generation: 2,
            }
        ),
        MetadataResponse::Ack { generation: 3 }
    );
    assert_eq!(
        machine.apply(
            9,
            &MetadataCommand::MarkSegmentVerified {
                env: requests.next(),
                topic_uuid: TOPIC,
                range_uuid: RANGE,
                segment_uuid: SEGMENT_B,
                content_root: [8; 32],
                expected_generation: u64::MAX - 2,
            }
        ),
        MetadataResponse::Ack {
            generation: u64::MAX - 1
        }
    );
    assert_eq!(
        machine.apply(
            10,
            &plan_retention(&mut requests, SEGMENT_B, u64::MAX - 1, 1)
        ),
        MetadataResponse::Ack {
            generation: u64::MAX
        }
    );
    assert!(matches!(
        machine.apply(
            11,
            &confirm_retention_expired(&mut requests, SEGMENT_B, u64::MAX)
        ),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
    assert_eq!(
        machine.apply(12, &cancel_retention(&mut requests, SEGMENT_B, u64::MAX)),
        rejected(MetadataError::invalid_transition(
            "retention cannot be cancelled after deletion is authorized"
        ))
    );
    let segment = segment_record(&machine, SEGMENT_B);
    assert_eq!(segment.state, SegmentState::RetentionPlanned);
    assert_eq!(segment.segment_generation, u64::MAX);
}

#[test]
fn confirm_retention_expired_validates_the_placement_ceiling_before_mutating() {
    // A placement generation at the ceiling is unreachable through commands
    // (it starts at 0 and only ever increments), so reach the defensive
    // branch through snapshot surgery and require all-or-nothing behaviour:
    // the segment must NOT flip to RetentionExpired when the placement bump
    // would overflow.
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, _replicas, _spare) =
        placed_segment_machine(&mut requests);
    assert_eq!(
        machine.apply(
            12,
            &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
        ),
        MetadataResponse::Ack { generation: 0 }
    );
    assert_eq!(
        machine.apply(
            13,
            &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
        ),
        MetadataResponse::Ack { generation: 2 }
    );
    let snapshot = machine.encode_snapshot().unwrap();
    // Placement record layout: tag 1, then the 8-byte generation.
    let mutated_snapshot = rewrite_snapshot_value(
        &snapshot,
        &MetaKey::SegmentPlacement {
            topic_uuid: TOPIC,
            range_uuid: RANGE,
            segment_uuid: SEGMENT,
        },
        |value| value[1..9].copy_from_slice(&u64::MAX.to_be_bytes()),
    );
    let mut mutated = MetaStateMachine::decode_snapshot(&mutated_snapshot).unwrap();
    assert!(matches!(
        mutated.apply(14, &confirm_retention_expired(&mut requests, SEGMENT, 2)),
        MetadataResponse::Rejected(MetadataError::Limit(_))
    ));
    let segment = segment_record(&mutated, SEGMENT);
    assert_eq!(segment.state, SegmentState::RetentionPlanned);
    assert_eq!(segment.segment_generation, 2);
    assert!(!placement_record(&mutated).replica_nodes.is_empty());
}

#[test]
fn retention_full_circle_produces_byte_identical_snapshots() {
    let build = || {
        let mut requests = Requests(0);
        let (mut machine, root, segment_generation, _replicas, _spare) =
            placed_segment_machine(&mut requests);
        assert_eq!(
            machine.apply(
                12,
                &commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE)
            ),
            MetadataResponse::Ack { generation: 0 }
        );
        assert_eq!(
            machine.apply(13, &set_retention_policy(&mut requests, false, None)),
            MetadataResponse::Ack { generation: 0 }
        );
        assert_eq!(
            machine.apply(
                14,
                &plan_retention(&mut requests, SEGMENT, segment_generation, 1)
            ),
            MetadataResponse::Ack { generation: 2 }
        );
        assert_eq!(
            machine.apply(15, &confirm_retention_expired(&mut requests, SEGMENT, 2)),
            MetadataResponse::Ack { generation: 3 }
        );
        machine
    };
    let first = build().encode_snapshot().unwrap();
    let second = build().encode_snapshot().unwrap();
    assert_eq!(first, second);
    let decoded = MetaStateMachine::decode_snapshot(&first).unwrap();
    assert_eq!(
        decoded.encode_snapshot().unwrap(),
        first,
        "decode/encode must be a fixed point"
    );
    // The full-circle audit trail survives the round trip.
    assert_eq!(
        segment_record(&decoded, SEGMENT).state,
        SegmentState::RetentionExpired
    );
    assert!(tier_record(&decoded, SEGMENT).is_some());
    let placement = placement_record(&decoded);
    assert!(placement.replica_nodes.is_empty());
    assert_eq!(placement.declared_replication_factor, 2);
}

#[test]
fn retention_commands_deduplicate_including_across_snapshot_restore() {
    let mut requests = Requests(0);
    let (mut machine, root, segment_generation, _replicas, _spare) =
        placed_segment_machine(&mut requests);

    let evidence = commit_tier_evidence(&mut requests, SEGMENT, root, segment_generation, 1, NODE);
    let policy = set_retention_policy(&mut requests, true, None);
    let plan = plan_retention(&mut requests, SEGMENT, segment_generation, 1);
    let cancel = cancel_retention(&mut requests, SEGMENT, 2);
    let confirm = confirm_retention_expired(&mut requests, SEGMENT, 2);

    // Drive the policy path and commit the evidence LAST so its dedup entry —
    // a rejection — is exercised too.
    let mut originals = Vec::new();
    for (at, command) in [&policy, &plan, &cancel, &confirm, &evidence]
        .into_iter()
        .enumerate()
    {
        originals.push(machine.apply(12 + at as u64, command));
    }
    assert_eq!(
        originals[..4],
        [
            MetadataResponse::Ack { generation: 0 },
            MetadataResponse::Ack { generation: 2 },
            rejected(MetadataError::invalid_transition(
                "retention cannot be cancelled after deletion is authorized"
            )),
            MetadataResponse::Ack { generation: 3 },
        ]
    );
    // Evidence proposed against the now-RetentionExpired segment is a
    // rejection — dedup must preserve rejections identically too.
    assert!(matches!(
        originals[4],
        MetadataResponse::Rejected(MetadataError::GenerationMismatch { .. })
    ));

    // Replays return the stored originals even though the state has moved on.
    for (at, (command, original)) in [&policy, &plan, &cancel, &confirm, &evidence]
        .into_iter()
        .zip(&originals)
        .enumerate()
    {
        assert_eq!(machine.apply(30 + at as u64, command), *original);
    }
    // And across a snapshot round trip.
    let mut restored =
        MetaStateMachine::decode_snapshot(&machine.encode_snapshot().unwrap()).unwrap();
    for (at, (command, original)) in [&policy, &plan, &cancel, &confirm, &evidence]
        .into_iter()
        .zip(&originals)
        .enumerate()
    {
        assert_eq!(restored.apply(40 + at as u64, command), *original);
    }
}
