//! Pinned golden vectors for every durable byte format the crate writes —
//! hard state, log chunk header, entry frame, and snapshot file — plus
//! corruption, trailing-byte, oversize, and unknown-version rejection for
//! each. The vectors are produced through the sim storage seam, so they pin
//! the exact bytes the production write paths emit.

use std::io::Write;
use std::path::Path;
use uuid::Uuid;
use vtop_log::env::{Env, OpenMode};
use vtop_log::sim::SimStorage;
use vtop_meta::{
    CommandEnvelope, HardState, HardStateFile, MetaLog, MetaLogConfig, MetaLogEntry,
    MetaLogPayload, MetaMembership, MetaNodeId, MetaSnapshots, MetaStateMachine, MetaStoreError,
    MetadataCommand,
};

const ROOT: &str = "/meta";
const SEED: u64 = 0x5eed_0093;

/// v1 hard state: term 3, vote for node 7 committed, generation 1.
const GOLDEN_HARD_STATE_HEX: &str = concat!(
    "56544f504d48533100010000000000000003010000000000000007010000000000000001",
    "185d717dbe6c4b3ccc4e0d711d1281258a1249a80449bfa1d1d12a1f2dff7df3"
);

/// v1 chunk header: cluster c1c1c1c1-0000-4000-8000-000000000001, shard 0,
/// first index 1.
const GOLDEN_CHUNK_HEADER_HEX: &str = concat!(
    "56544f504d4c47310001c1c1c1c100004000800000000000000100000000000000000001",
    "3e89b7d489076fcec0818eb1a0ca5b09a1abe2191996003a2c22592c9ca9c79a"
);

/// v1 Normal entry frame: term 2, index 1, payload = the pinned CreateTopic
/// command from the command-codec golden test.
const GOLDEN_ENTRY_FRAME_HEX: &str = concat!(
    "56544f504d4c4531000000790000000000000002000000000000000101000000",
    "44000300112233445566778899aabbccddeeff0102030405060708000861756469742e76",
    "31ffeeddccbbaa998877665544332211000f1e2d3c4b5a69788796a5b4c3d2e1f0",
    "745d281bdd16296fb0e9fbbd3b4aa5fca1a5d4393926b110ad558a6a2ca83b94"
);

/// v1 state-machine snapshot payload after applying the pinned CreateTopic
/// at index 1 (records plus the one-entry dedup FIFO).
const GOLDEN_STATE_PAYLOAD_HEX: &str = concat!(
    "000100000003000b00000361756469742e76310000001903ffeeddccbbaa998877665544",
    "3322110000000000000000010013000004ffeeddccbbaa99887766554433221100000000",
    "1b02000861756469742e7631000000000000000100000000000000000023000005ffeedd",
    "ccbbaa998877665544332211000f1e2d3c4b5a69788796a5b4c3d2e1f00000001b040000",
    "000000000000000000000000000000000000000000000000000000010011223344556677",
    "8899aabbccddeeff0000002a0002ffeeddccbbaa99887766554433221100000000000000",
    "00010f1e2d3c4b5a69788796a5b4c3d2e1f0"
);

/// v1 snapshot file: coverage (1, 2), voters {1,2,3}, learner (4, n4:9200),
/// snapshot id "golden-snap", payload = the golden state payload.
const GOLDEN_SNAPSHOT_FILE_HEX: &str = concat!(
    "56544f504d534e310001c1c1c1c100004000800000000000000100000000000000000001",
    "00000000000000020000002d000300000000000000010000000000000002000000000000",
    "00030001000000000000000400076e343a39323030000b676f6c64656e2d736e61700000",
    "0000000000ea000100000003000b00000361756469742e76310000001903ffeeddccbbaa",
    "9988776655443322110000000000000000010013000004ffeeddccbbaa99887766554433",
    "2211000000001b02000861756469742e7631000000000000000100000000000000000023",
    "000005ffeeddccbbaa998877665544332211000f1e2d3c4b5a69788796a5b4c3d2e1f000",
    "00001b040000000000000000000000000000000000000000000000000000000000010011",
    "2233445566778899aabbccddeeff0000002a0002ffeeddccbbaa99887766554433221100",
    "00000000000000010f1e2d3c4b5a69788796a5b4c3d2e1f0990634000023b9e4ec91533a",
    "b39964e2e2f1464a7da8fc9791a37d0e4375834e"
);

/// v1 state-machine snapshot payload containing the stage-8b retention
/// records: a tier-copy evidence record and a topic retention policy, plus
/// the node/topic/range/segment records and dedup FIFO that produced them.
/// Pins the new value tags (17, 18) byte-exactly.
const GOLDEN_RETENTION_PAYLOAD_HEX: &str = concat!(
    "000100000007001300000211111111222243338444555555555555000000190c00076e31",
    "3a39323030010000000000000000000000000064000b00000361756469742e7631000000",
    "1903ffeeddccbbaa9988776655443322110000000000000000010013000004ffeeddccbb",
    "aa998877665544332211000000001b02000861756469742e763100000000000000010000",
    "0000000000000023000005ffeeddccbbaa998877665544332211000f1e2d3c4b5a697887",
    "96a5b4c3d2e1f00000003b04000000000000000200000000000000000000000000000000",
    "010111111111222243338444555555555555000000000000000100000000000000030033",
    "000006ffeeddccbbaa998877665544332211000f1e2d3c4b5a69788796a5b4c3d2e1f0aa",
    "aaaaaabbbb4ccc8dddeeeeeeeeeeee000000420500000000000000010000000000000000",
    "0000000000000080abababababababababababababababababababababababababababab",
    "abababab0200000000000000010033000010ffeeddccbbaa998877665544332211000f1e",
    "2d3c4b5a69788796a5b4c3d2e1f0aaaaaaaabbbb4ccc8dddeeeeeeeeeeee000000db1100",
    "000000000000000000000000000001ababababababababababababababababababababab",
    "ababababababababababab0000000000001000000973332d6e6174697665002973333a2f",
    "2f746965722f6e61746976652f61756469742e76312f7365676d656e742e7365676d656e",
    "7401002033734c346b71434a6f3035714f574268427170664f464164543464524a567656",
    "5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d01111111",
    "112222433384445555555555550000000000000006000000000000000200000000000000",
    "010013000011ffeeddccbbaa998877665544332211000000000a12000000000000000000",
    "00000007000000000000000000009300000000010000000a000100000000000000000000",
    "00000000000000009300000000020000002a0002ffeeddccbbaa99887766554433221100",
    "00000000000000010f1e2d3c4b5a69788796a5b4c3d2e1f0000000000000000000009300",
    "000000030000000a00030000000000000001000000000000000000009300000000040000",
    "000a00010000000000000002000000000000000000009300000000050000000a00010000",
    "000000000001000000000000000000009300000000060000000a00010000000000000000",
    "000000000000000000009300000000070000000a00010000000000000000",
);

fn cluster_id() -> Uuid {
    Uuid::parse_str("c1c1c1c1-0000-4000-8000-000000000001").unwrap()
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn from_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

fn sim_env() -> (SimStorage, Env) {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    (sim, env)
}

fn write_sim_file(env: &Env, name: &str, bytes: &[u8]) {
    let path = Path::new(ROOT).join(name);
    let mut file = env.storage.open(&path, OpenMode::CreateNew).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_data().unwrap();
    env.storage.sync_dir(Path::new(ROOT)).unwrap();
}

fn golden_command() -> MetadataCommand {
    MetadataCommand::CreateTopic {
        env: CommandEnvelope {
            request_id: Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            issued_at_ms: 0x0102_0304_0506_0708,
        },
        name: "audit.v1".to_owned(),
        topic_uuid: Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap(),
        root_range_uuid: Uuid::parse_str("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0").unwrap(),
    }
}

fn golden_entry() -> MetaLogEntry {
    MetaLogEntry {
        term: 2,
        index: 1,
        payload: MetaLogPayload::Normal(golden_command()),
    }
}

#[test]
fn v1_hard_state_file_matches_golden_vector() {
    let (sim, env) = sim_env();
    let mut file = HardStateFile::open_in(&env, "/meta/meta.hardstate").unwrap();
    file.save(HardState {
        term: 3,
        voted_for: Some(MetaNodeId(7)),
        vote_committed: true,
    })
    .unwrap();
    let durable = &sim.snapshot().files[Path::new("/meta/meta.hardstate")];
    assert_eq!(to_hex(durable), GOLDEN_HARD_STATE_HEX);
}

#[test]
fn v1_hard_state_rejects_corruption_trailing_truncation_and_unknown_version() {
    let golden = from_hex(GOLDEN_HARD_STATE_HEX);

    // Every single-byte flip must be rejected, never silently defaulted.
    for byte_index in 0..golden.len() {
        let (_sim, env) = sim_env();
        let mut mutated = golden.clone();
        mutated[byte_index] ^= 0xff;
        write_sim_file(&env, "meta.hardstate", &mutated);
        assert!(
            HardStateFile::open_in(&env, "/meta/meta.hardstate").is_err(),
            "flip at byte {byte_index} was accepted"
        );
    }

    let (_sim, env) = sim_env();
    let mut trailing = golden.clone();
    trailing.push(0);
    write_sim_file(&env, "meta.hardstate", &trailing);
    assert!(HardStateFile::open_in(&env, "/meta/meta.hardstate").is_err());

    let (_sim, env) = sim_env();
    let mut truncated = golden.clone();
    truncated.pop();
    write_sim_file(&env, "meta.hardstate", &truncated);
    assert!(HardStateFile::open_in(&env, "/meta/meta.hardstate").is_err());

    // A future version with a valid checksum must be refused, not guessed at.
    let (_sim, env) = sim_env();
    let mut future = golden.clone();
    future[8..10].copy_from_slice(&2_u16.to_be_bytes());
    let checksum = *blake3::hash(&future[..36]).as_bytes();
    future[36..].copy_from_slice(&checksum);
    write_sim_file(&env, "meta.hardstate", &future);
    assert!(matches!(
        HardStateFile::open_in(&env, "/meta/meta.hardstate"),
        Err(MetaStoreError::UnsupportedVersion { version: 2, .. })
    ));
}

#[test]
fn v1_log_chunk_header_and_entry_frame_match_golden_vectors() {
    assert_eq!(
        to_hex(&golden_entry().encode_frame().unwrap()),
        GOLDEN_ENTRY_FRAME_HEX
    );

    let (sim, env) = sim_env();
    let mut log = MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default()).unwrap();
    log.append(&[golden_entry()]).unwrap();
    let durable = &sim.snapshot().files[Path::new("/meta/log-00000000000000000001.vmlog")];
    assert_eq!(to_hex(&durable[..68]), GOLDEN_CHUNK_HEADER_HEX);
    assert_eq!(
        to_hex(durable),
        format!("{GOLDEN_CHUNK_HEADER_HEX}{GOLDEN_ENTRY_FRAME_HEX}")
    );
}

#[test]
fn v1_log_rejects_corruption_trailing_oversize_unknown_version_and_foreign_cluster() {
    // Two entries so the first frame is provably not the tail.
    let second_entry = MetaLogEntry {
        term: 2,
        index: 2,
        payload: MetaLogPayload::Blank,
    };
    let header = from_hex(GOLDEN_CHUNK_HEADER_HEX);
    let first_frame = from_hex(GOLDEN_ENTRY_FRAME_HEX);
    let second_frame = second_entry.encode_frame().unwrap();
    let mut pristine = header.clone();
    pristine.extend_from_slice(&first_frame);
    pristine.extend_from_slice(&second_frame);
    let chunk_name = "log-00000000000000000001.vmlog";
    let original_entries = [golden_entry(), second_entry.clone()];
    // A flip inside a frame-length field can make the frame claim to run
    // past EOF, which is byte-indistinguishable from a genuine tear; the
    // frozen tail policy then truncates. Everywhere else corruption must be
    // a hard error.
    let length_field_bytes: Vec<usize> = [header.len(), header.len() + first_frame.len()]
        .into_iter()
        .flat_map(|frame_start| frame_start + 8..frame_start + 12)
        .collect();

    for byte_index in 0..pristine.len() {
        let (_sim, env) = sim_env();
        let mut mutated = pristine.clone();
        mutated[byte_index] ^= 0xff;
        write_sim_file(&env, chunk_name, &mutated);
        let opened = MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default());
        let tail_extent = byte_index >= header.len() + first_frame.len();
        match opened {
            Err(_) => {}
            Ok(log) if length_field_bytes.contains(&byte_index) || tail_extent => {
                // Tear-shaped outcome: only a strict prefix of the original
                // entries may survive, byte-identical; corrupted bytes must
                // never be served as valid entries.
                let count = log.entry_count();
                assert!(count < 2, "flip at byte {byte_index} kept both entries");
                let recovered = if count == 0 {
                    Vec::new()
                } else {
                    log.read_range(1, 1 + count as u64).unwrap()
                };
                assert_eq!(recovered, original_entries[..count]);
            }
            Ok(_) => panic!("flip at byte {byte_index} was accepted"),
        }
    }

    // Trailing garbage after the last frame is corruption (a tear always
    // leaves a prefix of a real frame, and 'X' is no such prefix).
    let (_sim, env) = sim_env();
    let mut trailing = pristine.clone();
    trailing.push(b'X');
    write_sim_file(&env, chunk_name, &trailing);
    assert!(matches!(
        MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default()),
        Err(MetaStoreError::Corrupt { .. })
    ));

    // A frame length above the payload bound is rejected outright.
    let (_sim, env) = sim_env();
    let mut oversized = header.clone();
    oversized.extend_from_slice(b"VTOPMLE1");
    oversized.extend_from_slice(&((53 + 512 * 1024 + 1) as u32).to_be_bytes());
    write_sim_file(&env, chunk_name, &oversized);
    assert!(matches!(
        MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default()),
        Err(MetaStoreError::Corrupt { .. })
    ));

    // An unknown header version with a valid checksum is refused by version,
    // even though the file is otherwise pristine.
    let (_sim, env) = sim_env();
    let mut future = header.clone();
    future[8..10].copy_from_slice(&2_u16.to_be_bytes());
    let checksum = *blake3::hash(&future[..36]).as_bytes();
    future[36..].copy_from_slice(&checksum);
    write_sim_file(&env, chunk_name, &future);
    assert!(matches!(
        MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default()),
        Err(MetaStoreError::UnsupportedVersion { version: 2, .. })
    ));

    // A chunk from another cluster must never be adopted.
    let (_sim, env) = sim_env();
    write_sim_file(&env, chunk_name, &pristine);
    assert!(matches!(
        MetaLog::open_in(&env, ROOT, Uuid::from_u128(0xbad), MetaLogConfig::default()),
        Err(MetaStoreError::Corrupt { .. })
    ));

    // An unknown entry kind with a correct checksum is corruption.
    let (_sim, env) = sim_env();
    let mut unknown_kind = header.clone();
    let mut frame = Vec::new();
    frame.extend_from_slice(b"VTOPMLE1");
    frame.extend_from_slice(&53_u32.to_be_bytes());
    frame.extend_from_slice(&2_u64.to_be_bytes());
    frame.extend_from_slice(&1_u64.to_be_bytes());
    frame.push(9);
    frame.extend_from_slice(&0_u32.to_be_bytes());
    let checksum = *blake3::hash(&frame).as_bytes();
    frame.extend_from_slice(&checksum);
    unknown_kind.extend_from_slice(&frame);
    write_sim_file(&env, chunk_name, &unknown_kind);
    assert!(matches!(
        MetaLog::open_in(&env, ROOT, cluster_id(), MetaLogConfig::default()),
        Err(MetaStoreError::Corrupt { .. })
    ));
}

/// Drive the deterministic command sequence that produces the stage-8b
/// retention records: register node, create topic, grant, seal, verify,
/// commit tier evidence, set the retention policy.
fn retention_machine() -> MetaStateMachine {
    let env = |request: u128| CommandEnvelope {
        request_id: Uuid::from_u128(0x9300_0000_0000 + request),
        issued_at_ms: 0x0102_0304_0506_0708,
    };
    let node = Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap();
    let topic = Uuid::parse_str("ffeeddcc-bbaa-9988-7766-554433221100").unwrap();
    let range = Uuid::parse_str("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0").unwrap();
    let segment = Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").unwrap();
    let mut machine = MetaStateMachine::new();
    machine.apply(
        1,
        &MetadataCommand::RegisterNode {
            env: env(1),
            node_uuid: node,
            addr: "n1:9200".to_owned(),
            expected_generation: None,
        },
    );
    machine.apply(
        2,
        &MetadataCommand::CreateTopic {
            env: env(2),
            name: "audit.v1".to_owned(),
            topic_uuid: topic,
            root_range_uuid: range,
        },
    );
    machine.apply(
        3,
        &MetadataCommand::GrantRangeLease {
            env: env(3),
            topic_uuid: topic,
            range_uuid: range,
            holder_node_uuid: node,
            expected_range_generation: 0,
        },
    );
    machine.apply(
        4,
        &MetadataCommand::RegisterSealedSegment {
            env: env(4),
            topic_uuid: topic,
            range_uuid: range,
            segment_uuid: segment,
            segment_generation: 0,
            base_offset: 0,
            next_offset: 128,
            content_root: [0xab; 32],
            sealed_by_epoch: 1,
            expected_range_generation: 1,
        },
    );
    machine.apply(
        5,
        &MetadataCommand::MarkSegmentVerified {
            env: env(5),
            topic_uuid: topic,
            range_uuid: range,
            segment_uuid: segment,
            content_root: [0xab; 32],
            expected_generation: 0,
        },
    );
    machine.apply(
        6,
        &MetadataCommand::CommitTierEvidence {
            env: env(6),
            topic_uuid: topic,
            range_uuid: range,
            segment_uuid: segment,
            expected_segment_generation: 1,
            content_root: [0xab; 32],
            byte_length: 4096,
            backend_id: "s3-native".to_owned(),
            object_uri: "s3://tier/native/audit.v1/segment.segment".to_owned(),
            manifest_version_id: Some("3sL4kqCJo05qOWBhBqpfOFAdT4dRJVvV".to_owned()),
            manifest_core_digest: [0x5d; 32],
            verification_method: vtop_meta::VerificationMethod::AuthenticatedContentRoot,
            verifier_node_uuid: node,
            fencing_epoch: 1,
            verified_term: 2,
        },
    );
    machine.apply(
        7,
        &MetadataCommand::SetTopicRetentionPolicy {
            env: env(7),
            topic_uuid: topic,
            unarchived_deletion_allowed: false,
            expected_generation: None,
        },
    );
    machine
}

#[test]
fn v1_snapshot_payload_with_retention_records_matches_golden_vector() {
    let payload = retention_machine().encode_snapshot().unwrap();
    assert_eq!(to_hex(&payload), GOLDEN_RETENTION_PAYLOAD_HEX);

    // The pinned bytes decode back to the identical machine (fixed point),
    // and the two new value tags are actually present in the vector.
    let decoded = MetaStateMachine::decode_snapshot(&payload).unwrap();
    assert_eq!(decoded.encode_snapshot().unwrap(), payload);
    assert_eq!(decoded, retention_machine());

    // An unknown verification-method tag inside the tier record must refuse
    // to decode, never default. The method byte immediately follows the
    // 32-byte manifest core digest (0x5d repeated), which appears exactly
    // once in the payload.
    let digest_start = payload
        .windows(32)
        .position(|window| window == [0x5d_u8; 32])
        .expect("manifest core digest present exactly once");
    let method_at = digest_start + 32;
    assert_eq!(payload[method_at], 1);
    let mut mutated = payload.clone();
    mutated[method_at] = 9;
    assert!(MetaStateMachine::decode_snapshot(&mutated).is_err());

    // Trailing bytes are rejected as everywhere else.
    let mut trailing = payload.clone();
    trailing.push(0);
    assert!(MetaStateMachine::decode_snapshot(&trailing).is_err());
}

#[test]
fn v1_snapshot_file_matches_golden_vector() {
    let mut machine = MetaStateMachine::new();
    machine.apply(1, &golden_command());
    let payload = machine.encode_snapshot().unwrap();
    assert_eq!(to_hex(&payload), GOLDEN_STATE_PAYLOAD_HEX);

    let (sim, env) = sim_env();
    let mut snapshots = MetaSnapshots::open_in(&env, ROOT, cluster_id()).unwrap();
    let membership = MetaMembership {
        voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
        learners: vec![(MetaNodeId(4), "n4:9200".to_owned())],
    };
    let meta = snapshots
        .write(1, 2, membership, "golden-snap", &payload, None)
        .unwrap();
    assert_eq!(
        meta.path,
        Path::new(ROOT).join("snapshot-00000000000000000001-00000000000000000002.vmsnap")
    );
    let durable = &sim.snapshot().files[&meta.path];
    assert_eq!(to_hex(durable), GOLDEN_SNAPSHOT_FILE_HEX);
}

#[test]
fn v1_snapshot_rejects_corruption_trailing_oversize_unknown_version_and_mismatches() {
    let golden = from_hex(GOLDEN_SNAPSHOT_FILE_HEX);
    let snapshot_name = "snapshot-00000000000000000001-00000000000000000002.vmsnap";

    // Every single-byte flip fails the trailer checksum (or, inside the
    // trailer, the comparison) and must be rejected at scan time.
    for byte_index in 0..golden.len() {
        let (_sim, env) = sim_env();
        let mut mutated = golden.clone();
        mutated[byte_index] ^= 0xff;
        write_sim_file(&env, snapshot_name, &mutated);
        assert!(
            MetaSnapshots::open_in(&env, ROOT, cluster_id()).is_err(),
            "flip at byte {byte_index} was accepted"
        );
    }

    let (_sim, env) = sim_env();
    let mut trailing = golden.clone();
    trailing.push(0);
    write_sim_file(&env, snapshot_name, &trailing);
    assert!(MetaSnapshots::open_in(&env, ROOT, cluster_id()).is_err());

    // Unknown version with a recomputed trailer is refused by version.
    // v2 is supported (membership LogId); probe with v3.
    let (_sim, env) = sim_env();
    let mut future = golden.clone();
    future[8..10].copy_from_slice(&3_u16.to_be_bytes());
    let body_len = future.len() - 32;
    let checksum = *blake3::hash(&future[..body_len]).as_bytes();
    future[body_len..].copy_from_slice(&checksum);
    write_sim_file(&env, snapshot_name, &future);
    assert!(matches!(
        MetaSnapshots::open_in(&env, ROOT, cluster_id()),
        Err(MetaStoreError::UnsupportedVersion { version: 3, .. })
    ));

    // A membership block length over its bound is rejected even with a
    // valid trailer.
    let (_sim, env) = sim_env();
    let mut oversized = golden.clone();
    oversized[44..48].copy_from_slice(&(65 * 1024_u32).to_be_bytes());
    let body_len = oversized.len() - 32;
    let checksum = *blake3::hash(&oversized[..body_len]).as_bytes();
    oversized[body_len..].copy_from_slice(&checksum);
    write_sim_file(&env, snapshot_name, &oversized);
    assert!(matches!(
        MetaSnapshots::open_in(&env, ROOT, cluster_id()),
        Err(MetaStoreError::Corrupt { .. })
    ));

    // Foreign cluster id.
    let (_sim, env) = sim_env();
    write_sim_file(&env, snapshot_name, &golden);
    assert!(matches!(
        MetaSnapshots::open_in(&env, ROOT, Uuid::from_u128(0xbad)),
        Err(MetaStoreError::Corrupt { .. })
    ));

    // Coverage in the header must match the file name it was published as.
    let (_sim, env) = sim_env();
    write_sim_file(
        &env,
        "snapshot-00000000000000000009-00000000000000000002.vmsnap",
        &golden,
    );
    assert!(matches!(
        MetaSnapshots::open_in(&env, ROOT, cluster_id()),
        Err(MetaStoreError::Corrupt { .. })
    ));
}
