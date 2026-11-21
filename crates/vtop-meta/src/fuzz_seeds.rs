//! The `meta_decode` fuzz target's seeds (#743): one value of every command
//! and response kind, the membership shapes, snapshot payloads and log entry
//! frames this build writes. The golden vectors of `tests/codec_golden.rs`
//! are written beside them as `golden-*` by that file.
//!
//! Every seed is `selector byte + bytes`; the selectors are the target's
//! (`fuzz/fuzz_targets/meta_decode.rs`). `the_committed_fuzz_seeds_are_what_this_build_writes`
//! keeps the committed files in step with the encoders, and decodes each, so a
//! format change that strands a seed fails here instead of quietly starting
//! the nightly from inputs rejected at the first byte. `write_fuzz_seeds`
//! rewrites them.

use crate::command::tests::{every_command, every_response};
use crate::command::{CommandEnvelope, MetadataCommand, MetadataResponse};
use crate::keys::MetaNodeId;
use crate::state::MetaStateMachine;
use crate::storage::log::{
    encode_chunk_header, MetaLog, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership,
    ENTRY_MAGIC,
};
use crate::wire::CodecError;
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;
use vtop_log::env::OpenMode;
use vtop_log::sim::SimStorage;

/// The selectors of `fuzz/fuzz_targets/meta_decode.rs`.
const COMMAND: u8 = 0;
const RESPONSE: u8 = 1;
const MEMBERSHIP: u8 = 2;
const SNAPSHOT: u8 = 3;
const LOG_FRAMES: u8 = 4;
const KIND_AND_PAYLOAD: u8 = 5;

/// The cluster and root the target opens its log under; keep in step with
/// fuzz/fuzz_targets/meta_decode.rs.
const ROOT: &str = "/meta";
const CLUSTER: u128 = 0xc1c1_c1c1_0000_4000_8000_0000_0000_0743;
const SEED: u64 = 0x5eed_0743;

fn seed_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/meta_decode")
}

fn with_selector(selector: u8, bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 1);
    out.push(selector);
    out.extend_from_slice(bytes);
    out
}

/// An entry frame of term 1 at index 1 with a valid length and checksum,
/// built apart from `encode_frame` so a payload this build cannot encode (a
/// newer writer's) can still be framed.
fn framed(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(ENTRY_MAGIC);
    out.extend_from_slice(&((53 + payload.len()) as u32).to_be_bytes());
    out.extend_from_slice(&1u64.to_be_bytes());
    out.extend_from_slice(&1u64.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    let sum = blake3::hash(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

fn memberships() -> Vec<(&'static str, MetaMembership)> {
    vec![
        (
            "single",
            MetaMembership {
                voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
                learners: vec![],
                joint_outgoing: None,
            },
        ),
        (
            "learner",
            MetaMembership {
                voters: vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)],
                learners: vec![(MetaNodeId(4), "n4:9200".to_owned())],
                joint_outgoing: None,
            },
        ),
        (
            "joint",
            MetaMembership {
                voters: vec![MetaNodeId(2), MetaNodeId(3), MetaNodeId(4)],
                learners: vec![],
                joint_outgoing: Some(vec![MetaNodeId(1), MetaNodeId(2), MetaNodeId(3)]),
            },
        ),
    ]
}

/// The state after every command kind has been proposed in order: the
/// accepted ones leave records, the refused ones a dedup entry.
fn full_machine() -> MetaStateMachine {
    let mut machine = MetaStateMachine::new();
    for (index, command) in every_command().iter().enumerate() {
        machine.apply(index as u64 + 1, command);
    }
    machine
}

fn seeds() -> Vec<(String, Vec<u8>)> {
    let mut seeds: Vec<(String, Vec<u8>)> = Vec::new();

    for (at, command) in every_command().iter().enumerate() {
        seeds.push((
            format!("command-{at:02}"),
            with_selector(COMMAND, &command.encode().unwrap()),
        ));
    }
    // A command kind this build does not know: what a newer release writes.
    seeds.push((
        "newer-command-kind".to_owned(),
        with_selector(COMMAND, &[0x03, 0xe7]),
    ));

    for (at, response) in every_response().iter().enumerate() {
        seeds.push((
            format!("response-{at:02}"),
            with_selector(RESPONSE, &response.encode().unwrap()),
        ));
    }

    for (name, membership) in memberships() {
        seeds.push((
            format!("membership-{name}"),
            with_selector(MEMBERSHIP, &membership.encode().unwrap()),
        ));
    }

    seeds.push((
        "snapshot-empty".to_owned(),
        with_selector(
            SNAPSHOT,
            &MetaStateMachine::new().encode_snapshot().unwrap(),
        ),
    ));
    seeds.push((
        "snapshot-every-command".to_owned(),
        with_selector(SNAPSHOT, &full_machine().encode_snapshot().unwrap()),
    ));

    // Log frames as written to a chunk: a command, a membership and a blank
    // entry, alone and as one run of three.
    let command = MetaLogEntry {
        term: 1,
        index: 1,
        payload: MetaLogPayload::Normal(MetadataCommand::CreateTopic {
            env: CommandEnvelope {
                request_id: Uuid::from_u128(1),
                issued_at_ms: 1_750_000_000_000,
            },
            name: "events".to_owned(),
            topic_uuid: Uuid::from_u128(2),
            root_range_uuid: Uuid::from_u128(3),
        }),
    };
    let membership = MetaLogEntry {
        term: 1,
        index: 2,
        payload: MetaLogPayload::Membership(memberships().remove(1).1),
    };
    let blank = MetaLogEntry {
        term: 2,
        index: 3,
        payload: MetaLogPayload::Blank,
    };
    let mut run = Vec::new();
    for (name, entry) in [
        ("command", &command),
        ("membership", &membership),
        ("blank", &blank),
    ] {
        let frame = entry.encode_frame().unwrap();
        run.extend_from_slice(&frame);
        // The entries of a run are 1, 2, 3: write each alone only when it
        // is the first.
        if entry.index == 1 {
            seeds.push((format!("frames-{name}"), with_selector(LOG_FRAMES, &frame)));
        }
    }
    seeds.push(("frames-run".to_owned(), with_selector(LOG_FRAMES, &run)));
    // A run whose last frame is cut short: the torn tail recovery truncates.
    let mut torn = run.clone();
    torn.truncate(torn.len() - 7);
    seeds.push((
        "frames-torn-tail".to_owned(),
        with_selector(LOG_FRAMES, &torn),
    ));

    // An entry kind and its payload: each kind the log knows, an unknown
    // kind, and a command kind a newer release added.
    let create_topic = match &command.payload {
        MetaLogPayload::Normal(command) => command.encode().unwrap(),
        _ => unreachable!(),
    };
    let membership_bytes = memberships().remove(2).1.encode().unwrap();
    for (name, kind, payload) in [
        ("normal", 1u8, create_topic),
        ("membership", 2, membership_bytes),
        ("blank", 3, Vec::new()),
        ("unknown-kind", 9, vec![1, 2, 3]),
        ("newer-command", 1, vec![0x03, 0xe7]),
    ] {
        let mut bytes = vec![kind];
        bytes.extend_from_slice(&payload);
        seeds.push((
            format!("entry-{name}"),
            with_selector(KIND_AND_PAYLOAD, &bytes),
        ));
    }
    seeds
}

/// Rewrites the committed seeds. Run on purpose after a format change:
/// `cargo test -p vtop-meta --lib -- --ignored write_fuzz_seeds`.
#[test]
#[ignore = "writes fuzz/corpus/meta_decode; run on purpose"]
fn write_fuzz_seeds() {
    let dir = seed_dir();
    std::fs::create_dir_all(&dir).unwrap();
    for (name, bytes) in seeds() {
        std::fs::write(dir.join(format!("{name}.seed")), bytes).unwrap();
    }
}

/// Open a log whose chunk is the header and `frames`, as the target does.
fn open_log(frames: &[u8]) -> Result<Vec<MetaLogEntry>, crate::storage::MetaStoreError> {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    let path = Path::new(ROOT).join("log-00000000000000000001.vmlog");
    let mut file = env.storage.open(&path, OpenMode::CreateNew).unwrap();
    file.write_all(&encode_chunk_header(Uuid::from_u128(CLUSTER), 1))
        .unwrap();
    file.write_all(frames).unwrap();
    file.sync_data().unwrap();
    let log = MetaLog::open_in(
        &env,
        ROOT,
        Uuid::from_u128(CLUSTER),
        MetaLogConfig::default(),
    )?;
    match (log.first_index(), log.last_index()) {
        (Some(first), Some(last)) => log.read_range(first, last + 1),
        _ => Ok(Vec::new()),
    }
}

/// What the target does with a seed that matters to whether it is a useful
/// start: a `newer-` seed must be read as a newer writer's, every other one
/// must decode.
fn decodes(name: &str, seed: &[u8]) {
    let (selector, bytes) = (seed[0], &seed[1..]);
    let newer = name.contains("newer-");
    match selector {
        COMMAND => {
            let result = MetadataCommand::decode(bytes).map(drop);
            if newer {
                assert!(
                    matches!(
                        result,
                        Err(CodecError::UnknownTag {
                            what: "command kind",
                            ..
                        })
                    ),
                    "{name}: expected an unknown command kind, got {result:?}"
                );
            } else {
                result.unwrap_or_else(|error| panic!("{name}: {error}"));
            }
        }
        RESPONSE => {
            MetadataResponse::decode(bytes).unwrap_or_else(|error| panic!("{name}: {error}"));
        }
        MEMBERSHIP => {
            MetaMembership::decode(bytes).unwrap_or_else(|error| panic!("{name}: {error}"));
        }
        SNAPSHOT => {
            MetaStateMachine::decode_snapshot(bytes)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
        }
        LOG_FRAMES => {
            let entries = open_log(bytes).unwrap_or_else(|error| panic!("{name}: {error}"));
            assert!(!entries.is_empty(), "{name}: no entry recovered");
        }
        KIND_AND_PAYLOAD => {
            let framed = framed(bytes[0], &bytes[1..]);
            let result = open_log(&framed);
            if name == "entry-unknown-kind" {
                assert!(
                    matches!(result, Err(crate::storage::MetaStoreError::Corrupt { .. })),
                    "{name}: {result:?}"
                );
            } else if newer {
                assert!(
                    matches!(
                        result,
                        Err(crate::storage::MetaStoreError::NewerFormat { .. })
                    ),
                    "{name}: {result:?}"
                );
            } else {
                let entries = result.unwrap_or_else(|error| panic!("{name}: {error}"));
                assert_eq!(entries.len(), 1, "{name}");
            }
        }
        other => panic!("{name}: selector {other} is not a decoder of the target"),
    }
}

/// The committed seeds are what this build writes, and each reaches the
/// decoder it is for.
#[test]
fn the_committed_fuzz_seeds_are_what_this_build_writes() {
    let dir = seed_dir();
    let seeds = seeds();
    for (name, bytes) in &seeds {
        let path = dir.join(format!("{name}.seed"));
        let committed = std::fs::read(&path)
            .unwrap_or_else(|error| panic!("{}: {error}; run write_fuzz_seeds", path.display()));
        assert_eq!(
            &committed, bytes,
            "{name}: the committed seed is stale; run write_fuzz_seeds"
        );
        decodes(name, bytes);
    }
    // A seed for every command and response kind this build builds.
    let commands = seeds
        .iter()
        .filter(|(name, _)| name.starts_with("command-"))
        .count();
    assert_eq!(commands, every_command().len());
    let responses = seeds
        .iter()
        .filter(|(name, _)| name.starts_with("response-"))
        .count();
    assert_eq!(responses, every_response().len());
}

/// The snapshot seed holds records, so the fuzzer starts inside the value
/// decoders and not at an empty state.
#[test]
fn the_every_command_snapshot_seed_holds_records() {
    let machine = full_machine();
    let payload = machine.encode_snapshot().unwrap();
    assert!(payload.len() > 512, "only {} bytes of state", payload.len());
}
