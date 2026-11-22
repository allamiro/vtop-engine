//! Fuzz the metadata tier's payload decoders (`vtop-meta`): the replicated
//! command and its response, the membership a log entry or snapshot header
//! carries, the state-machine snapshot payload, and the log entry frame.
//! These are the bytes a peer proposes and replicates and the bytes a voter
//! reads back after a crash, so no input may panic a decoder (#743).
//!
//! Every input is `selector byte + bytes`. The contract per selector is that
//! the decoder returns an error (a kind or tag it does not know is the
//! `NewerFormat` case on disk, anything else is corruption) or a value that
//! re-encodes consistently: it encodes, decodes back to the same value, and
//! encodes to the same bytes again.
//!
//! * `0` `MetadataCommand::decode`
//! * `1` `MetadataResponse::decode`
//! * `2` `MetaMembership::decode`
//! * `3` `MetaStateMachine::decode_snapshot`
//! * `4` one or more log entry frames, as written to a chunk, and again with
//!   the first frame's checksum repaired so the payload decoders behind it see
//!   hostile bytes. Opened through `MetaLog`, the path recovery takes.
//! * `5` an entry kind byte and its payload, wrapped in a frame with a valid
//!   length and checksum, so the log's classification of the payload (read,
//!   `NewerFormat` or corruption) is checked against the payload decoder's
//!   own verdict.
//!
//! The seeds are the committed golden vectors (`codec_golden`) and one value
//! of every command and response kind (`vtop-meta/src/fuzz_seeds.rs`). The
//! frames of the metadata transport and the store's other files are
//! `vtpm_frame`'s and `meta_storage_files`'s.
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::path::Path;
use std::sync::OnceLock;
use uuid::Uuid;
use vtop_fuzz::split_selector;
use vtop_log::sim::SimStorage;
use vtop_meta::{
    MetaLog, MetaLogConfig, MetaLogEntry, MetaLogPayload, MetaMembership, MetaStateMachine,
    MetaStoreError, MetadataCommand, MetadataResponse,
};

const SEED: u64 = 0x5eed_0743;
const ROOT: &str = "/meta";
const CLUSTER: u128 = 0xc1c1_c1c1_0000_4000_8000_0000_0000_0743;
const CHUNK: &str = "/meta/log-00000000000000000001.vmlog";

const ENTRY_MAGIC: &[u8; 8] = b"VTOPMLE1";
/// magic 8 + frame_len 4.
const FRAME_PREFIX_BYTES: usize = 12;
/// term 8 + index 8 + kind 1 + payload_len 4 + checksum 32.
const FRAME_FIXED_BODY_BYTES: usize = 53;
const CHECKSUM_LEN: usize = 32;
const ENTRY_KIND_NORMAL: u8 = 1;
const ENTRY_KIND_MEMBERSHIP: u8 = 2;
const ENTRY_KIND_BLANK: u8 = 3;
/// The log refuses a larger payload; the harness does not build one.
const MAX_ENTRY_PAYLOAD_BYTES: usize = 512 * 1024;

fn check_command(bytes: &[u8]) {
    let Ok(command) = MetadataCommand::decode(bytes) else {
        return;
    };
    let encoded = command
        .encode()
        .expect("a command that decodes also encodes");
    assert_eq!(
        MetadataCommand::decode(&encoded).as_ref(),
        Ok(&command),
        "a command did not decode to itself after re-encoding"
    );
    assert_eq!(
        MetadataCommand::decode(&encoded).unwrap().encode().unwrap(),
        encoded,
        "a command's encoding is not stable"
    );
    let _ = command.feature_level();
    let _ = command.envelope();
}

fn check_response(bytes: &[u8]) {
    let Ok(response) = MetadataResponse::decode(bytes) else {
        return;
    };
    let encoded = response
        .encode()
        .expect("a response that decodes also encodes");
    assert_eq!(
        MetadataResponse::decode(&encoded).as_ref(),
        Ok(&response),
        "a response did not decode to itself after re-encoding"
    );
    assert_eq!(
        MetadataResponse::decode(&encoded)
            .unwrap()
            .encode()
            .unwrap(),
        encoded,
        "a response's encoding is not stable"
    );
}

fn check_membership(bytes: &[u8]) {
    let Ok(membership) = MetaMembership::decode(bytes) else {
        return;
    };
    let encoded = membership
        .encode()
        .expect("a membership that decodes also encodes");
    assert_eq!(
        MetaMembership::decode(&encoded).as_ref(),
        Ok(&membership),
        "a membership did not decode to itself after re-encoding"
    );
    assert_eq!(
        MetaMembership::decode(&encoded).unwrap().encode().unwrap(),
        encoded,
        "a membership's encoding is not stable"
    );
}

fn check_snapshot(bytes: &[u8]) {
    let Ok(machine) = MetaStateMachine::decode_snapshot(bytes) else {
        return;
    };
    let encoded = machine
        .encode_snapshot()
        .expect("a snapshot that decodes also encodes");
    let again =
        MetaStateMachine::decode_snapshot(&encoded).expect("a snapshot this build wrote decodes");
    assert!(
        again == machine,
        "a snapshot did not decode to itself after re-encoding"
    );
    assert_eq!(
        again.encode_snapshot().unwrap(),
        encoded,
        "a snapshot's encoding is not stable"
    );
}

/// The chunk header a log of this cluster starts with, taken from a log this
/// build wrote so the target follows the header's layout (the bytes before
/// the first entry's magic).
fn chunk_header() -> &'static [u8] {
    static HEADER: OnceLock<Vec<u8>> = OnceLock::new();
    HEADER.get_or_init(|| {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new(ROOT));
        let env = sim.env(SEED);
        let mut log = MetaLog::open_in(
            &env,
            ROOT,
            Uuid::from_u128(CLUSTER),
            MetaLogConfig::default(),
        )
        .expect("an empty log opens");
        log.append(&[MetaLogEntry {
            term: 1,
            index: 1,
            payload: MetaLogPayload::Blank,
        }])
        .expect("a blank entry appends");
        drop(log);
        let bytes = sim.snapshot().files[Path::new(CHUNK)].clone();
        let at = bytes
            .windows(ENTRY_MAGIC.len())
            .position(|window| window == ENTRY_MAGIC)
            .expect("the chunk holds an entry");
        bytes[..at].to_vec()
    })
}

/// Open a log whose only chunk is the header and `frames`, and read it all.
fn open_log(frames: &[u8]) -> Result<Vec<MetaLogEntry>, MetaStoreError> {
    let sim = SimStorage::new();
    sim.create_dir_all(Path::new(ROOT));
    let env = sim.env(SEED);
    {
        use std::io::Write;
        use vtop_log::env::OpenMode;
        let mut file = env
            .storage
            .open(Path::new(CHUNK), OpenMode::CreateNew)
            .expect("the sim disk creates a file");
        file.write_all(chunk_header()).unwrap();
        file.write_all(frames).unwrap();
        file.sync_data().unwrap();
    }
    let log = MetaLog::open_in(
        &env,
        ROOT,
        Uuid::from_u128(CLUSTER),
        MetaLogConfig::default(),
    )?;
    match (log.first_index(), log.last_index()) {
        (Some(first), Some(last)) => log.read_range(first, last.saturating_add(1)),
        _ => Ok(Vec::new()),
    }
}

/// What recovery makes of `frames`; a log that opens must hold entries that
/// re-encode to a log that opens to the same entries.
fn check_log(frames: &[u8]) {
    let Ok(entries) = open_log(frames) else {
        return;
    };
    let mut rewritten = Vec::new();
    for entry in &entries {
        rewritten.extend_from_slice(
            &entry
                .encode_frame()
                .expect("an entry the log read also encodes"),
        );
    }
    let again = open_log(&rewritten).expect("a log this build wrote opens");
    assert_eq!(again, entries, "log entries changed across a re-encode");
}

/// `frames` with the checksum of the frame at its start recomputed, when the
/// frame's length field fits in the input.
fn with_entry_checksum(frames: &[u8]) -> Option<Vec<u8>> {
    if frames.len() < FRAME_PREFIX_BYTES + CHECKSUM_LEN || &frames[..8] != ENTRY_MAGIC {
        return None;
    }
    let frame_len = u32::from_be_bytes(frames[8..12].try_into().unwrap()) as usize;
    let total = FRAME_PREFIX_BYTES.checked_add(frame_len)?;
    if frame_len < FRAME_FIXED_BODY_BYTES || total > frames.len() {
        return None;
    }
    let mut out = frames.to_vec();
    let sum = blake3::hash(&out[..total - CHECKSUM_LEN]);
    out[total - CHECKSUM_LEN..total].copy_from_slice(sum.as_bytes());
    Some(out)
}

/// An entry of term 1 at index 1 carrying `payload` as kind `kind`, with a
/// valid length and checksum.
fn framed(kind: u8, payload: &[u8]) -> Vec<u8> {
    let frame_len = (FRAME_FIXED_BODY_BYTES + payload.len()) as u32;
    let mut out = Vec::with_capacity(FRAME_PREFIX_BYTES + frame_len as usize);
    out.extend_from_slice(ENTRY_MAGIC);
    out.extend_from_slice(&frame_len.to_be_bytes());
    out.extend_from_slice(&1u64.to_be_bytes());
    out.extend_from_slice(&1u64.to_be_bytes());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    let sum = blake3::hash(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

/// The log's verdict on a checksum-valid entry must agree with the payload
/// decoder's: a payload that decodes is read as that value, one that does
/// not is refused, and nothing is read that the decoder refused.
fn check_kind_and_payload(bytes: &[u8]) {
    let Some((&kind, payload)) = bytes.split_first() else {
        return;
    };
    if payload.len() > MAX_ENTRY_PAYLOAD_BYTES {
        return;
    }
    let result = open_log(&framed(kind, payload));
    match kind {
        ENTRY_KIND_NORMAL => match (MetadataCommand::decode(payload), result) {
            (Ok(command), Ok(entries)) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].payload, MetaLogPayload::Normal(command));
            }
            (Ok(_), Err(error)) => panic!("a decodable command was refused by the log: {error}"),
            (Err(_), Ok(_)) => panic!("the log read a command its decoder refuses"),
            (Err(_), Err(error)) => assert_refusal(&error),
        },
        ENTRY_KIND_MEMBERSHIP => match (MetaMembership::decode(payload), result) {
            (Ok(membership), Ok(entries)) => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].payload, MetaLogPayload::Membership(membership));
            }
            (Ok(_), Err(error)) => {
                panic!("a decodable membership was refused by the log: {error}")
            }
            (Err(_), Ok(_)) => panic!("the log read a membership its decoder refuses"),
            (Err(_), Err(error)) => assert_refusal(&error),
        },
        ENTRY_KIND_BLANK => match result {
            Ok(entries) => {
                assert!(payload.is_empty(), "a blank entry carried a payload");
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].payload, MetaLogPayload::Blank);
            }
            Err(error) => {
                assert!(!payload.is_empty(), "an empty blank entry was refused");
                assert_refusal(&error);
            }
        },
        _ => {
            let error = result.expect_err("an unknown entry kind was read");
            assert_refusal(&error);
        }
    }
}

/// A refusal is corruption or a newer writer's record, never an I/O error
/// or a misuse of the store: the bytes are all the log was given.
fn assert_refusal(error: &MetaStoreError) {
    assert!(
        matches!(
            error,
            MetaStoreError::Corrupt { .. } | MetaStoreError::NewerFormat { .. }
        ),
        "unexpected failure: {error}"
    );
}

fuzz_target!(|data: &[u8]| {
    let Some((selector, rest)) = split_selector(data) else {
        return;
    };
    match selector % 6 {
        0 => check_command(rest),
        1 => check_response(rest),
        2 => check_membership(rest),
        3 => check_snapshot(rest),
        4 => {
            check_log(rest);
            if let Some(repaired) = with_entry_checksum(rest) {
                check_log(&repaired);
            }
        }
        _ => check_kind_and_payload(rest),
    }
});
