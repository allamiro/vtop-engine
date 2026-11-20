//! Fuzz the segment log's on-disk formats (`vtop-log`): the v1 and v2
//! segment headers and record frames, the offset index, the `VTOPCHK1` chunk
//! sidecar, the `VTOPCMT1` commit marker, the JSON manifest and the producer
//! sidecar. A segment directory is read back after a crash, after a disk
//! fault, and from a peer during a transfer, so none of its files is trusted,
//! and no content may panic the reader, the verifier, crash recovery or the
//! startup scan (#729).
//!
//! The input is a bundle of files (`SimDiskState::from_bundle`: path, then
//! content, per file) restored onto the simulated disk, so the fuzzer can
//! damage one sidecar while the others stay valid. The seeds are bundles of
//! real sealed and active segments written through the crate's own API.
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::collections::BTreeSet;
use std::path::Path;
use vtop_log::sim::{SimDiskState, SimStorage};
use vtop_log::verify::{chunk_proof_in, verify_sealed_segment_in, VerifyExpectations};
use vtop_log::{ActiveSegment, SegmentReader, SegmentSet};

const SEED: u64 = 0x5eed_0729;

fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension().and_then(|value| value.to_str()) == Some(extension)
}

fuzz_target!(|data: &[u8]| {
    let disk = SimDiskState::from_bundle(data);
    if disk.files.is_empty() {
        return;
    }
    let sim = SimStorage::new();
    sim.restore(&disk);
    let env = sim.env(SEED);

    // The readers first: they do not write.
    for path in disk
        .files
        .keys()
        .filter(|path| has_extension(path, "segment"))
    {
        if let Ok(mut reader) = SegmentReader::open_in(&env, path) {
            let base = reader.base_offset();
            let _ = reader.fetch(base, 1 << 20, 1024);
        }
        let _ = verify_sealed_segment_in(&env, path, &VerifyExpectations::default());
        let _ = chunk_proof_in(&env, path, 0);
    }

    // Crash recovery of an active segment truncates a torn tail in place.
    for path in disk
        .files
        .keys()
        .filter(|path| has_extension(path, "active"))
    {
        if let Ok(segment) = ActiveSegment::recover_in(&env, path) {
            let _ = segment.next_offset();
        }
    }

    // The startup scan, from the disk as it was: discovery, quarantine,
    // the truncation and retention intents, and opening every segment.
    sim.restore(&disk);
    let directories: BTreeSet<&Path> = disk.files.keys().filter_map(|path| path.parent()).collect();
    for directory in directories {
        let _ = SegmentSet::open_in(&env, directory);
    }
});
