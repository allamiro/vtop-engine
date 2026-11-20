//! #743: a data node's directory that a RELEASED `vtop-node` wrote opens under
//! this build, serves every record, and its sealed segments verify.
//!
//! Each directory under `tests/fixtures/segments/<tag>/<profile>/` is the
//! leader's data directory after the release's published binary produced 300
//! records with quorum durability and was killed with SIGKILL
//! (`scripts/compat/regenerate-segment-fixtures.sh` says how; the tag's
//! `PROVENANCE` names the asset). So the files here are the first whose
//! bytes this code did not write: the golden vectors in `codec.rs` and
//! `codec_v2.rs` pin an encoding, and these show a release's writer and this
//! build's reader agree on a whole directory (`ddia-encoding-evolution`
//! rule 4).
//!
//! Profiles:
//! - `legacy`, a release before v0.3.0, which cannot roll: one `.active`;
//! - `rolling`, sealed segments, their indexes, commit markers, manifests and
//!   producer snapshots, in format v1;
//! - `rolling-v2`, the same in format v2, with the chunk sidecar.
//!
//! Per directory this build must open it without quarantining anything, read
//! every record from offset 0 contiguously, and verify every sealed segment
//! (the self-consistency level: the commit marker, the index, the chunk tree
//! and the manifest agree with the frames).

use std::path::{Path, PathBuf};

use vtop_log::env::Env;
use vtop_log::verify::{verify_sealed_segment_in, VerifyExpectations};
use vtop_log::SegmentSet;

const FIXTURES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/segments");

/// The releases whose directories are fixtures, with the profiles each wrote.
/// A fixture deleted without a decision fails here.
const FLOOR: &[(&str, &[&str])] = &[
    ("v0.2.1", &["legacy"]),
    ("v0.3.1", &["rolling"]),
    ("v0.5.0", &["rolling", "rolling-v2"]),
    ("v0.8.0", &["rolling", "rolling-v2"]),
    ("v0.10.1", &["rolling", "rolling-v2"]),
];

/// What the OLD phase of `scripts/upgrade-compat.sh` produces and acknowledges
/// before it stops the node.
const PRODUCED: u64 = 300;

fn fixture_dirs() -> Vec<(String, String, PathBuf)> {
    let mut found = Vec::new();
    for tag in std::fs::read_dir(FIXTURES).expect("the fixtures directory") {
        let tag = tag.unwrap();
        if !tag.path().is_dir() {
            continue;
        }
        for profile in std::fs::read_dir(tag.path()).unwrap() {
            let profile = profile.unwrap();
            if profile.path().is_dir() {
                found.push((
                    tag.file_name().into_string().unwrap(),
                    profile.file_name().into_string().unwrap(),
                    profile.path(),
                ));
            }
        }
    }
    found.sort();
    for (tag, profiles) in FLOOR {
        for profile in *profiles {
            assert!(
                found.iter().any(|(t, p, _)| t == tag && p == profile),
                "no fixture for {tag} {profile}: a release's directory format stays covered \
                 until a decision retires it (found {:?})",
                found
                    .iter()
                    .map(|(t, p, _)| format!("{t}/{p}"))
                    .collect::<Vec<_>>()
            );
        }
    }
    found
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

#[test]
fn every_released_directory_opens_serves_every_record_and_verifies() {
    for (tag, profile, source) in fixture_dirs() {
        let label = format!("{tag} {profile}");
        // Opening repairs a torn tail, so it works on a copy.
        let dir = tempfile::tempdir().unwrap();
        copy_dir(&source, dir.path());
        let env = Env::real();

        let mut set = SegmentSet::open_in(&env, dir.path())
            .unwrap_or_else(|e| panic!("{label}: this build opens the release's directory: {e}"))
            .unwrap_or_else(|| panic!("{label}: the directory holds a range"));
        assert_eq!(set.base_offset(), 0, "{label}: the range starts at 0");
        assert!(
            set.committed_offset() >= PRODUCED,
            "{label}: {PRODUCED} records were acknowledged with quorum durability, but \
             this build recovers a committed offset of {}",
            set.committed_offset()
        );

        // Every record from offset 0, contiguous, whatever the segment layout.
        let hw = set.committed_offset();
        let mut next = 0;
        while next < hw {
            let batch = set
                .fetch_through(next, 1 << 20, 1_000, hw)
                .unwrap_or_else(|e| panic!("{label}: fetch at {next}: {e}"));
            assert!(
                !batch.records.is_empty(),
                "{label}: a fetch at {next} (below the committed {hw}) returned nothing"
            );
            for record in &batch.records {
                assert_eq!(record.offset, next, "{label}: a gap or a repeat at {next}");
                next += 1;
            }
        }
        assert_eq!(next, hw, "{label}");

        // The sealed segments carry the format of the profile, and verify.
        let want_format: u16 = if profile == "rolling-v2" { 2 } else { 1 };
        let sealed: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "segment"))
            .collect();
        assert_eq!(
            !sealed.is_empty(),
            profile != "legacy",
            "{label}: a rolling profile holds sealed segments and the legacy one does not"
        );
        for path in sealed {
            let report = verify_sealed_segment_in(&env, &path, &VerifyExpectations::default())
                .unwrap_or_else(|e| panic!("{label}: {}: {e}", path.display()));
            assert!(
                report.passed(),
                "{label}: {} does not verify: {:?}",
                path.display(),
                report
                    .checks
                    .iter()
                    .filter(|c| !c.passed)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                report.format_version,
                want_format,
                "{label}: {} is not in the format the profile writes",
                path.display()
            );
        }
    }
}
