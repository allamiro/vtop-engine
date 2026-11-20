//! #743: the broker's journals that a RELEASED `vtop-node` wrote open under
//! this build and say what that release recorded.
//!
//! The fixtures are the data directories of `vtop-log`'s
//! `tests/fixtures/segments/<tag>/<profile>/` (a leader of the release, 300
//! records produced with quorum durability, then SIGKILL; see
//! `scripts/compat/regenerate-segment-fixtures.sh` and each tag's
//! `PROVENANCE`). Two journals sit beside the segments:
//!
//! - `epochs`, the producer-epoch journal (`VTOPEPC1`): the producer that
//!   wrote, at the epoch the client config names;
//! - `fencing-epochs`, the fencing-epoch journal (`VTOPFNC1`): the leader's
//!   fencing epoch of the fixture's config.
//!
//! A release's journal the current build cannot read, or reads as another
//! epoch, would reject the leader's producers or its own history after an
//! upgrade, with the data intact.

use std::path::{Path, PathBuf};

use uuid::Uuid;
use vtop_broker::fencing_epochs::FencingEpochJournal;
use vtop_broker::ProducerEpochJournal;

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../vtop-log/tests/fixtures/segments"
);

/// The fixed identities of `scripts/upgrade-compat.sh`'s fixture.
const PRODUCER: &str = "aaaaaaaa-0000-0000-0000-0000000000ce";
const PRODUCER_EPOCH: u64 = 1;
const FENCING_EPOCH: u64 = 18;

fn directories() -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    for tag in std::fs::read_dir(FIXTURES).expect("the segment fixtures") {
        let tag = tag.unwrap();
        if !tag.path().is_dir() {
            continue;
        }
        for profile in std::fs::read_dir(tag.path()).unwrap() {
            let profile = profile.unwrap();
            if profile.path().is_dir() {
                found.push((
                    format!(
                        "{} {}",
                        tag.file_name().to_string_lossy(),
                        profile.file_name().to_string_lossy()
                    ),
                    profile.path(),
                ));
            }
        }
    }
    found.sort();
    assert!(
        found.len() >= 8,
        "the released directories are the fixtures: {found:?}"
    );
    found
}

fn copy_of(path: &Path, name: &str) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let to = dir.path().join(name);
    std::fs::copy(path.join(name), &to).unwrap();
    (dir, to)
}

#[test]
fn a_released_producer_epoch_journal_reads_as_the_epoch_it_recorded() {
    for (label, dir) in directories() {
        let (_keep, journal) = copy_of(&dir, "epochs");
        let journal = ProducerEpochJournal::open(&journal)
            .unwrap_or_else(|e| panic!("{label}: this build opens the release's epochs: {e}"));
        assert_eq!(
            journal.current(Uuid::parse_str(PRODUCER).unwrap()),
            Some(PRODUCER_EPOCH),
            "{label}: the producer's epoch"
        );
    }
}

#[test]
fn a_released_fencing_epoch_journal_reads_as_the_epoch_it_recorded() {
    for (label, dir) in directories() {
        let (_keep, journal) = copy_of(&dir, "fencing-epochs");
        let journal = FencingEpochJournal::open(&journal)
            .unwrap_or_else(|e| panic!("{label}: this build opens the release's journal: {e}"));
        assert_eq!(
            journal.latest().map(|e| e.epoch),
            Some(FENCING_EPOCH),
            "{label}: the fencing epoch of the fixture's config: {:?}",
            journal.entries()
        );
    }
}
