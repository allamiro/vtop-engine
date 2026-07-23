//! Durable raft hard state: current term, vote, and a monotonic write
//! counter, in one fixed-size checksummed file replaced atomically on every
//! save.
//!
//! Layout (68 bytes exactly):
//!
//! ```text
//! magic "VTOPMHS1"        8
//! version u16             2
//! term u64                8
//! voted_for_present u8    1
//! voted_for u64           8   (0 when absent, kept canonical)
//! vote_committed u8       1
//! generation u64          8   (monotonic write counter)
//! BLAKE3-32 over prior   32
//! ```

use super::{corrupt, io_error, write_atomic, MetaStoreError, MetaStoreResult};
use crate::keys::MetaNodeId;
use crate::wire::{put_u16, put_u64, put_u8, Reader};
use std::path::{Path, PathBuf};
use vtop_log::env::Env;

pub(crate) const HARD_STATE_FILE_NAME: &str = "meta.hardstate";

const HARD_STATE_MAGIC: &[u8; 8] = b"VTOPMHS1";
const HARD_STATE_VERSION: u16 = 1;
const HARD_STATE_BYTES: usize = 8 + 2 + 8 + 1 + 8 + 1 + 8 + 32;
const CHECKSUM_LEN: usize = 32;

/// The raft-durable part of a node's identity for one term.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: u64,
    pub voted_for: Option<MetaNodeId>,
    pub vote_committed: bool,
}

/// Owner of the on-disk hard state file. Keeps the durable value cached and
/// enforces the raft safety guards on every save.
pub struct HardStateFile {
    env: Env,
    path: PathBuf,
    state: HardState,
    generation: u64,
    poisoned: bool,
}

impl HardStateFile {
    /// Open (or default) the hard state. A missing file is a fresh node; a
    /// present-but-invalid file is corruption and never silently defaulted.
    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> MetaStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        let exists = env
            .storage
            .exists(&path)
            .map_err(|source| io_error(&path, source))?;
        if !exists {
            return Ok(Self {
                env: env.clone(),
                path,
                state: HardState::default(),
                generation: 0,
                poisoned: false,
            });
        }
        let bytes = env
            .storage
            .read(&path)
            .map_err(|source| io_error(&path, source))?;
        let (state, generation) = decode(&path, &bytes)?;
        Ok(Self {
            env: env.clone(),
            path,
            state,
            generation,
            poisoned: false,
        })
    }

    pub fn state(&self) -> &HardState {
        &self.state
    }

    /// Monotonic count of successful saves across the file's whole life.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Persist a new hard state atomically. Raft safety guards are enforced
    /// here, next to the bytes: the term can never regress, and within a
    /// term a cast vote can never change or be forgotten.
    pub fn save(&mut self, next: HardState) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("hard state"));
        }
        if next.term < self.state.term {
            return Err(MetaStoreError::InvalidConfig(format!(
                "hard state term regressed from {} to {}",
                self.state.term, next.term
            )));
        }
        if next.term == self.state.term {
            if let Some(current_vote) = self.state.voted_for {
                if next.voted_for != Some(current_vote) {
                    return Err(MetaStoreError::InvalidConfig(format!(
                        "vote in term {} cannot change from node {} to {:?}",
                        next.term, current_vote, next.voted_for
                    )));
                }
            }
            // Within a term the committed flag is monotonic too: forgetting
            // that a vote was committed would let recovery treat it as
            // retractable, which is the same double-vote hazard.
            if self.state.vote_committed && !next.vote_committed {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "committed vote in term {} cannot be uncommitted",
                    next.term
                )));
            }
        }
        let generation = self.generation + 1;
        let bytes = encode(&next, generation);
        if let Err(error) = write_atomic(&self.env, &self.path, &bytes) {
            // The failure may have struck after the rename published the
            // new bytes, so the cached copy can no longer be trusted to
            // enforce the vote guards. Fail closed until a reopen re-reads
            // whatever was durably published.
            self.poisoned = true;
            return Err(error);
        }
        self.state = next;
        self.generation = generation;
        Ok(())
    }
}

fn encode(state: &HardState, generation: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(HARD_STATE_BYTES);
    out.extend_from_slice(HARD_STATE_MAGIC);
    put_u16(&mut out, HARD_STATE_VERSION);
    put_u64(&mut out, state.term);
    match state.voted_for {
        None => {
            put_u8(&mut out, 0);
            put_u64(&mut out, 0);
        }
        Some(node) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, node.0);
        }
    }
    put_u8(&mut out, u8::from(state.vote_committed));
    put_u64(&mut out, generation);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn decode(path: &Path, bytes: &[u8]) -> MetaStoreResult<(HardState, u64)> {
    if bytes.len() != HARD_STATE_BYTES {
        return Err(corrupt(
            path,
            format!(
                "hard state must be exactly {HARD_STATE_BYTES} bytes, got {}",
                bytes.len()
            ),
        ));
    }
    let (payload, stored_checksum) = bytes.split_at(HARD_STATE_BYTES - CHECKSUM_LEN);
    if blake3::hash(payload).as_bytes() != stored_checksum {
        return Err(corrupt(path, "hard state checksum mismatch"));
    }
    let mut reader = Reader::new(payload);
    let magic = reader
        .take(8, "hard state magic")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if magic != HARD_STATE_MAGIC {
        return Err(corrupt(path, "hard state magic mismatch"));
    }
    let version = reader
        .u16("hard state version")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if version != HARD_STATE_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let decode_error = |error: crate::wire::CodecError| corrupt(path, error.to_string());
    let term = reader.u64("term").map_err(decode_error)?;
    let vote_present = reader.flag("voted-for presence").map_err(decode_error)?;
    let voted_for_raw = reader.u64("voted-for node id").map_err(decode_error)?;
    if !vote_present && voted_for_raw != 0 {
        return Err(corrupt(path, "absent vote must encode a zero node id"));
    }
    let vote_committed = reader.flag("vote-committed flag").map_err(decode_error)?;
    let generation = reader.u64("write generation").map_err(decode_error)?;
    reader.finish().map_err(decode_error)?;
    Ok((
        HardState {
            term,
            voted_for: vote_present.then_some(MetaNodeId(voted_for_raw)),
            vote_committed,
        },
        generation,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    use std::path::Path;
    use vtop_log::sim::{FaultPlan, SimStorage};

    fn sim_env() -> (SimStorage, Env) {
        let sim = SimStorage::new();
        sim.create_dir_all(Path::new("/meta"));
        let env = sim.env(0x5eed);
        (sim, env)
    }

    #[test]
    fn save_and_reopen_round_trips_state_and_advances_the_generation() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.hardstate");
        let mut file = HardStateFile::open_in(&env, path).unwrap();
        assert_eq!(file.state(), &HardState::default());
        assert_eq!(file.generation(), 0);

        let voted = HardState {
            term: 3,
            voted_for: Some(MetaNodeId(7)),
            vote_committed: true,
        };
        file.save(voted).unwrap();
        assert_eq!(file.generation(), 1);

        let reopened = HardStateFile::open_in(&env, path).unwrap();
        assert_eq!(reopened.state(), &voted);
        assert_eq!(reopened.generation(), 1);
    }

    #[test]
    fn term_regression_and_same_term_vote_changes_are_rejected() {
        let (_sim, env) = sim_env();
        let mut file = HardStateFile::open_in(&env, "/meta/meta.hardstate").unwrap();
        file.save(HardState {
            term: 5,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: false,
        })
        .unwrap();

        assert!(matches!(
            file.save(HardState {
                term: 4,
                voted_for: None,
                vote_committed: false,
            }),
            Err(MetaStoreError::InvalidConfig(_))
        ));
        assert!(matches!(
            file.save(HardState {
                term: 5,
                voted_for: Some(MetaNodeId(2)),
                vote_committed: false,
            }),
            Err(MetaStoreError::InvalidConfig(_))
        ));
        assert!(matches!(
            file.save(HardState {
                term: 5,
                voted_for: None,
                vote_committed: false,
            }),
            Err(MetaStoreError::InvalidConfig(_))
        ));
        // A higher term may vote afresh.
        file.save(HardState {
            term: 6,
            voted_for: Some(MetaNodeId(2)),
            vote_committed: false,
        })
        .unwrap();
    }

    #[test]
    fn a_committed_vote_cannot_be_uncommitted_within_the_same_term() {
        let (_sim, env) = sim_env();
        let mut file = HardStateFile::open_in(&env, "/meta/meta.hardstate").unwrap();
        file.save(HardState {
            term: 5,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: true,
        })
        .unwrap();

        // Same term, same candidate, but the committed flag regresses:
        // recovery would forget the commitment, which is the double-vote
        // hazard the flag exists to prevent.
        assert!(matches!(
            file.save(HardState {
                term: 5,
                voted_for: Some(MetaNodeId(1)),
                vote_committed: false,
            }),
            Err(MetaStoreError::InvalidConfig(_))
        ));
        // Re-saving the identical committed state stays legal.
        file.save(HardState {
            term: 5,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: true,
        })
        .unwrap();
        // A new term starts uncommitted again.
        file.save(HardState {
            term: 6,
            voted_for: Some(MetaNodeId(2)),
            vote_committed: false,
        })
        .unwrap();
    }

    #[test]
    fn a_failed_save_poisons_the_file_until_reopened() {
        // Sweep an injected failure across every storage operation of one
        // save. Wherever the save errors — before or after the rename that
        // publishes the new bytes — the cached copy is untrustworthy, so
        // the file must refuse further saves until a reopen re-reads the
        // durable truth.
        let first = HardState {
            term: 1,
            voted_for: Some(MetaNodeId(1)),
            vote_committed: false,
        };
        let second = HardState {
            term: 2,
            voted_for: Some(MetaNodeId(2)),
            vote_committed: false,
        };
        let mut failing_positions = 0;
        for offset in 1..32 {
            let (sim, env) = sim_env();
            let path = Path::new("/meta/meta.hardstate");
            let mut file = HardStateFile::open_in(&env, path).unwrap();
            file.save(first).unwrap();

            sim.set_fault(FaultPlan::FailOp {
                op: sim.op_count() + offset,
                kind: io::ErrorKind::Other,
            });
            match file.save(second) {
                // The fault landed beyond this save's operation sequence;
                // the whole sequence has been swept.
                Ok(()) => break,
                Err(_) => {
                    failing_positions += 1;
                    // Even a perfectly valid follow-up must be refused.
                    assert!(matches!(
                        file.save(HardState {
                            term: 3,
                            voted_for: Some(MetaNodeId(3)),
                            vote_committed: false,
                        }),
                        Err(MetaStoreError::Poisoned(_))
                    ));
                    // Reopening re-reads the published truth: exactly the
                    // old or the new value, never a blend or a default.
                    let reopened = HardStateFile::open_in(&env, path).unwrap();
                    assert!(
                        reopened.state() == &first || reopened.state() == &second,
                        "reopened state {:?} matches neither save (offset {offset})",
                        reopened.state()
                    );
                }
            }
        }
        assert!(
            failing_positions >= 4,
            "the sweep must cover open/write/sync/rename/dir-sync, hit {failing_positions}"
        );
    }
}
