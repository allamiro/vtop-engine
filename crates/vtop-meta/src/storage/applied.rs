//! Durable applied/committed frontier for the metadata store.
//!
//! Layout (58 bytes exactly):
//!
//! ```text
//! magic "VTOPMAP1"        8
//! version u16             2
//! index u64               8
//! term u64                8
//! BLAKE3-32 over prior   32
//! ```
//!
//! Single-node recovery historically replayed every durable log entry. Under
//! the Raft adapter, append ≠ commit, so apply must flush this frontier and
//! reopen must replay only through it.

use super::{corrupt, io_error, write_atomic, MetaStoreError, MetaStoreResult};
use crate::wire::{put_u16, put_u64, Reader};
use std::path::{Path, PathBuf};
use vtop_log::env::Env;

pub(crate) const APPLIED_FILE_NAME: &str = "meta.applied";

const APPLIED_MAGIC: &[u8; 8] = b"VTOPMAP1";
const APPLIED_VERSION: u16 = 1;
const APPLIED_BYTES: usize = 8 + 2 + 8 + 8 + 32;
const CHECKSUM_LEN: usize = 32;

/// Last log index/term known to have been applied to the state machine.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AppliedFrontier {
    pub index: u64,
    pub term: u64,
}

/// Owner of the on-disk applied frontier file.
pub struct AppliedFrontierFile {
    env: Env,
    path: PathBuf,
    frontier: Option<AppliedFrontier>,
    poisoned: bool,
}

impl AppliedFrontierFile {
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
                frontier: None,
                poisoned: false,
            });
        }
        let bytes = env
            .storage
            .read(&path)
            .map_err(|source| io_error(&path, source))?;
        let frontier = decode(&path, &bytes)?;
        Ok(Self {
            env: env.clone(),
            path,
            frontier: Some(frontier),
            poisoned: false,
        })
    }

    pub fn get(&self) -> Option<AppliedFrontier> {
        self.frontier
    }

    /// Persist a new applied frontier. Index is monotonic: it may stay the
    /// same (idempotent re-apply) or advance, never regress.
    pub fn save(&mut self, next: AppliedFrontier) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("applied frontier"));
        }
        if let Some(prev) = self.frontier {
            if next.index < prev.index {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "applied frontier regressed from {} to {}",
                    prev.index, next.index
                )));
            }
        }
        let bytes = encode(&next);
        if let Err(error) = write_atomic(&self.env, &self.path, &bytes) {
            self.poisoned = true;
            return Err(error);
        }
        self.frontier = Some(next);
        Ok(())
    }
}

fn encode(frontier: &AppliedFrontier) -> Vec<u8> {
    let mut out = Vec::with_capacity(APPLIED_BYTES);
    out.extend_from_slice(APPLIED_MAGIC);
    put_u16(&mut out, APPLIED_VERSION);
    put_u64(&mut out, frontier.index);
    put_u64(&mut out, frontier.term);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn decode(path: &Path, bytes: &[u8]) -> MetaStoreResult<AppliedFrontier> {
    if bytes.len() != APPLIED_BYTES {
        return Err(corrupt(
            path,
            format!(
                "applied frontier must be exactly {APPLIED_BYTES} bytes, got {}",
                bytes.len()
            ),
        ));
    }
    let (payload, stored_checksum) = bytes.split_at(APPLIED_BYTES - CHECKSUM_LEN);
    if blake3::hash(payload).as_bytes() != stored_checksum {
        return Err(corrupt(path, "applied frontier checksum mismatch"));
    }
    let mut reader = Reader::new(payload);
    let magic = reader
        .take(8, "applied frontier magic")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if magic != APPLIED_MAGIC {
        return Err(corrupt(path, "applied frontier magic mismatch"));
    }
    let version = reader
        .u16("applied frontier version")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if version != APPLIED_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let decode_error = |error: crate::wire::CodecError| corrupt(path, error.to_string());
    let index = reader.u64("applied index").map_err(decode_error)?;
    let term = reader.u64("applied term").map_err(decode_error)?;
    reader.finish().map_err(decode_error)?;
    Ok(AppliedFrontier { index, term })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sim_env() -> (vtop_log::sim::SimStorage, Env) {
        let sim = vtop_log::sim::SimStorage::new();
        sim.create_dir_all(Path::new("/meta"));
        let env = sim.env(0xa91);
        (sim, env)
    }

    #[test]
    fn save_and_reopen_round_trips_frontier() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.applied");
        let mut file = AppliedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!(file.get(), None);
        file.save(AppliedFrontier { index: 9, term: 3 }).unwrap();
        let reopened = AppliedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!(reopened.get(), Some(AppliedFrontier { index: 9, term: 3 }));
    }

    #[test]
    fn regression_is_rejected() {
        let (_sim, env) = sim_env();
        let mut file = AppliedFrontierFile::open_in(&env, "/meta/meta.applied").unwrap();
        file.save(AppliedFrontier { index: 5, term: 1 }).unwrap();
        assert!(matches!(
            file.save(AppliedFrontier { index: 4, term: 2 }),
            Err(MetaStoreError::InvalidConfig(_))
        ));
    }
}
