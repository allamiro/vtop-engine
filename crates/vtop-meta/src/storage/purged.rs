//! Durable last-purged log id for the Raft adapter.
//!
//! Layout (59 bytes exactly):
//!
//! ```text
//! magic "VTOPMPG1"        8
//! version u16             2
//! present u8              1   (0 = none, 1 = some)
//! term u64                8
//! index u64               8   (meta / 1-based index)
//! BLAKE3-32 over prior   32
//! ```
//!
//! Inferring the purged frontier from chunk layout is lossy when purge only
//! drops whole chunks; the acknowledged LogId must be stored exactly.

use super::{corrupt, io_error, write_atomic, MetaStoreError, MetaStoreResult};
use crate::wire::{put_u16, put_u64, put_u8, Reader};
use std::path::{Path, PathBuf};
use vtop_log::env::Env;

pub(crate) const PURGED_FILE_NAME: &str = "meta.purged";

const PURGED_MAGIC: &[u8; 8] = b"VTOPMPG1";
const PURGED_VERSION: u16 = 1;
const PURGED_BYTES: usize = 8 + 2 + 1 + 8 + 8 + 32;
const CHECKSUM_LEN: usize = 32;

/// Last purged meta-coordinate log id (1-based index).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PurgedLogId {
    pub term: u64,
    pub index: u64,
}

/// Owner of the on-disk purged frontier file.
pub struct PurgedFrontierFile {
    env: Env,
    path: PathBuf,
    purged: Option<PurgedLogId>,
    poisoned: bool,
}

impl PurgedFrontierFile {
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
                purged: None,
                poisoned: false,
            });
        }
        let bytes = env
            .storage
            .read(&path)
            .map_err(|source| io_error(&path, source))?;
        let purged = decode(&path, &bytes)?;
        Ok(Self {
            env: env.clone(),
            path,
            purged,
            poisoned: false,
        })
    }

    pub fn get(&self) -> Option<PurgedLogId> {
        self.purged
    }

    /// Persist the acknowledged purge frontier. Index is monotonic.
    pub fn save(&mut self, next: PurgedLogId) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("purged frontier"));
        }
        if next.index == 0 {
            return Err(MetaStoreError::InvalidConfig(
                "purged log index must be non-zero".to_owned(),
            ));
        }
        if let Some(prev) = self.purged {
            if next.index < prev.index {
                return Err(MetaStoreError::InvalidConfig(format!(
                    "purged frontier regressed from {} to {}",
                    prev.index, next.index
                )));
            }
        }
        let bytes = encode(Some(next));
        if let Err(error) = write_atomic(&self.env, &self.path, &bytes) {
            self.poisoned = true;
            return Err(error);
        }
        self.purged = Some(next);
        Ok(())
    }
}

fn encode(purged: Option<PurgedLogId>) -> Vec<u8> {
    let mut out = Vec::with_capacity(PURGED_BYTES);
    out.extend_from_slice(PURGED_MAGIC);
    put_u16(&mut out, PURGED_VERSION);
    match purged {
        None => {
            put_u8(&mut out, 0);
            put_u64(&mut out, 0);
            put_u64(&mut out, 0);
        }
        Some(id) => {
            put_u8(&mut out, 1);
            put_u64(&mut out, id.term);
            put_u64(&mut out, id.index);
        }
    }
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn decode(path: &Path, bytes: &[u8]) -> MetaStoreResult<Option<PurgedLogId>> {
    if bytes.len() != PURGED_BYTES {
        return Err(corrupt(
            path,
            format!(
                "purged frontier must be exactly {PURGED_BYTES} bytes, got {}",
                bytes.len()
            ),
        ));
    }
    let (payload, stored_checksum) = bytes.split_at(PURGED_BYTES - CHECKSUM_LEN);
    if blake3::hash(payload).as_bytes() != stored_checksum {
        return Err(corrupt(path, "purged frontier checksum mismatch"));
    }
    let mut reader = Reader::new(payload);
    let magic = reader
        .take(8, "purged frontier magic")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if magic != PURGED_MAGIC {
        return Err(corrupt(path, "purged frontier magic mismatch"));
    }
    let version = reader
        .u16("purged frontier version")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if version != PURGED_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let decode_error = |error: crate::wire::CodecError| corrupt(path, error.to_string());
    let present = reader.flag("purged presence").map_err(decode_error)?;
    let term = reader.u64("purged term").map_err(decode_error)?;
    let index = reader.u64("purged index").map_err(decode_error)?;
    reader.finish().map_err(decode_error)?;
    if !present {
        if term != 0 || index != 0 {
            return Err(corrupt(path, "absent purged id must encode zeros"));
        }
        return Ok(None);
    }
    if index == 0 {
        return Err(corrupt(path, "present purged id must have non-zero index"));
    }
    Ok(Some(PurgedLogId { term, index }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sim_env() -> (vtop_log::sim::SimStorage, Env) {
        let sim = vtop_log::sim::SimStorage::new();
        sim.create_dir_all(Path::new("/meta"));
        let env = sim.env(0xb02);
        (sim, env)
    }

    #[test]
    fn save_and_reopen_round_trips_purged_id() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.purged");
        let mut file = PurgedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!(file.get(), None);
        file.save(PurgedLogId { term: 4, index: 12 }).unwrap();
        let reopened = PurgedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!(reopened.get(), Some(PurgedLogId { term: 4, index: 12 }));
    }
}
