//! Durable log id of the applied membership for the Raft adapter.
//!
//! Layout (59 bytes exactly):
//!
//! ```text
//! magic "VTOPMML1"        8
//! version u16             2
//! present u8              1   (0 = none, 1 = some)
//! term u64                8
//! index u64               8   (meta / 1-based index)
//! BLAKE3-32 over prior   32
//! ```
//!
//! After purge or blank-follower snapshot install the membership entry is gone
//! from the physical log, so recovery must not invent a membership LogId from
//! the applied/snapshot frontier (which may belong to a later normal entry).

use super::{corrupt, io_error, write_atomic, MetaStoreError, MetaStoreResult};
use crate::wire::{put_u16, put_u64, put_u8, Reader};
use std::path::{Path, PathBuf};
use vtop_log::env::Env;

pub(crate) const MEMBERSHIP_LOG_ID_FILE_NAME: &str = "meta.membership_log_id";

const MEMBERSHIP_LOG_ID_MAGIC: &[u8; 8] = b"VTOPMML1";
const MEMBERSHIP_LOG_ID_VERSION: u16 = 1;
const MEMBERSHIP_LOG_ID_BYTES: usize = 8 + 2 + 1 + 8 + 8 + 32;
const CHECKSUM_LEN: usize = 32;

/// Meta-coordinate log id of the last applied membership entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MembershipLogId {
    pub term: u64,
    pub index: u64,
}

/// Owner of the on-disk membership log-id file.
pub struct MembershipLogIdFile {
    env: Env,
    path: PathBuf,
    log_id: Option<MembershipLogId>,
    poisoned: bool,
}

impl MembershipLogIdFile {
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
                log_id: None,
                poisoned: false,
            });
        }
        let bytes = env
            .storage
            .read(&path)
            .map_err(|source| io_error(&path, source))?;
        let log_id = decode(&path, &bytes)?;
        Ok(Self {
            env: env.clone(),
            path,
            log_id,
            poisoned: false,
        })
    }

    pub fn get(&self) -> Option<MembershipLogId> {
        self.log_id
    }

    /// Persist the log id of the current applied membership.
    ///
    /// Snapshot install may replace this with an earlier index than a divergent
    /// local history, so regression is allowed (unlike applied/purged frontiers).
    pub fn save(&mut self, next: MembershipLogId) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("membership log id"));
        }
        if next.index == 0 {
            return Err(MetaStoreError::InvalidConfig(
                "membership log index must be non-zero".to_owned(),
            ));
        }
        let bytes = encode(Some(next));
        if let Err(error) = write_atomic(&self.env, &self.path, &bytes) {
            self.poisoned = true;
            return Err(error);
        }
        self.log_id = Some(next);
        Ok(())
    }
}

fn encode(log_id: Option<MembershipLogId>) -> Vec<u8> {
    let mut out = Vec::with_capacity(MEMBERSHIP_LOG_ID_BYTES);
    out.extend_from_slice(MEMBERSHIP_LOG_ID_MAGIC);
    put_u16(&mut out, MEMBERSHIP_LOG_ID_VERSION);
    match log_id {
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

fn decode(path: &Path, bytes: &[u8]) -> MetaStoreResult<Option<MembershipLogId>> {
    if bytes.len() != MEMBERSHIP_LOG_ID_BYTES {
        return Err(corrupt(
            path,
            format!(
                "membership log id must be exactly {MEMBERSHIP_LOG_ID_BYTES} bytes, got {}",
                bytes.len()
            ),
        ));
    }
    let (payload, stored_checksum) = bytes.split_at(MEMBERSHIP_LOG_ID_BYTES - CHECKSUM_LEN);
    if blake3::hash(payload).as_bytes() != stored_checksum {
        return Err(corrupt(path, "membership log id checksum mismatch"));
    }
    let mut reader = Reader::new(payload);
    let magic = reader
        .take(8, "membership log id magic")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if magic != MEMBERSHIP_LOG_ID_MAGIC {
        return Err(corrupt(path, "membership log id magic mismatch"));
    }
    let version = reader
        .u16("membership log id version")
        .map_err(|error| corrupt(path, error.to_string()))?;
    if version != MEMBERSHIP_LOG_ID_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let decode_error = |error: crate::wire::CodecError| corrupt(path, error.to_string());
    let present = reader
        .u8("membership log id present")
        .map_err(decode_error)?;
    let term = reader.u64("membership log id term").map_err(decode_error)?;
    let index = reader
        .u64("membership log id index")
        .map_err(decode_error)?;
    reader.finish().map_err(decode_error)?;
    match present {
        0 => Ok(None),
        1 => {
            if index == 0 {
                return Err(corrupt(path, "membership log id present with zero index"));
            }
            Ok(Some(MembershipLogId { term, index }))
        }
        _ => Err(corrupt(
            path,
            format!("membership log id present flag must be 0 or 1, got {present}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sim_env() -> (vtop_log::sim::SimStorage, Env) {
        let sim = vtop_log::sim::SimStorage::new();
        sim.create_dir_all(Path::new("/meta"));
        let env = sim.env(0xb71);
        (sim, env)
    }

    #[test]
    fn save_and_reopen_round_trips_log_id() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.membership_log_id");
        let mut file = MembershipLogIdFile::open_in(&env, path).unwrap();
        assert_eq!(file.get(), None);
        file.save(MembershipLogId { term: 2, index: 1 }).unwrap();
        let reopened = MembershipLogIdFile::open_in(&env, path).unwrap();
        assert_eq!(reopened.get(), Some(MembershipLogId { term: 2, index: 1 }));
    }

    #[test]
    fn snapshot_replace_may_set_earlier_index() {
        let (_sim, env) = sim_env();
        let mut file = MembershipLogIdFile::open_in(&env, "/meta/meta.membership_log_id").unwrap();
        file.save(MembershipLogId { term: 1, index: 5 }).unwrap();
        file.save(MembershipLogId { term: 3, index: 1 }).unwrap();
        assert_eq!(file.get(), Some(MembershipLogId { term: 3, index: 1 }));
    }
}
