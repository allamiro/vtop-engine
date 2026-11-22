//! Durable audit frontier (#706): the highest log index through which every
//! applied audit record this replica owes has been accepted by its sink.
//!
//! Layout (58 bytes exactly):
//!
//! ```text
//! magic "VTOPMAU1"        8
//! version u16             2
//! index u64               8
//! floor u64               8
//! BLAKE3-32 over prior   32
//! ```
//!
//! `floor` is the highest index a snapshot install brought this replica
//! past: entries at or below it were never applied here one by one, and the
//! log below it may be a stale branch. A replay never reads at or below it
//! (review of #767: a later local snapshot can make that branch look
//! like the committed log again, so the boundary is kept durably).
//!
//! # Why a second frontier
//!
//! `meta.applied` is saved before the applied records are handed to the
//! sink, and nothing re-derived a record after that: a crash between the two
//! lost this replica's record of the entry for good (#705). The records are
//! now delivered first and this frontier saved after, so a restart replays
//! `(audited, applied]` from the log - at least once, never lost while the
//! log still holds the entries.
//!
//! It never runs ahead of `meta.applied`, and it never regresses: a save at
//! or below the stored index is a no-op, because two writers (an apply and a
//! snapshot build) may save concurrently-derived values and the lower one
//! must not win.
//!
//! An older release ignores the file: its directory scans match chunk and
//! snapshot names only.

use super::{corrupt, io_error, write_atomic, MetaStoreError, MetaStoreResult};
use crate::wire::{put_u16, put_u64, Reader};
use std::path::{Path, PathBuf};
use vtop_log::env::Env;

pub(crate) const AUDITED_FILE_NAME: &str = "meta.audited";

const AUDITED_MAGIC: &[u8; 8] = b"VTOPMAU1";
const AUDITED_VERSION: u16 = 1;
const AUDITED_BYTES: usize = 8 + 2 + 8 + 8 + 32;
const CHECKSUM_LEN: usize = 32;

/// Owner of the on-disk audit frontier file.
pub struct AuditedFrontierFile {
    env: Env,
    path: PathBuf,
    index: Option<u64>,
    floor: u64,
    poisoned: bool,
}

impl AuditedFrontierFile {
    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> MetaStoreResult<Self> {
        let path = path.as_ref().to_path_buf();
        let exists = env
            .storage
            .exists(&path)
            .map_err(|source| io_error(&path, source))?;
        let (index, floor) = if exists {
            let bytes = env
                .storage
                .read(&path)
                .map_err(|source| io_error(&path, source))?;
            let (index, floor) = decode(&path, &bytes)?;
            (Some(index), floor)
        } else {
            (None, 0)
        };
        Ok(Self {
            env: env.clone(),
            path,
            index,
            floor,
            poisoned: false,
        })
    }

    /// `None`: no file, which is a store written before #706 (or a new one
    /// not yet pinned).
    pub fn get(&self) -> Option<u64> {
        self.index
    }

    /// The highest index a snapshot install brought this replica past.
    pub fn floor(&self) -> u64 {
        self.floor
    }

    /// Persist `index` if it is above the stored one; at or below it this is
    /// a no-op (see the module docs).
    pub fn save(&mut self, index: u64) -> MetaStoreResult<()> {
        if self.index.is_some_and(|stored| index <= stored) {
            return Ok(());
        }
        self.write(index, self.floor)
    }

    /// Persist the install floor if it is above the stored one.
    pub fn save_floor(&mut self, floor: u64) -> MetaStoreResult<()> {
        if floor <= self.floor {
            return Ok(());
        }
        self.write(self.index.unwrap_or(0), floor)
    }

    fn write(&mut self, index: u64, floor: u64) -> MetaStoreResult<()> {
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("audit frontier"));
        }
        if let Err(error) = write_atomic(&self.env, &self.path, &encode(index, floor)) {
            self.poisoned = true;
            return Err(error);
        }
        self.index = Some(index);
        self.floor = floor;
        Ok(())
    }

    /// Pin the frontier where none exists yet. Unlike [`Self::save`], a
    /// first pin at 0 is written: the file's presence is what says "this
    /// store tracks its audit frontier".
    pub fn pin(&mut self, index: u64) -> MetaStoreResult<()> {
        if self.index.is_some() {
            return Ok(());
        }
        if self.poisoned {
            return Err(MetaStoreError::Poisoned("audit frontier"));
        }
        self.write(index, self.floor)
    }
}

fn encode(index: u64, floor: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(AUDITED_BYTES);
    out.extend_from_slice(AUDITED_MAGIC);
    put_u16(&mut out, AUDITED_VERSION);
    put_u64(&mut out, index);
    put_u64(&mut out, floor);
    let checksum = blake3::hash(&out);
    out.extend_from_slice(checksum.as_bytes());
    out
}

fn decode(path: &Path, bytes: &[u8]) -> MetaStoreResult<(u64, u64)> {
    if bytes.len() != AUDITED_BYTES {
        return Err(corrupt(
            path,
            format!(
                "audit frontier must be exactly {AUDITED_BYTES} bytes, got {}",
                bytes.len()
            ),
        ));
    }
    let (payload, stored_checksum) = bytes.split_at(AUDITED_BYTES - CHECKSUM_LEN);
    if blake3::hash(payload).as_bytes() != stored_checksum {
        return Err(corrupt(path, "audit frontier checksum mismatch"));
    }
    let mut reader = Reader::new(payload);
    let decode_error = |error: crate::wire::CodecError| corrupt(path, error.to_string());
    let magic = reader
        .take(8, "audit frontier magic")
        .map_err(decode_error)?;
    if magic != AUDITED_MAGIC {
        return Err(corrupt(path, "audit frontier magic mismatch"));
    }
    let version = reader.u16("audit frontier version").map_err(decode_error)?;
    if version != AUDITED_VERSION {
        return Err(MetaStoreError::UnsupportedVersion {
            path: path.to_path_buf(),
            version,
        });
    }
    let index = reader.u64("audited index").map_err(decode_error)?;
    let floor = reader.u64("audit install floor").map_err(decode_error)?;
    reader.finish().map_err(decode_error)?;
    Ok((index, floor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn sim_env() -> (vtop_log::sim::SimStorage, Env) {
        let sim = vtop_log::sim::SimStorage::new();
        sim.create_dir_all(Path::new("/meta"));
        let env = sim.env(0x706);
        (sim, env)
    }

    /// The bytes, derived by hand from the layout above, so a change to the
    /// encoding fails here before a released reader meets it.
    #[test]
    fn the_audit_frontier_matches_its_golden_bytes() {
        let mut golden = Vec::new();
        golden.extend_from_slice(b"VTOPMAU1");
        golden.extend_from_slice(&[0x00, 0x01]);
        golden.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0x03, 0x2c]);
        golden.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0x02, 0x58]);
        let checksum = blake3::hash(&golden);
        golden.extend_from_slice(checksum.as_bytes());
        assert_eq!(golden.len(), 58);
        assert_eq!(encode(812, 600), golden);
        assert_eq!(decode(Path::new("/x"), &golden).unwrap(), (812, 600));
    }

    #[test]
    fn save_and_reopen_round_trips_and_never_regresses() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.audited");
        let mut file = AuditedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!(file.get(), None);
        file.pin(0).unwrap();
        assert_eq!(
            AuditedFrontierFile::open_in(&env, path).unwrap().get(),
            Some(0),
            "a first pin at 0 is written: the file's presence is the point"
        );
        file.save(9).unwrap();
        file.save(4).unwrap();
        file.pin(2).unwrap();
        assert_eq!(file.get(), Some(9), "a lower save or a re-pin is a no-op");
        assert_eq!(
            AuditedFrontierFile::open_in(&env, path).unwrap().get(),
            Some(9)
        );
        file.save_floor(20).unwrap();
        file.save_floor(5).unwrap();
        file.save(30).unwrap();
        let reopened = AuditedFrontierFile::open_in(&env, path).unwrap();
        assert_eq!((reopened.get(), reopened.floor()), (Some(30), 20));
    }

    #[test]
    fn a_damaged_frontier_is_refused_as_corrupt() {
        let (_sim, env) = sim_env();
        let path = Path::new("/meta/meta.audited");
        let good = encode(7, 0);
        let mut bad_magic = good.clone();
        bad_magic[0] = b'X';
        let checksum = blake3::hash(&bad_magic[..26]);
        bad_magic[26..].copy_from_slice(checksum.as_bytes());
        let mut flipped = good.clone();
        flipped[12] ^= 1;
        let mut trailing = good.clone();
        trailing.push(0);
        for (what, bytes) in [
            ("bad magic", bad_magic),
            ("checksum", flipped),
            ("trailing byte", trailing),
            ("truncated", good[..57].to_vec()),
        ] {
            write_atomic(&env, path, &bytes).unwrap();
            assert!(
                matches!(
                    AuditedFrontierFile::open_in(&env, path),
                    Err(MetaStoreError::Corrupt { .. })
                ),
                "{what}"
            );
        }
        let mut newer = Vec::new();
        newer.extend_from_slice(b"VTOPMAU1");
        newer.extend_from_slice(&[0x00, 0x02]);
        newer.extend_from_slice(&7u64.to_be_bytes());
        newer.extend_from_slice(&0u64.to_be_bytes());
        let checksum = blake3::hash(&newer);
        newer.extend_from_slice(checksum.as_bytes());
        write_atomic(&env, path, &newer).unwrap();
        assert!(matches!(
            AuditedFrontierFile::open_in(&env, path),
            Err(MetaStoreError::UnsupportedVersion { version: 2, .. })
        ));
    }
}
