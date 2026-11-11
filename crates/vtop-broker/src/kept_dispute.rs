//! The durable record of a suffix a fence kept in dispute (#635).
//!
//! A replica fenced by a candidate that merely LACKS its suffix keeps the
//! suffix (#611): nothing proves those records were never acknowledged. If
//! the candidate is established anyway, by a majority that outvoted this
//! replica, its first append or committed mark past the dispute's start is
//! that proof, and the suffix goes before anything is acked over it. The
//! replica has to remember the dispute until then. Remembered in memory only,
//! a restart between the fence reply and that settlement brought the replica
//! back holding the old records with no record that they were disputed. Its
//! catch-up path then acked the new leader's record at the disputed offset as
//! a duplicate of a DIFFERENT record (#261), and its reconnect status reported
//! the old records as held.
//!
//! This file is that memory. It lives beside the fencing-epoch journal
//! (`fencing-epochs.kept-dispute`) and is owned by it
//! ([`crate::fencing_epochs::FencingEpochJournal::record_kept_dispute`]), so
//! every path that opens the journal reloads it.
//!
//! # Ordering
//!
//! Written and fsynced, and the directory synced on creation, BEFORE the
//! fence returns the reply that announces the kept suffix. A crash before that
//! leaves no reply, and a candidate that never got one counted nothing.
//! Cleared, durably, once settled and before the settling append is applied.
//!
//! # Compatibility
//!
//! Older binaries do not know this file and would ignore it, which is not
//! safe: they would serve the suffix exactly as #616 did after a restart. So
//! while a record is present the journal beside it carries header version 2
//! (`fencing_epochs::VERSION_DISPUTE_PENDING`). Every release since the
//! journal shipped refuses any version but 1, so an older binary refuses to
//! open the directory for exactly as long as a dispute is pending, and opens
//! it normally once it is settled. Downgrades are not supported in general
//! (ROADMAP); this keeps the one unsafe outcome of one from being silent.
//!
//! # Torn writes
//!
//! A record is announced only after its fsync returns, so a crash can leave a
//! whole valid frame or a torn, never-announced one. A short file, or a full
//! frame whose checksum fails, reads as no dispute. That is the same rule the
//! epoch journal applies to a torn tail. A whole frame from a newer release
//! (valid magic, unknown version) or a file longer than a frame is refused, so
//! a format this build cannot read is never read as "no dispute".
//!
//! # Layout (66 bytes, or empty for "no dispute")
//!
//! ```text
//! magic "VTOPKDS1"   8
//! version u16        2
//! fence_epoch u64    8   the epoch of the fence that kept the suffix
//! from u64           8   the first offset in dispute
//! suffix_epoch u64   8   the epoch that wrote the record at `from` then,
//!                        0 if this replica could not say
//! BLAKE3-32         32   over everything above
//! ```
//!
//! Big-endian, like the journal beside it.

use crate::{BrokerError, BrokerResult};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use vtop_log::env::{Env, OpenMode, StorageFile};

const MAGIC: &[u8; 8] = b"VTOPKDS1";
const VERSION: u16 = 1;
const PAYLOAD_BYTES: usize = 8 + 2 + 8 + 8 + 8;
const CHECKSUM_BYTES: usize = 32;
const FRAME_BYTES: usize = PAYLOAD_BYTES + CHECKSUM_BYTES;

/// A suffix a fence kept in dispute, as recorded on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeptDisputeRecord {
    /// The epoch of the fence that kept the suffix. Only an append or a
    /// committed mark under this epoch settles it.
    pub fence_epoch: u64,
    /// The first offset in dispute.
    pub from: u64,
    /// The epoch that owned the record at `from` when the fence kept it; 0
    /// when this replica's history could not say. A reopen that finds a
    /// different owner there knows the log moved past the dispute without
    /// this record being cleared (see `InProcessFollower`'s reload).
    pub suffix_epoch: u64,
}

/// The sidecar's path for the journal at `journal_path`.
pub fn sidecar_path(journal_path: &Path) -> PathBuf {
    let mut name = journal_path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_default();
    name.push(".kept-dispute");
    journal_path.with_file_name(name)
}

/// Owner of the sidecar file. Opened lazily: a replica that never keeps a
/// dispute never creates it.
pub(crate) struct KeptDisputeFile {
    env: Env,
    path: PathBuf,
    file: Option<Box<dyn StorageFile>>,
    /// What the file durably says, as far as this handle knows.
    current: Option<KeptDisputeRecord>,
    /// The file may not say `current`: a write failed part-way, or open
    /// found torn bytes. The next record or clear rewrites it either way.
    dirty: bool,
    /// The directory entry is synced once per handle, on its first write,
    /// for a file this handle may have created.
    parent_synced: bool,
}

impl KeptDisputeFile {
    pub(crate) fn open_in(env: &Env, path: PathBuf) -> BrokerResult<Self> {
        let mut this = Self {
            env: env.clone(),
            path,
            file: None,
            current: None,
            dirty: false,
            parent_synced: false,
        };
        let exists = env
            .storage
            .exists(&this.path)
            .map_err(|source| this.io(source))?;
        if !exists {
            return Ok(this);
        }
        let mut file = env
            .storage
            .open(&this.path, OpenMode::ReadWrite)
            .map_err(|source| this.io(source))?;
        let len = file.len().map_err(|source| this.io(source))?;
        // Bounded BEFORE allocating: the length comes off disk.
        if len > FRAME_BYTES as u64 {
            let mut prefix = [0_u8; 10];
            file.seek(SeekFrom::Start(0))
                .and_then(|_| file.read_exact(&mut prefix))
                .map_err(|source| this.io(source))?;
            return Err(Self::refuse(&prefix, len));
        }
        let mut bytes = vec![0_u8; len as usize];
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.read_exact(&mut bytes))
            .map_err(|source| this.io(source))?;
        this.file = Some(file);
        match decode(&bytes)? {
            Some(record) => this.current = Some(record),
            None if bytes.is_empty() => {}
            None => {
                eprintln!(
                    "kept-dispute record {} is torn ({len} bytes, no valid frame); a record is \
                     announced only after its fsync, so this one never was, and it reads as no \
                     dispute (#635)",
                    this.path.display()
                );
                this.dirty = true;
            }
        }
        Ok(this)
    }

    pub(crate) fn current(&self) -> Option<KeptDisputeRecord> {
        self.current
    }

    /// Make `record` the durable dispute. A no-op when it already is.
    pub(crate) fn record(&mut self, record: KeptDisputeRecord) -> BrokerResult<()> {
        if self.current == Some(record) && !self.dirty {
            return Ok(());
        }
        // Until the write below is durable the file says neither the old
        // record nor the new one for certain.
        self.dirty = true;
        let frame = encode(record);
        self.write(|file| {
            file.seek(SeekFrom::Start(0))
                .and_then(|_| file.write_all(&frame))
                .and_then(|()| file.set_len(FRAME_BYTES as u64))
        })?;
        self.current = Some(record);
        self.dirty = false;
        Ok(())
    }

    /// Durably record that no dispute is pending. A no-op when none is.
    pub(crate) fn clear(&mut self) -> BrokerResult<()> {
        if self.current.is_none() && !self.dirty {
            return Ok(());
        }
        self.dirty = true;
        self.write(|file| file.set_len(0))?;
        self.current = None;
        self.dirty = false;
        Ok(())
    }

    fn write(
        &mut self,
        edit: impl FnOnce(&mut Box<dyn StorageFile>) -> std::io::Result<()>,
    ) -> BrokerResult<()> {
        if self.file.is_none() {
            let file = self
                .env
                .storage
                .open(&self.path, OpenMode::CreateAppend)
                .map_err(|source| self.io(source))?;
            self.file = Some(file);
        }
        let file = self.file.as_mut().expect("opened above");
        edit(file)
            .and_then(|()| file.sync_data())
            .map_err(|source| BrokerError::Io {
                path: self.path.clone(),
                source,
            })?;
        if !self.parent_synced {
            // fsync of the file does not make a newly created directory entry
            // durable; without this a power loss can take the whole record.
            let parent = match self.path.parent() {
                Some(parent) if parent.as_os_str().is_empty() => Path::new("."),
                Some(parent) => parent,
                None => return Ok(()),
            };
            self.env
                .storage
                .sync_dir(parent)
                .map_err(|source| BrokerError::Io {
                    path: parent.to_path_buf(),
                    source,
                })?;
            self.parent_synced = true;
        }
        Ok(())
    }

    fn io(&self, source: std::io::Error) -> BrokerError {
        BrokerError::Io {
            path: self.path.clone(),
            source,
        }
    }

    /// The error for a file this build cannot read: newer when its header
    /// names a version past this one, malformed otherwise.
    fn refuse(prefix: &[u8], len: u64) -> BrokerError {
        if prefix.len() >= 10 && &prefix[0..8] == MAGIC {
            let version = u16::from_be_bytes([prefix[8], prefix[9]]);
            if version > VERSION {
                return BrokerError::KeptDispute(format!(
                    "version {version} was written by a newer release than this one, which \
                     reads version {VERSION}; upgrade this node"
                ));
            }
        }
        BrokerError::KeptDispute(format!(
            "{len} bytes that are not a version-{VERSION} record"
        ))
    }
}

fn encode(record: KeptDisputeRecord) -> [u8; FRAME_BYTES] {
    let mut bytes = [0_u8; FRAME_BYTES];
    bytes[0..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
    bytes[10..18].copy_from_slice(&record.fence_epoch.to_be_bytes());
    bytes[18..26].copy_from_slice(&record.from.to_be_bytes());
    bytes[26..34].copy_from_slice(&record.suffix_epoch.to_be_bytes());
    let checksum = blake3::hash(&bytes[..PAYLOAD_BYTES]);
    bytes[PAYLOAD_BYTES..].copy_from_slice(checksum.as_bytes());
    bytes
}

/// `Ok(Some)` for a whole valid frame, `Ok(None)` for empty or torn bytes,
/// `Err` for a frame this build must not read as "no dispute".
fn decode(bytes: &[u8]) -> BrokerResult<Option<KeptDisputeRecord>> {
    // A newer release's header is recognisable before anything else is: a
    // newer frame may be laid out differently, so its checksum says nothing.
    if bytes.len() >= 10 && &bytes[0..8] == MAGIC {
        let version = u16::from_be_bytes([bytes[8], bytes[9]]);
        if version > VERSION {
            return Err(KeptDisputeFile::refuse(bytes, bytes.len() as u64));
        }
    }
    if bytes.len() != FRAME_BYTES {
        return Ok(None);
    }
    let (payload, checksum) = bytes.split_at(PAYLOAD_BYTES);
    if blake3::hash(payload).as_bytes() != checksum {
        return Ok(None);
    }
    // A valid checksum over a foreign header was not torn: it is some
    // other file under this name.
    if &payload[0..8] != MAGIC || u16::from_be_bytes([payload[8], payload[9]]) != VERSION {
        return Err(KeptDisputeFile::refuse(bytes, bytes.len() as u64));
    }
    let field = |at: usize| u64::from_be_bytes(payload[at..at + 8].try_into().expect("width"));
    Ok(Some(KeptDisputeRecord {
        fence_epoch: field(10),
        from: field(18),
        suffix_epoch: field(26),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const RECORD: KeptDisputeRecord = KeptDisputeRecord {
        fence_epoch: 19,
        from: 5,
        suffix_epoch: 18,
    };

    fn open(dir: &TempDir) -> BrokerResult<KeptDisputeFile> {
        KeptDisputeFile::open_in(&Env::real(), dir.path().join("fencing-epochs.kept-dispute"))
    }

    #[test]
    fn an_absent_file_is_no_dispute_and_is_not_created_by_opening() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = open(&dir).unwrap();
        assert_eq!(file.current(), None);
        file.clear().unwrap();
        assert!(!dir.path().join("fencing-epochs.kept-dispute").exists());
    }

    #[test]
    fn a_record_survives_reopening_and_a_clear_survives_too() {
        let dir = tempfile::tempdir().unwrap();
        open(&dir).unwrap().record(RECORD).unwrap();
        let mut file = open(&dir).unwrap();
        assert_eq!(file.current(), Some(RECORD));
        file.clear().unwrap();
        assert_eq!(open(&dir).unwrap().current(), None);
    }

    #[test]
    fn a_torn_frame_reads_as_no_dispute_and_a_later_record_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fencing-epochs.kept-dispute");
        let frame = encode(RECORD);
        for cut in 1..FRAME_BYTES {
            std::fs::write(&path, &frame[..cut]).unwrap();
            assert_eq!(open(&dir).unwrap().current(), None, "cut {cut}");
        }
        let mut flipped = frame;
        flipped[20] ^= 0x01;
        std::fs::write(&path, flipped).unwrap();
        let mut file = open(&dir).unwrap();
        assert_eq!(file.current(), None, "a failed checksum is a torn frame");
        file.record(RECORD).unwrap();
        assert_eq!(open(&dir).unwrap().current(), Some(RECORD));
    }

    #[test]
    fn a_newer_or_foreign_file_is_refused_not_read_as_no_dispute() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("fencing-epochs.kept-dispute");

        let mut newer = encode(RECORD).to_vec();
        newer[8..10].copy_from_slice(&2_u16.to_be_bytes());
        std::fs::write(&path, &newer).unwrap();
        let refused = open(&dir).err().expect("a newer version");
        assert!(refused.to_string().contains("newer release"), "{refused}");

        newer.extend_from_slice(&[0; 8]);
        std::fs::write(&path, &newer).unwrap();
        let refused = open(&dir).err().expect("a longer, newer frame");
        assert!(refused.to_string().contains("newer release"), "{refused}");

        std::fs::write(&path, [0_u8; FRAME_BYTES + 1]).unwrap();
        let refused = open(&dir).err().expect("too long to be a record");
        assert!(refused.to_string().contains("not a version-1"), "{refused}");
    }

    #[test]
    fn the_sidecar_sits_beside_the_journal() {
        assert_eq!(
            sidecar_path(Path::new("/data/fencing-epochs")),
            Path::new("/data/fencing-epochs.kept-dispute")
        );
    }
}
