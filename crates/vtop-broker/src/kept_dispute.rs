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
//! open the directory while a dispute may be pending, and opens it normally
//! once it is settled. The gate goes up before the record is written and
//! comes down after it is cleared, so a crash between those steps can leave
//! it up with no record; this release lowers it at its next open of the
//! journal. Downgrades are not supported in general (ROADMAP); this keeps the
//! one unsafe outcome of one from being silent.
//!
//! # Two slots, so a change never erases what it replaces
//!
//! The file holds two independently checksummed frames, each carrying a
//! sequence number, and the reader takes the valid frame with the highest
//! one. A change (a new dispute, or a "no dispute" frame that clears one)
//! writes the slot NOT holding the newest frame, so a crash anywhere in the
//! change leaves the newest durable frame intact: the file reads back as the
//! state before the change or the state after it, never as neither. A single
//! overwritten frame could not promise that: a write torn mid-frame read as
//! "no dispute" and erased one a fence reply had announced (#643 review).
//! This is the committed-floor file's layout, with a sequence number because
//! a dispute, unlike a floor, is not monotonic.
//!
//! A frame that fails its checksum, or is cut short, is a torn write and is
//! ignored. A frame from a newer release (valid magic, higher version) or a
//! file longer than two frames is refused, so a format this build cannot
//! read is never read as "no dispute".
//!
//! # Layout (two 75-byte frames; an empty or absent file is "no dispute")
//!
//! ```text
//! magic "VTOPKDS1"   8
//! version u16        2
//! sequence u64       8   higher is newer
//! kind u8            1   0 = no dispute, 1 = the dispute below
//! fence_epoch u64    8   the epoch of the fence that kept the suffix
//! from u64           8   the first offset in dispute
//! suffix_epoch u64   8   when known, the epoch that wrote the record at
//!                        `from` then; 0 if this replica could not say
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
const PAYLOAD_BYTES: usize = 8 + 2 + 8 + 1 + 8 + 8 + 8;
const CHECKSUM_BYTES: usize = 32;
const FRAME_BYTES: usize = PAYLOAD_BYTES + CHECKSUM_BYTES;
const FILE_BYTES: usize = 2 * FRAME_BYTES;

/// A suffix a fence kept in dispute, as recorded on disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeptDisputeRecord {
    /// The epoch of the fence that kept the suffix. Only an append or a
    /// committed mark under this epoch settles it.
    pub fence_epoch: u64,
    /// The first offset in dispute.
    pub from: u64,
    /// When known, the epoch that owned the record at `from` when the fence
    /// kept it; 0 when this replica's history could not say. A reopen that
    /// finds a different owner there knows the log moved past the dispute
    /// without this record being cleared (see `InProcessFollower`'s reload).
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
    /// What the newest durable frame says, as far as this handle knows.
    current: Option<KeptDisputeRecord>,
    /// The slot holding the newest valid frame, and its sequence number.
    /// `None` when no slot holds a valid frame. The next change writes the
    /// other slot, and only a change that succeeded moves this, so a failed
    /// write never makes the next one overwrite the frame still in force.
    newest: Option<(usize, u64)>,
    /// The file may not say `current`: a write failed part-way, or open
    /// found torn bytes and no valid frame. The next change writes either way.
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
            newest: None,
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
        if len > FILE_BYTES as u64 {
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
        for slot in 0..2 {
            let start = slot * FRAME_BYTES;
            let Some(frame) = bytes.get(start..(start + FRAME_BYTES).min(bytes.len())) else {
                continue;
            };
            if let Some((sequence, record)) = decode(frame)? {
                if this.newest.is_none_or(|(_, newest)| sequence > newest) {
                    this.newest = Some((slot, sequence));
                    this.current = record;
                }
            }
        }
        if this.newest.is_none() && !bytes.is_empty() {
            eprintln!(
                "kept-dispute record {} is torn ({len} bytes, no valid frame); a record is \
                 announced only after its fsync, so this one never was, and it reads as no \
                 dispute (#635)",
                this.path.display()
            );
            this.dirty = true;
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
        self.change(Some(record))
    }

    /// Durably record that no dispute is pending. A no-op when none is.
    pub(crate) fn clear(&mut self) -> BrokerResult<()> {
        if self.current.is_none() && !self.dirty {
            return Ok(());
        }
        self.change(None)
    }

    fn change(&mut self, record: Option<KeptDisputeRecord>) -> BrokerResult<()> {
        let (slot, sequence) = match self.newest {
            Some((slot, sequence)) => (1 - slot, sequence + 1),
            None => (0, 1),
        };
        // Until the write below is durable the file may say either state.
        self.dirty = true;
        let frame = encode(sequence, record);
        self.write(|file| {
            file.seek(SeekFrom::Start((slot * FRAME_BYTES) as u64))
                .and_then(|_| file.write_all(&frame))
                // Pinned to two frames: a fresh file extends with zeros (an
                // invalid frame the reader ignores), and nothing a previous
                // life left past the format survives.
                .and_then(|()| file.set_len(FILE_BYTES as u64))
        })?;
        self.newest = Some((slot, sequence));
        self.current = record;
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

    /// The error for a file this build cannot read: newer when a header
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

fn encode(sequence: u64, record: Option<KeptDisputeRecord>) -> [u8; FRAME_BYTES] {
    let mut bytes = [0_u8; FRAME_BYTES];
    bytes[0..8].copy_from_slice(MAGIC);
    bytes[8..10].copy_from_slice(&VERSION.to_be_bytes());
    bytes[10..18].copy_from_slice(&sequence.to_be_bytes());
    if let Some(record) = record {
        bytes[18] = 1;
        bytes[19..27].copy_from_slice(&record.fence_epoch.to_be_bytes());
        bytes[27..35].copy_from_slice(&record.from.to_be_bytes());
        bytes[35..43].copy_from_slice(&record.suffix_epoch.to_be_bytes());
    }
    let checksum = blake3::hash(&bytes[..PAYLOAD_BYTES]);
    bytes[PAYLOAD_BYTES..].copy_from_slice(checksum.as_bytes());
    bytes
}

/// One slot: `Ok(Some((sequence, state)))` for a whole valid frame,
/// `Ok(None)` for a short or torn one, `Err` for a frame this build must not
/// read as "no dispute" (a newer release's, or a foreign file's).
#[allow(clippy::type_complexity)]
fn decode(frame: &[u8]) -> BrokerResult<Option<(u64, Option<KeptDisputeRecord>)>> {
    // A newer release's header is recognisable before anything else is: a
    // newer frame may be laid out differently, so its checksum says nothing.
    if frame.len() >= 10 && &frame[0..8] == MAGIC {
        let version = u16::from_be_bytes([frame[8], frame[9]]);
        if version > VERSION {
            return Err(KeptDisputeFile::refuse(frame, frame.len() as u64));
        }
    }
    if frame.len() != FRAME_BYTES {
        return Ok(None);
    }
    let (payload, checksum) = frame.split_at(PAYLOAD_BYTES);
    if blake3::hash(payload).as_bytes() != checksum {
        return Ok(None);
    }
    // A valid checksum over a foreign header or an unknown kind was not
    // torn: it is some other file under this name.
    if &payload[0..8] != MAGIC
        || u16::from_be_bytes([payload[8], payload[9]]) != VERSION
        || payload[18] > 1
    {
        return Err(KeptDisputeFile::refuse(frame, frame.len() as u64));
    }
    let field = |at: usize| u64::from_be_bytes(payload[at..at + 8].try_into().expect("width"));
    let record = (payload[18] == 1).then(|| KeptDisputeRecord {
        fence_epoch: field(19),
        from: field(27),
        suffix_epoch: field(35),
    });
    Ok(Some((field(10), record)))
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

    /// #643 review (cubic P1). Replacing an announced record, or clearing
    /// one, must never leave neither on disk: a crash before or inside any
    /// write of the change reads back as the old state or the new one. A
    /// single overwritten frame failed this: a write torn mid-frame read as
    /// "no dispute", erasing one a fence reply had announced.
    #[test]
    fn a_crash_anywhere_in_a_replacement_or_a_clear_reads_back_old_or_new() {
        use vtop_log::sim::{FaultPlan, SimStorage, TraceKind};
        const PATH: &str = "/node/fencing-epochs.kept-dispute";
        const NEWER: KeptDisputeRecord = KeptDisputeRecord {
            fence_epoch: 20,
            from: 7,
            suffix_epoch: 18,
        };
        let open_sim = |env: &Env| KeptDisputeFile::open_in(env, PathBuf::from(PATH));
        // Disk holding RECORD, durably: the announced state.
        let base = {
            let sim = SimStorage::new();
            sim.create_dir_all(Path::new("/node"));
            open_sim(&sim.env(1)).unwrap().record(RECORD).unwrap();
            sim.snapshot()
        };
        type Change = fn(&mut KeptDisputeFile) -> BrokerResult<()>;
        let changes: [(&str, Change, Option<KeptDisputeRecord>); 2] = [
            ("replace", |file| file.record(NEWER), Some(NEWER)),
            ("clear", |file| file.clear(), None),
        ];
        for (name, change, after) in changes {
            let clean = SimStorage::new();
            clean.restore(&base);
            let mut file = open_sim(&clean.env(1)).unwrap();
            // Faults land in the change, not in the open before it.
            let opened = clean.op_count();
            change(&mut file).unwrap();
            let total = clean.op_count();
            let mut plans: Vec<FaultPlan> = (opened..total).map(FaultPlan::CrashBefore).collect();
            for entry in clean
                .trace()
                .into_iter()
                .filter(|entry| entry.kind == TraceKind::HandleWrite && entry.index >= opened)
            {
                for cut in 0..=entry.len as usize {
                    plans.push(FaultPlan::CrashDuringWrite {
                        op: entry.index,
                        byte_cut: cut,
                    });
                }
            }
            for plan in plans {
                let sim = SimStorage::new();
                sim.restore(&base);
                let env = sim.env(1);
                let mut file = open_sim(&env).unwrap();
                sim.set_fault(plan);
                let acked = change(&mut file).is_ok();
                sim.reboot();
                let read = open_sim(&env)
                    .unwrap_or_else(|error| panic!("{name} {plan:?}: reopen refused: {error}"))
                    .current();
                assert!(
                    read == Some(RECORD) || read == after,
                    "{name} {plan:?}: read back {read:?}, neither the announced {RECORD:?} nor \
                     the new {after:?}"
                );
                if acked {
                    assert_eq!(read, after, "{name} {plan:?}: an acked change was lost");
                }
            }
        }
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
        let frame = encode(1, Some(RECORD));
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

        let mut newer = encode(1, Some(RECORD)).to_vec();
        newer[8..10].copy_from_slice(&2_u16.to_be_bytes());
        std::fs::write(&path, &newer).unwrap();
        let refused = open(&dir).err().expect("a newer version");
        assert!(refused.to_string().contains("newer release"), "{refused}");

        newer.extend_from_slice(&[0; 8]);
        std::fs::write(&path, &newer).unwrap();
        let refused = open(&dir).err().expect("a longer, newer frame");
        assert!(refused.to_string().contains("newer release"), "{refused}");

        std::fs::write(&path, [0_u8; FILE_BYTES + 1]).unwrap();
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
