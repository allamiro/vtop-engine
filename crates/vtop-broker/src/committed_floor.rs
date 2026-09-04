//! The truncation guard's durable floor (#240).
//!
//! [`crate::replication::InProcessFollower::truncate_to`] refuses to discard
//! records below the cluster-committed high-water mark — the last-resort
//! protection for acknowledged records behind the promotion restriction. But
//! the mark it compares against lived only in memory and started at zero, so
//! every restart, candidate role flip, and rebuilt replica served a vacuous
//! guard precisely across the window in which a new leader's
//! fence-and-reconcile arrives: `observe_hwm` cannot raise the cell again
//! until the lease watcher has adopted the current epoch. The same zero
//! disabled follower retention after a restart — its reclaim floor is
//! `min(cell, local)`, and zero protects every sealed segment from reclaim
//! until the first HWM frame. This file is the cell's durable shadow: the
//! highest cluster-committed offset this replica has observed, read back at
//! open to seed the cell.
//!
//! # Why too-low is safe and too-high cannot happen
//!
//! A floor persisted too LOW merely reproduces the pre-floor weakness: the
//! guard protects less than it could, which is where every replica stood
//! before this file existed. A floor persisted too HIGH would be worse than
//! no floor at all — it would turn legitimate divergence reconciliation into
//! a refused fence and could exclude a valid replica from every promotion.
//! What prevents too-high is an invariant, not vigilance: only values of the
//! in-memory cell are ever written here, and the cell advances only through
//! `observe_hwm`, which epoch-checks the update and clamps it to this
//! replica's own durability (`update.min(local_committed_offset)`). Every
//! persisted floor is therefore at or below what this replica durably held
//! at persist time, and the offset recovered at the next open is at or above
//! it. A fence the recovered floor refuses is a genuine attempt to discard
//! records this replica durably holds below an acknowledged mark — exactly
//! the refusal the guard exists to make.
//!
//! # Why two alternating slots, not temp-and-rename
//!
//! Rename is out for two reasons: `write_atomic` is `pub(crate)` in both
//! crates that have one (vtop-log and vtop-meta) and unreachable from here,
//! and a private `.tmp` scratch of our own would be invisible to the #310
//! startup sweep, whose classifier deletes only the exact
//! `.{target}.{uuid}.tmp` shapes the log crate itself produces
//! (`catalog.rs::interrupted_atomic_write`) — an interrupted rename would
//! leave droppings nothing ever cleans.
//!
//! A single overwritten frame is out too, and the first review of this file
//! said why: the crash that tears a frame IS a restart, and a restart is
//! exactly what the floor exists to arm the guard for. Detect-and-degrade
//! would surrender the previous floor at the one moment it was needed. So
//! the file holds TWO independently checksummed frames, and a save writes
//! the slot NOT holding the newest durable floor: any single torn write can
//! only damage a frame whose content was already invalid or the lower
//! value. The reader takes the highest valid frame; a frame that fails its
//! checksum is ignored, never misread. An acked floor therefore rides
//! through every crash, and only damage to BOTH slots — or a floor that was
//! never saved — reads as absent, degrading to the pre-floor behaviour.
//!
//! # Placement, cadence, and scope
//!
//! One fixed name, `committed-floor`, flat in the range data directory
//! beside `fencing-epochs` and `epochs`. Discovery promises to ignore names
//! it does not recognize (`catalog.rs::classify_artifact`), and this name
//! shares no suffix with anything the catalog or the transfer sweep acts on.
//! The floor is persisted at durability barriers that already exist — the
//! follower's per-batch commit and `quiesce` — never from `observe_hwm`,
//! which runs in the per-connection dispatch loop that also carries append
//! frames and must stay I/O-free. The durable floor may therefore lag the
//! cell by one append batch plus the quiet tail before shutdown; a lagging
//! floor only weakens the guard toward yesterday's behaviour, never blocks
//! legitimate work.
//!
//! Follower-side only, deliberately. The leader's committed cell is not
//! persisted here: when a LEADER may publish a floor is bound up with the
//! §5.4.2 leadership-transition record design — #240's remaining
//! conversation — and must not be prejudged by follower persistence.
//!
//! # Layout (104 bytes exactly: two 52-byte frames)
//!
//! ```text
//! frame, twice:
//!   magic "VTOPFLR1"        8
//!   version u32             4
//!   floor u64               8
//!   BLAKE3-32 over prior   32
//! ```
//!
//! Big-endian, like the journals beside it. The frames carry no sequence
//! number — the higher floor IS the newer frame, because the value is
//! monotonic.

use crate::{BrokerError, BrokerResult};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use vtop_log::env::{Env, OpenMode, StorageFile};

const MAGIC: &[u8; 8] = b"VTOPFLR1";
const VERSION: u32 = 1;
const CHECKSUM_BYTES: usize = 32;
const PAYLOAD_BYTES: usize = 8 + 4 + 8;
const FRAME_BYTES: usize = PAYLOAD_BYTES + CHECKSUM_BYTES;
const FILE_BYTES: usize = 2 * FRAME_BYTES;

/// Owner of the on-disk committed floor for one range replica.
pub struct CommittedFloorFile {
    env: Env,
    path: PathBuf,
    /// Opened by the first save, not at construction: an absent floor stays
    /// absent on disk until there is a value worth protecting, so a replica
    /// that never observes an HWM leaves no file behind.
    file: Option<Box<dyn StorageFile>>,
    /// The newest floor known durable. Absent reads as zero — deliberately
    /// the same number, because zero protects nothing, exactly as absence
    /// does; giving them different representations would invite code to
    /// treat them differently, and nothing should.
    last_persisted: u64,
    /// The slot the NEXT save writes — always the one NOT holding the newest
    /// durable floor, so a torn write can only damage a frame whose loss
    /// costs nothing. This is what makes an acked floor ride through the
    /// very crash it exists to arm the guard for.
    write_slot: usize,
    /// The first save also syncs the parent directory — once per handle —
    /// because fsync of the file is not enough on creation: a power loss can
    /// lose the directory entry and take the floor with it. When the entry
    /// already existed the extra dir fsync is one-time insurance.
    parent_synced: bool,
    /// Set once a write or sync fails. The file may hold a torn frame from
    /// that attempt — detectable, so harmless to the next open — but this
    /// handle must stop claiming it can persist.
    poisoned: bool,
}

impl CommittedFloorFile {
    pub fn open(path: impl AsRef<Path>) -> Self {
        Self::open_in(&Env::real(), path)
    }

    /// Read the floor back, infallibly. Every unreadable shape — absent,
    /// torn, checksum-mismatched, foreign magic or version, wrong length,
    /// unprobeable — reads as an absent floor WITH a report, never as an
    /// error: this file is protection, and a replica must not be refused its
    /// own records because its protection got damaged. Losing the floor is
    /// the safe direction; refusing to open the range is not.
    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        let (floor, write_slot) = match read_floor(env, &path) {
            Ok(None) => (0, 0),
            Ok(Some((floor, holding_slot))) => (floor, 1 - holding_slot),
            Err(reason) => {
                eprintln!(
                    "committed-floor file {} {reason}; treating the floor as absent — the \
                     truncation guard and follower retention start from zero until the next \
                     observed cluster HWM (#240)",
                    path.display()
                );
                (0, 0)
            }
        };
        Self {
            env: env.clone(),
            path,
            file: None,
            last_persisted: floor,
            write_slot,
            parent_synced: false,
            poisoned: false,
        }
    }

    /// The recovered (or last persisted) floor; zero when absent.
    pub fn floor(&self) -> u64 {
        self.last_persisted
    }

    /// Persist `floor`. It may stay the same — a no-op, nothing advanced —
    /// or advance, never regress: the only legitimate source of values is
    /// the monotonic cell this file shadows, so a smaller value means the
    /// caller is not reading that cell, and quietly accepting it would
    /// un-protect records a previous save promised to protect.
    pub fn save(&mut self, floor: u64) -> BrokerResult<()> {
        if self.poisoned {
            return Err(BrokerError::CommittedFloor(
                "a previous write failed; this handle no longer persists and must be reopened"
                    .to_owned(),
            ));
        }
        if floor < self.last_persisted {
            return Err(BrokerError::CommittedFloor(format!(
                "floor would regress from {} to {floor}; it may stay the same or advance, \
                 never regress",
                self.last_persisted
            )));
        }
        if floor == self.last_persisted {
            return Ok(());
        }
        if let Err(problem) = self.write_frame(&encode(floor), self.write_slot) {
            self.poisoned = true;
            return Err(problem);
        }
        self.last_persisted = floor;
        // The slot just written now holds the newest durable floor; the next
        // save must spare it and take the other.
        self.write_slot = 1 - self.write_slot;
        Ok(())
    }

    fn write_frame(&mut self, bytes: &[u8], slot: usize) -> BrokerResult<()> {
        if self.file.is_none() {
            let file = self
                .env
                .storage
                .open(&self.path, OpenMode::CreateAppend)
                .map_err(|source| Self::io(&self.path, source))?;
            self.file = Some(file);
        }
        let file = self.file.as_mut().expect("opened above");
        file.seek(SeekFrom::Start((slot * FRAME_BYTES) as u64))
            .and_then(|_| file.write_all(bytes))
            // Pin the length to exactly two frames: a fresh file extends
            // with zeros (an invalid frame the reader ignores), and a tail
            // some previous life left beyond the format is trimmed rather
            // than surviving to confuse a later reader.
            .and_then(|()| file.set_len(FILE_BYTES as u64))
            .and_then(|()| file.sync_data())
            .map_err(|source| Self::io(&self.path, source))?;
        if !self.parent_synced {
            Self::sync_parent(&self.env, &self.path)?;
            self.parent_synced = true;
        }
        Ok(())
    }

    fn sync_parent(env: &Env, path: &Path) -> BrokerResult<()> {
        // `Path::parent` of a bare filename is `Some("")`, not `None`, and
        // syncing the empty path fails — the directory it means is the
        // current one. Same quirk, same fix as the fencing-epoch journal.
        let parent = match path.parent() {
            Some(parent) if parent.as_os_str().is_empty() => Path::new("."),
            Some(parent) => parent,
            None => return Ok(()),
        };
        env.storage
            .sync_dir(parent)
            .map_err(|source| Self::io(parent, source))
    }

    fn io(path: &Path, source: std::io::Error) -> BrokerError {
        BrokerError::Io {
            path: path.to_path_buf(),
            source,
        }
    }
}

fn encode(floor: u64) -> [u8; FRAME_BYTES] {
    let mut bytes = [0_u8; FRAME_BYTES];
    bytes[0..8].copy_from_slice(MAGIC);
    bytes[8..12].copy_from_slice(&VERSION.to_be_bytes());
    bytes[12..20].copy_from_slice(&floor.to_be_bytes());
    let checksum = blake3::hash(&bytes[..PAYLOAD_BYTES]);
    bytes[PAYLOAD_BYTES..].copy_from_slice(checksum.as_bytes());
    bytes
}

/// `Ok(None)` is the one clean absence; `Ok(Some((floor, slot)))` is the
/// HIGHEST floor any valid slot holds, and which slot holds it. The slots
/// validate independently, so a frame torn by the crash that interrupted a
/// save costs exactly that frame — the other slot still answers. Only a
/// file in which NEITHER slot validates carries a reason back for the open
/// to report.
fn read_floor(env: &Env, path: &Path) -> Result<Option<(u64, usize)>, String> {
    let exists = env
        .storage
        .exists(path)
        .map_err(|error| format!("could not be probed: {error}"))?;
    if !exists {
        return Ok(None);
    }
    let bytes = env
        .storage
        .read(path)
        .map_err(|error| format!("could not be read: {error}"))?;
    let best = (0..2)
        .filter_map(|slot| Some((read_frame(&bytes, slot)?, slot)))
        .max();
    match best {
        Some((floor, slot)) => Ok(Some((floor, slot))),
        None => Err(format!(
            "holds no readable floor frame in its {} bytes",
            bytes.len()
        )),
    }
}

/// One slot's floor, or `None` for any shape that cannot be trusted: short,
/// checksum-mismatched, foreign magic, future version. A frame's meaning
/// under a version this build does not know is unknowable, and guessing
/// could invent a floor — the one direction the design must never fail in.
fn read_frame(bytes: &[u8], slot: usize) -> Option<u64> {
    let frame = bytes.get(slot * FRAME_BYTES..slot * FRAME_BYTES + FRAME_BYTES)?;
    let (payload, checksum) = frame.split_at(PAYLOAD_BYTES);
    if blake3::hash(payload).as_bytes() != checksum {
        return None;
    }
    if &payload[0..8] != MAGIC {
        return None;
    }
    if u32::from_be_bytes(payload[8..12].try_into().expect("fixed width")) != VERSION {
        return None;
    }
    Some(u64::from_be_bytes(
        payload[12..20].try_into().expect("fixed width"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn open(dir: &TempDir) -> CommittedFloorFile {
        CommittedFloorFile::open(dir.path().join("committed-floor"))
    }

    fn reseal(bytes: &mut [u8]) {
        let checksum = blake3::hash(&bytes[..PAYLOAD_BYTES]);
        bytes[PAYLOAD_BYTES..].copy_from_slice(checksum.as_bytes());
    }

    /// No file is the state of every directory written before this file
    /// existed, and of every freshly repaired replica. It must read as zero
    /// — the pre-floor behaviour — not as a failure.
    #[test]
    fn an_absent_floor_file_is_zero_and_no_error() {
        let dir = TempDir::new().unwrap();
        assert_eq!(
            open(&dir).floor(),
            0,
            "an absent floor is the guard at zero, never an error that blocks the range"
        );
    }

    /// The monotonic contract, matching `meta.applied`: same is an
    /// idempotent no-op, forward advances, backward is refused — a floor
    /// that regressed would un-protect records a previous save promised to
    /// protect.
    #[test]
    fn a_floor_round_trips_and_only_ever_advances() {
        let dir = TempDir::new().unwrap();
        let mut file = open(&dir);
        file.save(5).expect("first save");
        assert_eq!(
            open(&dir).floor(),
            5,
            "the floor must be durable, not just remembered"
        );
        file.save(5)
            .expect("re-saving the same floor is an idempotent no-op");
        file.save(9).expect("advance");
        assert_eq!(open(&dir).floor(), 9);
        assert!(
            file.save(3).is_err(),
            "a floor regressing from 9 to 3 would un-protect records 3..9"
        );
        assert_eq!(
            open(&dir).floor(),
            9,
            "the refused save must not have touched the file"
        );
    }

    /// The point of the two slots, byte by byte: after two saves both hold
    /// valid frames (5 in slot 0, 9 in slot 1), and damaging EITHER — every
    /// byte, one at a time — must surface the other's floor. A save writes
    /// only the slot NOT holding the newest durable value, so this is the
    /// on-disk shape a torn save leaves behind.
    #[test]
    fn damage_to_one_slot_surrenders_only_that_slots_floor() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("committed-floor");
        {
            let mut file = CommittedFloorFile::open(&path);
            file.save(5).unwrap();
            file.save(9).unwrap();
        }
        let pristine = std::fs::read(&path).unwrap();
        assert_eq!(pristine.len(), FILE_BYTES);

        for index in 0..FRAME_BYTES {
            let mut damaged = pristine.clone();
            damaged[index] ^= 0xff;
            std::fs::write(&path, &damaged).unwrap();
            assert_eq!(
                CommittedFloorFile::open(&path).floor(),
                9,
                "a flip at byte {index} damaged only slot 0; slot 1's floor must answer"
            );
        }
        for index in FRAME_BYTES..FILE_BYTES {
            let mut damaged = pristine.clone();
            damaged[index] ^= 0xff;
            std::fs::write(&path, &damaged).unwrap();
            assert_eq!(
                CommittedFloorFile::open(&path).floor(),
                5,
                "a flip at byte {index} tore the newer frame; the crash that tears a frame \
                 is exactly the restart the floor must survive, so the previous floor \
                 must answer"
            );
        }
    }

    /// Only a file with NO valid frame reads as absent — and never as a
    /// value: this file arms a guard that refuses truncations, and a floor
    /// invented from damage (the too-high direction) is the one failure the
    /// design must not allow; absent merely weakens the guard to
    /// yesterday's behaviour.
    #[test]
    fn only_a_file_with_no_valid_frame_reads_as_absent_never_as_garbage() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("committed-floor");
        CommittedFloorFile::open(&path).save(7).unwrap();
        let pristine = std::fs::read(&path).unwrap();
        assert_eq!(
            pristine.len(),
            FILE_BYTES,
            "one save still pins the length: the unwritten slot is zeros, an invalid frame"
        );

        for index in 0..FRAME_BYTES {
            let mut damaged = pristine.clone();
            damaged[index] ^= 0xff;
            std::fs::write(&path, &damaged).unwrap();
            assert_eq!(
                CommittedFloorFile::open(&path).floor(),
                0,
                "a flip at byte {index} killed the only valid frame; absent, never garbage"
            );
        }
        for length in 0..FRAME_BYTES {
            std::fs::write(&path, &pristine[..length]).unwrap();
            assert_eq!(
                CommittedFloorFile::open(&path).floor(),
                0,
                "a {length}-byte prefix holds no whole frame; torn, not a shorter floor"
            );
        }
        for length in FRAME_BYTES..FILE_BYTES {
            std::fs::write(&path, &pristine[..length]).unwrap();
            assert_eq!(
                CommittedFloorFile::open(&path).floor(),
                7,
                "a cut at {length} bytes tore only the second slot; the first frame is \
                 whole and must still answer"
            );
        }
        // Foreign magic and a future version, RESEALED with valid checksums
        // so the refusal is proven structural rather than an accident of the
        // checksum failing first — in the valid slot, leaving no readable
        // frame at all.
        let mut foreign = pristine.clone();
        foreign[0..8].copy_from_slice(b"VTOPELSE");
        reseal(&mut foreign[..FRAME_BYTES]);
        std::fs::write(&path, &foreign).unwrap();
        assert_eq!(
            CommittedFloorFile::open(&path).floor(),
            0,
            "another artifact's bytes must not be read as a floor"
        );
        let mut future = pristine.clone();
        future[8..12].copy_from_slice(&2_u32.to_be_bytes());
        reseal(&mut future[..FRAME_BYTES]);
        std::fs::write(&path, &future).unwrap();
        assert_eq!(
            CommittedFloorFile::open(&path).floor(),
            0,
            "a future version's meaning of these bytes is unknown; guessing could invent a floor"
        );
    }

    /// A tail some previous life left beyond the format must not cost the
    /// floor — the frames validate independently of the file's length — and
    /// the next save trims it so the shape converges back to exactly two
    /// frames.
    #[test]
    fn a_tail_beyond_the_format_is_ignored_and_trimmed_by_the_next_save() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("committed-floor");
        CommittedFloorFile::open(&path).save(7).unwrap();
        let mut longer = std::fs::read(&path).unwrap();
        longer.extend_from_slice(&[0xEE; 8]);
        std::fs::write(&path, &longer).unwrap();

        let mut file = CommittedFloorFile::open(&path);
        assert_eq!(
            file.floor(),
            7,
            "trailing junk is not frame damage; the floor must survive it"
        );
        file.save(11).expect("saving trims the tail");
        assert_eq!(
            std::fs::read(&path).unwrap().len(),
            FILE_BYTES,
            "the stale tail must be gone after a save"
        );
        assert_eq!(CommittedFloorFile::open(&path).floor(), 11);
    }
}
