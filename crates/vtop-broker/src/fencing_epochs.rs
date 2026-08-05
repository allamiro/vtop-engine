//! Which fencing epoch wrote each stretch of the log (#240).
//!
//! Promotion establishes a committed boundary by asking replicas where their
//! disks are and taking the offset a majority can vouch for. That is sound
//! arithmetic over unsound inputs: an offset is a bare integer, and two
//! replicas both reporting 90 need not hold the same record at 90. One may
//! have taken 90 from a leader whose writes never reached a quorum before it
//! was deposed; the other from the leader that replaced it. Comparing them by
//! number alone cannot tell those apart.
//!
//! Kafka shipped exactly this bug and fixed it in KIP-101: give every replica a
//! leader-epoch → start-offset vector, and reconcile against the epoch rather
//! than the high-water mark. This is that vector.
//!
//! # What an entry means
//!
//! `(epoch, start_offset)` records that this replica's first record written
//! under `epoch` sits at `start_offset`. Together the entries partition the
//! log: epoch `e` owns `[start_offset(e), start_offset(next epoch))`.
//!
//! Two replicas agree on a prefix exactly as far as their vectors agree. The
//! first epoch at which they differ bounds divergence: everything below the
//! smaller of the two start offsets for that epoch was written under identical
//! leadership and is therefore identical, and everything above is suspect.
//! That is the fact truncation needs and a bare offset cannot supply.
//!
//! # Why it is durable
//!
//! The vector must survive the crash it exists to recover from. A replica that
//! forgot which epoch wrote its tail would be back to comparing bare offsets
//! at precisely the moment the answer matters, so entries are appended and
//! fsynced before the epoch they describe is used.
//!
//! # Determinism
//!
//! Pure over its inputs: encoding, recovery, and lookup are byte-deterministic
//! and depend on nothing but the file. Only the surrounding I/O is not.

use crate::{BrokerError, BrokerResult};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use vtop_log::env::{Env, OpenMode, StorageFile};

const MAGIC: &[u8; 8] = b"VTOPFNC1";
const VERSION: u16 = 1;
const HEADER_BYTES: u64 = 10;
/// `(epoch, start_offset)`, both big-endian u64.
const ENTRY_BYTES: usize = 16;
/// A range would need this many leadership changes to reach the bound; far
/// past that, something is flapping and the operator needs to know rather
/// than have the file grow without limit.
///
/// Deliberately the SAME bound the wire enforces, taken from there rather than
/// chosen here. A vector exists to be compared against a peer's, so one too
/// large to transmit cannot do the only job it has: the replica would hold a
/// perfectly valid local history, fail to encode it, and report "unknown" to
/// every peer forever. Sharing the constant makes "locally recoverable" imply
/// "transmittable" by construction instead of by coincidence.
const MAX_ENTRIES: usize = vtop_protocol::MAX_REPLICA_EPOCH_STARTS;

/// One epoch's first offset on this replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: u64,
    pub start_offset: u64,
}

/// Durable `(epoch → start offset)` vector for one range replica.
pub struct FencingEpochJournal {
    path: PathBuf,
    file: Box<dyn StorageFile>,
    entries: Vec<EpochStart>,
    /// Set once a write or fsync fails. The file may hold a partial record
    /// from that attempt, and appending after it would land the next entry at
    /// a misaligned offset — so no further append is allowed until the journal
    /// is reopened and revalidated.
    poisoned: bool,
}

impl FencingEpochJournal {
    pub fn open(path: impl AsRef<Path>) -> BrokerResult<Self> {
        Self::open_in(&Env::real(), path)
    }

    pub fn open_in(env: &Env, path: impl AsRef<Path>) -> BrokerResult<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = env
            .storage
            .open(&path, OpenMode::CreateAppend)
            .map_err(|source| Self::io(&path, source))?;
        let len = file.len().map_err(|source| Self::io(&path, source))?;
        if len == 0 {
            file.write_all(MAGIC)
                .and_then(|()| file.write_all(&VERSION.to_be_bytes()))
                .and_then(|()| file.sync_data())
                .map_err(|source| Self::io(&path, source))?;
            // fsync of the file is not enough: without syncing the directory,
            // a power loss can lose the directory entry itself and take the
            // whole history with it, putting this replica back to "unknown"
            // after a crash — the one moment the vector exists for.
            Self::sync_parent(env, &path)?;
            return Ok(Self {
                path,
                file,
                entries: Vec::new(),
                poisoned: false,
            });
        }
        if len < HEADER_BYTES {
            return Err(Self::corrupt(format!(
                "journal is {len} bytes, shorter than its {HEADER_BYTES}-byte header"
            )));
        }
        // Bounded BEFORE allocating. The length comes off a file this process
        // did not necessarily write, and reserving on it lets a malformed or
        // truncated-then-grown journal exhaust memory during recovery — taking
        // the broker down at exactly the moment it is trying to come back.
        let max_bytes = HEADER_BYTES + (MAX_ENTRIES as u64) * (ENTRY_BYTES as u64);
        if len > max_bytes {
            return Err(Self::corrupt(format!(
                "journal is {len} bytes; the bound for {MAX_ENTRIES} epochs is {max_bytes}"
            )));
        }
        let mut bytes = vec![0_u8; len as usize];
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.read_exact(&mut bytes))
            .map_err(|source| Self::io(&path, source))?;
        if &bytes[0..8] != MAGIC {
            return Err(Self::corrupt("bad magic".to_owned()));
        }
        let version = u16::from_be_bytes([bytes[8], bytes[9]]);
        if version != VERSION {
            return Err(Self::corrupt(format!("unsupported version {version}")));
        }
        let body = &bytes[HEADER_BYTES as usize..];
        // A torn tail is a partial entry, not corruption: the process died
        // mid-append. Everything before it was fsynced and is usable, so
        // recover the whole entries and drop the fragment — the same rule the
        // segment recovery path applies.
        let whole = body.len() / ENTRY_BYTES;
        let mut entries = Vec::with_capacity(whole);
        for index in 0..whole {
            let at = index * ENTRY_BYTES;
            let epoch = u64::from_be_bytes(body[at..at + 8].try_into().expect("fixed width"));
            let start_offset =
                u64::from_be_bytes(body[at + 8..at + 16].try_into().expect("fixed width"));
            // Both fields must be non-decreasing. A file that says otherwise
            // was not written by this code, and reading it would produce a
            // truncation target that silently discards acknowledged records.
            if let Some(previous) = entries.last().copied() {
                let previous: EpochStart = previous;
                if epoch <= previous.epoch || start_offset < previous.start_offset {
                    return Err(Self::corrupt(format!(
                        "entry {index} (epoch {epoch}, start {start_offset}) does not advance on \
                         (epoch {}, start {})",
                        previous.epoch, previous.start_offset
                    )));
                }
            }
            entries.push(EpochStart {
                epoch,
                start_offset,
            });
        }
        // A torn tail must be removed from the FILE, not just from memory.
        // Leaving the fragment means the next append lands after it, so every
        // entry from then on is misaligned by however many bytes it held — and
        // the restart after that reads (epoch, start) pairs straddling entry
        // boundaries. That either refuses to open, permanently losing a
        // history this code had already recovered once, or silently yields
        // wrong offsets. Same rule the segment recovery path applies.
        let valid_bytes = HEADER_BYTES + (whole as u64) * (ENTRY_BYTES as u64);
        if valid_bytes != len {
            file.set_len(valid_bytes)
                .and_then(|()| file.sync_data())
                .map_err(|source| Self::io(&path, source))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|source| Self::io(&path, source))?;
        Ok(Self {
            path,
            file,
            entries,
            poisoned: false,
        })
    }

    /// Record that `epoch` begins at `start_offset` on this replica.
    ///
    /// Idempotent for an epoch already recorded, so a replica that re-observes
    /// its own current epoch — which a polling watcher does constantly — does
    /// not append a duplicate. Recording the SAME epoch at a different offset
    /// is refused rather than merged: it would mean this replica wrote under
    /// one epoch from two different positions, which cannot happen, and
    /// accepting it would corrupt every later comparison.
    pub fn record(&mut self, epoch: u64, start_offset: u64) -> BrokerResult<()> {
        if self.poisoned {
            return Err(Self::corrupt(
                "a previous append failed; this journal may hold a partial record and must be \
                 reopened before it is written again"
                    .to_owned(),
            ));
        }
        if let Some(last) = self.entries.last().copied() {
            if epoch == last.epoch {
                if start_offset != last.start_offset {
                    return Err(Self::corrupt(format!(
                        "epoch {epoch} already starts at {} on this replica; refusing to \
                         re-record it at {start_offset}",
                        last.start_offset
                    )));
                }
                return Ok(());
            }
            if epoch < last.epoch {
                // Stale observation. Epochs are minted strictly increasing, so
                // this is a late-arriving read, not a rewind — ignore it
                // rather than corrupt the vector.
                return Ok(());
            }
            if start_offset < last.start_offset {
                return Err(Self::corrupt(format!(
                    "epoch {epoch} would start at {start_offset}, below epoch {}'s start {}",
                    last.epoch, last.start_offset
                )));
            }
        }
        if self.entries.len() >= MAX_ENTRIES {
            return Err(Self::corrupt(format!(
                "journal holds {MAX_ENTRIES} epochs; a range changing leadership this often \
                 needs an operator, not a larger file"
            )));
        }
        let mut record = [0_u8; ENTRY_BYTES];
        record[0..8].copy_from_slice(&epoch.to_be_bytes());
        record[8..16].copy_from_slice(&start_offset.to_be_bytes());
        // fsync before returning: the caller is about to serve under this
        // epoch, and a vector that loses its newest entry in a crash is back
        // to comparing bare offsets exactly when the answer matters.
        if let Err(source) = self
            .file
            .write_all(&record)
            .and_then(|()| self.file.sync_data())
        {
            // The file may now hold a partial record. Appending after it would
            // misalign every later entry, so refuse further writes rather than
            // retry onto uncertain bytes.
            self.poisoned = true;
            return Err(Self::io(&self.path, source));
        }
        self.entries.push(EpochStart {
            epoch,
            start_offset,
        });
        Ok(())
    }

    pub fn entries(&self) -> &[EpochStart] {
        &self.entries
    }

    pub fn latest(&self) -> Option<EpochStart> {
        self.entries.last().copied()
    }

    /// Where `epoch` ends on this replica: the start of the next recorded
    /// epoch, or `None` if `epoch` is the newest (its end is the log's tail).
    pub fn end_of_epoch(&self, epoch: u64) -> Option<u64> {
        let index = self.entries.iter().position(|entry| entry.epoch == epoch)?;
        self.entries.get(index + 1).map(|next| next.start_offset)
    }

    /// The highest offset below which this replica and `other` provably hold
    /// identical records.
    ///
    /// Walks both vectors while they agree. The first differing epoch bounds
    /// divergence, and the answer is the smaller of the two start offsets for
    /// it: below that, both replicas wrote under the same leadership in the
    /// same order, so their records are the same records. This is the value a
    /// follower must truncate to before it can safely follow `other`, and the
    /// reason it cannot be computed from a high-water mark.
    ///
    /// `None` means the vectors share no common prefix at all — the replicas
    /// have no provably identical history, which is a lineage fault rather
    /// than a lag problem and must not be resolved by truncating.
    pub fn divergence_point(&self, other: &[EpochStart]) -> Option<u64> {
        let mut agreed: Option<u64> = None;
        for (mine, theirs) in self.entries.iter().zip(other.iter()) {
            if mine.epoch != theirs.epoch {
                // Different leadership at the same position in history.
                return Some(mine.start_offset.min(theirs.start_offset));
            }
            if mine.start_offset != theirs.start_offset {
                // Same epoch, different starting point: they disagree about
                // what came before it.
                return Some(mine.start_offset.min(theirs.start_offset));
            }
            agreed = Some(mine.start_offset);
        }
        // One vector is a prefix of the other; everything the shorter one
        // covers is agreed. The caller bounds the answer by its own tail.
        //
        // Two EMPTY vectors are not agreement at offset 0 — they are two
        // replicas that cannot vouch for their own history at all. Reporting a
        // proven common prefix there would let a caller reconcile (and
        // truncate) on the strength of mutual ignorance, which is the opposite
        // of what empty means everywhere else in this API. `None` makes it
        // fail closed.
        agreed
    }

    fn sync_parent(env: &Env, path: &Path) -> BrokerResult<()> {
        // `Path::parent` of a bare filename is `Some("")`, not `None`, and
        // syncing the empty path fails — so a journal opened at a relative name
        // would fail on create for a reason that has nothing to do with the
        // journal. The directory it means is the current one.
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

    fn corrupt(message: String) -> BrokerError {
        BrokerError::EpochJournalCorrupt(format!("fencing-epoch journal: {message}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn journal(dir: &TempDir) -> FencingEpochJournal {
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).expect("open")
    }

    #[test]
    fn records_survive_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(1, 0).unwrap();
            j.record(2, 100).unwrap();
        }
        let reopened = journal(&dir);
        assert_eq!(
            reopened.entries(),
            &[
                EpochStart {
                    epoch: 1,
                    start_offset: 0
                },
                EpochStart {
                    epoch: 2,
                    start_offset: 100
                },
            ]
        );
    }

    /// A watcher polls constantly and re-observes its own epoch every time.
    /// That must not grow the file.
    #[test]
    fn re_recording_the_same_epoch_is_a_no_op() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(3, 50).unwrap();
        j.record(3, 50).unwrap();
        j.record(3, 50).unwrap();
        assert_eq!(j.entries().len(), 1);
    }

    /// The same epoch cannot have written from two positions. Merging that
    /// away would corrupt every later comparison, so it is refused.
    #[test]
    fn the_same_epoch_at_a_different_offset_is_refused() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(3, 50).unwrap();
        let error = j.record(3, 70).expect_err("must refuse");
        assert!(
            error.to_string().contains("already starts at 50"),
            "{error}"
        );
    }

    /// Observations can arrive late; epochs are minted strictly increasing, so
    /// an older one is stale news rather than a rewind.
    #[test]
    fn a_stale_epoch_is_ignored_not_recorded() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(5, 100).unwrap();
        j.record(2, 40).unwrap();
        assert_eq!(
            j.latest(),
            Some(EpochStart {
                epoch: 5,
                start_offset: 100
            })
        );
        assert_eq!(j.entries().len(), 1);
    }

    /// The property the whole file exists for: where two replicas stop being
    /// provably identical.
    #[test]
    fn divergence_is_bounded_by_the_first_differing_epoch() {
        let dir = TempDir::new().unwrap();
        let mut mine = journal(&dir);
        mine.record(1, 0).unwrap();
        mine.record(2, 100).unwrap();
        mine.record(4, 260).unwrap();

        // Agreed through epoch 2, then this replica followed epoch 4 from 260
        // while the other followed epoch 3 from 200.
        let theirs = [
            EpochStart {
                epoch: 1,
                start_offset: 0,
            },
            EpochStart {
                epoch: 2,
                start_offset: 100,
            },
            EpochStart {
                epoch: 3,
                start_offset: 200,
            },
        ];
        assert_eq!(
            mine.divergence_point(&theirs),
            Some(200),
            "records below the smaller of the two differing starts were written under \
             identical leadership"
        );
    }

    /// A prefix is not a divergence — the shorter replica is simply behind,
    /// and everything it holds is agreed.
    #[test]
    fn a_prefix_agrees_through_its_own_tail() {
        let dir = TempDir::new().unwrap();
        let mut mine = journal(&dir);
        mine.record(1, 0).unwrap();
        mine.record(2, 100).unwrap();
        mine.record(3, 200).unwrap();

        let theirs = [
            EpochStart {
                epoch: 1,
                start_offset: 0,
            },
            EpochStart {
                epoch: 2,
                start_offset: 100,
            },
        ];
        assert_eq!(mine.divergence_point(&theirs), Some(100));
    }

    /// Same epoch number, different start: they disagree about what preceded
    /// it, so the epoch's own records are not comparable either.
    #[test]
    fn the_same_epoch_starting_elsewhere_is_a_divergence() {
        let dir = TempDir::new().unwrap();
        let mut mine = journal(&dir);
        mine.record(1, 0).unwrap();
        mine.record(2, 100).unwrap();

        let theirs = [
            EpochStart {
                epoch: 1,
                start_offset: 0,
            },
            EpochStart {
                epoch: 2,
                start_offset: 80,
            },
        ];
        assert_eq!(mine.divergence_point(&theirs), Some(80));
    }

    /// A crash mid-append leaves a partial entry. Everything fsynced before it
    /// is still good, so recovery keeps the whole entries and drops the
    /// fragment rather than refusing to open.
    #[test]
    fn a_torn_tail_recovers_the_whole_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        {
            let mut j = FencingEpochJournal::open(&path).unwrap();
            j.record(1, 0).unwrap();
            j.record(2, 100).unwrap();
        }
        // Append a half-written entry.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 3]);
        std::fs::write(&path, bytes).unwrap();

        let recovered = FencingEpochJournal::open(&path).expect("torn tail is recoverable");
        assert_eq!(recovered.entries().len(), 2);
        assert_eq!(recovered.latest().unwrap().epoch, 2);
    }

    /// A file whose entries go backwards was not written by this code, and
    /// trusting it would yield a truncation target that discards acknowledged
    /// records. Refuse rather than guess.
    #[test]
    fn a_non_advancing_file_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        {
            let mut j = FencingEpochJournal::open(&path).unwrap();
            j.record(5, 100).unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&3_u64.to_be_bytes());
        bytes.extend_from_slice(&200_u64.to_be_bytes());
        std::fs::write(&path, bytes).unwrap();

        let error = match FencingEpochJournal::open(&path) {
            Ok(_) => panic!("a non-advancing file must be refused"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not advance"), "{error}");
    }

    /// Two replicas that cannot vouch for their own history have not agreed
    /// on anything. Reporting a proven prefix at 0 would let a caller truncate
    /// on the strength of mutual ignorance.
    #[test]
    fn two_unknown_histories_do_not_agree() {
        let dir = TempDir::new().unwrap();
        let mine = journal(&dir);
        assert_eq!(mine.divergence_point(&[]), None);
    }

    /// The fragment must leave the FILE, not just memory. If it survives, the
    /// next append lands after it and every entry from then on is misaligned —
    /// so the restart AFTER the recovery is the one that breaks.
    #[test]
    fn a_recovered_torn_tail_does_not_misalign_later_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        {
            let mut j = FencingEpochJournal::open(&path).unwrap();
            j.record(1, 0).unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 2]); // half an entry
        std::fs::write(&path, bytes).unwrap();

        {
            let mut recovered = FencingEpochJournal::open(&path).expect("recovers");
            recovered.record(2, 100).expect("appends after recovery");
        }
        // The reopen after that is where a surviving fragment would show up.
        let again = FencingEpochJournal::open(&path).expect("still readable");
        assert_eq!(
            again.entries(),
            &[
                EpochStart {
                    epoch: 1,
                    start_offset: 0
                },
                EpochStart {
                    epoch: 2,
                    start_offset: 100
                },
            ]
        );
    }

    /// A file claiming more epochs than the bound must be refused before the
    /// read allocates on its length.
    #[test]
    fn an_oversized_journal_is_refused_before_allocating() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        {
            let mut j = FencingEpochJournal::open(&path).unwrap();
            j.record(1, 0).unwrap();
        }
        let bound = HEADER_BYTES + (MAX_ENTRIES as u64) * (ENTRY_BYTES as u64);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.resize((bound + ENTRY_BYTES as u64) as usize, 0);
        std::fs::write(&path, bytes).unwrap();

        let error = match FencingEpochJournal::open(&path) {
            Ok(_) => panic!("an oversized journal must be refused"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("bound for"), "{error}");
    }

    #[test]
    fn end_of_epoch_is_the_next_epochs_start() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(1, 0).unwrap();
        j.record(2, 100).unwrap();
        assert_eq!(j.end_of_epoch(1), Some(100));
        assert_eq!(j.end_of_epoch(2), None, "the newest epoch ends at the tail");
        assert_eq!(j.end_of_epoch(9), None, "unknown epoch");
    }

    /// `Path::parent` of a bare filename is `Some("")`, which is not a
    /// directory anything can sync. Opening by relative name must still work.
    #[test]
    fn a_journal_opens_at_a_bare_relative_path() {
        let dir = TempDir::new().unwrap();
        let previous = std::env::current_dir().unwrap();
        // Serialised against other tests only by being the single one that
        // changes the process directory; it restores it before returning.
        std::env::set_current_dir(dir.path()).unwrap();
        let opened = FencingEpochJournal::open("fencing-epochs");
        std::env::set_current_dir(previous).unwrap();
        assert!(
            opened.is_ok(),
            "a bare relative path must open: {:?}",
            opened.err()
        );
    }

    /// The local bound and the wire bound must be the same number, so a journal
    /// this code accepts on open can always be transmitted. If they drift, a
    /// replica can hold a valid vector it is structurally unable to share.
    #[test]
    fn the_local_bound_matches_the_wire_bound() {
        assert_eq!(MAX_ENTRIES, vtop_protocol::MAX_REPLICA_EPOCH_STARTS);
    }
}
