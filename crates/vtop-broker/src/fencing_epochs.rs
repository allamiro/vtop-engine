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
//! # Whose epoch a follower's records carry (#640)
//!
//! An entry names the epoch that WROTE the records from its start, which on
//! a leader is the epoch it adopted at its own tail: a leader writes under an
//! epoch only from where its log stood when it took it. A follower that
//! adopts the same epoch at ITS tail is right only when that tail is the
//! leader's. When it is behind, the records the new leader then catches it up
//! on were written under older epochs, and an adoption entry at the
//! follower's tail filed them under the new one. Two replicas holding the
//! same bytes then disagreed about who wrote them, below both their tails,
//! and since #577 such an answer is Unknown and never counted: after one more
//! failover no candidate could be established.
//!
//! So an entry AT a follower's tail is provisional. It owns no record, and
//! the fence that installs a leader also hands the follower that leader's
//! history: the candidate's vector, which IS the leader's, since a leader
//! writes under its epoch only from the tail it sent. That holds only when
//! the vector ends with the new epoch at the candidate's tail, so a vector
//! that does not name it is not taken; the lease agent's probe adds the
//! entry for a candidate that adopts its grant only after the probe. When
//! the follower's own history agrees with it below the follower's tail, the
//! follower keeps it as
//! its LEADER LINEAGE ([`FencingEpochJournal::take_leader_lineage`]) and
//! files every record that leader sends it under the leader's own entries
//! ([`FencingEpochJournal::follow_leader_lineage`]), dropping its provisional
//! ones. After a catch-up its vector equals the leader's for the caught-up
//! range. A follower with no lineage for the epoch it appends under (one the
//! fence did not reach, or one whose history disagreed) keeps the tail
//! adoption: the pre-#640 behaviour, which costs a later election an Unknown
//! answer, never a wrong one.
//!
//! The lineage is persisted beside the journal in `<journal>.leader`, so a
//! restart between the fence and the catch-up does not lose it. That file is
//! a separate sidecar and the journal's own format is unchanged (see
//! "Compatibility" below).
//!
//! # Compatibility (#640)
//!
//! * **The journal format is unchanged** (`VTOPFNC1`, version 1). Re-deriving
//!   a follower's entries truncates and appends exactly as truncation and
//!   adoption always have, and keeps the invariants every reader checks:
//!   epochs strictly increase, starts never decrease, nothing starts past the
//!   log's tail. An older binary reads a journal written here as it reads its
//!   own, and simply goes back to adopting at its tail.
//! * **The sidecar is new, and only this code reads it.** An older binary does
//!   not know the name, and range discovery ignores names it does not
//!   recognise, so it is never misread. This code reads it with its own magic,
//!   version and BLAKE3 checksum, after bounding its length; a missing, torn,
//!   oversized, foreign or newer-version sidecar reads as "no lineage", which
//!   is the pre-#640 behaviour, not a
//!   refusal to open. A lineage is applied only to the epoch it names, and
//!   only after the follower's own history was checked to agree with it.
//! * **Journals mislabelled before this fix are not rewritten.** Nothing on a
//!   replica can tell a record it was caught up on under epoch `e` from one
//!   `e`'s leader wrote: both are a stretch filed under `e`. So an existing
//!   mislabel stays, and a candidate comparing it with a correctly labelled
//!   history finds a divergence below both tails and judges that answer
//!   Unknown, which #577/#607 never counts. That is the behaviour such a
//!   replica already has; it is repaired the way any unprovable replica is
//!   (`vtopctl node repair` from a replica whose history is right, #315), or
//!   by a truncation that cuts below the mislabel. Replicas mislabelled the
//!   SAME way (each caught up from the same leader at the same offset) still
//!   agree with each other.
//!
//! # Determinism
//!
//! Pure over its inputs: encoding, recovery, and lookup are byte-deterministic
//! and depend on nothing but the file. Only the surrounding I/O is not.

use crate::{BrokerError, BrokerResult};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use vtop_log::env::{Env, OpenMode, StorageFile};

const MAGIC: &[u8; 8] = b"VTOPFNC1";
const VERSION: u16 = 1;
const HEADER_BYTES: u64 = 10;
/// The leader-lineage sidecar (#640): `magic | version u16 | epoch u64 |
/// anchor u64 | count u32 | count x (epoch u64, start u64) | BLAKE3-32 over
/// everything before it`, big-endian like the journal.
const LINEAGE_MAGIC: &[u8; 8] = b"VTOPFLN1";
const LINEAGE_VERSION: u16 = 1;
const LINEAGE_HEADER_BYTES: usize = 8 + 2 + 8 + 8 + 4;
const LINEAGE_CHECKSUM_BYTES: usize = 32;
/// The longest sidecar a lineage of `MAX_ENTRIES` entries encodes to. A
/// longer file is refused before it is read (#648 review).
const MAX_LINEAGE_BYTES: u64 =
    (LINEAGE_HEADER_BYTES + MAX_ENTRIES * ENTRY_BYTES + LINEAGE_CHECKSUM_BYTES) as u64;
/// Appended to the journal's file name to name its lineage sidecar.
const LINEAGE_SUFFIX: &str = ".leader";
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

/// What comparing two replicas' lineages established (#240).
///
/// Three outcomes, not two, because "we could not tell" and "we agree" license
/// completely different actions and must not share a representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lineage {
    /// Every position both replicas recorded matches. Nothing to discard.
    Agreed,
    /// They disagree; everything at or above this offset is suspect and must
    /// go before the replicas can share a log again.
    DivergesAt(u64),
    /// No provable common history — at least one side cannot vouch for its own
    /// lineage. Not a licence to truncate anything.
    Unknown,
}

/// One epoch's first offset on this replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: u64,
    pub start_offset: u64,
}

/// The history of the leader this replica follows under `epoch` (#640): see
/// the module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
struct LeaderLineage {
    epoch: u64,
    /// This replica's tail when it took the lineage. Below it, its own
    /// history agreed with `entries`; from it, every record it holds came
    /// from `epoch`'s leader. Only ever lowered (a truncation, or records a
    /// crash lost), never raised.
    anchor: u64,
    /// The leader's vector as it sent it with the fence.
    entries: Vec<EpochStart>,
}

/// What [`FencingEpochJournal::follow_leader_lineage`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineageFollow {
    /// No lineage for that epoch: attribute the way adoption always has.
    NotHeld,
    /// The records through the tail are filed under the leader's entries.
    /// `complete` once the tail has reached the leader's last entry, after
    /// which further appends under the epoch change nothing.
    Following { complete: bool },
}

/// Durable `(epoch → start offset)` vector for one range replica.
pub struct FencingEpochJournal {
    env: Env,
    path: PathBuf,
    file: Box<dyn StorageFile>,
    entries: Vec<EpochStart>,
    /// See [`LeaderLineage`]; `None` when there is none, or its sidecar did
    /// not read back.
    lineage: Option<LeaderLineage>,
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
            let lineage = read_lineage(env, &lineage_path(&path));
            return Ok(Self {
                env: env.clone(),
                path,
                file,
                entries: Vec::new(),
                lineage,
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
        let lineage = read_lineage(env, &lineage_path(&path));
        Ok(Self {
            env: env.clone(),
            path,
            file,
            entries,
            lineage,
            poisoned: false,
        })
    }

    /// Record an epoch adoption, honoring what the journal already knows.
    ///
    /// The append path's [`Self::record`] is the right tool when this replica
    /// itself is living through the epoch change. Adoption — a lease watcher
    /// or fence observing the current epoch — has two extra cases that
    /// `record` must refuse but adoption must survive (#315):
    ///
    /// * **The epoch is already in the vector.** An installed history from a
    ///   repair ends with the source's current epoch at the offset it truly
    ///   began; the replica then re-observes that same epoch at its own tail.
    ///   Re-recording at the tail would be refused as a conflicting start and
    ///   latch the journal broken — destroying the transferred lineage at the
    ///   first metadata poll. The recorded start is the truth; the adoption
    ///   is a no-op.
    /// * **The vector is empty over a log that has records.** A first entry
    ///   at a non-zero tail is not lineage, it is ignorance dressed as
    ///   history: a lone `(epoch, tail)` entry compares as divergence at
    ///   offset zero against any real vector, and the reconciliation it
    ///   feeds would truncate everything below it. The journal stays empty —
    ///   honestly "unknown" — until a complete history is installed or the
    ///   log is truncated to its base. This mirrors the seeding rule in
    ///   `set_fencing_epoch_journal`, now held on every adoption instead of
    ///   only the first attach.
    pub fn record_adoption(&mut self, epoch: u64, start_offset: u64) -> BrokerResult<()> {
        if self.entries.iter().any(|entry| entry.epoch == epoch) {
            return Ok(());
        }
        if self.entries.is_empty() && start_offset > 0 {
            return Ok(());
        }
        self.record(epoch, start_offset)
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

    /// Drop every epoch that begins at or above `offset`, because the records
    /// it described have been truncated away.
    ///
    /// The vector's entries are claims about where records sit. Truncating the
    /// log without truncating this leaves entries pointing past the tail —
    /// claims about records that no longer exist. A peer comparing against them
    /// would compute a divergence point beyond the end of our log, and the next
    /// reconciliation would be arithmetic over a fiction.
    ///
    /// An epoch that STRADDLES the truncation point is kept, not dropped: it
    /// still owns the surviving records below `offset`, and its start offset is
    /// still where it began. Only epochs that begin at or above the cut lose
    /// their entire extent.
    ///
    /// Rewriting is a truncate-in-place: the surviving entries are a prefix of
    /// the file (entries are appended in increasing start offset), so shortening
    /// the file to the survivors' length is the whole edit. Unlike the segment,
    /// there is no separate boundary sidecar to order against — the file length
    /// IS the record count, so one `set_len` plus one fsync is atomic enough. A
    /// crash before the fsync leaves the longer file, which reads back as the
    /// pre-truncation vector: stale, but never a partial entry, because the
    /// length only ever moves between entry boundaries.
    pub fn truncate_to(&mut self, offset: u64) -> BrokerResult<usize> {
        if self.poisoned {
            return Err(Self::corrupt(
                "a previous append failed; this journal may hold a partial record and must be \
                 reopened before it is rewritten"
                    .to_owned(),
            ));
        }
        // A leader lineage anchored above the cut now starts at it (#640):
        // the records the replica takes from here on come from the leader of
        // the epoch it holds, and below the cut it agreed with that leader
        // already. Lowering is always safe; only raising could re-attribute
        // records some other leadership wrote.
        if let Some(lineage) = self.lineage.as_mut() {
            lineage.anchor = lineage.anchor.min(offset);
        }
        let keep = self
            .entries
            .iter()
            .take_while(|entry| entry.start_offset < offset)
            .count();
        self.truncate_entries(keep)
    }

    /// Shorten the vector to its first `keep` entries, durably. The shared
    /// body of [`Self::truncate_to`] and the lineage re-derivation.
    fn truncate_entries(&mut self, keep: usize) -> BrokerResult<usize> {
        let dropped = self.entries.len().saturating_sub(keep);
        if dropped == 0 {
            return Ok(0);
        }
        let new_len = HEADER_BYTES + (keep as u64) * ENTRY_BYTES as u64;
        if let Err(source) = self
            .file
            .set_len(new_len)
            .and_then(|()| self.file.sync_data())
        {
            self.poisoned = true;
            return Err(Self::io(&self.path, source));
        }
        if let Err(source) = self.file.seek(SeekFrom::Start(new_len)) {
            self.poisoned = true;
            return Err(Self::io(&self.path, source));
        }
        self.entries.truncate(keep);
        Ok(dropped)
    }

    /// Re-anchor the currently held epoch at `offset` after a truncation.
    ///
    /// Truncation can remove the entry that said where the held epoch began —
    /// it started above the cut, so its records are gone. The replica still
    /// holds that epoch and will write under it next, but the vector no longer
    /// says so, and the records it goes on to write would be attributed to
    /// whichever older epoch now sits at the end of the vector. That is a
    /// misattribution, and it is precisely what this file exists to prevent, so
    /// truncation has to close the hole it opened.
    ///
    /// A no-op when the vector already ends at or above `epoch`, and for epoch
    /// 0 — the "no grant yet" sentinel, which never wrote a record.
    pub fn record_held_epoch_at(&mut self, epoch: u64, offset: u64) -> BrokerResult<()> {
        if epoch == 0 {
            return Ok(());
        }
        if self.entries.last().is_some_and(|last| last.epoch >= epoch) {
            return Ok(());
        }
        self.record(epoch, offset)
    }

    /// Take `leader` as the history of the leader this replica follows under
    /// `epoch`, from `anchor` (this replica's tail) on (#640; see the module
    /// docs). Returns whether it was taken.
    ///
    /// `leader` is the vector the fence for `epoch` carried. It is taken only
    /// when it agrees with this replica's own history about every record the
    /// replica retains below `anchor`, compared on the retained window
    /// `[base, anchor)` exactly as reconciliation compares it
    /// ([`clamp_lineage`]). A replica with records the leader attributes
    /// otherwise, or a history either side cannot vouch for, keeps
    /// attributing the old way: the lineage tells it who wrote the records
    /// the leader SENDS it, and is no evidence about the ones it already has.
    ///
    /// Persisted before anything is re-derived from it, so a crash after this
    /// call finds the lineage again at the next open. A replica that cannot
    /// persist it does not take it. The caller re-derives with
    /// [`Self::follow_leader_lineage`].
    pub fn take_leader_lineage(
        &mut self,
        epoch: u64,
        leader: &[EpochStart],
        anchor: u64,
        base: u64,
    ) -> BrokerResult<bool> {
        if self.poisoned {
            return Err(Self::corrupt(
                "a previous append failed; this journal may hold a partial record and must be \
                 reopened before it is written again"
                    .to_owned(),
            ));
        }
        // The leader's vector must be one this file could hold: the same
        // bound and the same ordering `open` enforces. Anything else is not
        // a history and is ignored, never half-applied.
        let well_formed = !leader.is_empty()
            && leader.len() <= MAX_ENTRIES
            && leader.windows(2).all(|pair| {
                pair[1].epoch > pair[0].epoch && pair[1].start_offset >= pair[0].start_offset
            });
        // The vector must name `epoch` itself, as its last epoch: the entry
        // the caller recorded at its own tail, where its records under
        // `epoch` will begin (#648 review). Without it the history says where
        // every older epoch began and nothing about the new one, and taking
        // it would drop this replica's adoption of `epoch` and put nothing in
        // its place, filing the new leader's records under an older epoch. A
        // caller that fences before adopting (a lease-driven broker adopts
        // its grant after the probe) sends exactly that.
        let names_the_epoch = canonicalize_lineage(leader)
            .last()
            .is_some_and(|last| last.epoch == epoch);
        let vouches_for_anchor = leader.iter().any(|entry| entry.start_offset <= anchor);
        let agrees_below = anchor <= base || {
            let mine = clamp_lineage(&self.entries, base, anchor);
            let theirs = clamp_lineage(leader, base, anchor);
            !mine.is_empty() && mine == theirs
        };
        if !(well_formed && names_the_epoch && vouches_for_anchor && agrees_below) {
            self.drop_leader_lineage(epoch)?;
            return Ok(false);
        }
        let lineage = LeaderLineage {
            epoch,
            anchor,
            entries: leader.to_vec(),
        };
        if self.lineage.as_ref() != Some(&lineage) {
            self.write_lineage(Some(&lineage))?;
            self.lineage = Some(lineage);
        }
        Ok(true)
    }

    /// Forget a lineage held for `epoch`, durably (#640). A no-op for any
    /// other epoch's: a lineage applies only to the epoch it names, so an
    /// older one left on disk is never used again.
    ///
    /// Durable on purpose. A fence that keeps a disputed suffix at `epoch`
    /// must not leave behind a lineage for `epoch` that a restart would
    /// reload: the kept records are this replica's own, and filing them
    /// under the leader's entries would make them look like the leader's.
    pub fn drop_leader_lineage(&mut self, epoch: u64) -> BrokerResult<()> {
        if self
            .lineage
            .as_ref()
            .is_some_and(|held| held.epoch == epoch)
        {
            self.write_lineage(None)?;
            self.lineage = None;
        }
        Ok(())
    }

    /// The epoch whose leader's lineage this replica holds, if any.
    pub fn leader_lineage_epoch(&self) -> Option<u64> {
        self.lineage.as_ref().map(|lineage| lineage.epoch)
    }

    /// Replay a persisted lineage over the log as it opened (#640, #648
    /// review), before the journal is answered from or written to. Returns
    /// whether the lineage still has entries the log has not reached.
    ///
    /// The journal and the log are made durable separately: an append is
    /// fsynced, then the journal is re-derived from the lineage. A crash
    /// between the two leaves records filed under whatever the journal said
    /// before, and nothing later is sure to repair it. The restart's adoption
    /// and the next append re-derive only under the lineage's own epoch, and
    /// metadata may have moved past it while the replica was down. So the
    /// open replays the lineage itself, up to the tail and never past an
    /// entry of a later epoch (see [`Self::follow_leader_lineage`]).
    ///
    /// The anchor first comes down to the tail: a crash can lose records the
    /// replica held when it took the lineage, and the records it takes from
    /// the new tail on still come from the same leader. That is in memory
    /// only; every open recomputes it.
    pub fn replay_leader_lineage_at_open(&mut self, tail: u64) -> BrokerResult<bool> {
        let Some(lineage) = self.lineage.as_mut() else {
            return Ok(false);
        };
        lineage.anchor = lineage.anchor.min(tail);
        let epoch = lineage.epoch;
        Ok(matches!(
            self.follow_leader_lineage(epoch, tail)?,
            LineageFollow::Following { complete: false }
        ))
    }

    /// File this replica's records from the lineage's anchor through `tail`
    /// under the entries of `epoch`'s leader (#640), replacing whatever the
    /// replica recorded there itself. Called after each append under
    /// `epoch`, and at adoption of `epoch` instead of recording it at the
    /// tail.
    ///
    /// The result is this replica's own entries below the anchor, then every
    /// entry of the leader's starting from the anchor through `tail`. An
    /// entry exactly AT `tail` is kept, as an adoption at the tail always
    /// was: it names where an epoch begins and owns no record yet. Nothing
    /// past `tail` is written, so the journal never claims records it does
    /// not hold.
    ///
    /// Only entries at or above the anchor are rewritten, and those own
    /// records that came from `epoch`'s leader, or nothing at all. A crash
    /// between the truncation and the appends leaves a shorter vector, which
    /// the next call (the restart's adoption of `epoch`, or its next append)
    /// completes.
    pub fn follow_leader_lineage(&mut self, epoch: u64, tail: u64) -> BrokerResult<LineageFollow> {
        let Some(lineage) = self.lineage.as_mut() else {
            return Ok(LineageFollow::NotHeld);
        };
        if lineage.epoch != epoch {
            return Ok(LineageFollow::NotHeld);
        }
        lineage.anchor = lineage.anchor.min(tail);
        let anchor = lineage.anchor;
        let leader = lineage.entries.clone();
        let complete = leader.last().is_none_or(|last| last.start_offset <= tail);
        let keep = self
            .entries
            .iter()
            .take_while(|entry| entry.start_offset < anchor)
            .count();
        let mut target: Vec<EpochStart> = self.entries[..keep].to_vec();
        // Who owns the anchor itself. With the histories agreeing below the
        // anchor (checked when the lineage was taken), the leader's owner of
        // the record before it is this replica's last kept entry, so either
        // that same epoch carries on or the leader has an entry exactly at
        // the anchor, which the loop below adds. The one other shape is an
        // anchor at the retained base whose owner began in records both
        // sides reclaimed: it is anchored at the base, as `clamp_lineage`
        // anchors it.
        if let Some(owner) = leader
            .iter()
            .rev()
            .find(|entry| entry.start_offset <= anchor)
            .filter(|owner| owner.start_offset < anchor)
        {
            match target.last() {
                Some(last) if last.epoch == owner.epoch => {}
                Some(last) if last.epoch > owner.epoch => {
                    // Not the history it was checked against. Stop following
                    // rather than write a vector that goes backwards.
                    self.lineage = None;
                    return Ok(LineageFollow::NotHeld);
                }
                _ => target.push(EpochStart {
                    epoch: owner.epoch,
                    start_offset: anchor,
                }),
            }
        }
        // The lineage governs records only until this replica moved on to a
        // later epoch: an entry of a newer epoch at or above the anchor (an
        // adoption, or a truncation's re-anchoring) begins records that came
        // from some other leader. They and everything after them are kept as
        // they are, and the leader's entries are applied only below them.
        // While the replica follows `epoch` there is no such entry; it
        // matters to the replay at open, when the replica may have moved on
        // before a crash.
        let later = self
            .entries
            .iter()
            .position(|entry| entry.start_offset >= anchor && entry.epoch > epoch);
        let window_end = later.map_or(tail, |index| tail.min(self.entries[index].start_offset));
        for entry in leader
            .iter()
            .filter(|entry| entry.start_offset >= anchor && entry.start_offset <= window_end)
        {
            if target.last().is_none_or(|last| entry.epoch > last.epoch) {
                target.push(*entry);
            }
        }
        if let Some(index) = later {
            for entry in &self.entries[index..] {
                if target.last().is_none_or(|last| entry.epoch > last.epoch) {
                    target.push(*entry);
                }
            }
        }
        if target != self.entries {
            let common = self
                .entries
                .iter()
                .zip(&target)
                .take_while(|(have, want)| have == want)
                .count();
            self.truncate_entries(common)?;
            for entry in &target[common..] {
                self.record(entry.epoch, entry.start_offset)?;
            }
        }
        Ok(LineageFollow::Following { complete })
    }

    fn write_lineage(&mut self, lineage: Option<&LeaderLineage>) -> BrokerResult<()> {
        let path = lineage_path(&self.path);
        let bytes = lineage.map(encode_lineage).unwrap_or_default();
        let existed = self
            .env
            .storage
            .exists(&path)
            .map_err(|source| Self::io(&path, source))?;
        if !existed && bytes.is_empty() {
            return Ok(());
        }
        let mut file = self
            .env
            .storage
            .open(&path, OpenMode::CreateAppend)
            .map_err(|source| Self::io(&path, source))?;
        // Written whole at offset zero and then cut to length. A crash part
        // way leaves a frame whose checksum fails, which reads as "no
        // lineage": the attribution used before #640, never a misread one.
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&bytes))
            .and_then(|()| file.set_len(bytes.len() as u64))
            .and_then(|()| file.sync_data())
            .map_err(|source| Self::io(&path, source))?;
        if !existed {
            Self::sync_parent(&self.env, &path)?;
        }
        Ok(())
    }

    pub fn entries(&self) -> &[EpochStart] {
        &self.entries
    }

    pub fn latest(&self) -> Option<EpochStart> {
        self.entries.last().copied()
    }

    /// Which epoch wrote the record at `offset` on this replica.
    ///
    /// The entries partition the log, so the answer is the last epoch that
    /// began at or below `offset`. `None` means this replica cannot say —
    /// either it has no history or `offset` sits below the first epoch it
    /// recorded — and callers must treat that as unknown rather than assuming
    /// the oldest epoch wrote it.
    pub fn epoch_owning(&self, offset: u64) -> Option<u64> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.start_offset <= offset)
            .map(|entry| entry.epoch)
    }

    /// Where `epoch` ends on this replica: the start of the next recorded
    /// epoch, or `None` if `epoch` is the newest (its end is the log's tail).
    pub fn end_of_epoch(&self, epoch: u64) -> Option<u64> {
        let index = self.entries.iter().position(|entry| entry.epoch == epoch)?;
        self.entries.get(index + 1).map(|next| next.start_offset)
    }

    /// Compare this replica's lineage with `other`'s.
    ///
    /// Walks both vectors while they agree. The FIRST epoch at which they
    /// differ bounds divergence, and the answer is the smaller of the two start
    /// offsets for it: below that, both replicas wrote under the same
    /// leadership in the same order, so their records are the same records.
    ///
    /// # Why this returns a verdict and not an offset
    ///
    /// It used to return `Option<u64>`, and that conflated two facts which
    /// happen to be the same type and mean opposite things. A disagreement
    /// yields "everything at or above here is suspect" — a truncation target.
    /// A prefix relationship yields "agreed at least this far" — a FLOOR, and
    /// truncating to it discards records both replicas hold.
    ///
    /// Nothing in the signature distinguished them, and the first caller to use
    /// the value for its intended purpose got it wrong: given a peer whose
    /// vector was merely shorter, it read the start of the last common epoch as
    /// a divergence point and discarded a log the two replicas entirely agreed
    /// on. A verdict makes that mistake impossible to make quietly.
    pub fn compare_lineage(&self, other: &[EpochStart]) -> Lineage {
        compare_lineages(&self.entries, other)
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

/// Where the lineage sidecar of the journal at `path` lives.
fn lineage_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(LINEAGE_SUFFIX);
    path.with_file_name(name)
}

fn encode_lineage(lineage: &LeaderLineage) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        LINEAGE_HEADER_BYTES + lineage.entries.len() * ENTRY_BYTES + LINEAGE_CHECKSUM_BYTES,
    );
    bytes.extend_from_slice(LINEAGE_MAGIC);
    bytes.extend_from_slice(&LINEAGE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&lineage.epoch.to_be_bytes());
    bytes.extend_from_slice(&lineage.anchor.to_be_bytes());
    bytes.extend_from_slice(&(lineage.entries.len() as u32).to_be_bytes());
    for entry in &lineage.entries {
        bytes.extend_from_slice(&entry.epoch.to_be_bytes());
        bytes.extend_from_slice(&entry.start_offset.to_be_bytes());
    }
    let checksum = blake3::hash(&bytes);
    bytes.extend_from_slice(checksum.as_bytes());
    bytes
}

/// The lineage the sidecar at `path` holds. Every shape it cannot trust
/// (absent, empty, torn, checksum-mismatched, foreign magic, a version this
/// build does not know, an entry count past the bound, a vector that does
/// not advance) is "no lineage", reported unless it is a clean absence.
/// That is the attribution this replica used before #640: losing the sidecar
/// can cost a later election an Unknown answer, and misreading it is the only
/// direction that could do worse.
fn read_lineage(env: &Env, path: &Path) -> Option<LeaderLineage> {
    match env.storage.exists(path) {
        Ok(true) => {}
        Ok(false) => return None,
        Err(error) => {
            eprintln!(
                "leader-lineage sidecar {} could not be probed ({error}); following without it",
                path.display()
            );
            return None;
        }
    }
    // Bounded BEFORE it is read (#648 review), as the journal is: the length
    // comes off a file a crash or a damaged disk may have left any size, and
    // reading it whole first would let it exhaust memory while the replica
    // opens. No lineage this code writes is longer than `MAX_LINEAGE_BYTES`.
    let read = env.storage.open(path, OpenMode::Read).and_then(|mut file| {
        let len = file.len()?;
        if len > MAX_LINEAGE_BYTES {
            return Ok(Err(len));
        }
        let mut bytes = vec![0_u8; len as usize];
        file.read_exact(&mut bytes)?;
        Ok(Ok(bytes))
    });
    let bytes = match read {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(len)) => {
            eprintln!(
                "leader-lineage sidecar {} is {len} bytes, past the {MAX_LINEAGE_BYTES} of the \
                 largest lineage; following without it, as before #640",
                path.display()
            );
            return None;
        }
        Err(error) => {
            eprintln!(
                "leader-lineage sidecar {} could not be read ({error}); following without it",
                path.display()
            );
            return None;
        }
    };
    if bytes.is_empty() {
        return None;
    }
    let decoded = decode_lineage(&bytes);
    if let Err(reason) = &decoded {
        eprintln!(
            "leader-lineage sidecar {} {reason}; following without it, as before #640",
            path.display()
        );
    }
    decoded.ok()
}

fn decode_lineage(bytes: &[u8]) -> Result<LeaderLineage, String> {
    if bytes.len() < LINEAGE_HEADER_BYTES + LINEAGE_CHECKSUM_BYTES {
        return Err(format!("is {} bytes, shorter than its frame", bytes.len()));
    }
    let count = u32::from_be_bytes(bytes[26..30].try_into().expect("fixed width")) as usize;
    if count > MAX_ENTRIES {
        return Err(format!(
            "names {count} entries, past the bound of {MAX_ENTRIES}"
        ));
    }
    let body = LINEAGE_HEADER_BYTES + count * ENTRY_BYTES;
    let Some(checksum) = bytes.get(body..body + LINEAGE_CHECKSUM_BYTES) else {
        return Err(format!(
            "is {} bytes, shorter than the {count} entries it names",
            bytes.len()
        ));
    };
    // Checked before the magic and version are believed: a torn frame can
    // carry any bytes there.
    if blake3::hash(&bytes[..body]).as_bytes() != checksum {
        return Err("fails its checksum (a torn write)".to_owned());
    }
    if &bytes[0..8] != LINEAGE_MAGIC {
        return Err("has a foreign magic".to_owned());
    }
    let version = u16::from_be_bytes([bytes[8], bytes[9]]);
    if version != LINEAGE_VERSION {
        return Err(format!(
            "is version {version}, which this build does not know (it reads {LINEAGE_VERSION})"
        ));
    }
    let epoch = u64::from_be_bytes(bytes[10..18].try_into().expect("fixed width"));
    let anchor = u64::from_be_bytes(bytes[18..26].try_into().expect("fixed width"));
    let entries: Vec<EpochStart> = bytes[LINEAGE_HEADER_BYTES..body]
        .as_chunks::<ENTRY_BYTES>()
        .0
        .iter()
        .map(|entry| EpochStart {
            epoch: u64::from_be_bytes(entry[0..8].try_into().expect("fixed width")),
            start_offset: u64::from_be_bytes(entry[8..16].try_into().expect("fixed width")),
        })
        .collect();
    if entries.is_empty()
        || !entries.windows(2).all(|pair| {
            pair[1].epoch > pair[0].epoch && pair[1].start_offset >= pair[0].start_offset
        })
    {
        return Err("holds a vector that does not advance".to_owned());
    }
    Ok(LeaderLineage {
        epoch,
        anchor,
        entries,
    })
}

/// [`FencingEpochJournal::compare_lineage`] over two vectors neither of which
/// is this process's journal (#577): a promotion candidate judges each fenced
/// replica's answer by comparing the history it sent with the history the
/// replica returned, and holds no journal for the replica side. One function,
/// so the candidate's verdict and the replica's reconciliation cannot drift
/// apart on what "agreed" means.
pub fn compare_lineages(mine: &[EpochStart], theirs: &[EpochStart]) -> Lineage {
    // CANONICALIZED before comparing (#408 review): an epoch adopted
    // while idle writes nothing, and its successor starting at the same
    // offset is the epoch that actually governs records from there. One
    // replica recording the idle adoption and another not still
    // attribute every record identically — comparing the raw vectors
    // would call that divergence and mandate truncating records both
    // sides agree on.
    let mine_canonical = canonicalize_lineage(mine);
    let theirs_canonical = canonicalize_lineage(theirs);
    let mut compared = 0_usize;
    for (mine, theirs) in mine_canonical.iter().zip(theirs_canonical.iter()) {
        if mine.epoch != theirs.epoch || mine.start_offset != theirs.start_offset {
            // Either different leadership at the same position in history,
            // or the same epoch beginning in two different places — which
            // is a disagreement about what came before it.
            return Lineage::DivergesAt(mine.start_offset.min(theirs.start_offset));
        }
        compared += 1;
    }
    // Neither vector contradicted the other. Note this covers the case
    // where one is a PREFIX of the other, which is not divergence: the
    // shorter replica has simply recorded less, and nothing here proves
    // anything about the records beyond its last entry.
    if compared == 0 {
        // Nothing was actually compared, so nothing is proven. Two replicas
        // that cannot vouch for their own history are not in agreement —
        // they are mutually ignorant, and reconciling on that basis is the
        // opposite of what an empty vector means everywhere else here.
        return Lineage::Unknown;
    }
    Lineage::Agreed
}

/// Collapse every same-offset run to its last entry (#408): an epoch
/// adopted while a replica was idle wrote nothing, and the epoch starting
/// at the same offset after it is the one that actually governs records
/// from there. Zero-record adoptions are bookkeeping, not history — and
/// two replicas that disagree only about bookkeeping agree about every
/// record either of them holds.
pub fn canonicalize_lineage(entries: &[EpochStart]) -> Vec<EpochStart> {
    let mut canonical: Vec<EpochStart> = Vec::with_capacity(entries.len());
    for entry in entries {
        match canonical.last_mut() {
            Some(last) if last.start_offset == entry.start_offset => *last = *entry,
            _ => canonical.push(*entry),
        }
    }
    canonical
}

/// Whether a fence caller's history proves that NONE of this replica's
/// records in `[divergence, tail)` was ever acknowledged (#611). That proof
/// is the only licence a fence has to discard them before its vote is read.
///
/// The fence runs before the vote, so a record it discards cannot then
/// refuse the candidate. Raft never truncates a voter at vote time. A
/// truncation here is safe only for records no quorum can have
/// acknowledged, and a divergence point alone does not show that. It shows
/// where two histories stop agreeing, not which side holds the acknowledged
/// records.
///
/// The proof: the caller HOLDS a record at `divergence`, written under an
/// epoch newer than every epoch this replica attributes to the disputed
/// stretch. That epoch's leader was established under the vote restriction,
/// so it held every record acknowledged before it, and its record at
/// `divergence` is not this replica's. So no record of this replica's at or
/// above `divergence` was acknowledged before that epoch. None could be
/// acknowledged after it either, because the majority that granted it
/// refuses appends from older epochs.
///
/// Anything short of that keeps the records:
///
/// * The caller's entry for `fencing_epoch` is the adoption it made at its
///   own tail for this fence. It owns no record yet (a leader appends under
///   an epoch only after the epoch is established). A divergence there is
///   this replica holding records the caller never received. That is the
///   #611 loss: the caller may simply have missed an acknowledged write.
///   The replica keeps the records and answers with its full offset, and
///   the vote restriction then refuses the caller.
/// * The disputed stretch on this replica includes an epoch as new as the
///   caller's, or newer. Those records may belong to a leadership the
///   caller never followed, and may be acknowledged there.
/// * Either side cannot attribute the offsets at all.
pub fn caller_supersedes_suffix(
    mine: &[EpochStart],
    caller: &[EpochStart],
    divergence: u64,
    tail: u64,
    fencing_epoch: u64,
) -> bool {
    let caller = canonicalize_lineage(caller);
    let Some(theirs) = caller
        .iter()
        .rev()
        .find(|entry| entry.start_offset <= divergence)
    else {
        return false;
    };
    if theirs.epoch >= fencing_epoch {
        return false;
    }
    let mine = canonicalize_lineage(mine);
    if !mine.iter().any(|entry| entry.start_offset <= divergence) {
        return false;
    }
    // Epochs ascend with offset, so the newest epoch this replica holds a
    // record of in `[divergence, tail)` is its last entry starting below the
    // tail. Its own adoption of `fencing_epoch` sits AT the tail and owns
    // nothing.
    let Some(newest_disputed) = mine.iter().rev().find(|entry| entry.start_offset < tail) else {
        return false;
    };
    newest_disputed.epoch < theirs.epoch
}

/// Clamp a lineage vector to the history that governs exactly the retained
/// window `[base, tail)` (#408): the entry owning `base`, re-anchored AT
/// `base`, plus every entry starting inside the window.
///
/// Two replicas whose full vectors disagree may still agree about every
/// record either of them RETAINS — retention reclaims a prefix, and a
/// dispute confined to reclaimed records mandates nothing. The clamped
/// views make that distinction checkable: equal clamps mean every retained
/// record is attributed identically, unequal clamps mean retained records
/// are attributed to different leaderships.
///
/// The window is bounded on BOTH sides deliberately (review): an entry at
/// or beyond `tail` governs records this replica does not hold — the
/// fencing epoch's own adoption entry sits exactly at the tail, and a peer
/// that recorded a newer epoch while idle differs there legitimately —
/// so including either side's future would manufacture disagreements about
/// nothing, and tolerating them asymmetrically would excuse real ones.
/// With both bounds in place the comparison is exact equality, no prefix
/// tolerance needed.
///
/// Empty means the vector cannot vouch for `base` at all — unknown, which
/// callers must treat as "prove nothing" exactly as everywhere else here.
/// That answer is returned WHOLE (review): later in-window entries hanging
/// off an unvouched base are not evidence, and returning them would let a
/// caller mistake them for some.
pub fn clamp_lineage(entries: &[EpochStart], base: u64, tail: u64) -> Vec<EpochStart> {
    // An empty (or inverted) window holds no records to attribute; whatever
    // the vector says about it is not retained-lineage evidence (review).
    if tail <= base {
        return Vec::new();
    }
    let Some(owner) = entries
        .iter()
        .rev()
        .find(|entry| entry.start_offset <= base)
    else {
        return Vec::new();
    };
    let mut clamped = vec![EpochStart {
        epoch: owner.epoch,
        start_offset: base,
    }];
    // CANONICALIZED to the last epoch of every same-offset run (review): an
    // epoch adopted while idle writes nothing, and its successor starting at
    // the same offset is the epoch that actually governs records from there.
    // One replica may have recorded the idle adoption and another not, yet
    // both attribute every retained record identically — keeping the
    // zero-record entry would make those histories compare unequal and
    // reject a valid replica. (The base anchor is already canonical: the
    // reverse owner search lands on the last entry at or below `base`.)
    for entry in entries
        .iter()
        .filter(|entry| entry.start_offset > base && entry.start_offset < tail)
    {
        match clamped.last_mut() {
            Some(last) if last.start_offset == entry.start_offset => *last = *entry,
            _ => clamped.push(*entry),
        }
    }
    clamped
}

/// Install a source-provided epoch history into a repaired range directory
/// (#315).
///
/// Sealed-segment transfer carries the records but not their lineage, so a
/// repaired replica could not answer the epoch-qualified reconciliation the
/// next leader transition asks of it — and was truncated to the base by the
/// first fence it received. This writes the source's history alongside the
/// transferred prefix so the replica can answer honestly.
///
/// **Trust:** this extends exactly the trust the transfer already required.
/// A follower's journal is leader-derived in normal replication too — each
/// entry is recorded when the follower adopts an epoch over records the
/// leader replicated to it. Carrying the source's history with the source's
/// records grants the repaired replica the same standing, no more: the next
/// fence still compares this history against the caller's and truncates at
/// any genuine divergence.
///
/// **Normalization:** entries with `start_offset` strictly above `tail` are
/// dropped; an entry AT the tail is kept (#407 — it names where an epoch
/// began, which is true whether or not the records above it were copied).
/// The repaired replica holds only the sealed prefix; an entry beyond it
/// would claim lineage for records the replica does not hold, and —
/// concretely — would poison the journal at the next epoch adoption,
/// because `record` refuses a new entry whose start offset is below the
/// last one's.
///
/// Idempotent for a re-run repair fetching the same history. A conflicting
/// prior install fails loudly instead of merging — and the comparison covers
/// **every** overlapping entry, not only the last: `record`'s own guards
/// treat an incoming epoch below the journal's latest as a stale observation
/// and ignore it, which here would let a repair resumed against a different
/// source silently retain the previous source's divergent entry as if it were
/// this history's. The overlap must be an exact prefix or nothing installs.
pub fn install_transferred_history(
    env: &Env,
    path: impl AsRef<Path>,
    entries: &[EpochStart],
    tail: u64,
) -> BrokerResult<usize> {
    // AT the tail is kept, strictly above is dropped (#407). An entry
    // starting exactly at the sealed-prefix end claims no records this
    // replica lacks — it names where an epoch began, which is true whether
    // or not the records above it were copied — and keeping it means the
    // replica's next adoption of that same epoch is the documented no-op
    // instead of a fresh append. An entry strictly above the tail is
    // lineage for records that were never transferred, and would poison the
    // journal's non-decreasing rule the moment the replica appends.
    let incoming: Vec<EpochStart> = entries
        .iter()
        .take_while(|entry| entry.start_offset <= tail)
        .copied()
        .collect();
    let mut journal = FencingEpochJournal::open_in(env, &path)?;
    let existing = journal.entries().to_vec();
    if existing.len() > incoming.len() {
        return Err(BrokerError::EpochJournalCorrupt(format!(
            "fencing-epoch journal: {} entries already installed but the source offers only {} \
             within the sealed prefix; this directory was repaired from a different history — \
             delete it and repair from scratch",
            existing.len(),
            incoming.len()
        )));
    }
    for (have, want) in existing.iter().zip(&incoming) {
        if have != want {
            return Err(BrokerError::EpochJournalCorrupt(format!(
                "fencing-epoch journal: epoch {} already installed starting at {}, but the \
                 source says epoch {} starts at {}; a resumed repair may not mix two sources' \
                 lineages — delete the directory and repair from scratch",
                have.epoch, have.start_offset, want.epoch, want.start_offset
            )));
        }
    }
    for entry in &incoming[existing.len()..] {
        journal.record(entry.epoch, entry.start_offset)?;
    }
    Ok(incoming.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn journal(dir: &TempDir) -> FencingEpochJournal {
        FencingEpochJournal::open(dir.path().join("fencing-epochs")).expect("open")
    }

    fn starts(pairs: &[(u64, u64)]) -> Vec<EpochStart> {
        pairs
            .iter()
            .map(|&(epoch, start_offset)| EpochStart {
                epoch,
                start_offset,
            })
            .collect()
    }

    /// #611, case by case. The replica holds epoch-18 records to 10 and
    /// adopted 20 at its tail; the fence is at 20.
    #[test]
    fn only_a_caller_holding_a_newer_record_at_the_divergence_supersedes() {
        let mine = starts(&[(18, 0), (20, 10)]);
        // The caller's only entry at 5 is its own adoption of the fence
        // epoch at its tail: it holds nothing there.
        assert!(!caller_supersedes_suffix(
            &mine,
            &starts(&[(18, 0), (20, 5)]),
            5,
            10,
            20
        ));
        // An idle adoption of 19 at the same tail collapses into 20's.
        assert!(!caller_supersedes_suffix(
            &mine,
            &starts(&[(18, 0), (19, 5), (20, 5)]),
            5,
            10,
            20
        ));
        // The caller holds epoch-19 records from 5: newer than every record
        // the replica would lose.
        assert!(caller_supersedes_suffix(
            &mine,
            &starts(&[(18, 0), (19, 5), (20, 8)]),
            5,
            10,
            20
        ));
        // The replica's disputed stretch includes epoch-19 records of its
        // own from 7: not older than the caller's 19, so not superseded.
        assert!(!caller_supersedes_suffix(
            &starts(&[(18, 0), (19, 7), (20, 10)]),
            &starts(&[(18, 0), (19, 5), (20, 8)]),
            5,
            10,
            20
        ));
        // Neither side attributes the offset: nothing is proven.
        assert!(!caller_supersedes_suffix(
            &[],
            &starts(&[(18, 0), (19, 5)]),
            5,
            10,
            20
        ));
        assert!(!caller_supersedes_suffix(
            &mine,
            &starts(&[(19, 6)]),
            5,
            10,
            20
        ));
    }

    /// The transferred history keeps an entry AT the sealed-prefix end and
    /// drops what lies strictly beyond it (#407). An entry at the tail
    /// claims no records the replica lacks — it names where an epoch began,
    /// and keeping it makes the replica's re-observation of that epoch the
    /// documented adoption no-op. An entry beyond the tail is lineage for
    /// records that were never transferred, and would poison the journal
    /// at the next append.
    #[test]
    fn installing_a_transferred_history_keeps_the_tail_entry_and_drops_beyond() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        let entries = starts(&[(1, 0), (3, 400), (5, 900), (7, 950)]);

        let installed =
            install_transferred_history(&Env::real(), &path, &entries, 900).expect("install");

        assert_eq!(
            installed, 3,
            "the entry AT the tail is true history this replica may keep; only the one \
             beyond it claims records that were never transferred"
        );
        let reopened = FencingEpochJournal::open(&path).expect("reopen");
        assert_eq!(
            reopened.entries(),
            &starts(&[(1, 0), (3, 400), (5, 900)])[..]
        );
        // A later adoption at the same offset still works: the journal's
        // non-decreasing rule admits an equal offset under a higher epoch,
        // so keeping the tail entry poisons nothing.
        let mut journal = FencingEpochJournal::open(&path).expect("reopen for adoption");
        journal
            .record(6, 900)
            .expect("adopting at the tail must work with the tail entry present");
        // And re-observing the installed epoch itself is the #315 no-op the
        // kept entry exists to enable.
        journal
            .record_adoption(5, 900)
            .expect("re-observing the installed epoch is a no-op, not a conflict");
    }

    /// A repair that crashes and re-runs fetches the same history again; the
    /// second install must be a no-op, not a corruption.
    #[test]
    fn installing_the_same_history_twice_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        let entries = starts(&[(1, 0), (2, 250)]);

        install_transferred_history(&Env::real(), &path, &entries, 500).expect("first install");
        let second =
            install_transferred_history(&Env::real(), &path, &entries, 500).expect("re-run");

        assert_eq!(second, 2);
        let reopened = FencingEpochJournal::open(&path).expect("reopen");
        assert_eq!(reopened.entries(), &entries[..]);
    }

    /// A conflicting prior install — same epoch, different start — must fail
    /// loudly. Merging two sources' claims would corrupt every later
    /// comparison, which is the journal's own rule for `record`.
    #[test]
    fn installing_a_conflicting_history_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");

        install_transferred_history(&Env::real(), &path, &starts(&[(1, 0), (2, 250)]), 500)
            .expect("first install");
        let conflicting =
            install_transferred_history(&Env::real(), &path, &starts(&[(1, 0), (2, 300)]), 500);

        assert!(
            conflicting.is_err(),
            "same epoch at a different offset is a lineage conflict, not a merge"
        );
    }

    /// The conflict check covers EVERY overlapping entry, not just the last.
    /// `record` treats an incoming epoch below the journal's latest as a
    /// stale observation and ignores it — which, unguarded, would let a
    /// repair resumed against a different source keep the previous source's
    /// divergent entry while reporting success.
    #[test]
    fn a_conflict_in_an_earlier_installed_epoch_is_refused() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");

        install_transferred_history(&Env::real(), &path, &starts(&[(1, 0), (2, 250)]), 500)
            .expect("first install");
        let different_source =
            install_transferred_history(&Env::real(), &path, &starts(&[(1, 100), (2, 250)]), 500);

        assert!(
            different_source.is_err(),
            "epoch 1's start differs from what was installed; two sources' lineages must not mix"
        );
        // And a journal already holding MORE than the new source offers is a
        // different history too, not a prefix to silently keep.
        let shorter = install_transferred_history(&Env::real(), &path, &starts(&[(1, 0)]), 500);
        assert!(
            shorter.is_err(),
            "a shorter offering cannot vouch for what is already installed"
        );
    }

    /// Adoption honors what the journal already knows: re-observing an epoch
    /// the installed history contains must not re-anchor it at this replica's
    /// tail — that refusal used to latch the journal broken at the repaired
    /// replica's first metadata poll, destroying the transferred lineage.
    #[test]
    fn adopting_an_epoch_the_history_already_contains_is_a_no_op() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(1, 0).unwrap();

        j.record_adoption(1, 1400)
            .expect("re-observing the current epoch at the tail must not be refused");
        assert_eq!(
            j.entries(),
            &starts(&[(1, 0)])[..],
            "the installed start is the truth; the tail is not"
        );
        // A genuinely new epoch still records normally.
        j.record_adoption(2, 1400).unwrap();
        assert_eq!(j.entries(), &starts(&[(1, 0), (2, 1400)])[..]);
    }

    /// A first entry over a log that already has records is ignorance dressed
    /// as history; adoption leaves the journal empty — durably "unknown" —
    /// instead of writing a lone entry that compares as divergence-at-zero.
    #[test]
    fn adoption_does_not_fabricate_a_first_entry_over_existing_records() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);

        j.record_adoption(5, 900)
            .expect("staying unknown is a success, not an error");
        assert!(j.entries().is_empty(), "no lineage may be invented");
        // A fresh replica adopting at offset zero is real lineage, not
        // fabrication.
        j.record_adoption(5, 0).unwrap();
        assert_eq!(j.entries(), &starts(&[(5, 0)])[..]);
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
            mine.compare_lineage(&theirs),
            Lineage::DivergesAt(200),
            "records below the smaller of the two differing starts were written under \
             identical leadership"
        );
    }

    /// A prefix is not a divergence, and must not be reported as one.
    ///
    /// This assertion used to read `Some(100)` — the start of the last common
    /// epoch — on the reasoning that everything the shorter vector covers is
    /// agreed. True, but it is a FLOOR, and the first caller to use this value
    /// as a truncation target read it as a ceiling and discarded records
    /// 100..200 that both replicas hold. `Agreed` cannot be misread that way.
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
        assert_eq!(mine.compare_lineage(&theirs), Lineage::Agreed);
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
        assert_eq!(mine.compare_lineage(&theirs), Lineage::DivergesAt(80));
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
        assert_eq!(mine.compare_lineage(&[]), Lineage::Unknown);
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

    /// Truncation drops the epochs whose records are gone and keeps the one
    /// that straddles the cut — it still owns everything below it.
    #[test]
    fn truncation_drops_epochs_above_the_cut_and_keeps_the_straddler() {
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(1, 0).unwrap();
            j.record(2, 100).unwrap();
            j.record(3, 300).unwrap();
            j.record(4, 500).unwrap();

            assert_eq!(j.truncate_to(200).unwrap(), 2, "epochs 3 and 4 are gone");
            assert_eq!(
                j.entries(),
                &[
                    EpochStart {
                        epoch: 1,
                        start_offset: 0
                    },
                    EpochStart {
                        epoch: 2,
                        start_offset: 100
                    },
                ],
                "epoch 2 began at 100 and still owns 100..200"
            );
        }
        assert_eq!(
            journal(&dir).entries().len(),
            2,
            "the rewrite must be durable, not just in memory"
        );
    }

    /// An epoch starting exactly at the cut owns nothing below it, so it goes.
    #[test]
    fn truncation_drops_an_epoch_starting_exactly_at_the_cut() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(1, 0).unwrap();
        j.record(2, 100).unwrap();

        assert_eq!(j.truncate_to(100).unwrap(), 1);
        assert_eq!(j.latest().unwrap().epoch, 1);
    }

    #[test]
    fn truncation_above_every_entry_changes_nothing() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(1, 0).unwrap();
        j.record(2, 100).unwrap();

        assert_eq!(j.truncate_to(900).unwrap(), 0);
        assert_eq!(j.entries().len(), 2);
    }

    /// After truncating, the journal must still be appendable — and the next
    /// entry must land at the right byte, not after the corpses of the dropped
    /// ones. Reopening is what proves the file was really shortened.
    #[test]
    fn a_truncated_journal_still_appends_at_the_right_place() {
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(1, 0).unwrap();
            j.record(2, 100).unwrap();
            j.record(3, 300).unwrap();
            j.truncate_to(200).unwrap();
            j.record(7, 150).unwrap();
        }
        assert_eq!(
            journal(&dir).entries(),
            &[
                EpochStart {
                    epoch: 1,
                    start_offset: 0
                },
                EpochStart {
                    epoch: 2,
                    start_offset: 100
                },
                EpochStart {
                    epoch: 7,
                    start_offset: 150
                },
            ]
        );
    }

    /// Truncating everything leaves an empty vector, which reads as "unknown" —
    /// the correct answer for a replica whose entire history was discarded.
    #[test]
    fn truncating_to_zero_empties_the_vector() {
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(1, 0).unwrap();
            j.record(2, 100).unwrap();
            assert_eq!(j.truncate_to(0).unwrap(), 2);
            assert!(j.latest().is_none());
        }
        assert!(journal(&dir).latest().is_none());
    }

    /// The #640 shape at the journal level: C adopted 19 and 20 at its tail
    /// (5), while the leader R files 5..10 under 18 and begins 19 and 20 at
    /// 10. Following R's lineage, C ends with R's vector exactly.
    #[test]
    fn a_follower_files_caught_up_records_under_the_leaders_entries() {
        let dir = TempDir::new().unwrap();
        let leader = starts(&[(18, 0), (19, 10), (20, 10)]);
        let mut j = journal(&dir);
        j.record(18, 0).unwrap();
        j.record_adoption(19, 5).unwrap();
        j.record_adoption(20, 5).unwrap();
        assert!(j.take_leader_lineage(20, &leader, 5, 0).unwrap());
        assert_eq!(
            j.follow_leader_lineage(20, 5).unwrap(),
            LineageFollow::Following { complete: false }
        );
        assert_eq!(
            j.entries(),
            starts(&[(18, 0)]).as_slice(),
            "the provisional adoptions at the tail are gone: the leader says 18 owns 5"
        );
        for tail in 6..10 {
            j.follow_leader_lineage(20, tail).unwrap();
            assert_eq!(j.entries(), starts(&[(18, 0)]).as_slice());
        }
        assert_eq!(
            j.follow_leader_lineage(20, 11).unwrap(),
            LineageFollow::Following { complete: true }
        );
        assert_eq!(j.entries(), leader.as_slice());
        drop(j);
        assert_eq!(
            journal(&dir).entries(),
            leader.as_slice(),
            "durable, and still the plain v1 journal any reader of it expects"
        );
    }

    /// A lineage names one epoch. Appends or adoptions under any other epoch
    /// attribute the old way.
    #[test]
    fn a_lineage_applies_only_to_the_epoch_it_names() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(18, 0).unwrap();
        assert!(j
            .take_leader_lineage(20, &starts(&[(18, 0), (20, 10)]), 5, 0)
            .unwrap());
        assert_eq!(
            j.follow_leader_lineage(21, 7).unwrap(),
            LineageFollow::NotHeld
        );
        assert_eq!(j.entries(), starts(&[(18, 0)]).as_slice());
    }

    /// The lineage is evidence about the records the leader sends, never
    /// about the ones this replica already holds: a leader that files those
    /// otherwise is not followed, and a lineage held for that epoch is
    /// dropped durably.
    #[test]
    fn a_leader_that_disagrees_below_the_tail_is_not_followed() {
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(18, 0).unwrap();
            assert!(j
                .take_leader_lineage(20, &starts(&[(18, 0), (20, 10)]), 5, 0)
                .unwrap());
            // Epoch 19 wrote at 3 on the leader; this replica holds 18 there.
            assert!(!j
                .take_leader_lineage(20, &starts(&[(18, 0), (19, 3), (20, 10)]), 5, 0)
                .unwrap());
            assert_eq!(j.leader_lineage_epoch(), None);
            // Nor one that cannot vouch for this replica's tail, nor none.
            assert!(!j
                .take_leader_lineage(20, &starts(&[(19, 6)]), 5, 0)
                .unwrap());
            assert!(!j.take_leader_lineage(20, &[], 5, 0).unwrap());
        }
        assert_eq!(journal(&dir).leader_lineage_epoch(), None);
    }

    /// The sidecar survives a reopen, and every shape of it that cannot be
    /// trusted reads as "no lineage", never as an error that keeps the
    /// replica from opening: that is the attribution used before #640.
    #[test]
    fn the_lineage_survives_reopen_and_an_untrusted_sidecar_reads_as_none() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fencing-epochs");
        let sidecar = lineage_path(&path);
        assert_eq!(sidecar, dir.path().join("fencing-epochs.leader"));
        {
            let mut j = journal(&dir);
            j.record(18, 0).unwrap();
            assert!(j
                .take_leader_lineage(20, &starts(&[(18, 0), (19, 10), (20, 10)]), 5, 0)
                .unwrap());
        }
        let mut j = journal(&dir);
        assert_eq!(j.leader_lineage_epoch(), Some(20));
        j.follow_leader_lineage(20, 10).unwrap();
        assert_eq!(
            j.entries(),
            starts(&[(18, 0), (19, 10), (20, 10)]).as_slice()
        );
        drop(j);

        let good = std::fs::read(&sidecar).unwrap();
        let mut cases: Vec<(&str, Vec<u8>)> = vec![
            ("torn", good[..good.len() - 7].to_vec()),
            ("empty", Vec::new()),
        ];
        let mut flipped = good.clone();
        flipped[30] ^= 0x40;
        cases.push(("checksum mismatch", flipped));
        // A newer version under a valid checksum: not this build's to read.
        let mut newer = good.clone();
        newer[8..10].copy_from_slice(&(LINEAGE_VERSION + 1).to_be_bytes());
        let body = newer.len() - LINEAGE_CHECKSUM_BYTES;
        let checksum = *blake3::hash(&newer[..body]).as_bytes();
        newer[body..].copy_from_slice(&checksum);
        cases.push(("newer version", newer));
        for (what, bytes) in cases {
            std::fs::write(&sidecar, &bytes).unwrap();
            let j = journal(&dir);
            assert_eq!(j.leader_lineage_epoch(), None, "{what}");
            assert_eq!(
                j.entries(),
                starts(&[(18, 0), (19, 10), (20, 10)]).as_slice(),
                "{what}: the journal itself is untouched"
            );
        }
    }

    /// A journal written while following is the same v1 file an older
    /// binary reads: the header it checks, then whole 16-byte entries that
    /// advance. The sidecar is a separate name that binary never opens.
    #[test]
    fn a_journal_written_while_following_is_still_a_v1_journal() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(18, 0).unwrap();
        j.record_adoption(20, 5).unwrap();
        assert!(j
            .take_leader_lineage(20, &starts(&[(18, 0), (19, 10), (20, 10)]), 5, 0)
            .unwrap());
        j.follow_leader_lineage(20, 12).unwrap();
        drop(j);
        let bytes = std::fs::read(dir.path().join("fencing-epochs")).unwrap();
        assert_eq!(&bytes[0..8], MAGIC);
        assert_eq!(u16::from_be_bytes([bytes[8], bytes[9]]), 1);
        let body = &bytes[HEADER_BYTES as usize..];
        assert_eq!(
            body.len(),
            3 * ENTRY_BYTES,
            "no torn or stale bytes after the rewrite"
        );
        let decoded: Vec<(u64, u64)> = body
            .as_chunks::<ENTRY_BYTES>()
            .0
            .iter()
            .map(|entry| {
                (
                    u64::from_be_bytes(entry[0..8].try_into().unwrap()),
                    u64::from_be_bytes(entry[8..16].try_into().unwrap()),
                )
            })
            .collect();
        assert_eq!(decoded, vec![(18, 0), (19, 10), (20, 10)]);
    }

    /// A truncation below the anchor lowers it: the records written from the
    /// cut on come from the same leader.
    #[test]
    fn a_truncation_below_the_anchor_follows_from_the_cut() {
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(18, 0).unwrap();
        // The leader says 20 began at 4, below this replica's tail of 6: the
        // two disagree about 4..6, so it is not taken.
        assert!(!j
            .take_leader_lineage(20, &starts(&[(18, 0), (20, 4)]), 6, 0)
            .unwrap());
        assert!(j
            .take_leader_lineage(20, &starts(&[(18, 0), (20, 8)]), 6, 0)
            .unwrap());
        j.truncate_to(4).unwrap();
        j.follow_leader_lineage(20, 9).unwrap();
        assert_eq!(j.entries(), starts(&[(18, 0), (20, 8)]).as_slice());
    }

    /// The replay at open (#648 review) files the records a crash left
    /// unfiled, and stops at an entry of a later epoch: records from there
    /// on came from another leader, which the lineage says nothing about.
    #[test]
    fn the_replay_at_open_stops_at_a_later_epoch() {
        let leader = starts(&[(18, 0), (19, 10), (20, 10)]);
        // Caught up to 12 under 20, the follow lost to a crash, then 21
        // adopted at 12: the leader's entries at 10 belong below it.
        let dir = TempDir::new().unwrap();
        {
            let mut j = journal(&dir);
            j.record(18, 0).unwrap();
            assert!(j.take_leader_lineage(20, &leader, 5, 0).unwrap());
            j.record_adoption(21, 12).unwrap();
        }
        let mut j = journal(&dir);
        assert!(!j.replay_leader_lineage_at_open(12).unwrap());
        assert_eq!(
            j.entries(),
            starts(&[(18, 0), (19, 10), (20, 10), (21, 12)]).as_slice()
        );
        drop(j);
        assert_eq!(
            journal(&dir).entries(),
            starts(&[(18, 0), (19, 10), (20, 10), (21, 12)]).as_slice(),
            "durable"
        );

        // Moved on to 21 at 8, before the leader of 20 reached its own
        // entries: everything from 8 is 21's, and stays so.
        let dir = TempDir::new().unwrap();
        let mut j = journal(&dir);
        j.record(18, 0).unwrap();
        assert!(j.take_leader_lineage(20, &leader, 5, 0).unwrap());
        j.record_adoption(21, 8).unwrap();
        j.replay_leader_lineage_at_open(12).unwrap();
        assert_eq!(j.entries(), starts(&[(18, 0), (21, 8)]).as_slice());
    }

    /// #648 review (cubic, P2): the sidecar's length is bounded BEFORE it is
    /// read, as the journal's is. No lineage this code writes is longer than
    /// the frame for `MAX_ENTRIES` entries, so a longer file is not one, and
    /// reading it whole first would let a damaged file exhaust memory while
    /// the replica opens. A valid frame with junk after it, within the bound,
    /// is still read: that is a crash between the write and the cut to length.
    #[test]
    fn a_sidecar_longer_than_the_largest_frame_is_refused_before_it_is_read() {
        let dir = TempDir::new().unwrap();
        let sidecar = lineage_path(&dir.path().join("fencing-epochs"));
        {
            let mut j = journal(&dir);
            j.record(18, 0).unwrap();
            assert!(j
                .take_leader_lineage(20, &starts(&[(18, 0), (20, 10)]), 5, 0)
                .unwrap());
        }
        let good = std::fs::read(&sidecar).unwrap();
        let mut within = good.clone();
        within.resize(MAX_LINEAGE_BYTES as usize, 0xA5);
        std::fs::write(&sidecar, &within).unwrap();
        assert_eq!(
            journal(&dir).leader_lineage_epoch(),
            Some(20),
            "a stale tail within the bound is a crash before the cut to length"
        );
        let mut beyond = good;
        beyond.resize(MAX_LINEAGE_BYTES as usize + 1, 0xA5);
        std::fs::write(&sidecar, &beyond).unwrap();
        let j = journal(&dir);
        assert_eq!(j.leader_lineage_epoch(), None);
        assert_eq!(
            j.entries(),
            starts(&[(18, 0)]).as_slice(),
            "the journal itself still opens"
        );
    }
}
