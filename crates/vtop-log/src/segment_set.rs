//! A range as a SEQUENCE of segments: sealed prefixes plus one active tail.
//!
//! # Why a range stopped being one file
//!
//! It was one `range.active` growing for the life of the process, which meant
//! there was never an immutable prefix. Everything that needs one was blocked
//! behind that: transferring a segment to repair a replica that fell too far
//! behind to catch up, retention (nothing can be deleted from a file still
//! being appended to), and naming the segments either side of a leadership
//! transition.
//!
//! # What this type is responsible for
//!
//! Routing. A read names an offset, and exactly one segment holds it; a write
//! always lands on the tail, which rolls when it reaches its bound. Callers see
//! a range, not a filesystem layout.
//!
//! # Reads cross boundaries; writes never do
//!
//! A fetch that begins in a sealed segment continues into the next one until it
//! reaches the caller's limits or the high-water mark. Stopping at a boundary
//! would be correct but would make every reader responsible for knowing the
//! layout, and a consumer asking for 500 records from offset 0 would silently
//! get however many happened to fit in the first segment.
//!
//! A write never spans a roll: the batch either fits in the tail or the tail
//! rolls first and the whole batch lands in the successor. A group split across
//! two segments would have part of a producer's commit group on either side of
//! a boundary, and the two halves could then be transferred, truncated, or
//! retained independently.

use crate::env::Env;
use crate::segment::{roll_in, segment_stem, ActiveSegment, SegmentReader};
use crate::{
    CatalogSegmentState, Durability, FetchBatch, FetchedRecord, LogError, LogRecord,
    SegmentDescriptor, StartupCatalog, VtopLogResult,
};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// One range's segments, ordered and contiguous.
pub struct SegmentSet {
    env: Env,
    directory: PathBuf,
    /// Sealed segments in ascending offset order, contiguous with each other
    /// and with `active`.
    sealed: Vec<SegmentReader>,
    /// `Option` only so the tail can be moved out by value during a roll, which
    /// `roll_in` requires. It is `None` for the duration of that call and
    /// nowhere else; an earlier version parked a throwaway segment in the range
    /// directory instead, and its orphaned sidecars made discovery quarantine
    /// the whole range on the next open.
    active: Option<ActiveSegment>,
}

impl SegmentSet {
    /// Open every segment of the range in `directory`.
    ///
    /// Refuses to open a directory discovery quarantined anything in. A
    /// quarantined artifact means the layout is ambiguous — two active
    /// segments, overlapping intervals, a half-written atomic file — and
    /// opening the subset that happens to look fine would serve a range with a
    /// hole in it while reporting success.
    pub fn open_in(env: &Env, directory: impl AsRef<Path>) -> VtopLogResult<Option<Self>> {
        let directory = directory.as_ref().to_path_buf();
        let catalog = StartupCatalog::discover_in(env, &directory)?;
        if !catalog.quarantined.is_empty() {
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has {} quarantined artifact bundle(s); refusing to open a subset \
                 of an ambiguous range",
                directory.display(),
                catalog.quarantined.len()
            )));
        }
        if catalog.entries.is_empty() {
            return Ok(None);
        }

        let mut sealed_paths: Vec<(u64, PathBuf)> = Vec::new();
        let mut active_path: Option<PathBuf> = None;
        for entry in &catalog.entries {
            match entry.state {
                CatalogSegmentState::Sealed => {
                    sealed_paths.push((entry.descriptor.base_offset, entry.path.clone()));
                }
                CatalogSegmentState::Active => {
                    if active_path.is_some() {
                        // Discovery already quarantines this, so reaching here
                        // means the two disagree — refuse rather than pick.
                        return Err(LogError::InvalidDescriptor(
                            "range has more than one active segment".to_owned(),
                        ));
                    }
                    active_path = Some(entry.path.clone());
                }
            }
        }
        sealed_paths.sort_by_key(|(base, _)| *base);

        let Some(active_path) = active_path else {
            // A crash between sealing a segment and creating its successor
            // leaves a range whose tail is closed. That is recoverable — open a
            // new active at its end — but it is a decision for a writer, not
            // something a reader should do silently.
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has no active segment; its tail was sealed without a successor",
                directory.display()
            )));
        };

        let mut sealed = Vec::with_capacity(sealed_paths.len());
        for (_, path) in sealed_paths {
            sealed.push(SegmentReader::open_in(env, &path)?);
        }
        let active = ActiveSegment::recover_in(env, &active_path)?;

        let set = Self {
            env: env.clone(),
            directory,
            sealed,
            active: Some(active),
        };
        set.validate_contiguous()?;
        Ok(Some(set))
    }

    /// Create a range's first segment.
    pub fn create_in(
        env: &Env,
        directory: impl AsRef<Path>,
        descriptor: SegmentDescriptor,
        config: crate::SegmentConfig,
    ) -> VtopLogResult<Self> {
        let directory = directory.as_ref().to_path_buf();
        let path = directory.join(format!("{}.active", segment_stem(descriptor.base_offset)));
        let active = ActiveSegment::create_in(env, &path, descriptor, config)?;
        Ok(Self {
            env: env.clone(),
            directory,
            sealed: Vec::new(),
            active: Some(active),
        })
    }

    /// Every segment must begin where the previous one ended.
    ///
    /// A gap is unreadable and an overlap means two segments claim the same
    /// offset. Discovery rejects overlaps on its own; this also catches a gap,
    /// which it does not, because a gap looks like two individually valid
    /// segments.
    fn validate_contiguous(&self) -> VtopLogResult<()> {
        let mut expected: Option<u64> = None;
        for reader in &self.sealed {
            if let Some(expected) = expected {
                if reader.base_offset() != expected {
                    return Err(LogError::InvalidDescriptor(format!(
                        "segment begins at offset {} but the previous one ended at {expected}",
                        reader.base_offset()
                    )));
                }
            }
            expected = Some(reader.next_offset());
        }
        if let Some(expected) = expected {
            if self.tail().base_offset() != expected {
                return Err(LogError::InvalidDescriptor(format!(
                    "active segment begins at offset {} but the last sealed one ended at \
                     {expected}",
                    self.tail().base_offset()
                )));
            }
        }
        Ok(())
    }

    /// Where this range's segments live.
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// First offset the range holds.
    pub fn base_offset(&self) -> u64 {
        self.sealed
            .first()
            .map(SegmentReader::base_offset)
            .unwrap_or_else(|| self.tail().base_offset())
    }

    /// First offset it does not hold.
    pub fn next_offset(&self) -> u64 {
        self.tail().next_offset()
    }

    /// Durably committed frontier of the range.
    pub fn committed_offset(&self) -> u64 {
        self.tail().committed_offset()
    }

    /// Sealed segments, oldest first. This is what a transfer ships and what
    /// retention would delete from.
    pub fn sealed(&self) -> &[SegmentReader] {
        &self.sealed
    }

    pub fn active(&self) -> &ActiveSegment {
        self.tail()
    }

    pub fn active_mut(&mut self) -> &mut ActiveSegment {
        self.tail_mut()
    }

    /// The tail. Present except transiently inside [`Self::roll`].
    fn tail(&self) -> &ActiveSegment {
        self.active
            .as_ref()
            .expect("the tail is absent only during a roll")
    }

    fn tail_mut(&mut self) -> &mut ActiveSegment {
        self.active
            .as_mut()
            .expect("the tail is absent only during a roll")
    }

    /// Append a group, rolling if it does not fit in the tail.
    ///
    /// The bound is not re-derived here. `append_group` already refuses a batch
    /// that would exceed `max_segment_bytes`, and it refuses BEFORE writing any
    /// bytes — so the refusal is the signal to roll, and a second size estimate
    /// living here could only disagree with the one that matters.
    ///
    /// That refusal is what a range hitting its limit does today: it errors.
    /// Rolling turns it into a boundary.
    ///
    /// The retry lands the WHOLE group in the successor. A group split across a
    /// roll would leave half of a producer's commit group either side of a
    /// boundary, and the two halves could then be transferred, truncated, or
    /// retained independently of each other.
    pub fn append_group(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
        successor_id: Uuid,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        match self.tail_mut().append_group(records, durability) {
            Err(LogError::SegmentByteLimit { .. }) | Err(LogError::SegmentRecordLimit { .. }) => {}
            other => return other,
        }
        // An empty tail that still cannot hold the group means the group is
        // larger than a whole segment. Rolling would produce another empty
        // segment and fail identically, so report the original limit rather
        // than looping.
        if self.tail().next_offset() == self.tail().base_offset() {
            return self.tail_mut().append_group(records, durability);
        }
        self.roll(successor_id)?;
        self.tail_mut().append_group(records, durability)
    }

    /// Seal the tail and open a successor at its end.
    pub fn roll(&mut self, successor_id: Uuid) -> VtopLogResult<()> {
        // A segment holding nothing would seal into an empty sealed segment and
        // an identically-based successor, so rolling it is a no-op that costs a
        // file.
        if self.tail().next_offset() == self.tail().base_offset() {
            return Ok(());
        }
        let active = self
            .active
            .take()
            .expect("the tail is absent only during a roll");
        // If this fails the tail stays `None`, which every accessor treats as a
        // programming error rather than a state to serve from. That is
        // deliberate: a set whose roll failed part-way has no coherent tail, and
        // continuing to answer reads from a half-rolled range would be worse
        // than stopping.
        let (sealed, successor) = roll_in(&self.env, active, successor_id)?;
        self.active = Some(successor);
        self.sealed.push(sealed);
        Ok(())
    }

    /// Discard every record at or above `offset`.
    ///
    /// The repair for a replica that diverged from its range's current
    /// leadership (#240).
    ///
    /// # Only cuts inside the tail, deliberately
    ///
    /// A cut that would delete or replace a sealed segment is REFUSED. It is
    /// not a missing feature so much as a missing crash-recovery story, and the
    /// three ways it goes wrong are worth naming because each one produces a
    /// range that will not reopen:
    ///
    /// * Deleting the tail and creating its replacement cannot be ordered
    ///   safely without a durable record of the intent. Replacement-first
    ///   leaves two active segments, which discovery quarantines; delete-first
    ///   leaves none, which nothing can distinguish from a range whose tail was
    ///   lost. Recovery has to be able to FINISH an interrupted truncation, and
    ///   that needs a marker this format does not yet have.
    /// * A partial failure part-way through deleting segments leaves this value
    ///   describing a range that no longer exists on disk, while the caller has
    ///   only been handed an error.
    /// * The replacement tail would begin with no inherited producer frontier,
    ///   so the next append from a producer already in the retained prefix is
    ///   rejected as `FirstSequence` — the same failure the `.producers`
    ///   sidecar exists to prevent, reintroduced somewhere new.
    ///
    /// None of this is reachable in the running system today: nothing rolls,
    /// so every range is one active segment and every cut lands inside it. That
    /// is exactly why it refuses rather than doing something partial — the
    /// refusal is unreachable now and correct later, whereas a half-implemented
    /// cross-segment truncation would be silently wrong the moment rolling is
    /// switched on.
    pub fn truncate_to(&mut self, offset: u64) -> VtopLogResult<crate::TruncateOutcome> {
        if offset > self.next_offset() {
            return Err(LogError::TruncateBeyondTail {
                requested: offset,
                next_offset: self.next_offset(),
            });
        }
        if offset < self.tail().base_offset() {
            return Err(LogError::InvalidConfig(format!(
                "cannot truncate to offset {offset}: it is below the active segment's start \
                 {}, and truncating across sealed segments needs a durable record of the \
                 intent so an interrupted repair can be finished rather than leaving a range \
                 that cannot reopen",
                self.tail().base_offset()
            )));
        }
        self.tail_mut().truncate_to(offset)
    }

    /// Read from `start_offset`, crossing segment boundaries as needed.
    ///
    /// Visibility is clamped at `high_watermark` exactly as a single segment
    /// clamps it, so a set never exposes more than the same records would have
    /// been in one file.
    pub fn fetch_through(
        &mut self,
        start_offset: u64,
        max_bytes: usize,
        max_records: usize,
        high_watermark: u64,
    ) -> VtopLogResult<FetchBatch> {
        let high_watermark = high_watermark.min(self.committed_offset());
        let mut records: Vec<FetchedRecord> = Vec::new();
        let mut encoded_bytes = 0_usize;
        let mut cursor = start_offset.max(self.base_offset());

        while cursor < high_watermark && records.len() < max_records && encoded_bytes < max_bytes {
            let remaining_bytes = max_bytes - encoded_bytes;
            let remaining_records = max_records - records.len();
            let batch =
                self.fetch_one(cursor, remaining_bytes, remaining_records, high_watermark)?;
            if batch.records.is_empty() {
                // No progress: either the caller's byte budget cannot fit the
                // next record, or the cursor has reached the readable end.
                // Returning what we have beats looping.
                break;
            }
            encoded_bytes += batch.encoded_bytes;
            cursor = batch.next_offset;
            records.extend(batch.records);
        }

        Ok(FetchBatch {
            records,
            encoded_bytes,
            next_offset: cursor,
            high_watermark,
        })
    }

    /// Read from whichever single segment holds `offset`.
    fn fetch_one(
        &mut self,
        offset: u64,
        max_bytes: usize,
        max_records: usize,
        high_watermark: u64,
    ) -> VtopLogResult<FetchBatch> {
        if offset >= self.tail().base_offset() {
            return self
                .tail_mut()
                .fetch_through(offset, max_bytes, max_records, high_watermark);
        }
        let index = self
            .sealed
            .iter()
            .position(|reader| offset < reader.next_offset())
            .ok_or_else(|| {
                LogError::InvalidCursor(format!("offset {offset} is not in this range"))
            })?;
        // The caller's watermark is passed through, not just the segment's own
        // frontier. A watermark falling INSIDE a sealed segment would otherwise
        // be ignored and the read would return records above it — the one thing
        // a fetch path must never do. The segment still caps at its own
        // manifest, so the read stops at the boundary and the loop above moves
        // on to the next segment.
        self.sealed[index].fetch_through(offset, max_bytes, max_records, high_watermark)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{KeyRange, RangeLineage, SegmentConfig};
    use tempfile::tempdir;

    fn descriptor() -> SegmentDescriptor {
        SegmentDescriptor {
            segment_id: Uuid::from_u128(1),
            topic: "events.v1".to_owned(),
            topic_epoch: 7,
            lineage: RangeLineage {
                range_id: Uuid::from_u128(2),
                generation: 0,
                key_range: KeyRange::full(),
                parents: Vec::new(),
            },
            base_offset: 0,
        }
    }

    fn config() -> SegmentConfig {
        SegmentConfig {
            max_record_bytes: 256,
            max_group_bytes: 512,
            // Small enough that a handful of records fills a segment, so the
            // roll path is exercised rather than described.
            max_segment_bytes: 512,
            max_segment_records: 100,
            index_stride: 2,
        }
    }

    fn record(producer: Uuid, sequence: u64) -> LogRecord {
        LogRecord {
            producer_id: producer,
            producer_epoch: 0,
            sequence,
            timestamp_millis: 1_700_000_000_000 + sequence as i64,
            attributes: 0,
            key: b"key".to_vec(),
            value: format!("value-{sequence:04}").into_bytes(),
        }
    }

    /// Write enough to force several rolls, then read the whole range back in
    /// one call. The read must cross every boundary without the caller knowing
    /// there were any.
    #[test]
    fn a_read_crosses_every_segment_boundary() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(9);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();

        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(1000 + sequence as u128),
            )
            .unwrap();
        }

        assert!(
            !set.sealed().is_empty(),
            "40 records into 512-byte segments must have rolled at least once"
        );
        assert_eq!(set.next_offset(), 40);

        let batch = set.fetch_through(0, 1 << 20, 100, 40).unwrap();
        assert_eq!(
            batch.records.len(),
            40,
            "one read must return the whole range, not the first segment's worth"
        );
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
            assert_eq!(
                fetched.record.value,
                format!("value-{index:04}").into_bytes(),
                "records must come back in order across boundaries"
            );
        }
    }

    /// A read starting inside a sealed segment still crosses into the active
    /// one — the boundary is invisible from either side.
    #[test]
    fn a_read_starting_mid_range_crosses_into_the_active_segment() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(11);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(2000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(5, 1 << 20, 100, 30).unwrap();
        assert_eq!(batch.records.first().unwrap().offset, 5);
        assert_eq!(batch.records.last().unwrap().offset, 29);
        assert_eq!(batch.records.len(), 25);
    }

    /// The caller's record limit is honoured across boundaries rather than
    /// being reset per segment.
    #[test]
    fn caller_limits_are_honoured_across_boundaries() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(13);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(3000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(0, 1 << 20, 7, 30).unwrap();
        assert_eq!(batch.records.len(), 7);
        assert_eq!(batch.next_offset, 7);
    }

    /// Visibility is clamped at the high-water mark exactly as one segment
    /// clamps it: a set must never expose more than the same records would have
    /// in a single file.
    #[test]
    fn a_set_never_reads_past_the_high_water_mark() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(17);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..30 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(4000 + sequence as u128),
            )
            .unwrap();
        }

        let batch = set.fetch_through(0, 1 << 20, 100, 12).unwrap();
        assert_eq!(batch.records.len(), 12);
        assert_eq!(batch.high_watermark, 12);
    }

    /// A rolled range reopens from disk as one range, with its segments in
    /// order and its producer sequences intact.
    #[test]
    fn a_rolled_range_reopens_as_one_range() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(19);
        {
            let mut set =
                SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config())
                    .unwrap();
            for sequence in 0..30 {
                set.append_group(
                    &[record(producer, sequence)],
                    Durability::Fsync,
                    Uuid::from_u128(5000 + sequence as u128),
                )
                .unwrap();
            }
        }

        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range exists");
        assert_eq!(reopened.next_offset(), 30);
        assert_eq!(reopened.base_offset(), 0);

        let batch = reopened.fetch_through(0, 1 << 20, 100, 30).unwrap();
        assert_eq!(batch.records.len(), 30);

        // And it still accepts the next sequence: the producer frontier
        // survived both the rolls and the reopen.
        reopened
            .append_group(
                &[record(producer, 30)],
                Durability::Fsync,
                Uuid::from_u128(6000),
            )
            .unwrap();
        assert_eq!(reopened.next_offset(), 31);
    }

    /// An empty directory is not an error — it is a range that does not exist
    /// yet, which the caller creates.
    #[test]
    fn an_empty_directory_reports_no_range() {
        let directory = tempdir().unwrap();
        assert!(SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .is_none());
    }

    /// REGRESSION. A high-water mark falling INSIDE a sealed segment must still
    /// bound the read.
    ///
    /// `SegmentReader::fetch` caps at the segment's own manifest frontier, and
    /// routing to it without passing the caller's watermark returned records
    /// above that watermark — exposing data no quorum had acknowledged.
    ///
    /// The earlier watermark test passed only because the boundary happened to
    /// fall favourably for the value it chose. This one finds the first sealed
    /// segment's real end and puts the watermark strictly inside it, so it
    /// cannot pass by luck.
    #[test]
    fn a_watermark_inside_a_sealed_segment_still_bounds_the_read() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(23);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(7000 + sequence as u128),
            )
            .unwrap();
        }

        let first_sealed_end = set
            .sealed()
            .first()
            .expect("40 records must have rolled")
            .next_offset();
        assert!(
            first_sealed_end > 1,
            "need a sealed segment holding at least two records"
        );
        // Strictly inside the first sealed segment.
        let watermark = first_sealed_end - 1;

        let batch = set.fetch_through(0, 1 << 20, 1000, watermark).unwrap();
        assert_eq!(
            batch.records.len() as u64,
            watermark,
            "a read must stop at the caller's watermark even when it falls inside \
             a sealed segment"
        );
        assert!(
            batch.records.iter().all(|r| r.offset < watermark),
            "no record at or above the watermark may be returned"
        );
        assert_eq!(batch.next_offset, watermark);
    }

    /// A cut inside the tail is the single-segment case, unchanged.
    #[test]
    fn a_cut_inside_the_tail_truncates_only_the_tail() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(29);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(8000 + sequence as u128),
            )
            .unwrap();
        }
        let sealed_before = set.sealed().len();
        let cut = set.active().base_offset() + 1;

        let outcome = set.truncate_to(cut).unwrap();
        assert_eq!(outcome.next_offset, cut);
        assert_eq!(set.next_offset(), cut);
        assert_eq!(
            set.sealed().len(),
            sealed_before,
            "a cut inside the tail must not disturb sealed segments"
        );

        // And the range reopens, which is what proves nothing was left
        // inconsistent on disk.
        drop(set);
        let reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the truncated range must still open");
        assert_eq!(reopened.next_offset(), cut);
    }

    /// A cut below the tail is refused rather than half-done.
    ///
    /// Honouring it means deleting and replacing segments, and that cannot be
    /// ordered crash-safely without a durable record of the intent:
    /// replacement-first leaves two active segments, delete-first leaves none,
    /// and either way the range will not reopen.
    #[test]
    fn a_cut_below_the_tail_is_refused() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(37);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(10_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(!set.sealed().is_empty(), "need at least one sealed segment");
        let below = set.sealed()[0].base_offset();

        assert!(matches!(
            set.truncate_to(below),
            Err(LogError::InvalidConfig(_))
        ));
        assert_eq!(
            set.next_offset(),
            40,
            "a refused truncation must leave the range untouched"
        );
    }

    /// Truncation removes records; it can never invent them.
    #[test]
    fn truncating_a_range_beyond_its_tail_is_refused() {
        let directory = tempdir().unwrap();
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        set.append_group(
            &[record(Uuid::from_u128(41), 0)],
            Durability::Fsync,
            Uuid::from_u128(11_000),
        )
        .unwrap();

        assert!(matches!(
            set.truncate_to(99),
            Err(LogError::TruncateBeyondTail { requested: 99, .. })
        ));
    }
}
