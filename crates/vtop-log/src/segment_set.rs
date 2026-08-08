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
use crate::producer_snapshot::ProducerSnapshot;
use crate::segment::{io_error, roll_in, segment_stem, write_atomic, ActiveSegment, SegmentReader};
use crate::truncate_intent::{DoomedSegment, TruncateIntent, TRUNCATE_INTENT_FILE};
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
    /// `Option` only so the tail can be moved out by value: during a roll,
    /// which `roll_in` requires, and during a cross-segment truncation, which
    /// deletes the file the tail has open. It is `None` for the duration of
    /// those calls — and permanently if either fails part-way, which every
    /// accessor treats as a programming error rather than a state to serve
    /// from. An earlier version parked a throwaway segment in the range
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
        // A truncation that died between its marker and its final rename is
        // not an ambiguous layout: the marker says exactly which segments
        // were doomed and what replaces them. Finish it BEFORE reading the
        // layout, so the discovery below sees the completed range instead of
        // quarantining the half-made one. A marker that does not decode is
        // different — it names an intent that cannot be honoured — so it
        // falls through to discovery, which quarantines it by name, and the
        // open refuses like any other ambiguity.
        let marker = directory.join(TRUNCATE_INTENT_FILE);
        if env
            .storage
            .exists(&marker)
            .map_err(|source| io_error(&marker, source))?
        {
            let decoded = env
                .storage
                .read(&marker)
                .map_err(|source| io_error(&marker, source))
                .and_then(|bytes| TruncateIntent::decode(&bytes));
            if let Ok(intent) = decoded {
                finish_truncation(env, &directory, &intent)?;
            }
        }
        let catalog = StartupCatalog::discover_in(env, &directory)?;
        if !catalog.quarantined.is_empty() {
            // Name every quarantined bundle and its reasons in the refusal.
            // This error is what an operator sees at startup, and "N
            // bundles are quarantined" gives them nothing to act on — the
            // reason is the difference between deleting a stray .tmp file
            // and restoring from a replica.
            let details = catalog
                .quarantined
                .iter()
                .map(|bundle| {
                    let paths = bundle
                        .paths
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("[{paths}: {:?}]", bundle.reasons)
                })
                .collect::<Vec<_>>()
                .join("; ");
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has {} quarantined artifact bundle(s); refusing to open a subset \
                 of an ambiguous range: {details}",
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

    /// Open a received sealed prefix and mint the tail it needs to serve.
    ///
    /// The last unmet piece of #270. Segment transfer lands sealed segments and
    /// nothing else, and [`Self::open_in`] refuses a range whose tail is
    /// sealed:
    ///
    /// > range at {} has no active segment; its tail was sealed without a
    /// > successor
    ///
    /// That refusal is right and stays: a reader must not decide to extend a
    /// range it was only asked to read. Adoption is the writer that gets to
    /// decide, and this is the one place that decision is made.
    ///
    /// REFUSES A DIRECTORY THAT ALREADY HAS A TAIL, rather than adopting around
    /// it. A tail means the range is already live, and minting a second one
    /// would produce two writers for the same offsets — a split range that
    /// discovery would then quarantine, if it were lucky enough to notice.
    /// `open_in` is the call for that directory.
    pub fn adopt_in(
        env: &Env,
        directory: impl AsRef<Path>,
        successor_id: Uuid,
    ) -> VtopLogResult<Self> {
        let directory = directory.as_ref().to_path_buf();
        let catalog = StartupCatalog::discover_in(env, &directory)?;
        if !catalog.quarantined.is_empty() {
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} has {} quarantined artifact bundle(s); refusing to adopt an \
                 ambiguous range, because a tail minted onto a prefix with a hole in it would \
                 make the hole permanent",
                directory.display(),
                catalog.quarantined.len()
            )));
        }

        let mut sealed_paths: Vec<(u64, PathBuf)> = Vec::new();
        for entry in &catalog.entries {
            match entry.state {
                CatalogSegmentState::Sealed => {
                    sealed_paths.push((entry.descriptor.base_offset, entry.path.clone()))
                }
                CatalogSegmentState::Active => {
                    return Err(LogError::InvalidDescriptor(format!(
                        "range at {} already has an active segment at {}; adoption mints a tail \
                         and this range has one, so a second would give the same offsets two \
                         writers. Use `open_in` for a range that is already live.",
                        directory.display(),
                        entry.path.display()
                    )))
                }
            }
        }
        if sealed_paths.is_empty() {
            return Err(LogError::InvalidDescriptor(format!(
                "range at {} holds no sealed segments; there is nothing to adopt and no offset \
                 to begin a tail at. Use `create_in` for a new range.",
                directory.display()
            )));
        }
        sealed_paths.sort_by_key(|(base, _)| *base);

        let mut sealed = Vec::with_capacity(sealed_paths.len());
        for (_, path) in &sealed_paths {
            sealed.push(SegmentReader::open_in(env, path)?);
        }
        // Contiguity BEFORE the tail is minted. The wire decoder refuses a
        // gapped listing (#292) and discovery refuses an ambiguous directory,
        // but neither covers bytes that arrived by other means — a restored
        // backup, a hand-copied directory. Minting a tail onto a prefix with a
        // hole would bless the hole: the range opens from then on, and the
        // missing offsets are simply never served.
        for pair in sealed.windows(2) {
            if pair[0].next_offset() != pair[1].base_offset() {
                return Err(LogError::InvalidDescriptor(format!(
                    "cannot adopt {}: sealed segments are not contiguous — {} ends at offset {} \
                     but {} begins at {}. Adopting would make the gap permanent.",
                    directory.display(),
                    pair[0].path().display(),
                    pair[0].next_offset(),
                    pair[1].path().display(),
                    pair[1].base_offset()
                )));
            }
        }

        let last = sealed.last().expect("checked non-empty above");
        let active = crate::segment::open_successor_in(env, last.path(), successor_id)?;

        let set = Self {
            env: env.clone(),
            directory,
            sealed,
            active: Some(active),
        };
        // The same invariant `open_in` ends on, so an adopted range and a
        // reopened one are the same thing by the time either is returned.
        set.validate_contiguous()?;
        Ok(set)
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

    /// Append a group, minting the successor's identity from this set's own
    /// environment if the append forces a roll.
    ///
    /// [`Self::append_group`] takes the successor id as a parameter because
    /// crash sweeps need it deterministic per step. A broker's produce path
    /// has no meaningful id to choose, and threading a UUID through every
    /// produce call just to discard it when no roll happens would put the
    /// minting far from the one place that already owns an rng — the same
    /// place cross-segment truncation mints its replacement id.
    pub fn append_group_minting(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        let successor_id = Uuid::from_u128(self.env.rng.next_u128());
        self.append_group(records, durability, successor_id)
    }

    /// Advance the tail's durable commit boundary.
    ///
    /// Sealed segments are durable by construction — sealing commits first —
    /// so committing a range is exactly committing its tail. This exists so a
    /// follower batching appends under one durability barrier does not have
    /// to know which segment the barrier lands on.
    pub fn commit(&mut self) -> VtopLogResult<u64> {
        self.tail_mut().commit()
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

    /// The tail. Present except transiently inside [`Self::roll`] and
    /// [`Self::truncate_to`]'s cross-segment path — or permanently after
    /// either fails, which poisons the set.
    fn tail(&self) -> &ActiveSegment {
        self.active
            .as_ref()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed")
    }

    fn tail_mut(&mut self) -> &mut ActiveSegment {
        self.active
            .as_mut()
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed")
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
    /// Append to the tail WITHOUT rolling at the bound; the limit errors
    /// surface exactly as a single-segment range reported them.
    ///
    /// For callers whose durability is being deliberately withheld — the
    /// fault harness's fsync hold. A roll seals the tail, and sealing makes
    /// bytes durable, which would silently commit records the injection
    /// promised were still at risk of dying with a crash. Refusing at the
    /// bound keeps that promise exact; nothing in production appends through
    /// this.
    pub fn append_group_tail_only(
        &mut self,
        records: &[LogRecord],
        durability: Durability,
    ) -> VtopLogResult<Vec<crate::AppendOutcome>> {
        self.tail_mut().append_group(records, durability)
    }

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
            .expect("the tail is absent only mid-roll, mid-truncation, or after one failed");
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
    /// A cut inside the tail is the single-segment case, unchanged. A cut
    /// below the tail deletes whole segments and replaces the tail, which is
    /// only crash-safe under a durable intent marker; see
    /// [`Self::truncate_across_segments`] for the protocol.
    pub fn truncate_to(&mut self, offset: u64) -> VtopLogResult<crate::TruncateOutcome> {
        if offset > self.next_offset() {
            return Err(LogError::TruncateBeyondTail {
                requested: offset,
                next_offset: self.next_offset(),
            });
        }
        if offset < self.tail().base_offset() {
            return self.truncate_across_segments(offset);
        }
        self.tail_mut().truncate_to(offset)
    }

    /// Honour a cut below the tail: delete every segment at or above the cut
    /// and open a replacement tail there.
    ///
    /// # The marker is what makes this survivable
    ///
    /// Deleting the tail and creating its replacement cannot be ordered
    /// crash-safely on their own. Replacement-first leaves two active
    /// segments, which discovery quarantines; delete-first leaves none, which
    /// nothing can distinguish from a range whose tail was lost. So the
    /// intent is made durable FIRST — `range.truncate-intent`, at the range
    /// directory level, because a marker inside a segment being deleted would
    /// not survive the thing it protects. From that point an interruption at
    /// any step is finishable: [`finish_truncation`] is idempotent and
    /// [`Self::open_in`] runs it before reading the layout.
    ///
    /// # Cuts land on sealed-segment boundaries, deliberately
    ///
    /// A cut strictly inside a sealed segment would need the retained part of
    /// an immutable file rewritten into the replacement tail — a different
    /// repair, with its own recovery story, that deleting whole segments does
    /// not need. Refusing it loudly beats a half-implemented version of it.
    ///
    /// # The replacement inherits the retained prefix's producer frontier
    ///
    /// The first doomed segment begins at the cut, so the frontier IT
    /// inherited — its `.producers` sidecar — is exactly the producer state
    /// of everything retained. Those bytes are embedded in the marker (the
    /// sidecar itself shares a filename stem with the doomed segment and dies
    /// with it) and become the replacement's own sidecar. Without this, the
    /// next append from a producer already in the retained prefix would be
    /// rejected as `FirstSequence` — the exact failure `.producers` exists to
    /// prevent.
    ///
    /// # On failure the set poisons
    ///
    /// Once the marker is durable, the directory changes underneath this
    /// value, and an error part-way would leave it describing segments that
    /// no longer exist on disk. Ownership of the tail is therefore given up
    /// before any file is touched and only restored on success — the same
    /// posture as a failed [`Self::roll`], where every accessor treats the
    /// missing tail as a programming error rather than a state to serve
    /// from. Refusing to serve is safe precisely BECAUSE the marker is
    /// durable: the next open finishes the truncation instead of reopening
    /// the wreckage.
    fn truncate_across_segments(&mut self, offset: u64) -> VtopLogResult<crate::TruncateOutcome> {
        // The marker records a v1 segment identity. A v2 range would need the
        // v2 descriptor fields carried too; until the format does, refuse
        // loudly rather than have recovery rebuild a tail with the wrong
        // identity.
        if self.tail().descriptor_v2().is_some()
            || self.sealed.iter().any(|r| r.descriptor_v2().is_some())
        {
            return Err(LogError::InvalidConfig(
                "cannot truncate a v2 range across segments: the truncation intent marker \
                 records a v1 segment identity"
                    .to_owned(),
            ));
        }
        let Some(cut_index) = self
            .sealed
            .iter()
            .position(|reader| reader.base_offset() == offset)
        else {
            return Err(LogError::InvalidConfig(format!(
                "cannot truncate to offset {offset}: it is below the active segment's start \
                 {} and does not land on a sealed segment boundary; a cut inside a sealed \
                 segment would need the retained part of an immutable file rewritten, which \
                 is a different repair than deleting whole segments",
                self.tail().base_offset()
            )));
        };

        // Read the inherited frontier back from the sidecar rather than
        // trusting memory: these are the bytes that survive, and recovery
        // will have nothing else. Absent reads as empty, exactly as it does
        // for a range's first segment.
        let producers_path = self
            .directory
            .join(format!("{}.producers", segment_stem(offset)));
        let inherited = if self
            .env
            .storage
            .exists(&producers_path)
            .map_err(|source| io_error(&producers_path, source))?
        {
            let bytes = self
                .env
                .storage
                .read(&producers_path)
                .map_err(|source| io_error(&producers_path, source))?;
            ProducerSnapshot::decode(&bytes)?
        } else {
            ProducerSnapshot::default()
        };

        let mut replacement = self.tail().v1_descriptor_view();
        replacement.segment_id = Uuid::from_u128(self.env.rng.next_u128());
        replacement.base_offset = offset;
        let mut doomed: Vec<DoomedSegment> = self.sealed[cut_index..]
            .iter()
            .map(|reader| DoomedSegment {
                segment_id: reader.manifest().descriptor.segment_id,
                base_offset: reader.base_offset(),
            })
            .collect();
        doomed.push(DoomedSegment {
            segment_id: self.tail().v1_descriptor_view().segment_id,
            base_offset: self.tail().base_offset(),
        });
        let intent = TruncateIntent {
            target_offset: offset,
            replacement,
            config: self.tail().config(),
            doomed,
            inherited,
        };
        let records_removed = self.next_offset() - offset;
        let bytes_removed = self.sealed[cut_index..]
            .iter()
            .map(|reader| reader.manifest().content_bytes)
            .sum::<u64>()
            + self.tail().content_bytes();

        // Nothing above changed any state, so every refusal so far left the
        // range untouched. This write is the point of no return: once the
        // marker is durable the truncation WILL complete — in this call, or
        // in the next open if this process dies first.
        write_atomic(
            &self.env,
            &self.directory.join(TRUNCATE_INTENT_FILE),
            &intent.encode()?,
        )?;

        // Close every handle onto doomed files before deleting them, and
        // give up the tail so a failure below leaves the set refusing to
        // serve rather than serving a range that is no longer on disk.
        drop(
            self.active
                .take()
                .expect("the tail is absent only mid-roll, mid-truncation, or after one failed"),
        );
        self.sealed.truncate(cut_index);
        finish_truncation(&self.env, &self.directory, &intent)?;
        // Reopen through recovery rather than keeping a create-time handle:
        // recovery is the path that seeds producer state from the sidecar,
        // so the in-memory tail and the on-disk one cannot disagree about
        // what was inherited.
        let replacement_path = self
            .directory
            .join(format!("{}.active", segment_stem(offset)));
        self.active = Some(ActiveSegment::recover_in(&self.env, &replacement_path)?);
        Ok(crate::TruncateOutcome {
            records_removed,
            bytes_removed,
            next_offset: offset,
        })
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

/// An already-open tail is a set of one.
///
/// The broker grew up over a single `ActiveSegment`, and every constructor
/// and harness that hands one over is describing exactly the range this
/// conversion produces: no sealed prefix, one tail. Making it a `From` lets
/// those callers keep their shape while the node hands over a catalog-opened
/// set instead — the conversion is lossless, whereas the reverse (a set down
/// to its tail) would silently discard sealed segments and is deliberately
/// not provided.
///
/// The environment and directory come from the segment itself, not from the
/// caller: a tail opened against the simulator must roll its successor onto
/// the simulator, and its parent directory is by definition where its range
/// lives.
impl From<ActiveSegment> for SegmentSet {
    fn from(active: ActiveSegment) -> Self {
        let env = active.env().clone();
        let directory = active
            .path()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        Self {
            env,
            directory,
            sealed: Vec::new(),
            active: Some(active),
        }
    }
}

/// Carry a durably recorded truncation to completion.
///
/// Idempotent by construction: every step either deletes something that may
/// already be gone or recreates the replacement from what the marker says, so
/// it can run after a crash at ANY point — including inside a previous run of
/// itself. That is why the replacement is deleted and rebuilt even when a
/// prior attempt already created it: the marker is the one source of truth,
/// and rebuilding from it is what makes "how far did the last attempt get"
/// a question nobody has to answer. Nothing is lost by the rebuild — the
/// replacement cannot have accepted an append until the marker is gone.
///
/// Removing the marker is the commit point, and its removal is made durable
/// before returning: a resurrected marker would re-run this function and
/// delete a replacement that HAS accepted appends by then.
fn finish_truncation(env: &Env, directory: &Path, intent: &TruncateIntent) -> VtopLogResult<()> {
    // Sweep debris of an interrupted attempt first. A crash inside one of
    // this function's own atomic writes can leave a `.{name}.{uuid}.tmp`
    // behind, and the discovery that follows would quarantine it as an
    // incomplete write — leaving the range unopenable even though the
    // truncation itself completed. Only debris this truncation could have
    // produced is touched.
    remove_truncation_debris(env, directory, intent)?;
    for doomed in &intent.doomed {
        remove_segment_files(env, directory, doomed.base_offset)?;
    }
    // Deletions must be durable before anything is built on top of them: a
    // crash after the marker clears but before a deletion lands would leave
    // a doomed segment beside the replacement with no marker left to explain
    // it.
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))?;

    let stem = segment_stem(intent.target_offset);
    if !intent.inherited.is_empty() {
        // Sidecar before the segment file, matching `roll_in`: a tail must
        // never exist without the frontier that makes it readable.
        write_atomic(
            env,
            &directory.join(format!("{stem}.producers")),
            &intent.inherited.encode()?,
        )?;
    }
    let replacement_path = directory.join(format!("{stem}.active"));
    drop(ActiveSegment::create_in(
        env,
        &replacement_path,
        intent.replacement.clone(),
        intent.config,
    )?);

    let marker = directory.join(TRUNCATE_INTENT_FILE);
    env.storage
        .remove_file(&marker)
        .map_err(|source| io_error(&marker, source))?;
    env.storage
        .sync_dir(directory)
        .map_err(|source| io_error(directory, source))
}

/// Delete every file a segment beginning at `base_offset` could own, present
/// or not. Whether it was active or sealed at the crash is unknowable and
/// does not matter; a leftover sidecar is an orphan, which discovery
/// quarantines, so a partial delete would turn the repair into a range that
/// refuses to open.
fn remove_segment_files(env: &Env, directory: &Path, base_offset: u64) -> VtopLogResult<()> {
    let stem = segment_stem(base_offset);
    for name in [
        format!("{stem}.active"),
        format!("{stem}.segment"),
        format!("{stem}.commit"),
        format!("{stem}.index"),
        format!("{stem}.manifest.json"),
        format!("{stem}.chunks"),
        format!("{stem}.producers"),
    ] {
        let path = directory.join(name);
        if env
            .storage
            .exists(&path)
            .map_err(|source| io_error(&path, source))?
        {
            env.storage
                .remove_file(&path)
                .map_err(|source| io_error(&path, source))?;
        }
    }
    Ok(())
}

/// Delete leftover `.{name}.{uuid}.tmp` files from an interrupted attempt at
/// THIS truncation: the marker's own, or a sidecar of a doomed stem (the
/// replacement shares the first doomed stem, so its debris is covered too).
/// Anything else's temporary files are not this repair's to judge.
fn remove_truncation_debris(
    env: &Env,
    directory: &Path,
    intent: &TruncateIntent,
) -> VtopLogResult<()> {
    let stems: Vec<String> = intent
        .doomed
        .iter()
        .map(|doomed| format!(".{}.", segment_stem(doomed.base_offset)))
        .collect();
    let marker_prefix = format!(".{TRUNCATE_INTENT_FILE}.");
    let entries = env
        .storage
        .read_dir(directory)
        .map_err(|source| io_error(directory, source))?;
    for entry in entries {
        let Some(name) = entry.path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !(name.starts_with('.') && name.ends_with(".tmp")) {
            continue;
        }
        if name.starts_with(&marker_prefix) || stems.iter().any(|stem| name.starts_with(stem)) {
            env.storage
                .remove_file(&entry.path)
                .map_err(|source| io_error(&entry.path, source))?;
        }
    }
    Ok(())
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

    /// A cut below the tail that does not land on a sealed segment boundary
    /// is refused rather than half-done.
    ///
    /// Honouring it would mean rewriting the retained part of an immutable
    /// sealed segment into the replacement tail — a different repair from
    /// deleting whole segments, with its own recovery story the intent
    /// marker does not carry.
    #[test]
    fn a_cut_inside_a_sealed_segment_is_refused() {
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
        let inside = set.sealed()[0].base_offset() + 1;
        assert!(
            inside < set.sealed()[0].next_offset(),
            "the cut must fall strictly inside the first sealed segment"
        );

        assert!(matches!(
            set.truncate_to(inside),
            Err(LogError::InvalidConfig(_))
        ));
        assert_eq!(
            set.next_offset(),
            40,
            "a refused truncation must leave the range untouched"
        );
        assert!(
            !directory.path().join(TRUNCATE_INTENT_FILE).exists(),
            "a refusal must not leave an intent marker behind"
        );
        // Still fully usable: the refusal happened before anything changed.
        set.append_group(
            &[record(producer, 40)],
            Durability::Fsync,
            Uuid::from_u128(10_999),
        )
        .unwrap();
        assert_eq!(set.next_offset(), 41);
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

    /// Write a rolled range to `directory` and return the second sealed
    /// segment's base offset — a cut there leaves a non-empty retained prefix,
    /// so the frontier the replacement must inherit is non-trivial.
    fn build_rolled_range(directory: &Path, producer: Uuid) -> u64 {
        let mut set = SegmentSet::create_in(&Env::real(), directory, descriptor(), config())
            .expect("the range must be creatable");
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(30_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(
            set.sealed().len() >= 2,
            "the workload must roll at least twice"
        );
        set.sealed()[1].base_offset()
    }

    /// The intent the live path would write for a cut at `target`: doomed
    /// segments from the catalog, frontier from the sidecar of the segment at
    /// the cut. Staging it by hand lets a test park the on-disk state at any
    /// point of the protocol.
    fn staged_intent(directory: &Path, target: u64) -> TruncateIntent {
        let catalog = StartupCatalog::discover_in(&Env::real(), directory).unwrap();
        assert!(catalog.quarantined.is_empty(), "{:?}", catalog.quarantined);
        let doomed = catalog
            .entries
            .iter()
            .filter(|entry| entry.descriptor.base_offset >= target)
            .map(|entry| DoomedSegment {
                segment_id: entry.descriptor.segment_id,
                base_offset: entry.descriptor.base_offset,
            })
            .collect::<Vec<_>>();
        let producers = directory.join(format!("{}.producers", segment_stem(target)));
        let inherited = if producers.exists() {
            ProducerSnapshot::decode(&std::fs::read(&producers).unwrap()).unwrap()
        } else {
            ProducerSnapshot::default()
        };
        let mut replacement = descriptor();
        replacement.segment_id = Uuid::from_u128(777);
        replacement.base_offset = target;
        TruncateIntent {
            target_offset: target,
            replacement,
            config: config(),
            doomed,
            inherited,
        }
    }

    /// A cut at a sealed segment boundary removes whole segments and replaces
    /// the tail — and, the regression this feature exists to prevent: the
    /// replacement inherits the retained prefix's producer frontier, so a
    /// producer already in the range continues without `FirstSequence`.
    #[test]
    fn a_cut_at_a_sealed_boundary_removes_whole_segments() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(43);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        for sequence in 0..40 {
            set.append_group(
                &[record(producer, sequence)],
                Durability::Fsync,
                Uuid::from_u128(12_000 + sequence as u128),
            )
            .unwrap();
        }
        assert!(set.sealed().len() >= 2, "need at least two sealed segments");
        let cut = set.sealed()[1].base_offset();

        let outcome = set.truncate_to(cut).unwrap();
        assert_eq!(outcome.next_offset, cut);
        assert_eq!(outcome.records_removed, 40 - cut);
        assert!(outcome.bytes_removed > 0);
        assert_eq!(set.next_offset(), cut);
        assert_eq!(set.base_offset(), 0);
        assert_eq!(
            set.sealed().len(),
            1,
            "only segments at or above the cut may be removed"
        );
        assert!(
            !directory.path().join(TRUNCATE_INTENT_FILE).exists(),
            "the marker must be cleared once the truncation completes"
        );

        // The producer's frontier survived the cut: its next sequence is
        // accepted, not rejected as a first-sequence violation.
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(13_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);

        // Contiguity across the surviving boundary.
        let batch = set.fetch_through(0, 1 << 20, 1000, cut + 1).unwrap();
        assert_eq!(batch.records.len() as u64, cut + 1);
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
        }

        // And it reopens, which is what proves nothing was left inconsistent
        // on disk.
        drop(set);
        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the truncated range must still open");
        assert_eq!(reopened.next_offset(), cut + 1);
        reopened
            .append_group(
                &[record(producer, cut + 1)],
                Durability::Fsync,
                Uuid::from_u128(13_001),
            )
            .unwrap();
    }

    /// Recovery completes a truncation that died right after its marker
    /// became durable, before any file was touched.
    #[test]
    fn recovery_completes_a_truncation_that_wrote_only_the_marker() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(47);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the truncation completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        // The frontier travelled through the marker: the producer continues.
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(14_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Recovery completes a truncation that died after creating the
    /// replacement but before deleting anything — the replacement-first crash
    /// whose two active segments discovery quarantines when no marker
    /// explains them.
    #[test]
    fn recovery_completes_a_truncation_interrupted_after_creating_the_replacement() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(53);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        let stem = segment_stem(cut);
        if !intent.inherited.is_empty() {
            std::fs::write(
                directory.path().join(format!("{stem}.producers")),
                intent.inherited.encode().unwrap(),
            )
            .unwrap();
        }
        drop(
            ActiveSegment::create_in(
                &Env::real(),
                directory.path().join(format!("{stem}.active")),
                intent.replacement.clone(),
                intent.config,
            )
            .unwrap(),
        );
        // Without the marker this layout is exactly the ambiguity the whole
        // protocol exists to remove: two active segments, and a stem holding
        // both a sealed segment and an active file.
        assert!(
            SegmentSet::open_in(&Env::real(), directory.path()).is_err(),
            "the staged state must be unopenable without the marker"
        );

        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();
        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("with the marker the same state must open, completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(15_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Recovery clears a marker whose work was already done: deletions and
    /// replacement both landed, only the final unlink was lost.
    #[test]
    fn recovery_clears_a_marker_whose_work_is_already_done() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(59);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        for doomed in &intent.doomed {
            let stem = segment_stem(doomed.base_offset);
            for suffix in [
                "active",
                "segment",
                "commit",
                "index",
                "manifest.json",
                "chunks",
                "producers",
            ] {
                let _ = std::fs::remove_file(directory.path().join(format!("{stem}.{suffix}")));
            }
        }
        let stem = segment_stem(cut);
        if !intent.inherited.is_empty() {
            std::fs::write(
                directory.path().join(format!("{stem}.producers")),
                intent.inherited.encode().unwrap(),
            )
            .unwrap();
        }
        drop(
            ActiveSegment::create_in(
                &Env::real(),
                directory.path().join(format!("{stem}.active")),
                intent.replacement.clone(),
                intent.config,
            )
            .unwrap(),
        );
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the marker cleared");
        assert_eq!(set.next_offset(), cut);
        assert!(!directory.path().join(TRUNCATE_INTENT_FILE).exists());
        // The replacement was rebuilt from the marker, so it carries the
        // marker's identity — proof that completion is deterministic rather
        // than minting a new segment per attempt.
        assert_eq!(
            set.active().descriptor().segment_id,
            intent.replacement.segment_id
        );
        set.append_group(
            &[record(producer, cut)],
            Durability::Fsync,
            Uuid::from_u128(16_000),
        )
        .unwrap();
        assert_eq!(set.next_offset(), cut + 1);
    }

    /// Completion sweeps debris of its own interrupted atomic writes.
    /// Otherwise discovery would quarantine the finished range for an
    /// incomplete write that no longer matters.
    #[test]
    fn recovery_sweeps_debris_of_its_own_interrupted_writes() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(61);
        let cut = build_rolled_range(directory.path(), producer);
        let intent = staged_intent(directory.path(), cut);
        let marker_debris = directory
            .path()
            .join(".range.truncate-intent.00000000-0000-0000-0000-000000000001.tmp");
        let sidecar_debris = directory.path().join(format!(
            ".{}.producers.00000000-0000-0000-0000-000000000002.tmp",
            segment_stem(cut)
        ));
        std::fs::write(&marker_debris, b"debris").unwrap();
        std::fs::write(&sidecar_debris, b"debris").unwrap();
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            intent.encode().unwrap(),
        )
        .unwrap();

        let set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the range must open with the truncation completed");
        assert_eq!(set.next_offset(), cut);
        assert!(!marker_debris.exists(), "marker debris must be swept");
        assert!(!sidecar_debris.exists(), "sidecar debris must be swept");
    }

    /// The fault harness's fsync hold appends through the non-rolling path:
    /// a roll seals the tail and sealing makes bytes durable, which would
    /// silently commit records the injection promised were still at risk.
    /// At the bound that path must refuse — never roll.
    #[test]
    fn the_tail_only_append_refuses_at_the_bound_instead_of_rolling() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(72);
        let mut set =
            SegmentSet::create_in(&Env::real(), directory.path(), descriptor(), config()).unwrap();
        let mut refused = false;
        for sequence in 0..200 {
            match set.append_group_tail_only(&[record(producer, sequence)], Durability::Fsync) {
                Ok(_) => {}
                Err(LogError::SegmentByteLimit { .. })
                | Err(LogError::SegmentRecordLimit { .. }) => {
                    refused = true;
                    break;
                }
                Err(other) => panic!("unexpected refusal: {other:?}"),
            }
        }
        assert!(refused, "the bound must eventually refuse");
        assert!(
            set.sealed().is_empty(),
            "a tail-only append must never seal: sealing would commit bytes a \
             fault injection is deliberately holding out of durability"
        );
    }

    /// A data directory holding only the legacy single `range.active` — every
    /// deployment before the broker opened through the catalog — opens as a
    /// set of one, keeps its records, and ROLLS: the sealed prefix keeps the
    /// legacy stem, the successor takes an offset-based one, and the two read
    /// as one range. No migration step, because none is needed: discovery is
    /// stem-agnostic and rolling derives the successor's name from its base
    /// offset, never from its predecessor's.
    #[test]
    fn a_legacy_single_file_range_opens_as_a_set_of_one_and_rolls() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(71);
        let legacy = directory.path().join("range.active");
        {
            let mut segment =
                ActiveSegment::create_in(&Env::real(), &legacy, descriptor(), config()).unwrap();
            for sequence in 0..3 {
                segment
                    .append_group(&[record(producer, sequence)], Durability::Fsync)
                    .unwrap();
            }
        }

        let mut set = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("the legacy layout is a valid range of one segment");
        assert_eq!(set.next_offset(), 3);
        assert!(set.sealed().is_empty());

        // Write past the bound: the legacy file seals under its own name and
        // the successor opens under the offset-based stem.
        for sequence in 3..40 {
            set.append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(!set.sealed().is_empty(), "the range must have rolled");
        assert!(
            directory.path().join("range.segment").exists(),
            "the legacy tail seals under its legacy stem"
        );

        // And the mixed-stem range reopens and reads as one.
        drop(set);
        let mut reopened = SegmentSet::open_in(&Env::real(), directory.path())
            .unwrap()
            .expect("a mixed legacy/offset-stem range must reopen");
        assert_eq!(reopened.next_offset(), 40);
        let batch = reopened.fetch_through(0, 1 << 20, 100, 40).unwrap();
        assert_eq!(batch.records.len(), 40);
        reopened
            .append_group_minting(&[record(producer, 40)], Durability::Fsync)
            .unwrap();
    }

    /// A quarantined bundle refuses the open WITH its reason in the error.
    /// "N bundles are quarantined" gives an operator nothing to act on; the
    /// reason is the difference between deleting a stray temp file and
    /// restoring from a replica.
    #[test]
    fn a_quarantine_refusal_names_the_reason_and_the_path() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(73);
        build_rolled_range(directory.path(), producer);
        let stray = directory.path().join("stray.active");
        std::fs::write(&stray, b"not a segment").unwrap();

        let Err(problem) = SegmentSet::open_in(&Env::real(), directory.path()) else {
            panic!("a quarantined bundle must refuse the open");
        };
        let message = problem.to_string();
        assert!(
            message.contains("InvalidArtifact"),
            "the refusal must carry the quarantine reason: {message}"
        );
        assert!(
            message.contains("stray.active"),
            "the refusal must name the offending path: {message}"
        );
    }

    /// A marker that does not decode names an intent that cannot be honoured.
    /// It is quarantined by name and the open refuses — recovery never
    /// guesses at which segments to delete.
    #[test]
    fn a_corrupt_marker_quarantines_instead_of_guessing() {
        let directory = tempdir().unwrap();
        let producer = Uuid::from_u128(67);
        build_rolled_range(directory.path(), producer);
        std::fs::write(
            directory.path().join(TRUNCATE_INTENT_FILE),
            b"not an intent",
        )
        .unwrap();

        let catalog = StartupCatalog::discover_in(&Env::real(), directory.path()).unwrap();
        assert!(catalog.truncate_intent.is_none());
        assert!(
            catalog.quarantined.iter().any(|item| item
                .reasons
                .iter()
                .any(|reason| matches!(reason, crate::QuarantineReason::InvalidTruncateIntent(_)))),
            "{:?}",
            catalog.quarantined
        );
        assert!(
            !catalog.entries.is_empty(),
            "the segments themselves are intact and stay cataloged"
        );
        assert!(
            SegmentSet::open_in(&Env::real(), directory.path()).is_err(),
            "an undecodable intent must refuse the open, not be acted on"
        );
    }

    /// A range that is only a sealed prefix — what segment transfer produces —
    /// is adopted, served, and reopens as an ordinary range.
    ///
    /// The last unmet piece of #270. `open_in` refuses this directory by
    /// design, so before adoption existed a transferred prefix was bytes on
    /// disk that nothing could turn back into a replica.
    ///
    /// The producer sequence is the assertion that matters. A sealed segment's
    /// `.producers` holds what it INHERITED, not what it ends with, so a tail
    /// seeded from that sidecar would rewind every producer to where the last
    /// segment began and the next append would be rejected as a sequence gap.
    /// This continues the same producer across the adoption boundary, which is
    /// only possible if the frontier came from the scan.
    #[test]
    fn a_sealed_prefix_is_adopted_into_a_range_that_serves_and_reopens() {
        let source = tempdir().unwrap();
        let env = Env::real();
        let producer = Uuid::from_u128(0x9001);

        // A leader-shaped range: rolled several times, one producer throughout.
        let mut leader =
            SegmentSet::create_in(&env, source.path(), descriptor(), config()).unwrap();
        for sequence in 0..24 {
            leader
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(leader.sealed().len() >= 2, "the fixture must roll");
        let sealed_count = leader.sealed().len();
        let prefix_end = leader.sealed()[sealed_count - 1].next_offset();

        // The receiver's directory: the SEALED PREFIX ONLY, which is exactly
        // what a transfer lands — the tail is never shipped.
        let received = tempdir().unwrap();
        for reader in leader.sealed() {
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, received.path().join(name)).unwrap();
                }
            }
        }

        // Precondition: this is the refusal adoption exists to answer.
        let refused = SegmentSet::open_in(&env, received.path())
            .map(|_| ())
            .expect_err("a prefix without a tail must not open as a range");
        assert!(
            refused.to_string().contains("no active segment"),
            "{refused}"
        );

        let mut adopted =
            SegmentSet::adopt_in(&env, received.path(), Uuid::from_u128(0xADD1)).unwrap();
        assert_eq!(adopted.sealed().len(), sealed_count);
        assert_eq!(
            adopted.next_offset(),
            prefix_end,
            "the tail must begin where the prefix ended, not at zero"
        );

        // THE POINT: the producer continues from where the PREFIX left it —
        // which is not where the leader left it, because the leader's unsealed
        // tail never ships. One record per group here, so the next sequence is
        // the offset the prefix ended at; deriving it rather than hardcoding is
        // what makes this an assertion about the frontier instead of about the
        // fixture.
        //
        // A tail seeded from the last sealed segment's inherited sidecar would
        // rewind to where that segment BEGAN and reject the very first append
        // as a gap. Asked for 24 while the frontier sat at 19, this reported
        // `SequenceGap { expected: 20, actual: 24 }` — the mechanism working,
        // and the reason the expected value is computed here.
        let appended = 6;
        for sequence in prefix_end..prefix_end + appended {
            adopted
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .expect("the adopted tail must continue the producer's sequence");
        }

        // And it is an ordinary range afterwards: reopens without adoption, and
        // every record from both sides reads back in one call.
        drop(adopted);
        let reopened = SegmentSet::open_in(&env, received.path())
            .unwrap()
            .expect("an adopted range reopens like any other");
        assert_eq!(reopened.next_offset(), prefix_end + appended);
        let mut reopened = reopened;
        let through = reopened.next_offset();
        let batch = reopened.fetch_through(0, 1 << 20, 100, through).unwrap();
        assert_eq!(
            batch.records.len() as u64,
            prefix_end + appended,
            "every record, across the adoption boundary"
        );
        for (index, fetched) in batch.records.iter().enumerate() {
            assert_eq!(fetched.offset, index as u64);
        }
    }

    /// Adoption refuses a range that already has a tail.
    ///
    /// Minting a second tail would give the same offsets two writers, which is
    /// a split range that discovery quarantines only if it happens to notice.
    /// Refusing names the alternative rather than leaving the caller to guess.
    #[test]
    fn adoption_refuses_a_range_that_is_already_live() {
        let directory = tempdir().unwrap();
        let env = Env::real();
        drop(SegmentSet::create_in(&env, directory.path(), descriptor(), config()).unwrap());

        let refused = SegmentSet::adopt_in(&env, directory.path(), Uuid::from_u128(0xADD2))
            .map(|_| ())
            .expect_err("a live range must not be adopted");
        assert!(
            refused
                .to_string()
                .contains("already has an active segment"),
            "{refused}"
        );
        assert!(
            refused.to_string().contains("open_in"),
            "and name the alternative: {refused}"
        );
    }

    /// A gapped prefix is refused rather than blessed.
    ///
    /// The wire decoder refuses a gapped listing (#292) and discovery refuses
    /// an ambiguous directory, but neither covers bytes that arrived some other
    /// way — a restored backup, a hand-copied directory. Minting a tail onto a
    /// hole makes the hole permanent: the range opens from then on and the
    /// missing offsets are simply never served.
    #[test]
    fn adoption_refuses_a_prefix_with_a_hole_in_it() {
        let source = tempdir().unwrap();
        let env = Env::real();
        let producer = Uuid::from_u128(0x9002);
        let mut leader =
            SegmentSet::create_in(&env, source.path(), descriptor(), config()).unwrap();
        for sequence in 0..36 {
            leader
                .append_group_minting(&[record(producer, sequence)], Durability::Fsync)
                .unwrap();
        }
        assert!(
            leader.sealed().len() >= 3,
            "need three to drop a middle one"
        );

        // Copy the prefix, SKIPPING one segment in the middle.
        let received = tempdir().unwrap();
        let skip = leader.sealed()[1].base_offset();
        for reader in leader.sealed() {
            if reader.base_offset() == skip {
                continue;
            }
            for artifact in reader.paths().unwrap() {
                if artifact.exists() {
                    let name = artifact.file_name().unwrap();
                    std::fs::copy(&artifact, received.path().join(name)).unwrap();
                }
            }
        }

        let refused = SegmentSet::adopt_in(&env, received.path(), Uuid::from_u128(0xADD3))
            .map(|_| ())
            .expect_err("a gapped prefix must not be adopted");
        let text = refused.to_string();
        assert!(
            text.contains("not contiguous") || text.contains("quarantined"),
            "the refusal must name the gap: {text}"
        );
    }
}
