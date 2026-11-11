//! Syslog spool source adapter.
//!
//! Treats rsyslog / syslog-ng spool files as append-only files, tracking a
//! `spool_id`, path, and byte range. External collectors (rsyslog, syslog-ng)
//! own delivery; the VTOP engine owns batching, checksum, manifest, upload,
//! verification, replay state, and the commit rule.

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use crate::continuity::{self, Discontinuity, Observed};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use vtop_core::errors::VtopError;
use vtop_core::types::{ContentFingerprint, ProgressMarker, SourceType, TelemetryFormat};

#[derive(Debug, Clone, Default)]
struct SpoolCursor {
    read_byte: u64,
    committed_byte: u64,
    /// See `FileCursor::confirmed_for` (#624).
    confirmed_for: Option<continuity::ConfirmedFile>,
    /// What the spool looked like when this cursor last caught up with it.
    ///
    /// Kept so a cycle can decide NOT to open the file at all (#101). `None`
    /// until a read has actually observed the file, so an unseen spool is
    /// always read rather than skipped on an assumption.
    seen: Option<SpoolSeen>,
    /// Which generation of the spool each handed-out range was read in.
    generations: continuity::Generations,
    /// `(inode, read_byte)` recorded when a replay validated and rewound this
    /// cursor; valid only while `read_byte` is still where the rewind put it.
    rewound_for: Option<(u64, u64)>,
}

/// Identity of a spool file as a read last observed it: enough to prove,
/// with one stat, that nothing has happened to it since.
#[derive(Debug, Clone, PartialEq)]
struct SpoolSeen {
    inode: Option<u64>,
    file_size: u64,
    mtime: String,
    /// The fingerprint of the spool's first bytes (#578); `None` from a stat,
    /// for an empty spool, and for a seed from a row that recorded none.
    content: Option<ContentFingerprint>,
}

impl SpoolSeen {
    /// What the next read is judged against.
    fn observed(&self) -> Observed {
        Observed {
            inode: self.inode,
            size: self.file_size,
            content: self.content.clone(),
            // The spool records no tail fingerprint (#620 covers the file
            // source); its cursor is judged by inode, length and first page.
            tail: None,
        }
    }
}

pub struct SyslogSpoolSource {
    paths: Vec<String>,
    cursors: HashMap<String, SpoolCursor>,
}

impl SyslogSpoolSource {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            paths,
            cursors: HashMap::new(),
        }
    }

    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
        c.confirmed_for = None;
        c.read_byte = committed_byte;
    }

    /// Seed a committed offset AND the inode of the spool it was taken from,
    /// so the first read can tell a rotated spool from the one the offset
    /// belongs to and restart it at byte 0 (#558) - with the length and the
    /// content fingerprint that batch saw (#578), so a spool truncated, or
    /// replaced at the same inode, while the engine was down is caught even
    /// when it is longer than the offset (`FileSource::seed_committed_from`).
    /// All `None` (a row from before the fields) behaves as
    /// [`Self::seed_committed`].
    pub fn seed_committed_from(
        &mut self,
        path: &str,
        committed_byte: u64,
        inode: Option<u64>,
        observed_size: Option<u64>,
        content: Option<ContentFingerprint>,
    ) {
        self.seed_committed(path, committed_byte);
        if inode.is_some() || observed_size.is_some() || content.is_some() {
            // An empty mtime can never satisfy `can_skip`.
            self.cursors.get_mut(path).expect("seeded above").seen = Some(SpoolSeen {
                inode,
                file_size: observed_size.unwrap_or(0),
                mtime: String::new(),
                content,
            });
        }
    }

    /// A read failed after finding the spool replaced or truncated: restart the
    /// cursor on the new content as a successful read would
    /// (`FileSource::apply_failed_read`).
    fn apply_failed_read(&mut self, path: &str, failed: SpoolReadError) -> VtopError {
        if let Some(restart) = failed.restart {
            let (why, seen) = *restart;
            let cursor = self.cursors.entry(path.to_string()).or_default();
            tracing::warn!(
                path,
                previous_read_byte = cursor.read_byte,
                "{why}; the read of the new content then failed"
            );
            cursor.committed_byte = 0;
            cursor.confirmed_for = None;
            cursor.read_byte = 0;
            cursor.generations.discontinuity();
            cursor.seen = Some(seen);
        }
        failed.error
    }

    /// Fold a completed read into the spool's cursor and build its result. A
    /// discontinuity restarts the cursor on the new content, including the
    /// committed point (see `FileSource::apply_slice`).
    fn apply_slice(&mut self, path: &str, slice: SpoolSlice) -> ReadResult {
        let SpoolSlice {
            records,
            start,
            end,
            seen,
            discontinuity,
        } = slice;
        let progress_start = Self::marker(path, &seen, start, start);
        let progress_end = Self::marker(path, &seen, start, end);
        let cursor = self.cursors.entry(path.to_string()).or_default();
        if let Some(why) = discontinuity {
            tracing::warn!(path, previous_read_byte = cursor.read_byte, "{why}");
            cursor.committed_byte = 0;
            cursor.confirmed_for = None;
            cursor.generations.discontinuity();
        }
        cursor.generations.issue(start, end);
        cursor.read_byte = end;
        // What the read ACTUALLY observed, not what a stat saw before it.
        cursor.seen = Some(seen);
        ReadResult {
            progress_start,
            progress_end,
            records,
            first_timestamp: None,
            last_timestamp: None,
            // Spool lines are re-framed with newlines on serialization.
            verbatim: false,
        }
    }

    /// Whether this cycle can skip opening `path` entirely (#101 — the
    /// stat-before-read mechanism #363 built for the file source, ported on
    /// the measured 2.5x: an open-seek-read-fstat per cold spool versus the
    /// one stat this replaces).
    ///
    /// A spool directory is mostly cold — a few files the collector is
    /// appending to, and history it will never touch again.
    ///
    /// THE CONDITION IS DELIBERATELY NARROW. All four must hold: the file
    /// must be the same INODE this cursor last saw, at the same SIZE, at the
    /// same MTIME, and the cursor must already have read through to that
    /// size. Anything else — an append, a truncation, a rotation that reused
    /// the name, a partial trailing line still being written (its cursor
    /// sits before the end), a file this cursor has never observed — falls
    /// through to a real read. And EQUALITY on the cursor, not `>=`: a
    /// cursor beyond the file's end is not a caught-up cursor, it is an
    /// inconsistency, and the right response to an inconsistency is to look,
    /// not to skip.
    ///
    /// This cannot trap the way the #99 backoff did: the stat is a reliable
    /// emptiness check at full strength every cycle, and any byte the
    /// collector appends changes both the size and the mtime compared
    /// against. A spool that starts moving again is read on the very next
    /// pass.
    fn can_skip(cursor: &SpoolCursor, current: &SpoolSeen) -> bool {
        let Some(seen) = cursor.seen.as_ref() else {
            return false;
        };
        seen.inode == current.inode
            && seen.file_size == current.file_size
            // An UNKNOWN mtime never matches (#411) — the same rule as
            // `FileSource::can_skip`, with the same narrow claim (review):
            // unknown evidence forfeits the skip fast path; the read it
            // falls through to applies the append-only contract, which no
            // stat — mtime or not — can stretch to detecting an in-place
            // same-size rewrite.
            && !seen.mtime.is_empty()
            && seen.mtime == current.mtime
            && cursor.read_byte == current.file_size
    }

    /// Stat a path without opening it, for the skip decision. A path that
    /// cannot be stat'd is NOT skipped: it is handed to the read, which
    /// reports the error properly instead of having it swallowed here as a
    /// silent skip.
    fn stat_identity(path: &str) -> Option<SpoolSeen> {
        std::fs::metadata(path).ok().map(|md| Self::seen_of(&md))
    }

    fn seen_of(md: &std::fs::Metadata) -> SpoolSeen {
        SpoolSeen {
            inode: inode_of(md),
            file_size: md.len(),
            mtime: md
                .modified()
                .ok()
                .map(|t| {
                    let dt: chrono::DateTime<chrono::Utc> = t.into();
                    dt.to_rfc3339()
                })
                .unwrap_or_default(),
            content: None,
        }
    }

    fn spool_id(path: &str) -> String {
        Path::new(path)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string())
    }

    /// A marker naming the spool as `seen` describes it: the inode, and (#578)
    /// the length and first-bytes fingerprint a spool marker had no room for,
    /// without which the coverage audit could not split a spool into the
    /// contents that lived at one inode.
    fn marker(path: &str, seen: &SpoolSeen, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::SyslogSpool {
            spool_id: Self::spool_id(path),
            path: path.to_string(),
            inode: seen.inode,
            start_byte: start,
            end_byte: end,
            received_time_start: None,
            received_time_end: None,
            file_size: Some(seen.file_size),
            content_fingerprint: seen.content.clone(),
        }
    }

    /// Read one spool file from `start`, honouring the budgets. No `&self`, so
    /// many spool files can be read CONCURRENTLY in one pass (#96 B2). Only
    /// complete (newline-terminated) lines are accepted — a partial line still
    /// being written by rsyslog is left for the next pass.
    ///
    /// `last_seen` is what the cursor last observed of the spool; the open
    /// descriptor is judged against it before any byte is read, and a rotated
    /// or truncated spool restarts at byte 0 (#558).
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        last_seen: Option<Observed>,
    ) -> Result<SpoolSlice, SpoolReadError> {
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(SpoolReadError::before_judged)?;
        let mut opened = Self::seen_of(
            &file
                .metadata()
                .await
                .map_err(SpoolReadError::before_judged)?,
        );
        // The first page, through the same descriptor (#578; see
        // `FileSource::read_slice`).
        let head = continuity::read_head(&mut file)
            .await
            .map_err(SpoolReadError::before_judged)?;
        opened.content = ContentFingerprint::of(&head);
        let discontinuity = continuity::check(
            last_seen.as_ref(),
            opened.inode,
            opened.file_size,
            &head,
            start,
        );
        let start = if discontinuity.is_some() { 0 } else { start };
        // A failure from here still restarts the cursor on what the judgement
        // found (see `FileSource::read_slice`, review of #564).
        let restart = discontinuity.map(|why| Box::new((why, opened.clone())));
        Self::read_opened(file, &path, start, max_records, max_bytes, discontinuity)
            .await
            .map(|mut slice| {
                // Size and mtime after the read; the content as it was at
                // open, which an append cannot change.
                slice.seen.content = opened.content;
                slice
            })
            .map_err(|error| SpoolReadError { error, restart })
    }

    async fn read_opened(
        mut file: tokio::fs::File,
        path: &str,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        discontinuity: Option<Discontinuity>,
    ) -> Result<SpoolSlice, VtopError> {
        file.seek(std::io::SeekFrom::Start(start)).await?;
        // Put the whole-call byte limiter BELOW BufReader. If Take wrapped the
        // BufReader instead, its default 8 KiB fill could read far beyond a
        // small max_bytes before the outer limit observed the line.
        let read_limit = u64::try_from(max_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        let buffer_capacity = max_bytes.saturating_add(1).clamp(1, 8 * 1024);
        let mut reader = BufReader::with_capacity(buffer_capacity, file.take(read_limit));

        let mut records = Vec::new();
        let mut bytes_read: u64 = 0;
        let mut pos = start;

        loop {
            if records.len() >= max_records || bytes_read as usize >= max_bytes {
                break;
            }
            let remaining = max_bytes.saturating_sub(bytes_read as usize);
            let limit = u64::try_from(remaining)
                .unwrap_or(u64::MAX)
                .saturating_add(1);
            let mut line = Vec::new();
            let n = (&mut reader)
                .take(limit)
                .read_until(b'\n', &mut line)
                .await?;
            if n > remaining {
                if records.is_empty() {
                    return Err(VtopError::Source(format!(
                        "record in {path} exceeds max_bytes={max_bytes}"
                    )));
                }
                break;
            }
            if n == 0 || !line.ends_with(b"\n") {
                break;
            }
            pos += n as u64;
            bytes_read += n as u64;
            line.pop();
            records.push(line);
        }
        // Fingerprint the descriptor whose bytes were actually consumed. A
        // path lookup here could instead describe a replacement installed by
        // a concurrent spool rotation (#127). The whole identity comes back,
        // not only the inode: what the read OBSERVED is what the next
        // cycle's skip decision must compare against (#101).
        let seen = Self::seen_of(&reader.get_ref().get_ref().metadata().await?);
        Ok(SpoolSlice {
            records,
            start,
            end: pos,
            seen,
            discontinuity,
        })
    }
}

/// A failed spool read, and the discontinuity it had already found, if any.
struct SpoolReadError {
    error: VtopError,
    // Boxed: the identity is large and failures are rare.
    restart: Option<Box<(Discontinuity, SpoolSeen)>>,
}

impl SpoolReadError {
    fn before_judged(error: std::io::Error) -> Self {
        Self {
            error: error.into(),
            restart: None,
        }
    }
}

/// What a completed spool read hands back.
struct SpoolSlice {
    records: Vec<Vec<u8>>,
    /// Where the read STARTED: the cursor's position, or byte 0 after a
    /// rotation or truncation.
    start: u64,
    end: u64,
    seen: SpoolSeen,
    discontinuity: Option<Discontinuity>,
}

/// How many spool files are read concurrently in one pass.
const SPOOL_READ_CONCURRENCY: usize = 8;

#[async_trait]
impl SourceAdapter for SyslogSpoolSource {
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        let mut out = Vec::new();
        for pattern in &self.paths {
            for p in glob::glob(pattern)
                .map_err(|e| VtopError::Source(format!("bad glob {pattern}: {e}")))?
                .flatten()
            {
                if p.is_file() {
                    out.push(DiscoveredSource {
                        source_type: SourceType::SyslogSpool,
                        source_name: p.to_string_lossy().into_owned(),
                        format: TelemetryFormat::Syslog,
                    });
                }
            }
        }
        Ok(out)
    }

    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        _max_wait: Duration,
    ) -> Result<Vec<ReadResult>, VtopError> {
        let path = source.source_name.clone();
        let start = self.cursors.entry(path.clone()).or_default().read_byte;
        // Same skip as the shared pass (#101): one stat instead of an open
        // the cycle would learn nothing from.
        if let Some(current) = Self::stat_identity(&path) {
            let cursor = self.cursors.get(&path).expect("entry created above");
            if Self::can_skip(cursor, &current) {
                return Ok(vec![ReadResult {
                    progress_start: Self::marker(&path, &current, start, start),
                    progress_end: Self::marker(&path, &current, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]);
            }
        }
        let last_seen = self
            .cursors
            .get(&path)
            .and_then(|c| c.seen.as_ref())
            .map(SpoolSeen::observed);
        match Self::read_slice(path.clone(), start, max_records, max_bytes, last_seen).await {
            Ok(slice) => Ok(vec![self.apply_slice(&path, slice)]),
            Err(failed) => Err(self.apply_failed_read(&path, failed)),
        }
    }

    /// Read every spool file CONCURRENTLY (#96 B2): independent handles and
    /// snapshotted cursors per file, cursor updates applied serially after the
    /// joins.
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        _max_wait: Duration,
    ) -> Result<AdapterReadReport, VtopError> {
        use futures::StreamExt;
        let started = std::time::Instant::now();

        // STAT BEFORE READ (#101). Cold spool files — cursor caught up,
        // identity unchanged since the read that caught it up — are
        // separated out here and never opened; what is left is the work the
        // pass actually has to do.
        let mut jobs: Vec<(usize, String, u64, Option<Observed>)> = Vec::new();
        let mut skipped: Vec<(usize, String, u64, SpoolSeen)> = Vec::new();
        for (i, s) in sources.iter().enumerate() {
            let path = s.source_name.clone();
            let cursor = self.cursors.entry(path.clone()).or_default();
            let start = cursor.read_byte;
            match Self::stat_identity(&path) {
                Some(current) if Self::can_skip(cursor, &current) => {
                    skipped.push((i, path, start, current));
                }
                _ => {
                    let last_seen = cursor.seen.as_ref().map(SpoolSeen::observed);
                    jobs.push((i, path, start, last_seen));
                }
            }
        }
        // COUNTED, not silent — the #100 visibility rule holds here too: a
        // cycle that skipped nine spools out of ten is a very different
        // cycle from one that read all ten and found them empty, and they
        // look identical in every other metric.
        if !skipped.is_empty() {
            tracing::debug!(
                skipped = skipped.len(),
                read = jobs.len(),
                "spool_cycle_skipped_unchanged"
            );
        }

        let mut results: Vec<(usize, String, Result<SpoolSlice, SpoolReadError>)> =
            futures::stream::iter(
                jobs.into_iter()
                    .map(|(i, path, start, last_seen)| async move {
                        let res = Self::read_slice(
                            path.clone(),
                            start,
                            max_records,
                            max_bytes,
                            last_seen,
                        )
                        .await;
                        (i, path, res)
                    }),
            )
            .buffer_unordered(SPOOL_READ_CONCURRENCY)
            .collect()
            .await;
        results.sort_by_key(|(i, ..)| *i);

        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(results.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
            retention_lost: Vec::new(),
        };
        let mut any_records = false;
        let mut any_failed = false;
        // Skipped spools report an empty read at the cursor they already
        // hold, so the caller sees one outcome per source however the cycle
        // got there; sorted back into source order below.
        for (source_index, path, start, current) in skipped {
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result: Ok(vec![ReadResult {
                    progress_start: Self::marker(&path, &current, start, start),
                    progress_end: Self::marker(&path, &current, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]),
            });
        }
        for (source_index, path, res) in results {
            let result = match res {
                Ok(slice) => {
                    any_records |= !slice.records.is_empty();
                    Ok(vec![self.apply_slice(&path, slice)])
                }
                Err(failed) => {
                    any_failed = true;
                    Err(self.apply_failed_read(&path, failed))
                }
            };
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result,
            });
        }
        // ONE ORDER OUT, whatever order the work happened in: source order
        // is part of this report's contract and a skip must not change it.
        report.outcomes.sort_by_key(|o| o.source_index);
        // Shared attribution, same convention as the other overrides: the
        // overlapped reads are one wall-clock bucket.
        let elapsed = started.elapsed().as_millis() as u64;
        if any_records {
            report.productive_ms = elapsed;
        } else if any_failed {
            report.failed_ms = elapsed;
        } else {
            report.empty_ms = elapsed;
        }
        Ok(report)
    }

    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::SyslogSpool {
            path,
            start_byte,
            end_byte,
            inode: marker_inode,
            file_size: marker_file_size,
            content_fingerprint: marker_content,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "spool adapter given non-spool marker".into(),
            ));
        };
        // See `FileSource::commit_progress`: a batch read before a rotation or
        // truncation must not carry the restarted cursor to an offset in a
        // spool that is no longer there (#558), nor one read before a
        // replacement at the same inode (#578).
        let (current_inode, current_size, current_head) = match continuity::probe(Path::new(path)) {
            Some((md, head)) => (inode_of(&md), md.len(), head),
            None => (None, 0, Vec::new()),
        };
        let generation = self
            .cursors
            .entry(path.clone())
            .or_default()
            .generations
            .settle(*start_byte, *end_byte);
        if generation != continuity::Settled::Stale
            && continuity::commit_is_current(
                *marker_inode,
                marker_content.as_ref(),
                current_inode,
                current_size,
                &current_head,
                *end_byte,
            )
        {
            let c = self.cursors.entry(path.clone()).or_default();
            // Superseded by a commit this process confirmed (#624; see
            // `FileSource::commit_progress`).
            if c.committed_byte >= *end_byte
                && c.confirmed_for
                    .as_ref()
                    .is_some_and(|file| file.is(*marker_inode, marker_content.as_ref()))
            {
                tracing::info!(
                    path,
                    end_byte,
                    committed_byte = c.committed_byte,
                    "syslog spool commit skipped: the range is superseded by a later committed \
                     one, and committing it would move the cursor back"
                );
                return Ok(());
            }
            c.committed_byte = *end_byte;
            c.confirmed_for = continuity::ConfirmedFile::of(*marker_inode, marker_content.as_ref());
            // See `FileSource::commit_progress`: a recovered commit below the
            // seeded head restarts the head there; and, whatever its end, the
            // cursor adopts the identity the batch saw - the seed may be of
            // the spool it replaced at the same inode (review of #578). A
            // legacy marker keeps the seed's inode, so a rotation before
            // recovery is still seen, else the current one (review of #564),
            // and the seed's fingerprint of it: a re-read, never a skip.
            let recovered = generation == continuity::Settled::Unknown;
            if c.read_byte < *end_byte || (recovered && c.read_byte > *end_byte) {
                c.read_byte = *end_byte;
            }
            if recovered {
                let prior = c.seen.as_ref();
                let inode = (*marker_inode)
                    .or(prior.and_then(|seen| seen.inode))
                    .or(current_inode);
                let kept = prior
                    .filter(|seen| seen.inode == inode)
                    .and_then(|seen| seen.content.clone());
                c.seen = Some(SpoolSeen {
                    inode,
                    file_size: marker_file_size.unwrap_or(0),
                    mtime: String::new(),
                    content: marker_content.clone().or(kept),
                });
            }
            tracing::info!(path, end_byte, "syslog spool progress committed");
        } else {
            tracing::warn!(
                path,
                end_byte,
                "committed batch belongs to a spool that was rotated or truncated since it was read; \
                 the archive has it, the read position is left on the current spool"
            );
        }
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::SyslogSpool {
            path,
            inode: marker_inode,
            start_byte,
            content_fingerprint: marker_content,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "spool adapter given non-spool marker".into(),
            ));
        };
        // Open once and validate the descriptor, not a path-stat result: the
        // same descriptor supplies both identity and length even if rotation
        // races this check. Old markers have no inode and retain their former
        // size-only behavior; `None` never means identity was verified.
        let mut file = tokio::fs::File::open(path).await?;
        let metadata = file.metadata().await?;
        let current_inode = inode_of(&metadata);
        if marker_inode.is_some() && marker_inode != &current_inode {
            return Err(VtopError::Source(format!(
                "cannot replay syslog spool {path}: file identity changed (rotation/replacement)"
            )));
        }
        let c = self.cursors.entry(path.clone()).or_default();
        let replay_byte = (*start_byte).max(c.committed_byte);
        if replay_byte > metadata.len() {
            return Err(VtopError::Source(format!(
                "cannot replay syslog spool {path} at byte {replay_byte}: file length is {} (truncated)",
                metadata.len()
            )));
        }
        // The same inode, other first bytes: replaced in place (#578).
        if let Some(content) = marker_content {
            let head = continuity::read_head(&mut file).await?;
            if !content.matches_head(&head) {
                return Err(VtopError::Source(format!(
                    "cannot replay syslog spool {path}: its first {} bytes are not the ones \
                     this range was read after (replaced at the same inode)",
                    content.len
                )));
            }
        }
        c.read_byte = replay_byte;
        c.rewound_for = current_inode.map(|inode| (inode, replay_byte));
        c.generations.reissue_from(replay_byte);
        Ok(())
    }

    /// Where the next read of this marker's spool starts - only when the
    /// cursor is positioned in THAT file (see `FileSource::read_position`).
    fn read_position(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::SyslogSpool {
            path,
            inode: marker_inode,
            content_fingerprint: marker_content,
            ..
        } = marker
        else {
            return None;
        };
        let c = self.cursors.get(path)?;
        let (identity, content) = match c.rewound_for {
            // A rewind checked the marker against the spool it rewound.
            Some((inode, at)) if at == c.read_byte => (Some(inode), None),
            _ => (
                c.seen.as_ref().and_then(|seen| seen.inode),
                c.seen.as_ref().and_then(|seen| seen.content.as_ref()),
            ),
        };
        if marker_inode.is_some() && identity != *marker_inode {
            return None;
        }
        // The cursor read other content at that inode since (#578) - or
        // content its fingerprint cannot be compared with (a file under one
        // page fingerprints a different length as it grows): unverified is
        // answered as unknown, the conservative answer (review of #578).
        if content
            .zip(marker_content.as_ref())
            .is_some_and(|(seen, marker)| seen.same_content(marker) != Some(true))
        {
            return None;
        }
        Some(i128::from(c.read_byte))
    }

    /// The cursor's committed byte, only when the cursor is PROVEN to describe
    /// the marker's spool: the same inode, and the same content - equal
    /// fingerprints, or both matching the first bytes of the spool now at the
    /// path, which still has that inode. See `FileSource::committed_through`
    /// (review of #567); a spool marker records no tail.
    fn committed_through(&self, marker: &ProgressMarker) -> Option<i128> {
        let ProgressMarker::SyslogSpool {
            path,
            inode: Some(marker_inode),
            content_fingerprint: Some(marker_content),
            ..
        } = marker
        else {
            return None;
        };
        let c = self.cursors.get(path)?;
        let seen = c.seen.as_ref()?;
        if seen.inode != Some(*marker_inode) {
            return None;
        }
        let seen_content = seen.content.as_ref()?;
        match seen_content.same_content(marker_content) {
            Some(true) => {}
            Some(false) => return None,
            None => {
                let (metadata, head) = continuity::probe(Path::new(path))?;
                if inode_of(&metadata) != Some(*marker_inode)
                    || !seen_content.matches_head(&head)
                    || !marker_content.matches_head(&head)
                {
                    return None;
                }
            }
        }
        Some(i128::from(c.committed_byte))
    }

    fn source_type(&self) -> SourceType {
        SourceType::SyslogSpool
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[cfg(unix)]
fn inode_of(md: &std::fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    Some(md.ino())
}

#[cfg(not(unix))]
fn inode_of(_md: &std::fs::Metadata) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// #567 (review): as for `FileSource` - a spool's committed point is
    /// answered only for a marker proven to be the cursor's spool: the same
    /// inode and content shown to be the same, settled against the spool at
    /// the path when the fingerprints are of different lengths.
    #[cfg(unix)]
    #[test]
    fn the_committed_point_is_answered_only_for_a_marker_proven_to_be_the_cursors_spool() {
        use std::os::unix::fs::MetadataExt;
        let bytes: Vec<u8> = (0..100u8).map(|i| b'a' + i % 26).collect();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&bytes).unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let fp = |b: &[u8]| ContentFingerprint::of(b);
        let marker =
            |inode: Option<u64>, content: Option<ContentFingerprint>| ProgressMarker::SyslogSpool {
                spool_id: "s".into(),
                path: path.clone(),
                inode,
                start_byte: 0,
                end_byte: 30,
                received_time_start: None,
                received_time_end: None,
                file_size: Some(100),
                content_fingerprint: content,
            };
        let mut s = SyslogSpoolSource::new(vec![path.clone()]);
        s.seed_committed_from(&path, 40, Some(inode), Some(100), fp(&bytes));
        assert_eq!(
            s.committed_through(&marker(Some(inode), fp(&bytes))),
            Some(40)
        );
        assert_eq!(
            s.committed_through(&marker(Some(inode), fp(&bytes[..50]))),
            Some(40)
        );
        assert_eq!(
            s.committed_through(&marker(None, fp(&bytes))),
            None,
            "no inode"
        );
        assert_eq!(
            s.committed_through(&marker(Some(inode), None)),
            None,
            "no content"
        );
        assert_eq!(
            s.committed_through(&marker(Some(inode + 1), fp(&bytes))),
            None
        );
        assert_eq!(
            s.committed_through(&marker(Some(inode), fp(&[b'z'; 50]))),
            None,
            "other content of another length"
        );
    }

    #[tokio::test]
    async fn reads_spool_and_resumes() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "<13>msg one").unwrap();
        writeln!(f, "<13>msg two").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();

        let mut s = SyslogSpoolSource::new(vec![path.clone()]);
        let src = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };
        let reads = s
            .read_batch_candidates(&src, 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // A spool file is a single committable unit, so the Vec is always
        // length 1; assert it rather than indexing blind, so a regression that
        // returns 0 or 2 fails here instead of panicking on the index.
        assert_eq!(reads.len(), 1);
        let r = &reads[0];
        assert_eq!(r.records.len(), 1);
        if let ProgressMarker::SyslogSpool { spool_id, .. } = &r.progress_end {
            assert!(!spool_id.is_empty());
        } else {
            panic!("expected spool marker");
        }
        s.commit_progress(&r.progress_end).await.unwrap();
        let reads2 = s
            .read_batch_candidates(&src, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads2.len(), 1);
        assert_eq!(reads2[0].records.len(), 1);
        assert_eq!(reads2[0].records[0], b"<13>msg two");
    }

    /// A partial trailing line is left alone, and arrives once it is finished
    /// (#101).
    ///
    /// This is not a hypothetical for a spool: rsyslog appends to the file the
    /// engine is reading, so a read landing mid-write sees a line with no
    /// newline on the end of it. Consuming that would emit half a syslog
    /// record as a whole one AND advance the cursor past it, so the other half
    /// would arrive as a second, equally wrong record — a corruption that no
    /// later pass can undo, and one nothing in the suite pinned. `FileSource`
    /// has carried this test since it had the same problem; the spool source
    /// documented the behaviour without ever asserting it.
    #[tokio::test]
    async fn a_partial_trailing_line_waits_until_it_is_finished() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "<13>complete one\n<13>still being writ").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();

        let mut s = SyslogSpoolSource::new(vec![path.clone()]);
        let src = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };

        let reads = s
            .read_batch_candidates(&src, 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads.len(), 1);
        assert_eq!(
            reads[0].records,
            vec![b"<13>complete one".to_vec()],
            "only the newline-terminated line is a record; the fragment is not \
             a short record, it is half of one"
        );
        s.commit_progress(&reads[0].progress_end).await.unwrap();

        // rsyslog finishes the line.
        {
            let mut fh = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(fh, "ten now").unwrap();
            fh.flush().unwrap();
        }

        let reads2 = s
            .read_batch_candidates(&src, 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // Asserted rather than indexed blind, matching the sibling test above:
        // a regression returning nothing should fail here with a readable
        // message instead of panicking on the index (review).
        assert_eq!(reads2.len(), 1);
        assert_eq!(
            reads2[0].records,
            vec![b"<13>still being written now".to_vec()],
            "the cursor never advanced past the fragment, so the finished line \
             arrives whole exactly once — not as two halves, and not twice"
        );
    }

    #[tokio::test]
    async fn rejects_an_oversized_spool_record_without_advancing() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "12345678").unwrap(); // nine bytes including newline
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let source = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);

        let error = spool
            .read_batch_candidates(&source, 10, 8, Duration::ZERO)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("exceeds max_bytes=8"));
        assert_eq!(
            spool.cursors[&path].read_byte, 0,
            "oversized data is not skipped"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn marker_fingerprints_the_open_spool_descriptor() {
        use std::os::unix::fs::MetadataExt;

        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "<13>old message").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let expected_inode = std::fs::metadata(&path).unwrap().ino();
        let source = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };

        let mut spool = SyslogSpoolSource::new(vec![path]);
        let reads = spool
            .read_batch_candidates(&source, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let ProgressMarker::SyslogSpool { inode, .. } = reads[0].progress_end else {
            panic!("expected syslog-spool marker");
        };
        assert_eq!(inode, Some(expected_inode));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn replay_rejects_a_rotated_spool() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log");
        std::fs::write(&path, "old message\n").unwrap();
        let path_string = path.to_string_lossy().into_owned();
        let source = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path_string.clone(),
            format: TelemetryFormat::Syslog,
        };
        let mut spool = SyslogSpoolSource::new(vec![path_string]);
        let reads = spool
            .read_batch_candidates(&source, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        // Keep both files allocated until the atomic replacement. Removing
        // first lets Linux reuse the freed inode and makes this test flaky.
        let replacement = dir.path().join("replacement.log");
        std::fs::write(&replacement, "new message\n").unwrap();
        std::fs::rename(&replacement, &path).unwrap();
        let error = spool
            .replay_from_marker(&reads[0].progress_start)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("identity changed"));
    }

    #[tokio::test]
    async fn replay_rejects_a_truncated_spool() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "first").unwrap();
        writeln!(f, "second").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let source = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let first = spool
            .read_batch_candidates(&source, 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        spool.commit_progress(&first[0].progress_end).await.unwrap();
        let second = spool
            .read_batch_candidates(&source, 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        f.as_file_mut().set_len(0).unwrap();
        let error = spool
            .replay_from_marker(&second[0].progress_start)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("truncated"));
    }

    #[test]
    fn legacy_marker_without_inode_remains_deserializable() {
        let json = r#"{
            "source_type":"syslog_spool",
            "spool_id":"legacy.log",
            "path":"/var/spool/legacy.log",
            "start_byte":10,
            "end_byte":20,
            "received_time_start":null,
            "received_time_end":null
        }"#;
        let marker: ProgressMarker = serde_json::from_str(json).unwrap();
        let ProgressMarker::SyslogSpool { inode, .. } = marker else {
            panic!("expected syslog-spool marker");
        };
        assert_eq!(inode, None);
    }

    fn seen(inode: Option<u64>, file_size: u64, mtime: &str) -> SpoolSeen {
        SpoolSeen {
            inode,
            file_size,
            mtime: mtime.to_string(),
            content: None,
        }
    }

    fn caught_up_cursor(at: u64, identity: SpoolSeen) -> SpoolCursor {
        SpoolCursor {
            read_byte: at,
            committed_byte: at,
            seen: Some(identity),
            ..SpoolCursor::default()
        }
    }

    #[test]
    fn a_caught_up_unchanged_spool_is_skippable() {
        let id = seen(Some(7), 100, "2026-08-28T12:00:00+00:00");
        assert!(
            SyslogSpoolSource::can_skip(&caught_up_cursor(100, id.clone()), &id),
            "identical identity with the cursor at its end is the ONE state \
             the skip exists for; refusing it makes the port a no-op"
        );
    }

    #[test]
    fn an_appended_spool_is_not_skippable() {
        let id = seen(Some(7), 100, "2026-08-28T12:00:00+00:00");
        let grown = seen(Some(7), 150, "2026-08-28T12:00:05+00:00");
        assert!(
            !SyslogSpoolSource::can_skip(&caught_up_cursor(100, id), &grown),
            "an appended spool has unread bytes; skipping it is starvation, \
             the exact trap the #99 backoff fell into"
        );
    }

    #[test]
    fn a_rotated_spool_with_the_same_size_is_not_skippable() {
        let id = seen(Some(7), 100, "2026-08-28T12:00:00+00:00");
        let replaced = seen(Some(9), 100, "2026-08-28T12:00:00+00:00");
        assert!(
            !SyslogSpoolSource::can_skip(&caught_up_cursor(100, id), &replaced),
            "a rotation that reused the name at the same size is a different \
             file with 100 unread bytes; a size-keyed skip would never read it"
        );
    }

    #[test]
    fn a_cursor_short_of_the_end_is_not_skippable() {
        let id = seen(Some(7), 100, "2026-08-28T12:00:00+00:00");
        assert!(
            !SyslogSpoolSource::can_skip(&caught_up_cursor(80, id.clone()), &id),
            "a cursor before the end means a partial trailing line is still \
             pending; skipping would leave its completion unread forever"
        );
    }

    #[test]
    fn a_cursor_beyond_the_end_is_not_skippable() {
        let id = seen(Some(7), 100, "2026-08-28T12:00:00+00:00");
        assert!(
            !SyslogSpoolSource::can_skip(&caught_up_cursor(120, id.clone()), &id),
            "a cursor past the file's end is an inconsistency (the file \
             shrank under a read head that had passed the new end), and the \
             right response to an inconsistency is to look, not to skip — \
             this is the `==` vs `>=` review case from #363"
        );
    }

    #[test]
    fn a_spool_whose_mtime_cannot_be_read_is_never_skipped() {
        // The identity records "" when the filesystem cannot report a
        // modified time (#411), and "" matching "" would skip an in-place
        // rewrite that preserved inode and size — indefinitely. Unknown
        // must fall through to a real read, same rule as FileSource.
        let id = seen(Some(7), 100, "");
        assert!(
            !SyslogSpoolSource::can_skip(&caught_up_cursor(100, id.clone()), &id),
            "inode and size agree and the cursor is caught up, but an unknown \
             mtime is not evidence the content is unchanged"
        );
    }

    #[test]
    fn a_spool_never_seen_before_is_never_skipped() {
        let id = seen(Some(7), 0, "2026-08-28T12:00:00+00:00");
        let unseen = SpoolCursor::default();
        assert!(
            !SyslogSpoolSource::can_skip(&unseen, &id),
            "no read has observed this file; skipping on an assumption \
             would make an empty-so-far spool invisible forever"
        );
    }

    /// The no-starvation proof for the skip (#101): a spool that goes cold
    /// and then starts moving again is read on the very next pass, because
    /// the appended byte changes the size and mtime the stat compares
    /// against. A skip that latched — recording an identity it never
    /// refreshes, or never re-statting a file it once skipped — fails here.
    #[tokio::test]
    async fn a_skipped_spool_that_grows_again_is_read_on_the_very_next_pass() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        writeln!(f, "<13>cold one").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let src = DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.clone(),
            format: TelemetryFormat::Syslog,
        };

        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let first = spool
            .read_batch_candidates(&src, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            first[0].records.len(),
            1,
            "the warm-up read must consume the line"
        );

        let second = spool
            .read_batch_candidates(&src, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert!(
            second[0].records.is_empty(),
            "a caught-up unchanged spool yields nothing, skipped or read"
        );

        writeln!(f, "<13>warm again").unwrap();
        f.flush().unwrap();
        let third = spool
            .read_batch_candidates(&src, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            third[0].records.len(),
            1,
            "the appended line must arrive on the pass after the append; \
             anything later means the skip starved a live spool"
        );
        assert_eq!(third[0].records[0], b"<13>warm again");
    }

    /// The shared pass reports one outcome per source in source order,
    /// however each source got its outcome (#101): a skip must be
    /// indistinguishable from an empty read in the report's shape, and it
    /// must not reorder the sources around it.
    #[tokio::test]
    async fn a_cold_spool_in_a_shared_pass_keeps_its_place_in_the_report() {
        let mut cold = tempfile::NamedTempFile::new().unwrap();
        writeln!(cold, "<13>cold").unwrap();
        cold.flush().unwrap();
        let mut hot = tempfile::NamedTempFile::new().unwrap();
        writeln!(hot, "<13>hot one").unwrap();
        hot.flush().unwrap();
        let cold_path = cold.path().to_string_lossy().into_owned();
        let hot_path = hot.path().to_string_lossy().into_owned();
        let srcs: Vec<DiscoveredSource> = [&cold_path, &hot_path]
            .iter()
            .map(|p| DiscoveredSource {
                source_type: SourceType::SyslogSpool,
                source_name: (*p).clone(),
                format: TelemetryFormat::Syslog,
            })
            .collect();

        let mut spool = SyslogSpoolSource::new(vec![cold_path.clone(), hot_path.clone()]);
        spool
            .read_all_batch_candidates(&srcs, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        writeln!(hot, "<13>hot two").unwrap();
        hot.flush().unwrap();
        let report = spool
            .read_all_batch_candidates(&srcs, 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            report.outcomes.len(),
            2,
            "one outcome per source is the report's contract, and a skipped \
             source must not vanish from it"
        );
        assert_eq!(
            (
                report.outcomes[0].source_index,
                report.outcomes[1].source_index
            ),
            (0, 1),
            "source order is part of the contract; a skip must not push the \
             cold file behind the hot one"
        );
        let cold_reads = report.outcomes[0].result.as_ref().unwrap();
        assert!(
            cold_reads[0].records.is_empty(),
            "the cold spool has nothing new, skipped or read"
        );
        let hot_reads = report.outcomes[1].result.as_ref().unwrap();
        assert_eq!(
            hot_reads[0].records.len(),
            1,
            "the hot spool's append must arrive in the same pass that \
             skipped its cold neighbour"
        );
        assert_eq!(hot_reads[0].records[0], b"<13>hot two");
    }

    // ---- rotation and truncation (#558) -------------------------------------

    fn spool_source(path: &str) -> DiscoveredSource {
        DiscoveredSource {
            source_type: SourceType::SyslogSpool,
            source_name: path.to_string(),
            format: TelemetryFormat::Syslog,
        }
    }

    /// Review of #564, for the spool: a LEGACY recovered marker (no inode)
    /// that restarts the head below the seed must keep the inode the spool
    /// had, or a rotation to a longer spool goes unseen and its first lines
    /// are skipped.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_legacy_recovered_commit_keeps_the_spool_inode_so_a_rotation_is_still_seen() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        let old: String = (0..12).map(|i| format!("old-{i:02}\n")).collect(); // 84 bytes
        std::fs::write(&path, &old).unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        spool.seed_committed_from(&path, 60, Some(inode), None, None);
        let recovered = ProgressMarker::SyslogSpool {
            spool_id: SyslogSpoolSource::spool_id(&path),
            path: path.clone(),
            inode: None,
            start_byte: 0,
            end_byte: 21,
            received_time_start: None,
            received_time_end: None,
            file_size: None,
            content_fingerprint: None,
        };
        spool.commit_progress(&recovered).await.unwrap();

        std::fs::rename(&path, format!("{path}.1")).unwrap();
        let fresh: String = (0..12).map(|i| format!("new-{i:02}\n")).collect();
        std::fs::write(&path, &fresh).unwrap();

        let after = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            after[0].records.first().map(Vec::as_slice),
            Some(b"new-00".as_slice()),
            "the rotated spool must be read from byte 0"
        );
        assert_eq!(after[0].records.len(), 12);
    }

    /// The new spool is longer than the old read position — the case that used
    /// to skip its first bytes without a word.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_rotated_spool_is_read_from_byte_zero_even_when_it_outgrows_the_old_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "old one\nold two\n").unwrap();
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let first = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        spool.commit_progress(&first[0].progress_end).await.unwrap();

        std::fs::rename(&path, format!("{path}.1")).unwrap();
        let fresh: String = (0..40).map(|i| format!("new message {i}\n")).collect();
        std::fs::write(&path, &fresh).unwrap();

        let after = spool
            .read_batch_candidates(&spool_source(&path), 1000, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            after[0].records.len(),
            40,
            "every line, not the tail after the old offset"
        );
        let ProgressMarker::SyslogSpool { start_byte, .. } = after[0].progress_start else {
            panic!("a spool marker");
        };
        assert_eq!(start_byte, 0);
    }

    #[tokio::test]
    async fn a_truncated_spool_is_read_from_byte_zero_and_a_late_commit_cannot_skip_its_appends() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap(); // 14 bytes
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let old = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = old[0].progress_end.clone();

        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap()
            .write_all(b"zz\n")
            .unwrap();
        let fresh = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(fresh[0].records, vec![b"zz".to_vec()]);

        // The batch read before the truncation commits late: its offset (14)
        // is beyond the new spool, so the cursor must stay put.
        spool.commit_progress(&old_end).await.unwrap();
        assert_eq!(spool.cursors[&path].read_byte, 3);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"yy\n")
            .unwrap();
        let next = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(next[0].records, vec![b"yy".to_vec()]);
    }

    /// The same pre-truncation commit, but the spool has regrown past the old
    /// range's end before it lands: by inode and length it looks current (see
    /// `FileSource`'s test of the same shape; review of #564).
    #[tokio::test]
    async fn a_pre_truncation_commit_cannot_move_the_cursor_after_the_spool_regrows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap(); // 14 bytes
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let old = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = old[0].progress_end.clone();

        std::fs::write(&path, "zz\n").unwrap();
        let fresh = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(fresh[0].records, vec![b"zz".to_vec()]);

        // Regrown to 20 bytes, past the old range's end (14).
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"aa\nbb\ncc\ndd\nee\nff\n")
            .unwrap();
        spool.commit_progress(&old_end).await.unwrap();
        assert_eq!(
            spool.cursors[&path].read_byte, 3,
            "the stale commit must leave the cursor where the restart put it"
        );
    }

    /// The spool twin of the file test (review of #564): a truncation found
    /// by a read that then fails still restarts the cursor.
    #[tokio::test]
    async fn a_truncation_found_by_a_read_that_then_fails_still_restarts_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap(); // 14 bytes
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let old = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let old_end = old[0].progress_end.clone();

        std::fs::write(&path, "xxxxxxxxxx\n").unwrap(); // 11 bytes, over budget
        assert!(spool
            .read_batch_candidates(&spool_source(&path), 100, 5, Duration::ZERO)
            .await
            .is_err());

        std::fs::write(&path, "aa\nbb\ncc\ndd\nee\nff\n").unwrap(); // 18 > 14
        spool.commit_progress(&old_end).await.unwrap();
        let next = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            next[0].records.len(),
            6,
            "the new content is read from byte 0, not from the old offset 14"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_seed_resumes_the_same_spool_and_restarts_a_replaced_one() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();

        let mut same = SyslogSpoolSource::new(vec![path.clone()]);
        same.seed_committed_from(&path, 8, Some(inode), None, None);
        let got = same
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(got[0].records, vec![b"three".to_vec()]);

        let mut replaced = SyslogSpoolSource::new(vec![path.clone()]);
        replaced.seed_committed_from(&path, 8, Some(inode + 1_000_000), None, None);
        let got = replaced
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            got[0].records.len(),
            3,
            "the replaced spool is read from byte 0"
        );
    }
    /// See `FileSource`'s test of the same name: the cursor's position belongs
    /// to the spool it was validated against (review of #563).
    #[cfg(unix)]
    #[tokio::test]
    async fn a_read_position_is_only_reported_for_the_spool_the_cursor_is_in() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "aaa\nbbb\nccc\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let marker = |inode: Option<u64>| ProgressMarker::SyslogSpool {
            spool_id: "s".into(),
            path: path.clone(),
            inode,
            start_byte: 4,
            end_byte: 8,
            received_time_start: None,
            received_time_end: None,
            file_size: None,
            content_fingerprint: None,
        };
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        assert_eq!(spool.read_position(&marker(Some(inode))), None);
        spool
            .replay_from_marker(&marker(Some(inode)))
            .await
            .unwrap();
        assert_eq!(spool.read_position(&marker(Some(inode))), Some(4));
        assert_eq!(spool.read_position(&marker(Some(inode + 1))), None);
        assert_eq!(spool.read_position(&marker(None)), Some(4));
        // A rejected replay (a different file at the path) leaves no identity.
        let mut fresh = SyslogSpoolSource::new(vec![path.clone()]);
        assert!(fresh
            .replay_from_marker(&marker(Some(inode + 1)))
            .await
            .is_err());
        assert_eq!(fresh.read_position(&marker(Some(inode + 1))), None);
    }

    // ---- replacement at the same inode (#578) -------------------------------

    fn spool_lines(reads: &[ReadResult]) -> Vec<String> {
        reads
            .iter()
            .flat_map(|read| &read.records)
            .map(|record| String::from_utf8_lossy(record).into_owned())
            .collect()
    }

    /// #578, for the spool: rewritten at the same inode and already as long
    /// as the old read position when next read, a spool agrees with the old
    /// one by inode and length, and was resumed at the old offset.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_same_inode_replacement_that_is_no_shorter_is_read_from_byte_zero() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "<13>old one\n<13>old two\n").unwrap(); // 24 bytes
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let first = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(spool_lines(&first), ["<13>old one", "<13>old two"]);

        std::fs::write(&path, "<13>NEW one\n<13>NEW two\n<13>NEW three\n").unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().ino(),
            inode,
            "the case under test keeps the inode"
        );
        let next = spool
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            spool_lines(&next),
            ["<13>NEW one", "<13>NEW two", "<13>NEW three"],
            "the replacement must be read from byte 0, not resumed at the old spool's \
             offset 24 (which hands out only its third line)"
        );
    }

    /// #578: a spool marker records the length and the fingerprint the read
    /// saw - what the coverage audit needs to split a spool's contents at one
    /// inode - and a seed from it catches a spool truncated or replaced at
    /// the same inode while the engine was down, even when it is now longer
    /// than the committed offset.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_spool_marker_records_size_and_content_and_a_seed_uses_them() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "<13>old one\n<13>old two\n").unwrap();
        let inode = std::fs::metadata(&path).unwrap().ino();
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let read = spool
            .read_batch_candidates(&spool_source(&path), 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let ProgressMarker::SyslogSpool {
            file_size,
            content_fingerprint,
            end_byte,
            ..
        } = read[0].progress_end.clone()
        else {
            panic!("a spool marker");
        };
        assert_eq!(file_size, Some(24));
        assert_eq!(
            content_fingerprint,
            ContentFingerprint::of(b"<13>old one\n<13>old two\n")
        );
        assert_eq!(end_byte, 12, "a bounded read: one line of two");

        // Replaced at the same inode while the engine was down: longer than
        // the offset (12) AND than the length the batch saw (24).
        std::fs::write(&path, "<13>NEW one\n<13>NEW two\n<13>NEW three\n").unwrap();
        let mut seeded = SyslogSpoolSource::new(vec![path.clone()]);
        seeded.seed_committed_from(&path, end_byte, Some(inode), file_size, content_fingerprint);
        let reads = seeded
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            spool_lines(&reads),
            ["<13>NEW one", "<13>NEW two", "<13>NEW three"]
        );

        // Truncated and regrown past the offset but not past what the batch
        // saw: the recorded size catches it, as it does for a file.
        std::fs::write(&path, "<13>xx one\n<13>x\n").unwrap(); // 17 bytes
        let mut seeded = SyslogSpoolSource::new(vec![path.clone()]);
        seeded.seed_committed_from(&path, 12, Some(inode), Some(24), None);
        let reads = seeded
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(spool_lines(&reads), ["<13>xx one", "<13>x"]);
    }

    /// A batch read before an in-place replacement commits after it: the
    /// ledger row commits, but the cursor must not move to an offset in
    /// content it never read, and the range is never replayed into it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_range_from_before_a_same_inode_replacement_neither_commits_the_cursor_nor_replays() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spool.log").to_string_lossy().into_owned();
        std::fs::write(&path, "<13>old one\n<13>old two\n").unwrap();
        let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
        let read = spool
            .read_batch_candidates(&spool_source(&path), 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        std::fs::write(&path, "<13>NEW one\n<13>NEW two\n<13>NEW three\n").unwrap();

        let err = spool
            .replay_from_marker(&read[0].progress_start)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("replaced at the same inode"), "{err}");

        // Simulate a restart: no in-process record of the range, so only the
        // file at the path can judge the commit.
        let mut restarted = SyslogSpoolSource::new(vec![path.clone()]);
        restarted
            .commit_progress(&read[0].progress_end)
            .await
            .unwrap();
        let reads = restarted
            .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(
            spool_lines(&reads),
            ["<13>NEW one", "<13>NEW two", "<13>NEW three"],
            "the old range's end must not become the replacement's read position"
        );
    }

    /// Review of #578 (Codex), for the spool: see the file source's
    /// `a_recovered_commit_at_or_above_the_seed_adopts_the_replacements_fingerprint`.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_recovered_commit_at_or_above_the_seed_adopts_the_replacements_fingerprint() {
        use std::os::unix::fs::MetadataExt;
        let a = "<13>old one\n<13>old two\n"; // 24 bytes
        let b = "<13>NEW one\n<13>NEW two\n<13>NEW three\n"; // lines end at 12, 24, 38
        for end in [24u64, 38] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("spool.log").to_string_lossy().into_owned();
            std::fs::write(&path, b).unwrap();
            let inode = std::fs::metadata(&path).unwrap().ino();
            let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
            spool.seed_committed_from(
                &path,
                24,
                Some(inode),
                Some(24),
                ContentFingerprint::of(a.as_bytes()),
            );
            let recovered = ProgressMarker::SyslogSpool {
                spool_id: "spool.log".into(),
                path: path.clone(),
                inode: Some(inode),
                start_byte: 0,
                end_byte: end,
                received_time_start: None,
                received_time_end: None,
                file_size: Some(b.len() as u64),
                content_fingerprint: ContentFingerprint::of(b.as_bytes()),
            };
            spool.commit_progress(&recovered).await.unwrap();
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"<13>NEW four\n")
                .unwrap();
            let reads = spool
                .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            let expected: &[&str] = if end == 24 {
                &["<13>NEW three", "<13>NEW four"]
            } else {
                &["<13>NEW four"]
            };
            assert_eq!(
                spool_lines(&reads),
                expected,
                "recovered end {end}: B's committed prefix [0,{end}) must not be read again"
            );
        }
    }

    /// Review of #578 (cubic), for the spool: see the file source's
    /// `a_legacy_recovered_commit_after_a_rotation_keeps_the_seeds_inode`.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_legacy_recovered_commit_after_a_rotation_keeps_the_seeds_inode() {
        use std::os::unix::fs::MetadataExt;
        for (seed, end) in [(12u64, 36u64), (36, 12)] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("spool.log").to_string_lossy().into_owned();
            let old: String = (0..8).map(|i| format!("<13>old-{i:02}\n")).collect(); // 12 bytes each
            std::fs::write(&path, &old).unwrap();
            let inode = std::fs::metadata(&path).unwrap().ino();
            let mut spool = SyslogSpoolSource::new(vec![path.clone()]);
            spool.seed_committed_from(&path, seed, Some(inode), None, None);

            std::fs::rename(&path, format!("{path}.1")).unwrap();
            let fresh: String = (0..8).map(|i| format!("<13>new-{i:02}\n")).collect();
            std::fs::write(&path, &fresh).unwrap();
            let recovered = ProgressMarker::SyslogSpool {
                spool_id: "spool.log".into(),
                path: path.clone(),
                inode: None,
                start_byte: 0,
                end_byte: end,
                received_time_start: None,
                received_time_end: None,
                file_size: None,
                content_fingerprint: None,
            };
            spool.commit_progress(&recovered).await.unwrap();

            let next = spool
                .read_batch_candidates(&spool_source(&path), 100, 1 << 20, Duration::ZERO)
                .await
                .unwrap();
            assert_eq!(
                spool_lines(&next).first().map(String::as_str),
                Some("<13>new-00"),
                "seed {seed}, recovered end {end}: the rotated-in spool must be read from \
                 byte 0, not resumed at {end}"
            );
            assert_eq!(next[0].records.len(), 8);
        }
    }
}
