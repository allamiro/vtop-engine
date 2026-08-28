//! Syslog spool source adapter.
//!
//! Treats rsyslog / syslog-ng spool files as append-only files, tracking a
//! `spool_id`, path, and byte range. External collectors (rsyslog, syslog-ng)
//! own delivery; the VTOP engine owns batching, checksum, manifest, upload,
//! verification, replay state, and the commit rule.

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, BufReader};
use vtop_core::errors::VtopError;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

#[derive(Debug, Clone, Default)]
struct SpoolCursor {
    read_byte: u64,
    committed_byte: u64,
    /// What the spool looked like when this cursor last caught up with it.
    ///
    /// Kept so a cycle can decide NOT to open the file at all (#101). `None`
    /// until a read has actually observed the file, so an unseen spool is
    /// always read rather than skipped on an assumption.
    seen: Option<SpoolSeen>,
}

/// Identity of a spool file as a read last observed it: enough to prove,
/// with one stat, that nothing has happened to it since.
#[derive(Debug, Clone, PartialEq)]
struct SpoolSeen {
    inode: Option<u64>,
    file_size: u64,
    mtime: String,
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
        c.read_byte = committed_byte;
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
        }
    }

    fn spool_id(path: &str) -> String {
        Path::new(path)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| path.to_string())
    }

    fn marker(path: &str, inode: Option<u64>, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::SyslogSpool {
            spool_id: Self::spool_id(path),
            path: path.to_string(),
            inode,
            start_byte: start,
            end_byte: end,
            received_time_start: None,
            received_time_end: None,
        }
    }

    /// Read one spool file from `start`, honouring the budgets. No `&self`, so
    /// many spool files can be read CONCURRENTLY in one pass (#96 B2). Only
    /// complete (newline-terminated) lines are accepted — a partial line still
    /// being written by rsyslog is left for the next pass.
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<(Vec<Vec<u8>>, u64, SpoolSeen), VtopError> {
        let mut file = tokio::fs::File::open(&path).await?;
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
        Ok((records, pos, seen))
    }
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
                    progress_start: Self::marker(&path, current.inode, start, start),
                    progress_end: Self::marker(&path, current.inode, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]);
            }
        }
        let (records, pos, seen) =
            Self::read_slice(path.clone(), start, max_records, max_bytes).await?;
        let inode = seen.inode;
        let cursor = self.cursors.get_mut(&path).unwrap();
        cursor.read_byte = pos;
        cursor.seen = Some(seen);

        Ok(vec![ReadResult {
            progress_start: Self::marker(&path, inode, start, start),
            progress_end: Self::marker(&path, inode, start, pos),
            records,
            first_timestamp: None,
            last_timestamp: None,
            // Spool lines are re-framed with newlines on serialization.
            verbatim: false,
        }])
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
        let mut jobs: Vec<(usize, String, u64)> = Vec::new();
        let mut skipped: Vec<(usize, String, u64, SpoolSeen)> = Vec::new();
        for (i, s) in sources.iter().enumerate() {
            let path = s.source_name.clone();
            let cursor = self.cursors.entry(path.clone()).or_default();
            let start = cursor.read_byte;
            match Self::stat_identity(&path) {
                Some(current) if Self::can_skip(cursor, &current) => {
                    skipped.push((i, path, start, current));
                }
                _ => jobs.push((i, path, start)),
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

        let mut results: Vec<(
            usize,
            String,
            u64,
            Result<(Vec<Vec<u8>>, u64, SpoolSeen), VtopError>,
        )> = futures::stream::iter(jobs.into_iter().map(|(i, path, start)| async move {
            let res = Self::read_slice(path.clone(), start, max_records, max_bytes).await;
            (i, path, start, res)
        }))
        .buffer_unordered(SPOOL_READ_CONCURRENCY)
        .collect()
        .await;
        results.sort_by_key(|(i, ..)| *i);

        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(results.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
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
                    progress_start: Self::marker(&path, current.inode, start, start),
                    progress_end: Self::marker(&path, current.inode, start, start),
                    records: Vec::new(),
                    first_timestamp: None,
                    last_timestamp: None,
                    verbatim: false,
                }]),
            });
        }
        for (source_index, path, start, res) in results {
            let result = match res {
                Ok((records, pos, seen)) => {
                    let inode = seen.inode;
                    let cursor = self.cursors.get_mut(&path).unwrap();
                    cursor.read_byte = pos;
                    // What the read ACTUALLY observed, not what the stat saw
                    // before it: the spool can grow between the two, and
                    // recording the earlier identity would let the next
                    // cycle skip bytes this one never read.
                    cursor.seen = Some(seen);
                    any_records |= !records.is_empty();
                    Ok(vec![ReadResult {
                        progress_start: Self::marker(&path, inode, start, start),
                        progress_end: Self::marker(&path, inode, start, pos),
                        records,
                        first_timestamp: None,
                        last_timestamp: None,
                        verbatim: false,
                    }])
                }
                Err(e) => {
                    any_failed = true;
                    Err(e)
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
        let ProgressMarker::SyslogSpool { path, end_byte, .. } = marker else {
            return Err(VtopError::Source(
                "spool adapter given non-spool marker".into(),
            ));
        };
        let c = self.cursors.entry(path.clone()).or_default();
        c.committed_byte = *end_byte;
        if c.read_byte < *end_byte {
            c.read_byte = *end_byte;
        }
        tracing::info!(path, end_byte, "syslog spool progress committed");
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::SyslogSpool {
            path,
            inode: marker_inode,
            start_byte,
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
        let file = tokio::fs::File::open(path).await?;
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
        c.read_byte = replay_byte;
        Ok(())
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
        }
    }

    fn caught_up_cursor(at: u64, identity: SpoolSeen) -> SpoolCursor {
        SpoolCursor {
            read_byte: at,
            committed_byte: at,
            seen: Some(identity),
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
}
