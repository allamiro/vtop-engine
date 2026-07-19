//! File source adapter.
//!
//! Reads append-only log files line by line, tracking byte offsets, file
//! identity (inode), size, and mtime. Resumes from the last committed byte and
//! never deletes source files unless explicitly configured and only after the
//! batch is committed.

use crate::base::{
    AdapterReadReport, DiscoveredSource, ReadResult, SourceAdapter, SourceReadOutcome,
};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use vtop_core::errors::VtopError;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// Per-file read/commit cursor.
#[derive(Debug, Clone, Default)]
struct FileCursor {
    /// Next byte to read from (uncommitted read head).
    read_byte: u64,
    /// Highest byte durably committed to object storage.
    committed_byte: u64,
}

pub struct FileSource {
    paths: Vec<String>,
    format: TelemetryFormat,
    delete_after_commit: bool,
    /// Read each file as a single whole-file record (for binary / compressed
    /// source files that have no line structure) instead of line by line.
    whole_file: bool,
    cursors: HashMap<String, FileCursor>,
}

impl FileSource {
    pub fn new(paths: Vec<String>, format: TelemetryFormat, delete_after_commit: bool) -> Self {
        Self::with_mode(paths, format, delete_after_commit, false)
    }

    /// Construct with an explicit whole-file mode.
    pub fn with_mode(
        paths: Vec<String>,
        format: TelemetryFormat,
        delete_after_commit: bool,
        whole_file: bool,
    ) -> Self {
        Self {
            paths,
            format,
            delete_after_commit,
            whole_file,
            cursors: HashMap::new(),
        }
    }

    /// Seed a committed byte offset (used by the replay engine on startup).
    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
        c.read_byte = committed_byte;
    }

    fn identity_of(md: &std::fs::Metadata) -> FileIdentity {
        FileIdentity {
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

    /// Identity of whatever the PATH names right now. Only for comparing the
    /// current file against a previously recorded marker (the delete guard in
    /// `commit_progress`) — never for building a marker, which must describe
    /// the file actually READ (#65): use the open descriptor's identity.
    fn file_identity(path: &Path) -> (Option<u64>, u64, String) {
        match std::fs::metadata(path) {
            Ok(md) => {
                let id = Self::identity_of(&md);
                (id.inode, id.file_size, id.mtime)
            }
            Err(_) => (None, 0, String::new()),
        }
    }

    /// Marker from a descriptor-derived identity: the inode/size/mtime are of
    /// the file whose BYTES were read, so a rotation between read and marker
    /// construction cannot mix the old file's offsets with the new file's
    /// identity (#65) — which previously let `delete_after_commit` match and
    /// delete the replacement.
    fn marker_from(path: &str, id: &FileIdentity, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::File {
            path: path.to_string(),
            inode: id.inode,
            start_byte: start,
            end_byte: end,
            file_size: id.file_size,
            mtime: id.mtime.clone(),
        }
    }

    /// Read one file from `start`, honouring the budgets. Pure with respect to
    /// the adapter (no `&self`): takes everything it needs by value so many
    /// files can be read CONCURRENTLY in one pass (#96 B2) — each file's
    /// cursor is snapshotted before and applied after, so concurrent reads
    /// never touch shared state.
    ///
    /// Returns `(records, end_pos, verbatim)`.
    async fn read_slice(
        path: String,
        start: u64,
        max_records: usize,
        max_bytes: usize,
        whole_file: bool,
    ) -> Result<(Vec<Vec<u8>>, u64, bool, FileIdentity), VtopError> {
        // Whole-file mode: read the entire remaining file as one opaque record.
        // Used for binary / already-compressed source files with no line
        // structure. The whole file commits as a single byte range.
        if whole_file {
            // Read AND fingerprint through one open descriptor: a rotation
            // between "read the bytes" and "stat the path" would otherwise mix
            // the old file's offsets with the NEW file's identity (#65).
            let mut file = tokio::fs::File::open(&path).await?;
            let md = file.metadata().await?; // fstat: the opened file, not the path
            let fsize = md.len();
            if fsize.saturating_sub(start) as usize > max_bytes {
                // Whole-file mode loads the entire remaining file into memory;
                // streaming is a documented follow-up (README known limits).
                tracing::warn!(
                    path,
                    file_bytes = fsize,
                    max_bytes,
                    "whole-file source exceeds max_bytes; loading entire file into memory"
                );
            }
            let mut data = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut file, &mut data).await?;
            let end = data.len() as u64;
            let records = if start >= end || data.is_empty() {
                Vec::new()
            } else {
                vec![data[start as usize..].to_vec()]
            };
            let identity = Self::identity_of(&file.metadata().await?);
            return Ok((records, end, true, identity));
        }

        let file = tokio::fs::File::open(&path).await?;
        let mut reader = BufReader::new(file);
        reader.seek(std::io::SeekFrom::Start(start)).await?;

        let mut records = Vec::new();
        let mut bytes_read: u64 = 0;
        let mut pos = start;

        loop {
            if records.len() >= max_records || bytes_read as usize >= max_bytes {
                break;
            }
            let mut line = Vec::new();
            let n = reader.read_until(b'\n', &mut line).await?;
            if n == 0 {
                break; // EOF
            }
            // Only accept complete (newline-terminated) lines so a partially
            // written tail is not committed.
            if !line.ends_with(b"\n") {
                break;
            }
            if n > max_bytes {
                // A single line larger than the whole batch budget is a soft-
                // limit overrun (read_until already buffered it); surface it.
                tracing::warn!(
                    path,
                    line_bytes = n,
                    max_bytes,
                    "single record exceeds max_bytes"
                );
            }
            pos += n as u64;
            bytes_read += n as u64;
            // Strip trailing newline for the stored record.
            if line.last() == Some(&b'\n') {
                line.pop();
            }
            records.push(line);
        }
        // fstat on the descriptor we READ from — the same file even if the
        // path was rotated away mid-read (#65).
        let identity = Self::identity_of(&reader.get_ref().metadata().await?);
        Ok((records, pos, false, identity))
    }
}

/// Inode / size / mtime of the file a read actually consumed, taken from the
/// OPEN descriptor (`fstat`), never from a second path lookup.
#[derive(Debug, Clone)]
struct FileIdentity {
    inode: Option<u64>,
    file_size: u64,
    mtime: String,
}

/// How many files are read concurrently in one pass. Bounded so a glob that
/// matches thousands of files does not open them all at once.
const FILE_READ_CONCURRENCY: usize = 8;

#[async_trait]
impl SourceAdapter for FileSource {
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError> {
        let mut out = Vec::new();
        for pattern in &self.paths {
            for entry in glob::glob(pattern)
                .map_err(|e| VtopError::Source(format!("bad glob {pattern}: {e}")))?
            {
                match entry {
                    Ok(p) if p.is_file() => out.push(DiscoveredSource {
                        source_type: SourceType::File,
                        source_name: p.to_string_lossy().into_owned(),
                        format: self.format.clone(),
                    }),
                    _ => {}
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
        let (records, end, verbatim, id) =
            Self::read_slice(path.clone(), start, max_records, max_bytes, self.whole_file).await?;
        self.cursors.get_mut(&path).unwrap().read_byte = end;
        Ok(vec![ReadResult {
            progress_start: Self::marker_from(&path, &id, start, start),
            progress_end: Self::marker_from(&path, &id, start, end),
            records,
            first_timestamp: None,
            last_timestamp: None,
            verbatim,
        }])
    }

    /// Read every file CONCURRENTLY (#96 B2). Each file's read is independent
    /// — its own handle, its own snapshotted cursor — so disk I/O overlaps
    /// instead of queueing behind the slowest file. Cursor updates are applied
    /// serially after the joins, keeping all shared state on this thread.
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        _max_wait: Duration,
    ) -> Result<AdapterReadReport, VtopError> {
        use futures::StreamExt;
        let started = std::time::Instant::now();

        let whole_file = self.whole_file;
        let jobs: Vec<(usize, String, u64)> = sources
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let start = self
                    .cursors
                    .entry(s.source_name.clone())
                    .or_default()
                    .read_byte;
                (i, s.source_name.clone(), start)
            })
            .collect();

        let mut results: Vec<(
            usize,
            String,
            u64,
            Result<(Vec<Vec<u8>>, u64, bool, FileIdentity), VtopError>,
        )> = futures::stream::iter(jobs.into_iter().map(|(i, path, start)| async move {
            let res =
                Self::read_slice(path.clone(), start, max_records, max_bytes, whole_file).await;
            (i, path, start, res)
        }))
        .buffer_unordered(FILE_READ_CONCURRENCY)
        .collect()
        .await;
        // buffer_unordered completes in I/O order; report in source order.
        results.sort_by_key(|(i, ..)| *i);

        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(results.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
        };
        let mut any_records = false;
        let mut any_failed = false;
        for (source_index, path, start, res) in results {
            let result = match res {
                Ok((records, end, verbatim, id)) => {
                    self.cursors.get_mut(&path).unwrap().read_byte = end;
                    any_records |= !records.is_empty();
                    Ok(vec![ReadResult {
                        progress_start: Self::marker_from(&path, &id, start, start),
                        progress_end: Self::marker_from(&path, &id, start, end),
                        records,
                        first_timestamp: None,
                        last_timestamp: None,
                        verbatim,
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
        // The reads overlapped, so the pass's wall-clock is one shared bucket
        // (splitting per source would double-count it): productive if ANY file
        // yielded, else failed if ANY file errored, else empty. File reads
        // never block on a poll window, so this is microseconds either way.
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
        let ProgressMarker::File {
            path,
            end_byte,
            inode: marker_inode,
            ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        let c = self.cursors.entry(path.clone()).or_default();
        c.committed_byte = *end_byte;
        if c.read_byte < *end_byte {
            c.read_byte = *end_byte;
        }
        tracing::info!(path, end_byte, "file source progress committed");

        if self.delete_after_commit {
            // Validate file identity before the irreversible delete: if the path
            // now points to a different inode than the batch's marker (rotation
            // / replacement), deleting would destroy an unrelated/newer file.
            let (cur_inode, size, _) = Self::file_identity(Path::new(path));
            let identity_ok = match (marker_inode, cur_inode) {
                (Some(m), Some(cur)) => *m == cur,
                (None, None) => true, // platform without inode support: size-only
                _ => false,
            };
            if !identity_ok {
                tracing::warn!(
                    path,
                    "skipping delete-after-commit: file identity changed (possible rotation/replacement)"
                );
            } else if *end_byte >= size {
                let _ = std::fs::remove_file(path);
            }
        }
        Ok(())
    }

    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError> {
        let ProgressMarker::File {
            path, start_byte, ..
        } = marker
        else {
            return Err(VtopError::Source(
                "file adapter given non-file marker".into(),
            ));
        };
        // Rewind the read head to the *start* of the uncommitted range so the
        // data is reprocessed. Never moves the committed point forward.
        let c = self.cursors.entry(path.clone()).or_default();
        c.read_byte = (*start_byte).max(c.committed_byte);
        tracing::warn!(
            path,
            read_byte = c.read_byte,
            "file source rewound for replay"
        );
        Ok(())
    }

    fn source_type(&self) -> SourceType {
        SourceType::File
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

    fn write_log(lines: &[&str]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        f.flush().unwrap();
        f
    }

    fn src(path: &str) -> DiscoveredSource {
        DiscoveredSource {
            source_type: SourceType::File,
            source_name: path.to_string(),
            format: TelemetryFormat::Raw,
        }
    }

    #[tokio::test]
    async fn reads_lines_and_tracks_offset() {
        let f = write_log(&["a", "b", "c"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let reads = fs
            .read_batch_candidates(&src(&path), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // A file is a single committable unit, so the Vec is always length 1;
        // assert it rather than indexing blind, so a regression that returns 0
        // or 2 fails here instead of panicking on the index.
        assert_eq!(reads.len(), 1);
        let r = &reads[0];
        assert_eq!(r.records.len(), 3);
        assert_eq!(r.records[0], b"a");
        // end marker byte offset is past the data.
        if let ProgressMarker::File { end_byte, .. } = &r.progress_end {
            assert_eq!(*end_byte, 6); // "a\nb\nc\n"
        } else {
            panic!("expected file marker");
        }
    }

    #[tokio::test]
    async fn resumes_from_committed_byte() {
        let f = write_log(&["one", "two", "three"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let reads1 = fs
            .read_batch_candidates(&src(&path), 1, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads1.len(), 1);
        let r1 = &reads1[0];
        assert_eq!(r1.records, vec![b"one".to_vec()]);

        // Commit only the first record.
        fs.commit_progress(&r1.progress_end).await.unwrap();

        // Next read resumes after "one\n".
        let reads2 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads2.len(), 1);
        assert_eq!(reads2[0].records, vec![b"two".to_vec(), b"three".to_vec()]);
    }

    #[tokio::test]
    async fn replay_rewinds_uncommitted_read() {
        let f = write_log(&["x", "y", "z"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);

        let reads1 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads1.len(), 1);
        assert_eq!(reads1[0].records.len(), 3);
        // No commit. Simulate crash + replay from start of range.
        fs.replay_from_marker(&reads1[0].progress_start)
            .await
            .unwrap();
        let reads2 = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(reads2.len(), 1);
        assert_eq!(
            reads2[0].records.len(),
            3,
            "uncommitted data must be replayable"
        );
    }

    #[tokio::test]
    async fn ignores_partial_trailing_line() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        write!(f, "complete\npartial-no-newline").unwrap();
        f.flush().unwrap();
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::new(vec![path.clone()], TelemetryFormat::Raw, false);
        let reads = fs
            .read_batch_candidates(&src(&path), 10, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        // One file == one committable unit; see `reads_lines_and_tracks_offset`.
        assert_eq!(reads.len(), 1);
        assert_eq!(reads[0].records, vec![b"complete".to_vec()]);
    }

    /// #65: the marker's identity must describe the file whose BYTES were
    /// read (fstat on the open descriptor), so that after a rotation the
    /// recorded inode disagrees with the replacement and the delete guard
    /// protects it. A path-stat at marker-build time could fingerprint the
    /// replacement instead.
    #[cfg(unix)]
    #[tokio::test]
    async fn marker_identity_comes_from_the_file_actually_read() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rotating.log");
        std::fs::write(&path, "old-1\nold-2\n").unwrap();
        let read_inode = std::fs::metadata(&path).unwrap().ino();
        let spath = path.to_string_lossy().into_owned();

        let mut fs = FileSource::new(vec![spath.clone()], TelemetryFormat::Raw, true);
        let reads = fs
            .read_batch_candidates(&src(&spath), 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        let ProgressMarker::File { inode, .. } = &reads[0].progress_end else {
            panic!("expected file marker");
        };
        assert_eq!(
            *inode,
            Some(read_inode),
            "marker carries the READ file's inode"
        );

        // Rotate: replace the path with a NEW file (new inode, same size so a
        // size-only check would be fooled).
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, "new-1\nnew-2\n").unwrap();
        assert_ne!(std::fs::metadata(&path).unwrap().ino(), read_inode);

        // Committing the OLD read with delete_after_commit=true must not
        // delete the replacement: the recorded identity disagrees with what
        // the path now names.
        fs.commit_progress(&reads[0].progress_end).await.unwrap();
        assert!(
            path.exists(),
            "rotation replacement must survive delete_after_commit"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new-1\nnew-2\n");
    }

    #[tokio::test]
    async fn delete_after_commit_skips_on_identity_mismatch() {
        let f = write_log(&["a"]);
        let path = f.path().to_string_lossy().into_owned();
        let mut fs = FileSource::with_mode(vec![path.clone()], TelemetryFormat::Raw, true, false);
        // Marker carries a stale inode (as if the file rotated since the read);
        // committing must NOT delete the now-different file on disk.
        let marker = ProgressMarker::File {
            path: path.clone(),
            inode: Some(u64::MAX),
            start_byte: 0,
            end_byte: 2,
            file_size: 2,
            mtime: String::new(),
        };
        fs.commit_progress(&marker).await.unwrap();
        assert!(
            std::path::Path::new(&path).exists(),
            "must not delete when the file identity no longer matches the marker"
        );
    }

    /// #96 B2: one pass reads MANY files concurrently. Every file's records
    /// must land in its own outcome with its own marker, cursors must advance
    /// per file, and a missing file must fail only its own outcome.
    #[tokio::test]
    async fn read_all_reads_many_files_concurrently_with_isolated_outcomes() {
        let f1 = write_log(&["one-a", "one-b"]);
        let f2 = write_log(&["two-a"]);
        let p1 = f1.path().to_string_lossy().into_owned();
        let p2 = f2.path().to_string_lossy().into_owned();
        let missing = format!("{p1}.does-not-exist");
        let mut fs = FileSource::new(vec![p1.clone(), p2.clone()], TelemetryFormat::Raw, false);

        let sources = vec![src(&p1), src(&p2), src(&missing)];
        let report = fs
            .read_all_batch_candidates(&sources, 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();

        assert_eq!(report.outcomes.len(), 3);
        // Outcomes come back in source order regardless of I/O completion order.
        let r1 = report.outcomes[0].result.as_ref().unwrap();
        assert_eq!(r1[0].records, vec![b"one-a".to_vec(), b"one-b".to_vec()]);
        let ProgressMarker::File { path, .. } = &r1[0].progress_end else {
            panic!("expected file marker")
        };
        assert_eq!(path, &p1, "marker names the outcome's own file");
        let r2 = report.outcomes[1].result.as_ref().unwrap();
        assert_eq!(r2[0].records, vec![b"two-a".to_vec()]);
        // The missing file fails ITS outcome only; the others are unaffected.
        assert!(report.outcomes[2].result.is_err());
        // Any data => the shared wall-clock bucket is productive.
        assert_eq!(report.empty_ms, 0);
        assert_eq!(report.failed_ms, 0);

        // Cursors advanced: a second pass over the good files reads nothing new.
        let report2 = fs
            .read_all_batch_candidates(&sources[..2], 100, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        for o in &report2.outcomes {
            let reads = o.result.as_ref().unwrap();
            assert!(
                reads[0].records.is_empty(),
                "no re-read after cursor advance"
            );
        }
    }
}
