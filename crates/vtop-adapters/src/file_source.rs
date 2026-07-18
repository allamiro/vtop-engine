//! File source adapter.
//!
//! Reads append-only log files line by line, tracking byte offsets, file
//! identity (inode), size, and mtime. Resumes from the last committed byte and
//! never deletes source files unless explicitly configured and only after the
//! batch is committed.

use crate::base::{DiscoveredSource, ReadResult, SourceAdapter};
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
    active: Option<String>,
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
            active: None,
        }
    }

    /// Seed a committed byte offset (used by the replay engine on startup).
    pub fn seed_committed(&mut self, path: &str, committed_byte: u64) {
        let c = self.cursors.entry(path.to_string()).or_default();
        c.committed_byte = committed_byte;
        c.read_byte = committed_byte;
    }

    fn file_identity(path: &Path) -> (Option<u64>, u64, String) {
        match std::fs::metadata(path) {
            Ok(md) => {
                let inode = inode_of(&md);
                let size = md.len();
                let mtime = md
                    .modified()
                    .ok()
                    .map(|t| {
                        let dt: chrono::DateTime<chrono::Utc> = t.into();
                        dt.to_rfc3339()
                    })
                    .unwrap_or_default();
                (inode, size, mtime)
            }
            Err(_) => (None, 0, String::new()),
        }
    }

    fn marker(&self, path: &str, start: u64, end: u64) -> ProgressMarker {
        let (inode, file_size, mtime) = Self::file_identity(Path::new(path));
        ProgressMarker::File {
            path: path.to_string(),
            inode,
            start_byte: start,
            end_byte: end,
            file_size,
            mtime,
        }
    }
}

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
        self.active = Some(path.clone());
        let start = self.cursors.entry(path.clone()).or_default().read_byte;

        // Whole-file mode: read the entire remaining file as one opaque record.
        // Used for binary / already-compressed source files with no line
        // structure. The whole file commits as a single byte range.
        if self.whole_file {
            // Whole-file mode loads the entire remaining file into memory. Warn
            // when it exceeds the batch byte budget so operators know a single
            // large file can dominate memory (streaming is a documented
            // follow-up; see README known limitations).
            let (_, fsize, _) = Self::file_identity(Path::new(&path));
            if fsize.saturating_sub(start) as usize > max_bytes {
                tracing::warn!(
                    path,
                    file_bytes = fsize,
                    max_bytes,
                    "whole-file source exceeds max_bytes; loading entire file into memory"
                );
            }
            let data = tokio::fs::read(&path).await?;
            let end = data.len() as u64;
            let records = if start >= end || data.is_empty() {
                Vec::new()
            } else {
                vec![data[start as usize..].to_vec()]
            };
            self.cursors.get_mut(&path).unwrap().read_byte = end;
            return Ok(vec![ReadResult {
                progress_start: self.marker(&path, start, start),
                progress_end: self.marker(&path, start, end),
                records,
                first_timestamp: None,
                last_timestamp: None,
                // Whole-file: the record is raw object bytes — emit verbatim.
                verbatim: true,
            }]);
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

        // Advance the (uncommitted) read head.
        self.cursors.get_mut(&path).unwrap().read_byte = pos;

        Ok(vec![ReadResult {
            progress_start: self.marker(&path, start, start),
            progress_end: self.marker(&path, start, pos),
            records,
            first_timestamp: None,
            last_timestamp: None,
            // Line mode: records are logical lines (newline stripped); re-framed
            // on serialization to stay byte-exact with the source range.
            verbatim: false,
        }])
    }

    async fn get_progress_marker(&self) -> Result<ProgressMarker, VtopError> {
        let path = self
            .active
            .clone()
            .ok_or_else(|| VtopError::Source("no active file source".into()))?;
        let c = self.cursors.get(&path).cloned().unwrap_or_default();
        Ok(self.marker(&path, c.committed_byte, c.read_byte))
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

    fn source_name(&self) -> String {
        self.active.clone().unwrap_or_default()
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
}
