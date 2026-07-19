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
    ) -> Result<(Vec<Vec<u8>>, u64, Option<u64>), VtopError> {
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
        // a concurrent spool rotation (#127).
        let inode = inode_of(&reader.get_ref().metadata().await?);
        Ok((records, pos, inode))
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
        let (records, pos, inode) =
            Self::read_slice(path.clone(), start, max_records, max_bytes).await?;
        self.cursors.get_mut(&path).unwrap().read_byte = pos;

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
            Result<(Vec<Vec<u8>>, u64, Option<u64>), VtopError>,
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
        for (source_index, path, start, res) in results {
            let result = match res {
                Ok((records, pos, inode)) => {
                    self.cursors.get_mut(&path).unwrap().read_byte = pos;
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
}
