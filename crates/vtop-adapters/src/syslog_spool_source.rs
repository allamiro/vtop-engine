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
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
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

    fn marker(path: &str, start: u64, end: u64) -> ProgressMarker {
        ProgressMarker::SyslogSpool {
            spool_id: Self::spool_id(path),
            path: path.to_string(),
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
    ) -> Result<(Vec<Vec<u8>>, u64), VtopError> {
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
            if n == 0 || !line.ends_with(b"\n") {
                break;
            }
            pos += n as u64;
            bytes_read += n as u64;
            line.pop();
            records.push(line);
        }
        Ok((records, pos))
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
        let (records, pos) = Self::read_slice(path.clone(), start, max_records, max_bytes).await?;
        self.cursors.get_mut(&path).unwrap().read_byte = pos;

        Ok(vec![ReadResult {
            progress_start: Self::marker(&path, start, start),
            progress_end: Self::marker(&path, start, pos),
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

        let mut results: Vec<(usize, String, u64, Result<(Vec<Vec<u8>>, u64), VtopError>)> =
            futures::stream::iter(jobs.into_iter().map(|(i, path, start)| async move {
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
                Ok((records, pos)) => {
                    self.cursors.get_mut(&path).unwrap().read_byte = pos;
                    any_records |= !records.is_empty();
                    Ok(vec![ReadResult {
                        progress_start: Self::marker(&path, start, start),
                        progress_end: Self::marker(&path, start, pos),
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
            path, start_byte, ..
        } = marker
        else {
            return Err(VtopError::Source(
                "spool adapter given non-spool marker".into(),
            ));
        };
        let c = self.cursors.entry(path.clone()).or_default();
        c.read_byte = (*start_byte).max(c.committed_byte);
        Ok(())
    }

    fn source_type(&self) -> SourceType {
        SourceType::SyslogSpool
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
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
}
