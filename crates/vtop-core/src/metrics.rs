//! End-to-end pipeline metrics for a single batch.
//!
//! Captures the cost and efficiency of moving one batch through the pipeline:
//! per-stage timing, payload sizes, compression ratio, and derived throughput.
//! The engine fills these in and emits them as a structured `batch_metrics`
//! event; the CLI can print a per-batch summary. These per-batch records are
//! the raw material for the aggregate counters described in the README
//! observability section (e.g. `bytes_uploaded_total`, `upload_latency_seconds`).

use serde::{Deserialize, Serialize};

/// Timing + size metrics for one processed batch.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BatchMetrics {
    pub batch_id: String,
    pub records: usize,

    /// Uncompressed payload size (sum of record bytes + separators).
    pub uncompressed_bytes: u64,
    /// Size of the compressed object actually uploaded.
    pub compressed_bytes: u64,
    /// `uncompressed / compressed` (higher = better). 1.0 when uncompressed.
    pub compression_ratio: f64,
    /// Space saved as a percentage: `(1 - compressed/uncompressed) * 100`.
    pub space_saved_pct: f64,

    // Per-stage wall-clock, milliseconds.
    pub compress_ms: u64,
    pub checksum_ms: u64,
    pub object_upload_ms: u64,
    pub manifest_upload_ms: u64,
    pub verify_ms: u64,
    pub commit_ms: u64,
    /// Cumulative time in ledger writes across the whole pipeline (~8 writes
    /// per batch: initial save + 6 transitions + commit). Previously the
    /// unmeasured gap between staged time and `total_ms` (#87).
    pub state_write_ms: u64,
    /// Total time from batch start to source-committed.
    pub total_ms: u64,

    // Derived throughput (based on uncompressed size and total time).
    pub records_per_sec: f64,
    pub uncompressed_mib_per_sec: f64,
    /// Effective upload throughput of the compressed object (bytes on the wire).
    pub upload_mib_per_sec: f64,
}

impl BatchMetrics {
    pub fn new(batch_id: impl Into<String>, records: usize, uncompressed_bytes: u64) -> Self {
        Self {
            batch_id: batch_id.into(),
            records,
            uncompressed_bytes,
            compression_ratio: 1.0,
            ..Default::default()
        }
    }

    /// Record compression output and derive ratio / space saved.
    pub fn set_compression(&mut self, compressed_bytes: u64) {
        self.compressed_bytes = compressed_bytes;
        if compressed_bytes > 0 {
            self.compression_ratio = self.uncompressed_bytes as f64 / compressed_bytes as f64;
        }
        if self.uncompressed_bytes > 0 {
            self.space_saved_pct =
                (1.0 - (compressed_bytes as f64 / self.uncompressed_bytes as f64)) * 100.0;
        }
    }

    /// Finalize derived throughput numbers from `total_ms`.
    pub fn finalize(&mut self, total_ms: u64) {
        self.total_ms = total_ms;
        let secs = (total_ms as f64) / 1000.0;
        if secs > 0.0 {
            self.records_per_sec = self.records as f64 / secs;
            self.uncompressed_mib_per_sec =
                (self.uncompressed_bytes as f64 / (1024.0 * 1024.0)) / secs;
        }
        let upload_secs = (self.object_upload_ms as f64) / 1000.0;
        if upload_secs > 0.0 {
            self.upload_mib_per_sec =
                (self.compressed_bytes as f64 / (1024.0 * 1024.0)) / upload_secs;
        }
    }

    /// One-line human-readable summary.
    pub fn summary(&self) -> String {
        format!(
            "{} records, {}->{} ({:.2}x, {:.1}% saved) in {} ms | {:.0} rec/s, {:.2} MiB/s up | \
             stages: compress={}ms checksum={}ms put_obj={}ms put_manifest={}ms verify={}ms commit={}ms",
            self.records,
            human_bytes(self.uncompressed_bytes),
            human_bytes(self.compressed_bytes),
            self.compression_ratio,
            self.space_saved_pct,
            self.total_ms,
            self.records_per_sec,
            self.upload_mib_per_sec,
            self.compress_ms,
            self.checksum_ms,
            self.object_upload_ms,
            self.manifest_upload_ms,
            self.verify_ms,
            self.commit_ms,
        )
    }
}

fn human_bytes(b: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_ratio_and_savings() {
        let mut m = BatchMetrics::new("b1", 100, 1000);
        m.set_compression(250);
        assert!((m.compression_ratio - 4.0).abs() < 1e-9);
        assert!((m.space_saved_pct - 75.0).abs() < 1e-9);
    }

    #[test]
    fn throughput_from_total() {
        let mut m = BatchMetrics::new("b1", 1000, 2 * 1024 * 1024);
        m.object_upload_ms = 500;
        m.set_compression(1024 * 1024);
        m.finalize(1000); // 1 second
        assert!((m.records_per_sec - 1000.0).abs() < 1e-6);
        assert!((m.uncompressed_mib_per_sec - 2.0).abs() < 1e-6);
        // 1 MiB compressed uploaded in 0.5s = 2 MiB/s
        assert!((m.upload_mib_per_sec - 2.0).abs() < 1e-6);
    }

    #[test]
    fn uncompressed_ratio_is_one() {
        let mut m = BatchMetrics::new("b1", 1, 100);
        m.set_compression(100);
        assert!((m.compression_ratio - 1.0).abs() < 1e-9);
        assert!(m.space_saved_pct.abs() < 1e-9);
    }
}
