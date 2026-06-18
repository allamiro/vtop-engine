//! Telemetry batch model and the adaptive batcher.

use crate::errors::VtopError;
use crate::state_machine::{transition, BatchState};
use crate::types::{ProgressMarker, SourceType, TelemetryFormat};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A sealed or in-progress collection of telemetry records bound to a source
/// progress range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryBatch {
    pub batch_id: String,
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    /// Raw record bytes, in source order. Each entry is one record (line).
    pub records: Vec<Vec<u8>>,
    pub record_count: usize,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    pub progress_start: ProgressMarker,
    pub progress_end: ProgressMarker,
    pub created_at: String,
    pub sealed_at: Option<String>,
    pub state: BatchState,
}

impl TelemetryBatch {
    /// Total uncompressed byte size of all records (excluding separators).
    pub fn byte_size(&self) -> usize {
        self.records.iter().map(|r| r.len()).sum()
    }

    /// Serialize the records into a single contiguous buffer, one record per
    /// line (newline-terminated). Source order is preserved.
    pub fn to_record_bytes(&self) -> Vec<u8> {
        let total: usize = self.byte_size() + self.records.len();
        let mut buf = Vec::with_capacity(total);
        for rec in &self.records {
            buf.extend_from_slice(rec);
            if !rec.ends_with(b"\n") {
                buf.push(b'\n');
            }
        }
        buf
    }

    /// Seal the batch: transition `Batching -> Sealed`, stamp `sealed_at`, and
    /// finalize the record count. A sealed batch is immutable.
    pub fn seal(&mut self) -> Result<(), VtopError> {
        self.state = transition(self.state, BatchState::Sealed)?;
        self.sealed_at = Some(Utc::now().to_rfc3339());
        self.record_count = self.records.len();
        Ok(())
    }
}

/// The reason a batch was sealed (for observability and tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealReason {
    MaxRecords,
    MaxBytes,
    MaxAge,
    PartitionChanged,
    ManualFlush,
    ShutdownFlush,
}

/// Thresholds that control adaptive batch sealing.
#[derive(Debug, Clone)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_batch_age_seconds: u64,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            max_bytes: 104_857_600,
            max_batch_age_seconds: 60,
        }
    }
}

/// Adaptive batcher. Accumulates records for a single source partition and
/// decides when to seal. It preserves source order and, for Kafka, does not
/// mix partitions (the engine constructs one batcher per partition).
pub struct AdaptiveBatcher {
    pub tenant: String,
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
    pub limits: BatchLimits,
    records: Vec<Vec<u8>>,
    bytes: usize,
    started_at: Option<chrono::DateTime<Utc>>,
    first_timestamp: Option<String>,
    last_timestamp: Option<String>,
    progress_start: Option<ProgressMarker>,
}

impl AdaptiveBatcher {
    pub fn new(
        tenant: impl Into<String>,
        source_type: SourceType,
        source_name: impl Into<String>,
        format: TelemetryFormat,
        limits: BatchLimits,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            source_type,
            source_name: source_name.into(),
            format,
            limits,
            records: Vec::new(),
            bytes: 0,
            started_at: None,
            first_timestamp: None,
            last_timestamp: None,
            progress_start: None,
        }
    }

    /// Number of records currently buffered.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Add a record. `marker` is the progress position *after* this record.
    /// The first marker observed becomes the batch's `progress_start`.
    pub fn push(
        &mut self,
        record: Vec<u8>,
        start_marker: &ProgressMarker,
        timestamp: Option<String>,
    ) {
        if self.progress_start.is_none() {
            self.progress_start = Some(start_marker.clone());
            self.started_at = Some(now_or_epoch());
            self.first_timestamp = timestamp.clone();
        }
        self.last_timestamp = timestamp.or_else(|| self.last_timestamp.clone());
        self.bytes += record.len();
        self.records.push(record);
    }

    /// Check whether any sealing threshold is met based on the buffered state.
    pub fn should_seal(&self, now: chrono::DateTime<Utc>) -> Option<SealReason> {
        if self.records.is_empty() {
            return None;
        }
        if self.records.len() >= self.limits.max_records {
            return Some(SealReason::MaxRecords);
        }
        if self.bytes >= self.limits.max_bytes {
            return Some(SealReason::MaxBytes);
        }
        if let Some(started) = self.started_at {
            let age = (now - started).num_seconds().max(0) as u64;
            if age >= self.limits.max_batch_age_seconds {
                return Some(SealReason::MaxAge);
            }
        }
        None
    }

    /// Build and seal a [`TelemetryBatch`] from the buffered records, resetting
    /// the batcher. `progress_end` is the marker covering the final record.
    pub fn seal(&mut self, progress_end: ProgressMarker) -> Result<TelemetryBatch, VtopError> {
        let progress_start = self
            .progress_start
            .take()
            .ok_or_else(|| VtopError::Other("cannot seal an empty batch".into()))?;

        let records = std::mem::take(&mut self.records);
        let record_count = records.len();
        let batch_id = build_batch_id(&self.source_name, &progress_end);

        let mut batch = TelemetryBatch {
            batch_id,
            tenant: self.tenant.clone(),
            source_type: self.source_type.clone(),
            source_name: self.source_name.clone(),
            format: self.format.clone(),
            records,
            record_count,
            first_timestamp: self.first_timestamp.take(),
            last_timestamp: self.last_timestamp.take(),
            progress_start,
            progress_end,
            created_at: Utc::now().to_rfc3339(),
            sealed_at: None,
            // The batcher hands over a batch already past BATCHING.
            state: BatchState::Batching,
        };

        // reset counters
        self.bytes = 0;
        self.started_at = None;

        batch.seal()?;
        Ok(batch)
    }
}

fn now_or_epoch() -> chrono::DateTime<Utc> {
    Utc::now()
}

/// Build a deterministic, filesystem-safe batch id of the form
/// `vtop-<UTCstamp>-<source>-<range>-<short-uuid>`.
pub fn build_batch_id(source_name: &str, end_marker: &ProgressMarker) -> String {
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let safe_source = sanitize_token(source_name);
    let range = end_marker.range_token();
    let suffix = Uuid::new_v4().simple().to_string();
    let short = &suffix[..8];
    format!("vtop-{stamp}-{safe_source}-{range}-{short}")
}

/// Replace characters that are unsafe in object keys / file names.
pub fn sanitize_token(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kafka_marker(start: i64, end: i64) -> ProgressMarker {
        ProgressMarker::Kafka {
            topic: "app_events".into(),
            partition: 0,
            start_offset: start,
            end_offset: end,
            consumer_group: "vtop-engine".into(),
        }
    }

    #[test]
    fn seals_on_max_records() {
        let limits = BatchLimits {
            max_records: 3,
            max_bytes: usize::MAX,
            max_batch_age_seconds: u64::MAX,
        };
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            limits,
        );
        for i in 0..3 {
            b.push(b"rec".to_vec(), &kafka_marker(i, i), None);
        }
        assert_eq!(b.should_seal(Utc::now()), Some(SealReason::MaxRecords));
        let batch = b.seal(kafka_marker(0, 2)).unwrap();
        assert_eq!(batch.record_count, 3);
        assert_eq!(batch.state, BatchState::Sealed);
        assert!(batch.sealed_at.is_some());
    }

    #[test]
    fn seals_on_max_bytes() {
        let limits = BatchLimits {
            max_records: usize::MAX,
            max_bytes: 10,
            max_batch_age_seconds: u64::MAX,
        };
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            limits,
        );
        b.push(vec![0u8; 20], &kafka_marker(0, 0), None);
        assert_eq!(b.should_seal(Utc::now()), Some(SealReason::MaxBytes));
    }

    #[test]
    fn preserves_record_order() {
        let mut b = AdaptiveBatcher::new(
            "default",
            SourceType::Kafka,
            "app_events",
            TelemetryFormat::Cef,
            BatchLimits::default(),
        );
        for i in 0..5u8 {
            b.push(vec![i], &kafka_marker(i as i64, i as i64), None);
        }
        let batch = b.seal(kafka_marker(0, 4)).unwrap();
        let order: Vec<u8> = batch.records.iter().map(|r| r[0]).collect();
        assert_eq!(order, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn empty_batcher_does_not_seal() {
        let b = AdaptiveBatcher::new(
            "default",
            SourceType::File,
            "/x.log",
            TelemetryFormat::Raw,
            BatchLimits::default(),
        );
        assert_eq!(b.should_seal(Utc::now()), None);
    }

    #[test]
    fn batch_id_is_deterministic_in_shape() {
        let id = build_batch_id("app_events", &kafka_marker(481000, 482499));
        assert!(id.starts_with("vtop-"));
        assert!(id.contains("app_events"));
        assert!(id.contains("p0-481000-482499"));
    }
}
