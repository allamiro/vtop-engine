//! Source adapter trait and shared types.
//!
//! `commit_progress()` MUST only be called by the engine after the batch state
//! is `VERIFIED`. Adapters MUST NOT commit progress automatically.

use async_trait::async_trait;
use std::time::Duration;
use vtop_core::errors::VtopError;
use vtop_core::types::{ProgressMarker, SourceType, TelemetryFormat};

/// A source the engine can read batches from.
#[derive(Debug, Clone)]
pub struct DiscoveredSource {
    pub source_type: SourceType,
    pub source_name: String,
    pub format: TelemetryFormat,
}

/// The result of reading a set of batch candidates from a source.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// Records in source order. May be empty if nothing was available.
    pub records: Vec<Vec<u8>>,
    /// Marker at the start of this read range (the resumable position).
    pub progress_start: ProgressMarker,
    /// Marker at the end of this read range (the candidate commit point).
    pub progress_end: ProgressMarker,
    pub first_timestamp: Option<String>,
    pub last_timestamp: Option<String>,
    /// When true, the records are raw object bytes that MUST be concatenated
    /// verbatim (whole-file / binary mode). When false, records are logical
    /// lines and are re-framed with a trailing newline on serialization, so the
    /// stored object is byte-exact with the covered source range.
    pub verbatim: bool,
}

impl ReadResult {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// A telemetry source adapter.
#[async_trait]
pub trait SourceAdapter: Send + Sync {
    /// Enumerate the sources currently visible to this adapter.
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError>;

    /// Read up to `max_records` / `max_bytes` from `source`, waiting at most
    /// `max_wait` for data. Does NOT advance committed progress.
    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<ReadResult, VtopError>;

    /// The current resumable progress marker for the adapter's active source.
    async fn get_progress_marker(&self) -> Result<ProgressMarker, VtopError>;

    /// Commit source progress. MUST only be invoked by the engine after the
    /// batch reaches `VERIFIED`.
    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError>;

    /// Rewind the read position to a marker so uncommitted data is replayed.
    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError>;

    fn source_type(&self) -> SourceType;

    fn source_name(&self) -> String;

    /// Downcast hook so the engine can seed concrete adapters (e.g. file /
    /// syslog committed byte offsets) during recovery.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
