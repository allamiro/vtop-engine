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

/// Result of reading one source during a whole-adapter read pass.
pub struct SourceReadOutcome {
    /// Index into the `sources` slice passed to `read_all_batch_candidates`.
    pub source_index: usize,
    pub result: Result<Vec<ReadResult>, VtopError>,
}

/// Everything an adapter read across all its sources in one pass, plus how the
/// pass's wall-clock divided between reads that produced data, reads that
/// waited on nothing, and reads that failed. The buckets MUST sum to the time
/// the pass took: the engine's `read_cycle_profile` reconciles them against the
/// phase total, and unattributed time is indistinguishable from a hang.
pub struct AdapterReadReport {
    pub outcomes: Vec<SourceReadOutcome>,
    pub productive_ms: u64,
    pub empty_ms: u64,
    pub failed_ms: u64,
}

/// A telemetry source adapter.
#[async_trait]
pub trait SourceAdapter: Send + Sync {
    /// Enumerate the sources currently visible to this adapter.
    async fn discover_sources(&self) -> Result<Vec<DiscoveredSource>, VtopError>;

    /// Read up to `max_records` / `max_bytes` from `source`, waiting at most
    /// `max_wait` for data. Does NOT advance committed progress.
    /// Read up to the given budgets from one source.
    ///
    /// Returns a Vec because a single source can yield SEVERAL independently
    /// committable units: a Kafka topic interleaves partitions, and each
    /// partition needs its own progress marker and its own batch. Adapters with
    /// only one unit per source (file, syslog spool) return exactly one entry.
    /// The budgets apply across the whole call, not per returned entry.
    ///
    /// "Nothing to read" may be signalled EITHER as an empty Vec or as entries
    /// whose `records` are empty — Kafka returns the former (no partition
    /// yielded anything), file and syslog the latter (one unit, no new bytes).
    /// Callers must tolerate both; `Engine::run_cycle` does, by skipping any
    /// entry for which `ReadResult::is_empty()` holds. Adapters are not required
    /// to normalise, because doing so would mean inventing a progress marker for
    /// a partition that was never read.
    async fn read_batch_candidates(
        &mut self,
        source: &DiscoveredSource,
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<Vec<ReadResult>, VtopError>;

    /// Read from EVERY source in one pass. `max_records` / `max_bytes` are
    /// per-source budgets, exactly as passed to [`Self::read_batch_candidates`].
    ///
    /// The default walks the sources serially, paying up to `max_wait` per
    /// source — correct for adapters whose sources are independent handles
    /// (file, syslog spool). An adapter whose sources share ONE underlying
    /// consumer (Kafka: 29 topics, one `BaseConsumer`) must override this to
    /// multiplex a single wait across all of them: serial per-source polling on
    /// a shared consumer was measured at 87-92% of the read cycle waiting on
    /// empty sources (#96).
    ///
    /// A returned `Err` means the whole pass failed and NO source progressed
    /// (per-source failures are reported inside `outcomes`).
    async fn read_all_batch_candidates(
        &mut self,
        sources: &[DiscoveredSource],
        max_records: usize,
        max_bytes: usize,
        max_wait: Duration,
    ) -> Result<AdapterReadReport, VtopError> {
        let mut report = AdapterReadReport {
            outcomes: Vec::with_capacity(sources.len()),
            productive_ms: 0,
            empty_ms: 0,
            failed_ms: 0,
        };
        for (source_index, source) in sources.iter().enumerate() {
            let started = std::time::Instant::now();
            let result = self
                .read_batch_candidates(source, max_records, max_bytes, max_wait)
                .await;
            let elapsed = started.elapsed().as_millis() as u64;
            match &result {
                Err(_) => report.failed_ms += elapsed,
                Ok(reads) if reads.iter().any(|r| !r.records.is_empty()) => {
                    report.productive_ms += elapsed
                }
                Ok(_) => report.empty_ms += elapsed,
            }
            report.outcomes.push(SourceReadOutcome {
                source_index,
                result,
            });
        }
        Ok(report)
    }

    /// Commit source progress. MUST only be invoked by the engine after the
    /// batch reaches `VERIFIED`.
    ///
    /// Progress is ONLY ever expressed through the explicit `marker` (taken
    /// from a VERIFIED batch). There is deliberately no "current source"
    /// accessor on this trait: a single-slot notion of the active source is
    /// meaningless once one read pass serves many sources and partitions, and
    /// carrying one invites committing against whatever was touched last
    /// (#96 B1).
    async fn commit_progress(&mut self, marker: &ProgressMarker) -> Result<(), VtopError>;

    /// Rewind the read position to a marker so uncommitted data is replayed.
    async fn replay_from_marker(&mut self, marker: &ProgressMarker) -> Result<(), VtopError>;

    fn source_type(&self) -> SourceType;

    /// Downcast hook so the engine can seed concrete adapters (e.g. file /
    /// syslog committed byte offsets) during recovery.
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}
