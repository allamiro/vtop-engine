//! # vtop-core
//!
//! Protocol-independent engine logic for the **VTOP Engine** (Verified
//! Telemetry Object Protocol Engine) — a prototype / reference implementation
//! of a replay-safe, manifest-driven telemetry object transfer engine.
//!
//! This crate has **no** dependency on Kafka, S3, or the CLI. It defines the
//! batch model, the state machine, checksums, compression, manifests,
//! partitioning, config, and replay-decision logic.
//!
//! The central safety rule, enforced in [`state_machine`]:
//!
//! ```text
//! SOURCE_COMMITTED is forbidden until VERIFIED is true.
//! ```

pub mod batch;
pub mod checksum;
pub mod compression;
pub mod config;
pub mod detect;
pub mod errors;
pub mod manifest;
pub mod metrics;
pub mod partitioning;
pub mod replay;
pub mod state_machine;
pub mod telemetry;
pub mod types;

pub use errors::{VtopError, VtopResult};
pub use state_machine::{can_transition, transition, BatchState};
pub use types::{
    BatchId, ChecksumAlgorithm, CompressionType, ManifestUri, ObjectUri, ProgressMarker,
    SourceName, SourceType, TelemetryFormat, TenantId,
};
