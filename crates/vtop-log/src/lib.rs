//! Kafka-independent storage kernel for the native VTOP broker.
//!
//! The crate deliberately owns only the single-node durability boundary. It
//! has no Kafka, database, object-store, networking, or consensus dependency.
//! Replication and the control plane can therefore build on a small, stable
//! segment contract without leaking an external platform into that contract.

mod catalog;
mod codec;
mod segment;
mod types;

pub use catalog::{
    CatalogEntry, CatalogSegmentState, QuarantineReason, QuarantinedArtifacts, StartupCatalog,
};
pub use segment::{rebuild_index, ActiveSegment, SegmentReader};
pub use types::{
    AppendOutcome, Durability, FetchBatch, FetchedRecord, KeyRange, LogError, LogRecord,
    ParentRange, RangeLineage, RecoveryReport, SegmentConfig, SegmentCursor, SegmentDescriptor,
    SegmentId, SegmentManifest, VtopLogResult,
};
