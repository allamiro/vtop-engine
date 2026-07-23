//! Kafka-independent storage kernel for the native VTOP broker.
//!
//! The crate deliberately owns only the single-node durability boundary. It
//! has no Kafka, database, object-store, networking, or consensus dependency.
//! Replication and the control plane can therefore build on a small, stable
//! segment contract without leaking an external platform into that contract.

mod catalog;
mod codec;
mod codec_v2;
pub mod env;
pub mod proof;
mod segment;
pub mod sim;
mod types;

pub use catalog::{
    CatalogEntry, CatalogSegmentState, QuarantineReason, QuarantinedArtifacts, StartupCatalog,
};
pub use codec_v2::RECORD_FRAME_OVERHEAD_BYTES_V2;
pub use segment::{
    rebuild_chunk_index, rebuild_chunk_index_in, rebuild_index, rebuild_index_in, ActiveSegment,
    SegmentReader,
};
pub use types::{
    AppendOutcome, CommitStatementV1, Durability, FetchBatch, FetchedRecord, KeyRange, LogError,
    LogRecord, ParentRange, ProducerSummaryEntry, RangeLineage, RecoveryReport, SegmentCommitKey,
    SegmentConfig, SegmentConfigV2, SegmentCursor, SegmentDescriptor, SegmentDescriptorV2,
    SegmentEvidence, SegmentId, SegmentManifest, SegmentManifestV2, VtopLogResult,
    CHUNK_SIDECAR_MAGIC, CHUNK_TREE_SCHEME_V1, COMMIT_SCHEME_KEYED, COMMIT_SCHEME_UNKEYED,
    FORMAT_VERSION_V2, PRODUCER_SEQUENCE_WINDOW, RECORD_SCHEMA_VERSION_V2,
};
