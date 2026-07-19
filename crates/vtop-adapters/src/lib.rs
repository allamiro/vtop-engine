//! # vtop-adapters
//!
//! Source adapters for the VTOP Engine. Each adapter implements
//! [`base::SourceAdapter`]. `commit_progress` MUST only be invoked by the
//! engine after a batch reaches `VERIFIED`; adapters never auto-commit.

pub mod base;
pub mod file_source;
#[cfg(feature = "kafka")]
pub mod kafka_source;
pub mod syslog_spool_source;

pub use base::{DiscoveredSource, ReadResult, SourceAdapter};
pub use file_source::FileSource;
#[cfg(feature = "kafka")]
pub use kafka_source::KafkaSource;
pub use syslog_spool_source::SyslogSpoolSource;
