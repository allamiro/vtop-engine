//! Common protocol-independent type aliases and enums for the VTOP Engine.

use serde::{Deserialize, Serialize};

/// Unique identifier for a telemetry batch.
pub type BatchId = String;
/// Logical tenant that owns a batch / stream.
pub type TenantId = String;
/// Human-readable source name (topic, file path, spool id, ...).
pub type SourceName = String;
/// Fully-qualified object storage URI (e.g. `s3://bucket/key`).
pub type ObjectUri = String;
/// Fully-qualified manifest storage URI.
pub type ManifestUri = String;

/// The category of telemetry source a batch was produced from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Kafka,
    File,
    SyslogSpool,
}

impl SourceType {
    /// Stable string used in the state store and CLI.
    pub fn as_str(&self) -> &'static str {
        match self {
            SourceType::Kafka => "kafka",
            SourceType::File => "file",
            SourceType::SyslogSpool => "syslog_spool",
        }
    }
}

impl std::str::FromStr for SourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "kafka" => Ok(SourceType::Kafka),
            "file" => Ok(SourceType::File),
            "syslog_spool" | "syslog-spool" | "syslog" => Ok(SourceType::SyslogSpool),
            other => Err(format!("unknown source type: {other}")),
        }
    }
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The on-the-wire telemetry record format inside a batch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TelemetryFormat {
    Cef,
    Leef,
    Json,
    Jsonl,
    Syslog,
    Raw,
}

impl TelemetryFormat {
    /// Object file extension fragment for this format (no leading dot).
    pub fn extension(&self) -> &'static str {
        match self {
            TelemetryFormat::Cef => "cef",
            TelemetryFormat::Leef => "leef",
            TelemetryFormat::Json => "json",
            TelemetryFormat::Jsonl => "jsonl",
            TelemetryFormat::Syslog => "syslog",
            TelemetryFormat::Raw => "raw",
        }
    }

    pub fn as_str(&self) -> &'static str {
        self.extension()
    }
}

impl std::str::FromStr for TelemetryFormat {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "cef" => Ok(TelemetryFormat::Cef),
            "leef" => Ok(TelemetryFormat::Leef),
            "json" => Ok(TelemetryFormat::Json),
            "jsonl" | "ndjson" => Ok(TelemetryFormat::Jsonl),
            "syslog" => Ok(TelemetryFormat::Syslog),
            "raw" => Ok(TelemetryFormat::Raw),
            other => Err(format!("unknown telemetry format: {other}")),
        }
    }
}

impl std::fmt::Display for TelemetryFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Compression algorithm applied to a sealed batch.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompressionType {
    Gzip,
    Zstd,
    None,
}

impl CompressionType {
    /// File extension fragment for the compressed object (no leading dot),
    /// or `None` when compression is disabled.
    pub fn extension(&self) -> Option<&'static str> {
        match self {
            CompressionType::Gzip => Some("gz"),
            CompressionType::Zstd => Some("zst"),
            CompressionType::None => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::Gzip => "gzip",
            CompressionType::Zstd => "zstd",
            CompressionType::None => "none",
        }
    }
}

impl std::str::FromStr for CompressionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "gzip" | "gz" => Ok(CompressionType::Gzip),
            "zstd" | "zst" => Ok(CompressionType::Zstd),
            "none" => Ok(CompressionType::None),
            other => Err(format!("unknown compression type: {other}")),
        }
    }
}

/// Checksum algorithm used for object integrity. `None` disables checksums
/// (verification falls back to size + existence only — a comparison mode).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChecksumAlgorithm {
    Sha256,
    Blake3,
    None,
}

impl ChecksumAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            ChecksumAlgorithm::Sha256 => "sha256",
            ChecksumAlgorithm::Blake3 => "blake3",
            ChecksumAlgorithm::None => "none",
        }
    }

    /// Whether this algorithm produces a digest (false when disabled).
    pub fn is_enabled(&self) -> bool {
        !matches!(self, ChecksumAlgorithm::None)
    }
}

impl std::str::FromStr for ChecksumAlgorithm {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "sha256" | "sha-256" => Ok(ChecksumAlgorithm::Sha256),
            "blake3" | "blake-3" => Ok(ChecksumAlgorithm::Blake3),
            "none" | "disabled" | "off" => Ok(ChecksumAlgorithm::None),
            other => Err(format!("unknown checksum algorithm: {other}")),
        }
    }
}

impl std::fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Source-agnostic progress marker. This is the central object that binds a
/// physical source position to a batch, and ultimately to a verified object.
///
/// A marker MUST NOT be committed back to the source until the batch reaches
/// [`crate::state_machine::BatchState::Verified`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum ProgressMarker {
    Kafka {
        topic: String,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
        consumer_group: String,
    },
    File {
        path: String,
        inode: Option<u64>,
        start_byte: u64,
        end_byte: u64,
        file_size: u64,
        mtime: String,
    },
    SyslogSpool {
        spool_id: String,
        path: String,
        start_byte: u64,
        end_byte: u64,
        received_time_start: Option<String>,
        received_time_end: Option<String>,
    },
}

impl ProgressMarker {
    /// The [`SourceType`] this marker belongs to.
    pub fn source_type(&self) -> SourceType {
        match self {
            ProgressMarker::Kafka { .. } => SourceType::Kafka,
            ProgressMarker::File { .. } => SourceType::File,
            ProgressMarker::SyslogSpool { .. } => SourceType::SyslogSpool,
        }
    }

    /// A short, stable, filesystem-safe token describing the position range.
    /// Used to build deterministic batch ids.
    pub fn range_token(&self) -> String {
        match self {
            ProgressMarker::Kafka {
                partition,
                start_offset,
                end_offset,
                ..
            } => format!("p{partition}-{start_offset}-{end_offset}"),
            ProgressMarker::File {
                start_byte,
                end_byte,
                ..
            } => format!("b{start_byte}-{end_byte}"),
            ProgressMarker::SyslogSpool {
                start_byte,
                end_byte,
                ..
            } => format!("b{start_byte}-{end_byte}"),
        }
    }
}
