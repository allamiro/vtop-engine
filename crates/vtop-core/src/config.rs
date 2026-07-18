//! Configuration model, parsed from `config.yaml` / `streams.yaml`.
//!
//! Secrets (access keys, passwords) are NOT part of this model; they come from
//! environment variables or mounted secrets and are never serialized.

use crate::errors::VtopError;
use crate::types::{ChecksumAlgorithm, CompressionType, SourceType, TelemetryFormat};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtopConfig {
    pub engine: EngineConfig,
    pub batching: BatchingConfig,
    pub compression: CompressionConfig,
    #[serde(default)]
    pub checksum: ChecksumConfig,
    pub sources: SourcesConfig,
    pub upload: UploadConfig,
    #[serde(default)]
    pub partitioning: PartitioningConfig,
}

/// Object integrity checksum configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumConfig {
    #[serde(default = "default_checksum")]
    pub algorithm: ChecksumAlgorithm,
}

impl Default for ChecksumConfig {
    fn default() -> Self {
        Self {
            algorithm: default_checksum(),
        }
    }
}

fn default_checksum() -> ChecksumAlgorithm {
    ChecksumAlgorithm::Sha256
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub name: String,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    pub state_store: String,
    pub work_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_tenant() -> String {
    "default".to_string()
}
fn default_log_level() -> String {
    "INFO".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchingConfig {
    #[serde(default = "default_max_records")]
    pub max_records: usize,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_max_age")]
    pub max_batch_age_seconds: u64,
    /// How long a single source read may block waiting for data.
    ///
    /// This is paid PER SOURCE, serially: with N Kafka topics, a cycle costs up
    /// to `N * source_poll_wait_ms` even when every topic is empty, because an
    /// empty topic burns the whole window before returning nothing. It used to
    /// be a hard-coded 2s, which put ~28 topics at ~56s per cycle. Kafka
    /// prefetches into a local queue, so a backlogged topic returns
    /// immediately regardless of this value — it only bounds the idle case.
    #[serde(default = "default_source_poll_wait_ms")]
    pub source_poll_wait_ms: u64,
    /// Pause between cycles when the previous cycle read nothing at all.
    ///
    /// A cycle that DID read data skips this entirely and loops straight into
    /// the next one, so a backlog is drained at full speed instead of being
    /// throttled by a fixed timer.
    #[serde(default = "default_idle_poll_interval_ms")]
    pub idle_poll_interval_ms: u64,
}

fn default_max_records() -> usize {
    10_000
}
fn default_max_bytes() -> usize {
    104_857_600
}
fn default_max_age() -> u64 {
    60
}
fn default_source_poll_wait_ms() -> u64 {
    250
}
fn default_idle_poll_interval_ms() -> u64 {
    2_000
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    #[serde(rename = "type", default = "default_compression")]
    pub kind: CompressionType,
    #[serde(default = "default_level")]
    pub level: i32,
}

fn default_compression() -> CompressionType {
    CompressionType::Gzip
}
fn default_level() -> i32 {
    6
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourcesConfig {
    #[serde(default)]
    pub kafka: Option<KafkaSourceConfig>,
    #[serde(default)]
    pub file: Option<FileSourceConfig>,
    #[serde(default)]
    pub syslog_spool: Option<SyslogSpoolConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceConfig {
    #[serde(default)]
    pub enabled: bool,
    pub bootstrap_servers: Vec<String>,
    #[serde(default = "default_group")]
    pub consumer_group: String,
    #[serde(default = "default_include")]
    pub topic_include_regex: String,
    #[serde(default = "default_exclude")]
    pub topic_exclude_regex: String,
    #[serde(default = "default_offset_reset")]
    pub auto_offset_reset: String,
    /// MUST be false. The engine never uses Kafka auto-commit.
    #[serde(default)]
    pub enable_auto_commit: bool,
    #[serde(default)]
    pub security_protocol: Option<String>,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username: Option<String>,
    /// Env var *name* holding the SASL password (never the secret itself).
    #[serde(default)]
    pub sasl_password_env: Option<String>,
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
}

fn default_group() -> String {
    "vtop-engine".to_string()
}
fn default_include() -> String {
    ".*".to_string()
}
fn default_exclude() -> String {
    "^__.*".to_string()
}
fn default_offset_reset() -> String {
    "earliest".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSourceConfig {
    #[serde(default)]
    pub enabled: bool,
    pub paths: Vec<String>,
    /// If true, the source file may be deleted *after* the batch is committed.
    #[serde(default)]
    pub delete_after_commit: bool,
    /// Read each file as a single whole-file record instead of line by line.
    /// Required for binary / already-compressed source files (which have no
    /// line structure). Default false (line-oriented).
    #[serde(default)]
    pub whole_file: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyslogSpoolConfig {
    #[serde(default)]
    pub enabled: bool,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    #[serde(default = "default_backend")]
    pub backend: String,
    pub bucket: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub force_path_style: bool,
    #[serde(default = "default_true")]
    pub verify_tls: bool,
    /// Optional named profile / alias for command-based backends.
    #[serde(default)]
    pub profile: Option<String>,
    /// Create the target bucket if it does not exist (native S3 backend only).
    /// Useful with a templated `bucket` like `telemetry-{format}` so per-format
    /// buckets are provisioned automatically. Defaults to false (least
    /// privilege — do not grant CreateBucket in production unless intended).
    #[serde(default)]
    pub create_bucket: bool,
    /// Root directory for the `localfs` backend (objects are written under
    /// `<local_path>/<bucket>/<key>`).
    #[serde(default)]
    pub local_path: Option<String>,
    /// If true, a batch is only committed when the backend can perform STRONG
    /// (checksum) verification. Backend-limited (size/existence-only) results
    /// then fail the batch instead of committing. Default false (backend-limited
    /// is accepted, as documented). Production should enable this.
    #[serde(default)]
    pub require_strong_verification: bool,
}

fn default_backend() -> String {
    "s3_native".to_string()
}
fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitioningConfig {
    #[serde(default = "default_template")]
    pub template: String,
}

impl Default for PartitioningConfig {
    fn default() -> Self {
        Self {
            template: default_template(),
        }
    }
}

fn default_template() -> String {
    crate::partitioning::DEFAULT_TEMPLATE.to_string()
}

impl VtopConfig {
    /// Load and validate config from a YAML file.
    pub fn from_path(path: &Path) -> Result<Self, VtopError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| VtopError::Config(format!("reading {}: {e}", path.display())))?;
        let cfg: VtopConfig = serde_yaml::from_str(&text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Enforce invariants that cannot be expressed by the type system.
    pub fn validate(&self) -> Result<(), VtopError> {
        if let Some(k) = &self.sources.kafka {
            if k.enabled && k.enable_auto_commit {
                return Err(VtopError::Config(
                    "kafka.enable_auto_commit MUST be false: the engine commits offsets only after VERIFIED".into(),
                ));
            }
            if k.enabled && k.bootstrap_servers.is_empty() {
                return Err(VtopError::Config(
                    "kafka.bootstrap_servers must not be empty when kafka is enabled".into(),
                ));
            }
        }
        if self.upload.bucket.trim().is_empty() {
            return Err(VtopError::Config("upload.bucket must not be empty".into()));
        }

        // Batching limits must be positive, or the engine would seal degenerate
        // (empty/one-record) batches or never bound memory sensibly.
        // A zero idle interval is a busy-wait, not a fast engine. With no data
        // available, `cycle_had_data` stays false and the loop takes a
        // Duration::ZERO backoff every iteration — re-running source discovery
        // and reads as fast as the CPU allows, pinning a core for no throughput.
        // File and syslog adapters return immediately at EOF, so they hit this
        // even without a broker involved.
        if self.batching.idle_poll_interval_ms == 0 {
            return Err(VtopError::Config(
                "batching.idle_poll_interval_ms must be > 0: zero busy-waits on an idle source \
                 instead of backing off"
                    .into(),
            ));
        }
        if self.batching.max_records == 0 {
            return Err(VtopError::Config("batching.max_records must be > 0".into()));
        }
        if self.batching.max_bytes == 0 {
            return Err(VtopError::Config("batching.max_bytes must be > 0".into()));
        }

        // An enabled file/syslog source with no paths silently does nothing.
        if let Some(f) = &self.sources.file {
            if f.enabled && f.paths.iter().all(|p| p.trim().is_empty()) {
                return Err(VtopError::Config(
                    "sources.file is enabled but has no (non-empty) paths".into(),
                ));
            }
        }
        if let Some(s) = &self.sources.syslog_spool {
            if s.enabled && s.paths.iter().all(|p| p.trim().is_empty()) {
                return Err(VtopError::Config(
                    "sources.syslog_spool is enabled but has no (non-empty) paths".into(),
                ));
            }
        }
        Ok(())
    }
}

/// A single configured stream mapping from `streams.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub source_name: String,
    pub source_type: SourceType,
    pub format: TelemetryFormat,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// Optional rename used in the object storage path (e.g. app_events -> app).
    #[serde(default)]
    pub s3_source_name: Option<String>,
    #[serde(default)]
    pub retention_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsConfig {
    pub streams: Vec<StreamConfig>,
}

impl StreamsConfig {
    pub fn from_path(path: &Path) -> Result<Self, VtopError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| VtopError::Config(format!("reading {}: {e}", path.display())))?;
        Ok(serde_yaml::from_str(&text)?)
    }

    /// Find the stream definition that matches a discovered source name.
    pub fn lookup(&self, source_name: &str) -> Option<&StreamConfig> {
        self.streams.iter().find(|s| s.source_name == source_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_kafka_auto_commit() {
        let yaml = r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching: {}
compression: {}
sources:
  kafka:
    enabled: true
    bootstrap_servers: ["kafka:9092"]
    enable_auto_commit: true
upload:
  bucket: telemetry-data
"#;
        let cfg: VtopConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, VtopError::Config(_)));
    }

    #[test]
    fn accepts_minimal_valid_config() {
        let yaml = r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching: {}
compression: {}
sources:
  file:
    enabled: true
    paths: ["/data/*.log"]
upload:
  bucket: telemetry-data
  backend: s3_native
"#;
        let cfg: VtopConfig = serde_yaml::from_str(yaml).unwrap();
        cfg.validate().unwrap();
        assert_eq!(cfg.batching.max_records, 10_000);
        assert_eq!(cfg.compression.kind, CompressionType::Gzip);
    }

    #[test]
    fn polling_knobs_default_when_absent() {
        // Back-compat: every config written before these fields existed must
        // still parse, and must get the tuned defaults rather than 0 (which
        // would busy-spin) or the old hard-coded 2s-per-source.
        let cfg: BatchingConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.source_poll_wait_ms, 250);
        assert_eq!(cfg.idle_poll_interval_ms, 2_000);
    }

    #[test]
    fn zero_idle_poll_interval_is_rejected() {
        // Zero would busy-wait: an idle cycle takes a Duration::ZERO backoff and
        // re-runs discovery/reads as fast as the CPU allows. A poll wait of zero
        // is fine by contrast (a non-blocking poll), so only the idle interval
        // is constrained.
        let yaml = r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching:
  idle_poll_interval_ms: 0
compression: {}
sources:
  file:
    enabled: true
    paths: ["/data/*.log"]
upload:
  bucket: telemetry-data
  backend: s3_native
"#;
        let cfg: VtopConfig = serde_yaml::from_str(yaml).unwrap();
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("idle_poll_interval_ms"),
            "unexpected error: {err}"
        );

        // A zero poll wait must still be accepted.
        let ok = yaml.replace("idle_poll_interval_ms: 0", "source_poll_wait_ms: 0");
        let cfg: VtopConfig = serde_yaml::from_str(&ok).unwrap();
        cfg.validate()
            .expect("source_poll_wait_ms: 0 is legitimate");
    }

    #[test]
    fn polling_knobs_are_overridable() {
        let cfg: BatchingConfig =
            serde_yaml::from_str("source_poll_wait_ms: 50\nidle_poll_interval_ms: 100").unwrap();
        assert_eq!(cfg.source_poll_wait_ms, 50);
        assert_eq!(cfg.idle_poll_interval_ms, 100);
    }

    #[test]
    fn parses_streams() {
        let yaml = r#"
streams:
  - source_name: app_events
    source_type: kafka
    format: cef
    tenant: default
    s3_source_name: app
    retention_class: standard
"#;
        let s: StreamsConfig = serde_yaml::from_str(yaml).unwrap();
        let m = s.lookup("app_events").unwrap();
        assert_eq!(m.source_type, SourceType::Kafka);
        assert_eq!(m.s3_source_name.as_deref(), Some("app"));
    }
}
