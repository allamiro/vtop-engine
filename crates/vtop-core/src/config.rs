//! Configuration model, parsed from `config.yaml` / `streams.yaml`.
//!
//! Secrets (access keys, passwords) are NOT part of this model; they come from
//! environment variables or mounted secrets and are never serialized.

use crate::errors::VtopError;
use crate::types::{CompressionType, SourceType, TelemetryFormat};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtopConfig {
    pub engine: EngineConfig,
    pub batching: BatchingConfig,
    pub compression: CompressionConfig,
    pub sources: SourcesConfig,
    pub upload: UploadConfig,
    #[serde(default)]
    pub partitioning: PartitioningConfig,
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
