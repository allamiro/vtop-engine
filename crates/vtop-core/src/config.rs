//! Configuration model, parsed from `config.yaml` / `streams.yaml`.
//!
//! Secrets (access keys, passwords) are NOT part of this model; they come from
//! environment variables or mounted secrets and are never serialized.

use crate::errors::VtopError;
use crate::manifest::ManifestMacKey;
use crate::types::{ChecksumAlgorithm, CompressionType, SourceType, TelemetryFormat};
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtopConfig {
    pub engine: EngineConfig,
    pub batching: BatchingConfig,
    pub compression: CompressionConfig,
    #[serde(default)]
    pub checksum: ChecksumConfig,
    /// Name of the environment variable holding a 32-byte hex manifest MAC
    /// key. The secret itself is never part of this serializable config.
    #[serde(default)]
    pub manifest_mac_key_env: Option<String>,
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
    /// SQLite may be configured inline. PostgreSQL connection URLs may contain
    /// credentials and therefore must be resolved from an environment variable
    /// or mounted secret file instead of living in serializable config.
    pub state_store: StateStoreConfig,
    pub work_dir: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

/// Serializable reference to the engine's state-store connection.
///
/// Supported YAML forms:
///
/// ```yaml
/// state_store: "sqlite:///data/state/vtop-state.db"
/// state_store: { env: VTOP_STATE_STORE }
/// state_store: { file: /run/secrets/vtop-state-store }
/// ```
///
/// An inline PostgreSQL URL is rejected by [`VtopConfig::validate`]. Custom
/// `Debug` and `Serialize` implementations additionally redact one if an
/// unvalidated value is constructed programmatically.
#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub enum StateStoreConfig {
    Inline(String),
    Env(StateStoreEnvRef),
    File(StateStoreFileRef),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreEnvRef {
    pub env: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateStoreFileRef {
    pub file: String,
}

/// A resolved state-store connection string. It is deliberately neither
/// serializable nor printable: callers must opt in to borrowing the secret for
/// the narrow connection/lock operations that need it.
#[derive(Clone)]
pub struct ResolvedStateStore(String);

impl ResolvedStateStore {
    pub fn expose_secret(&self) -> &str {
        &self.0
    }

    pub fn is_postgres(&self) -> bool {
        is_postgres_url(&self.0)
    }
}

impl fmt::Debug for ResolvedStateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResolvedStateStore([REDACTED])")
    }
}

impl StateStoreConfig {
    pub fn resolve(&self) -> Result<ResolvedStateStore, VtopError> {
        let value = match self {
            Self::Inline(value) => value.clone(),
            Self::Env(reference) => std::env::var(reference.env.trim()).map_err(|_| {
                VtopError::Config(format!(
                    "state-store environment variable {} is missing or not valid Unicode",
                    reference.env.trim()
                ))
            })?,
            Self::File(reference) => {
                std::fs::read_to_string(reference.file.trim()).map_err(|e| {
                    VtopError::Config(format!(
                        "reading state-store secret file {}: {e}",
                        reference.file.trim()
                    ))
                })?
            }
        };
        let value = value.trim();
        if value.is_empty() {
            return Err(VtopError::Config(
                "resolved state-store connection must not be empty".into(),
            ));
        }
        Ok(ResolvedStateStore(value.to_owned()))
    }

    fn validate(&self) -> Result<(), VtopError> {
        match self {
            Self::Inline(value) if value.trim().is_empty() => Err(VtopError::Config(
                "engine.state_store must not be empty".into(),
            )),
            Self::Inline(value) if is_postgres_url(value) => Err(VtopError::Config(
                "PostgreSQL engine.state_store URLs must be loaded from a secret reference: use state_store: { env: VTOP_STATE_STORE } or state_store: { file: /run/secrets/vtop-state-store }"
                    .into(),
            )),
            Self::Env(reference) if reference.env.trim().is_empty() => Err(VtopError::Config(
                "engine.state_store.env must name a non-empty environment variable".into(),
            )),
            Self::File(reference) if reference.file.trim().is_empty() => Err(VtopError::Config(
                "engine.state_store.file must name a non-empty secret file path".into(),
            )),
            _ => Ok(()),
        }
    }
}

impl From<String> for StateStoreConfig {
    fn from(value: String) -> Self {
        Self::Inline(value)
    }
}

impl From<&str> for StateStoreConfig {
    fn from(value: &str) -> Self {
        Self::Inline(value.to_owned())
    }
}

impl fmt::Debug for StateStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inline(value) if is_postgres_url(value) => {
                f.write_str("Inline(\"postgres://[REDACTED]\")")
            }
            Self::Inline(value) => f.debug_tuple("Inline").field(value).finish(),
            Self::Env(reference) => f.debug_tuple("Env").field(reference).finish(),
            Self::File(reference) => f.debug_tuple("File").field(reference).finish(),
        }
    }
}

impl Serialize for StateStoreConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Inline(value) if is_postgres_url(value) => {
                serializer.serialize_str("postgres://[REDACTED]")
            }
            Self::Inline(value) => serializer.serialize_str(value),
            Self::Env(reference) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("env", &reference.env)?;
                map.end()
            }
            Self::File(reference) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("file", &reference.file)?;
                map.end()
            }
        }
    }
}

fn is_postgres_url(value: &str) -> bool {
    let lower = value.trim().to_ascii_lowercase();
    lower.starts_with("postgres://") || lower.starts_with("postgresql://")
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
    /// How many batches may be in the verify phase (compress, checksum,
    /// upload, manifest, verify) at once.
    ///
    /// That phase is ~64% blocking network I/O by measurement, so running one
    /// batch at a time leaves the CPU idle waiting on the object store — under
    /// a 1M rec/s load the engine sustained 9,140 rec/s at 1.44% CPU. Source
    /// commits stay serial regardless (they need exclusive adapter access),
    /// and the verify-before-commit invariant is per batch, so raising this
    /// cannot weaken it.
    #[serde(default = "default_max_concurrent_batches")]
    pub max_concurrent_batches: usize,
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
fn default_max_concurrent_batches() -> usize {
    8
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
    /// Absolute executable path for compatibility backends (`awscli`,
    /// `s3cmd`, `minio`). PATH lookup is deliberately forbidden.
    #[serde(default)]
    pub command_binary: Option<String>,
    /// Wall-clock limit for each external command invocation.
    #[serde(default = "default_command_timeout_seconds")]
    pub command_timeout_seconds: u64,
    /// Maximum captured stdout or stderr for command metadata/version calls.
    #[serde(default = "default_command_max_output_bytes")]
    pub command_max_output_bytes: usize,
    /// Exact environment-variable names copied into an otherwise empty child
    /// environment. Values remain runtime-only and are never serialized.
    #[serde(default)]
    pub command_env_allowlist: Vec<String>,
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
    /// If true, a batch is only committed when verification is derived from
    /// stored content (or a storage-service-computed digest). Backend-limited
    /// size/existence results fail the batch. Defaults to true; setting false
    /// is an explicit compatibility/lab opt-out.
    #[serde(default = "default_true")]
    pub require_strong_verification: bool,
}

fn default_backend() -> String {
    "s3_native".to_string()
}
fn default_region() -> String {
    "us-east-1".to_string()
}
fn default_command_timeout_seconds() -> u64 {
    300
}
fn default_command_max_output_bytes() -> usize {
    1024 * 1024
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
        self.engine.state_store.validate()?;
        if self
            .manifest_mac_key_env
            .as_deref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(VtopError::Config(
                "manifest_mac_key_env must name a non-empty environment variable".into(),
            ));
        }
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
        if matches!(self.upload.backend.as_str(), "awscli" | "s3cmd" | "minio") {
            let binary = self
                .upload
                .command_binary
                .as_deref()
                .filter(|path| !path.trim().is_empty())
                .ok_or_else(|| {
                    VtopError::Config(format!(
                        "upload.command_binary must be an explicit absolute path for the {} backend",
                        self.upload.backend
                    ))
                })?;
            if !Path::new(binary).is_absolute() {
                return Err(VtopError::Config(
                    "upload.command_binary must be an absolute path; PATH lookup is forbidden"
                        .into(),
                ));
            }
            if self.upload.command_timeout_seconds == 0 {
                return Err(VtopError::Config(
                    "upload.command_timeout_seconds must be > 0".into(),
                ));
            }
            if self.upload.command_max_output_bytes == 0 {
                return Err(VtopError::Config(
                    "upload.command_max_output_bytes must be > 0".into(),
                ));
            }
            for name in &self.upload.command_env_allowlist {
                if name.trim().is_empty() || name.contains('=') {
                    return Err(VtopError::Config(
                        "upload.command_env_allowlist entries must be non-empty environment-variable names"
                            .into(),
                    ));
                }
            }
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
        if self.batching.max_concurrent_batches == 0 {
            return Err(VtopError::Config(
                "batching.max_concurrent_batches must be > 0: zero would flush nothing".into(),
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

    /// Resolve the optional manifest authentication key from its named
    /// environment variable. A configured-but-missing key is a hard startup
    /// error, never a silent downgrade to unsigned manifests.
    pub fn resolve_manifest_mac_key(&self) -> Result<Option<ManifestMacKey>, VtopError> {
        let Some(name) = self.manifest_mac_key_env.as_deref() else {
            return Ok(None);
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(VtopError::Config(
                "manifest_mac_key_env must name a non-empty environment variable".into(),
            ));
        }
        let value = std::env::var(name).map_err(|_| {
            VtopError::Config(format!(
                "manifest MAC key environment variable {name} is missing or not valid Unicode"
            ))
        })?;
        ManifestMacKey::from_hex(value.trim()).map(Some)
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
        assert!(cfg.upload.require_strong_verification);
        assert!(cfg.resolve_manifest_mac_key().unwrap().is_none());
    }

    #[test]
    fn command_backends_require_an_absolute_binary_and_bounded_policy() {
        let yaml = r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching: {}
compression: {}
sources: {}
upload:
  bucket: telemetry-data
  backend: awscli
"#;
        let missing: VtopConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(missing
            .validate()
            .unwrap_err()
            .to_string()
            .contains("command_binary"));

        let relative = yaml.replace(
            "  backend: awscli",
            "  backend: awscli\n  command_binary: aws",
        );
        let relative: VtopConfig = serde_yaml::from_str(&relative).unwrap();
        assert!(relative
            .validate()
            .unwrap_err()
            .to_string()
            .contains("absolute path"));

        let absolute = yaml.replace(
            "  backend: awscli",
            "  backend: awscli\n  command_binary: /usr/bin/aws",
        );
        let mut absolute: VtopConfig = serde_yaml::from_str(&absolute).unwrap();
        absolute.validate().unwrap();
        assert_eq!(absolute.upload.command_timeout_seconds, 300);
        assert_eq!(absolute.upload.command_max_output_bytes, 1024 * 1024);

        absolute.upload.command_timeout_seconds = 0;
        assert!(absolute
            .validate()
            .unwrap_err()
            .to_string()
            .contains("command_timeout_seconds"));
    }

    #[test]
    fn configured_manifest_key_must_exist_without_silent_downgrade() {
        let name = format!("VTOP_TEST_MISSING_MAC_{}", uuid::Uuid::new_v4());
        let yaml = format!(
            r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching: {{}}
compression: {{}}
manifest_mac_key_env: {name}
sources:
  file:
    enabled: true
    paths: ["/data/*.log"]
upload:
  bucket: telemetry-data
  backend: s3_native
"#
        );
        let cfg: VtopConfig = serde_yaml::from_str(&yaml).unwrap();
        cfg.validate().unwrap();
        let err = cfg.resolve_manifest_mac_key().unwrap_err().to_string();
        assert!(err.contains(&name));
        assert!(err.contains("missing"));
    }

    #[test]
    fn postgres_state_store_requires_a_secret_reference() {
        let secret = "database-password-that-must-not-leak";
        let state_store = StateStoreConfig::Inline(format!(
            "postgres://vtop:{secret}@db.example/vtop?sslmode=verify-full"
        ));
        let err = state_store.validate().unwrap_err().to_string();
        assert!(err.contains("secret reference"));
        assert!(!err.contains(secret));

        let serialized = serde_yaml::to_string(&state_store).unwrap();
        let debug = format!("{state_store:?}");
        assert!(!serialized.contains(secret));
        assert!(!debug.contains(secret));
        assert!(serialized.contains("REDACTED"));
        assert!(debug.contains("REDACTED"));
    }

    #[test]
    fn state_store_secret_references_round_trip_without_secret_material() {
        let env: StateStoreConfig = serde_yaml::from_str("env: VTOP_STATE_STORE\n").unwrap();
        let file: StateStoreConfig =
            serde_yaml::from_str("file: /run/secrets/vtop-state-store\n").unwrap();

        let env_yaml = serde_yaml::to_string(&env).unwrap();
        let file_yaml = serde_yaml::to_string(&file).unwrap();
        assert!(env_yaml.contains("VTOP_STATE_STORE"));
        assert!(file_yaml.contains("/run/secrets/vtop-state-store"));
        env.validate().unwrap();
        file.validate().unwrap();
    }

    #[test]
    fn state_store_secret_file_is_trimmed_and_resolved_once() {
        use std::io::Write;

        let mut secret = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            secret,
            "postgres://vtop:password@localhost/vtop?sslmode=disable"
        )
        .unwrap();
        let reference = StateStoreConfig::File(StateStoreFileRef {
            file: secret.path().to_string_lossy().into_owned(),
        });
        let resolved = reference.resolve().unwrap();
        assert_eq!(
            resolved.expose_secret(),
            "postgres://vtop:password@localhost/vtop?sslmode=disable"
        );
        assert!(!format!("{resolved:?}").contains("password"));
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
    fn concurrency_knob_defaults_and_rejects_zero() {
        // Absent -> tuned default (not 1, which would be the old serial
        // behaviour, and not 0, which would flush nothing).
        let cfg: BatchingConfig = serde_yaml::from_str("{}").unwrap();
        assert_eq!(cfg.max_concurrent_batches, 8);

        let yaml = r#"
engine:
  name: vtop-engine
  state_store: "sqlite::memory:"
  work_dir: /tmp/work
batching:
  max_concurrent_batches: 0
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
            err.contains("max_concurrent_batches"),
            "unexpected error: {err}"
        );
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
