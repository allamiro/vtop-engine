// The same file one release later.
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VtopConfig {
    /// Never serialized; not a key.
    #[serde(skip)]
    pub configured_fields: std::collections::BTreeSet<String>,
    pub engine: EngineConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    pub upload: UploadConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    pub name: String,
    #[serde(default = "default_tenant")]
    pub tenant: String,
    /// A new key with a default.
    #[serde(default = "default_work_cap")]
    pub work_cap: u64,
    /// A new key that is unset by default, and the reference says nothing.
    #[serde(default)]
    pub pinned_zone: Option<String>,
    /// A new key a config must set.
    pub region_id: String,
    /// Written under another name, as the config reads it.
    #[serde(rename = "labelName", default)]
    pub label_name: Option<String>,
    /// A new key the reference says an older engine ignores.
    #[serde(default)]
    pub audit_sink: Option<String>,
    /// Never part of the YAML.
    #[serde(skip)]
    pub resolved_zone: Option<String>,
}

/// Unknown keys are refused.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    #[serde(default)]
    pub strict_mode: bool,
    #[serde(default)]
    pub new_toggle: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    pub bucket: String,
    #[serde(default)]
    pub transports: BTreeMap<String, Tuning>,
    /// A new block.
    #[serde(default)]
    pub lock: Option<LockConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuning {
    #[serde(default)]
    pub window: Option<u32>,
    #[serde(default = "default_burst")]
    pub burst: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockConfig {
    pub mode: String,
    pub days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsConfig {
    pub streams: Vec<StreamConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub source_name: String,
    #[serde(default)]
    pub label: Option<String>,
}
