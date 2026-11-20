// A reduced config.rs: the shapes the key diff reads.
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
}

/// Unknown keys are refused.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    #[serde(default)]
    pub strict_mode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UploadConfig {
    pub bucket: String,
    #[serde(default)]
    pub transports: BTreeMap<String, Tuning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tuning {
    #[serde(default)]
    pub window: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamsConfig {
    pub streams: Vec<StreamConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    pub source_name: String,
}
