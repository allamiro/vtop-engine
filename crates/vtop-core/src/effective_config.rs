//! Read-only, explicit allowlist for configuration explanations. No secret
//! reference or unlisted future config field can enter this representation.
use crate::config::{StreamsConfig, VtopConfig};
use crate::types::ChecksumAlgorithm;
use serde::Serialize;
use std::collections::BTreeSet;

/// Preferred verification path. Native S3 may fall back from a service SHA-256
/// to read-back when the service has no whole-object checksum (multipart).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationMethod {
    StoreChecksum,
    ReadBack,
    SizeOnly,
}
impl VerificationMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::StoreChecksum => "store_checksum",
            Self::ReadBack => "read_back",
            Self::SizeOnly => "size_only",
        }
    }
}

/// Pure policy shared by the display and upload callers. This lives with the
/// value model to avoid a core -> upload -> core dependency; vtop-upload exposes
/// it publicly next to the backends that execute the policy.
pub fn verification_method(backend: &str, algorithm: ChecksumAlgorithm) -> VerificationMethod {
    match (backend, algorithm) {
        (_, ChecksumAlgorithm::None) | ("mock_limited", _) => VerificationMethod::SizeOnly,
        ("s3_native", ChecksumAlgorithm::Sha256) => VerificationMethod::StoreChecksum,
        _ => VerificationMethod::ReadBack,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SettingOrigin {
    #[serde(rename = "stream")]
    Stream,
    #[serde(rename = "global")]
    Global,
    #[serde(rename = "built-in default")]
    BuiltInDefault,
}
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveSetting<T> {
    pub value: T,
    pub origin: SettingOrigin,
}

/// The complete printed allowlist. Do not flatten or serialize VtopConfig here.
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveStream {
    pub source_type: EffectiveSetting<String>,
    pub source_name: EffectiveSetting<String>,
    pub tenant: EffectiveSetting<String>,
    pub format: EffectiveSetting<String>,
    pub s3_source_name: EffectiveSetting<String>,
    pub retention_class: EffectiveSetting<Option<String>>,
    #[serde(rename = "checksum.algorithm")]
    pub checksum_algorithm: EffectiveSetting<ChecksumAlgorithm>,
    pub verification_method: EffectiveSetting<VerificationMethod>,
    #[serde(rename = "upload.require_strong_verification")]
    pub require_strong_verification: EffectiveSetting<bool>,
    pub manifest_mac: EffectiveSetting<String>,
    #[serde(rename = "upload.require_object_versioning")]
    pub require_object_versioning: EffectiveSetting<bool>,
    #[serde(rename = "upload.backend")]
    pub upload_backend: EffectiveSetting<String>,
    #[serde(rename = "upload.prefix")]
    pub upload_prefix: EffectiveSetting<String>,
    pub path_template: EffectiveSetting<String>,
    #[serde(rename = "batching.max_records")]
    pub max_records: EffectiveSetting<usize>,
    #[serde(rename = "batching.max_bytes")]
    pub max_bytes: EffectiveSetting<usize>,
    #[serde(rename = "batching.max_age")]
    pub max_age: EffectiveSetting<u64>,
    #[serde(rename = "batching.max_concurrent_batches")]
    pub max_concurrent_batches: EffectiveSetting<usize>,
    pub resolved_upload_width: EffectiveSetting<usize>,
}
#[derive(Debug, Clone, Serialize)]
pub struct EffectiveStreams {
    pub default: EffectiveStream,
    pub streams: Vec<EffectiveStream>,
}

/// Every dotted key path a YAML document sets to a non-null value, with
/// sequence items numbered (`streams.0.tenant`). Presence, not comparison with
/// the default: an explicit value equal to its default still came from the
/// operator. Values themselves are never retained.
///
/// Runs only AFTER the typed parse accepted the same text, and never fails on
/// its own: a document the engine accepts must not be refused, or reworded,
/// because its keys were being recorded. That is why this is a visitor over
/// the raw events and not a `serde_yaml::Value`, which refuses a duplicate key
/// inside a map-typed field that the typed parse lets through.
pub(crate) fn present_keys(text: &str) -> BTreeSet<String> {
    use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};
    use std::fmt;

    /// Walks one node; yields whether it was non-null.
    struct Walk<'a> {
        prefix: String,
        out: &'a mut BTreeSet<String>,
    }
    impl Walk<'_> {
        fn child(&self, key: &str) -> String {
            if self.prefix.is_empty() {
                key.to_owned()
            } else {
                format!("{}.{key}", self.prefix)
            }
        }
    }
    impl<'de> DeserializeSeed<'de> for Walk<'_> {
        type Value = bool;
        fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
            d.deserialize_any(self)
        }
    }
    /// A mapping key: its text when it is a scalar, else nothing to record.
    struct Key(Option<String>);
    impl<'de> serde::Deserialize<'de> for Key {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let value = serde_yaml::Value::deserialize(d)?;
            Ok(Key(match value {
                serde_yaml::Value::String(s) => Some(s),
                serde_yaml::Value::Bool(b) => Some(b.to_string()),
                serde_yaml::Value::Number(n) => Some(n.to_string()),
                _ => None,
            }))
        }
    }
    impl<'de> Visitor<'de> for Walk<'_> {
        type Value = bool;
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("any YAML node")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
            while let Some(Key(key)) = map.next_key()? {
                match key {
                    Some(key) => {
                        let path = self.child(&key);
                        let present = map.next_value_seed(Walk {
                            prefix: path.clone(),
                            out: &mut *self.out,
                        })?;
                        if present {
                            self.out.insert(path);
                        }
                    }
                    None => {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
            }
            Ok(true)
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<bool, A::Error> {
            let mut index = 0usize;
            while seq
                .next_element_seed(Walk {
                    prefix: self.child(&index.to_string()),
                    out: &mut *self.out,
                })?
                .is_some()
            {
                index += 1;
            }
            Ok(true)
        }
        fn visit_unit<E>(self) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_none<E>(self) -> Result<bool, E> {
            Ok(false)
        }
        fn visit_some<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
            d.deserialize_any(self)
        }
        fn visit_bool<E>(self, _: bool) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_i64<E>(self, _: i64) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_u64<E>(self, _: u64) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_i128<E>(self, _: i128) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_u128<E>(self, _: u128) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_f64<E>(self, _: f64) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_str<E>(self, _: &str) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_bytes<E>(self, _: &[u8]) -> Result<bool, E> {
            Ok(true)
        }
        fn visit_enum<A: serde::de::EnumAccess<'de>>(self, data: A) -> Result<bool, A::Error> {
            // A tagged node (`!Tag value`): present; its content is not walked.
            let (IgnoredAny, variant) = data.variant::<IgnoredAny>()?;
            serde::de::VariantAccess::newtype_variant::<IgnoredAny>(variant)?;
            Ok(true)
        }
    }

    let mut paths = BTreeSet::new();
    // The typed parse already accepted `text`, so an error here is not
    // expected; if one ever happens, what was recorded before it stands.
    let _ = Walk {
        prefix: String::new(),
        out: &mut paths,
    }
    .deserialize(serde_yaml::Deserializer::from_str(text));
    paths
}

/// The keys one `streams.yaml` entry set, from that file's [`present_keys`].
pub(crate) fn stream_keys(all: &BTreeSet<String>, index: usize) -> BTreeSet<String> {
    let prefix = format!("streams.{index}.");
    all.iter()
        .filter_map(|path| path.strip_prefix(&prefix).map(str::to_owned))
        .collect()
}

/// Explain static config only: no environment resolution or service access.
pub fn effective_streams(config: &VtopConfig, streams: Option<&StreamsConfig>) -> EffectiveStreams {
    use SettingOrigin::{BuiltInDefault, Global, Stream};
    let global = |path: &str| {
        if config.configured_fields.contains(path) {
            Global
        } else {
            BuiltInDefault
        }
    };
    let make = |stream: Option<&crate::config::StreamConfig>| {
        let identity = |value: String, field: &str| EffectiveSetting {
            value,
            origin: if stream.is_some_and(|s| s.configured_fields.contains(field)) {
                Stream
            } else {
                BuiltInDefault
            },
        };
        let backend_origin = global("upload.backend");
        // The same resolution the pipeline runs (#669): a stream's own
        // `checksum` wins, else the global algorithm.
        let algorithm = crate::config::effective_algorithm(config, stream);
        let algorithm_origin = if stream.is_some_and(|s| s.checksum.is_some()) {
            Stream
        } else {
            global("checksum.algorithm")
        };
        // The width is `max_concurrent_batches` unless an active transport
        // tuning narrows it (s3_native only); `max_concurrency_ceiling` is a
        // refusal bound and never sets it.
        let width = config
            .upload
            .resolved_upload_ceiling(config.batching.max_concurrent_batches);
        let width_origin = if width < config.batching.max_concurrent_batches {
            Global
        } else {
            global("batching.max_concurrent_batches")
        };
        EffectiveStream {
            source_type: identity(
                stream
                    .map(|s| s.source_type.to_string())
                    .unwrap_or_else(|| "discovered".into()),
                "source_type",
            ),
            source_name: identity(
                stream
                    .map(|s| s.source_name.clone())
                    .unwrap_or_else(|| "discovered".into()),
                "source_name",
            ),
            tenant: EffectiveSetting {
                value: stream
                    .map(|s| s.tenant.clone())
                    .unwrap_or_else(|| config.engine.tenant.clone()),
                origin: stream
                    .map(|s| {
                        if s.configured_fields.contains("tenant") {
                            Stream
                        } else {
                            BuiltInDefault
                        }
                    })
                    .unwrap_or_else(|| global("engine.tenant")),
            },
            format: identity(
                stream
                    .map(|s| s.format.to_string())
                    .unwrap_or_else(|| "detected".into()),
                "format",
            ),
            s3_source_name: identity(
                stream
                    .and_then(|s| s.s3_source_name.clone())
                    .unwrap_or_else(|| {
                        stream
                            .map(|s| s.source_name.clone())
                            .unwrap_or_else(|| "discovered".into())
                    }),
                "s3_source_name",
            ),
            retention_class: EffectiveSetting {
                value: stream.and_then(|s| s.retention_class.clone()),
                origin: if stream.is_some_and(|s| s.retention_class.is_some()) {
                    Stream
                } else {
                    BuiltInDefault
                },
            },
            checksum_algorithm: EffectiveSetting {
                value: algorithm,
                origin: algorithm_origin,
            },
            verification_method: EffectiveSetting {
                value: verification_method(&config.upload.backend, algorithm),
                origin: if algorithm_origin == Stream {
                    Stream
                } else if algorithm_origin == Global || backend_origin == Global {
                    Global
                } else {
                    BuiltInDefault
                },
            },
            require_strong_verification: EffectiveSetting {
                value: config.upload.require_strong_verification,
                origin: global("upload.require_strong_verification"),
            },
            manifest_mac: EffectiveSetting {
                value: if config.manifest_mac_key_env.is_some() {
                    "enabled"
                } else {
                    "disabled"
                }
                .into(),
                origin: global("manifest_mac_key_env"),
            },
            require_object_versioning: EffectiveSetting {
                value: config.upload.require_object_versioning,
                origin: global("upload.require_object_versioning"),
            },
            upload_backend: EffectiveSetting {
                value: config.upload.backend.clone(),
                origin: backend_origin,
            },
            upload_prefix: EffectiveSetting {
                value: config.upload.prefix.clone(),
                origin: global("upload.prefix"),
            },
            path_template: EffectiveSetting {
                value: config.partitioning.template.clone(),
                origin: global("partitioning.template"),
            },
            max_records: EffectiveSetting {
                value: config.batching.max_records,
                origin: global("batching.max_records"),
            },
            max_bytes: EffectiveSetting {
                value: config.batching.max_bytes,
                origin: global("batching.max_bytes"),
            },
            max_age: EffectiveSetting {
                value: config.batching.max_batch_age_seconds,
                origin: global("batching.max_batch_age_seconds"),
            },
            max_concurrent_batches: EffectiveSetting {
                value: config.batching.max_concurrent_batches,
                origin: global("batching.max_concurrent_batches"),
            },
            resolved_upload_width: EffectiveSetting {
                value: width,
                origin: width_origin,
            },
        }
    };
    EffectiveStreams {
        default: make(None),
        streams: streams
            .map(|s| s.streams.iter().map(|s| make(Some(s))).collect())
            .unwrap_or_default(),
    }
}
