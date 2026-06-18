//! Telemetry-aware object partitioning.
//!
//! Objects are laid out by tenant, source, format, and time so the archive is
//! useful for any downstream consumer (log analytics, observability, audit,
//! compliance, SIEM, ...). The scheme is general-purpose, not tied to any one
//! domain.
//!
//! Resolves a path template into a concrete storage prefix and builds object /
//! manifest URIs. Source names are never hardcoded — everything is derived from
//! config and the batch.

use crate::types::{CompressionType, TelemetryFormat};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Default partition path template.
pub const DEFAULT_TEMPLATE: &str =
    "tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/";

/// Context used to resolve a path template. Includes the required partition
/// fields plus optional extension fields (environment, facility, severity,
/// retention_class, region, site).
#[derive(Debug, Clone)]
pub struct PartitionContext {
    pub tenant: String,
    pub source: String,
    pub format: TelemetryFormat,
    pub timestamp: DateTime<Utc>,
    pub extra: HashMap<String, String>,
}

impl PartitionContext {
    pub fn new(
        tenant: impl Into<String>,
        source: impl Into<String>,
        format: TelemetryFormat,
        timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            source: source.into(),
            format,
            timestamp,
            extra: HashMap::new(),
        }
    }

    /// Add a future / optional field such as `environment`, `facility`,
    /// `severity`, `retention_class`, `region`, or `site`.
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    fn lookup(&self, key: &str) -> Option<String> {
        Some(match key {
            "tenant" => self.tenant.clone(),
            "source" => self.source.clone(),
            "format" => self.format.extension().to_string(),
            "yyyy" => self.timestamp.format("%Y").to_string(),
            "mm" => self.timestamp.format("%m").to_string(),
            "dd" => self.timestamp.format("%d").to_string(),
            "hh" => self.timestamp.format("%H").to_string(),
            other => self.extra.get(other).cloned()?,
        })
    }
}

/// Resolve `{placeholder}` tokens in `template` against `ctx`. Unknown tokens
/// resolve to empty (and collapse cleanly). The result never contains leading
/// or doubled slashes.
pub fn resolve_template(template: &str, ctx: &PartitionContext) -> String {
    let mut out = String::with_capacity(template.len() * 2);
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut key = String::new();
            for k in chars.by_ref() {
                if k == '}' {
                    break;
                }
                key.push(k);
            }
            if let Some(val) = ctx.lookup(&key) {
                out.push_str(&val);
            }
        } else {
            out.push(c);
        }
    }
    normalize_prefix(&out)
}

/// Collapse duplicate slashes and trim leading/trailing slashes.
pub fn normalize_prefix(p: &str) -> String {
    let mut parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    parts.retain(|s| !s.is_empty());
    parts.join("/")
}

/// Resolve a bucket name template against `ctx` and sanitize it to valid
/// S3 bucket characters. Supports the same placeholders as [`resolve_template`]
/// (`{format}`, `{tenant}`, `{source}`, ...), enabling **one bucket per data
/// format** with e.g. `bucket: "telemetry-{format}"` → `telemetry-cef`,
/// `telemetry-json`, `telemetry-syslog`, ...
///
/// Sanitization: lowercased; characters outside `[a-z0-9-]` become `-`;
/// repeated/edge `-` are trimmed; empty falls back to `vtop`.
pub fn resolve_bucket(template: &str, ctx: &PartitionContext) -> String {
    let mut out = String::with_capacity(template.len() * 2);
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut key = String::new();
            for k in chars.by_ref() {
                if k == '}' {
                    break;
                }
                key.push(k);
            }
            if let Some(val) = ctx.lookup(&key) {
                out.push_str(&val);
            }
        } else {
            out.push(c);
        }
    }
    sanitize_bucket(&out)
}

/// Lowercase + restrict to S3-safe bucket characters `[a-z0-9-]`.
pub fn sanitize_bucket(name: &str) -> String {
    let mut s = String::with_capacity(name.len());
    let mut prev_dash = false;
    for ch in name.to_ascii_lowercase().chars() {
        let mapped = if ch.is_ascii_alphanumeric() { ch } else { '-' };
        if mapped == '-' {
            if prev_dash {
                continue;
            }
            prev_dash = true;
        } else {
            prev_dash = false;
        }
        s.push(mapped);
    }
    let trimmed = s.trim_matches('-').to_string();
    if trimmed.is_empty() {
        "vtop".to_string()
    } else {
        trimmed
    }
}

/// The object file name: `{batch_id}.{format}.{compression_ext}` (compression
/// extension omitted when [`CompressionType::None`]).
pub fn object_file_name(
    batch_id: &str,
    format: TelemetryFormat,
    compression: CompressionType,
) -> String {
    match compression.extension() {
        Some(ext) => format!("{batch_id}.{}.{ext}", format.extension()),
        None => format!("{batch_id}.{}", format.extension()),
    }
}

/// The manifest file name: `{batch_id}.manifest.json`.
pub fn manifest_file_name(batch_id: &str) -> String {
    format!("{batch_id}.manifest.json")
}

/// Build a full object URI:
/// `s3://{bucket}/{prefix}/{resolved}/{object_file_name}`.
pub fn object_uri(
    bucket: &str,
    prefix: &str,
    resolved_prefix: &str,
    batch_id: &str,
    format: TelemetryFormat,
    compression: CompressionType,
) -> String {
    let key = join_key(&[
        prefix,
        resolved_prefix,
        &object_file_name(batch_id, format, compression),
    ]);
    format!("s3://{bucket}/{key}")
}

/// Build a full manifest URI alongside the object.
pub fn manifest_uri(bucket: &str, prefix: &str, resolved_prefix: &str, batch_id: &str) -> String {
    let key = join_key(&[prefix, resolved_prefix, &manifest_file_name(batch_id)]);
    format!("s3://{bucket}/{key}")
}

fn join_key(parts: &[&str]) -> String {
    parts
        .iter()
        .flat_map(|p| p.split('/'))
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ctx() -> PartitionContext {
        let ts = Utc.with_ymd_and_hms(2026, 6, 18, 15, 0, 0).unwrap();
        PartitionContext::new("default", "app_events", TelemetryFormat::Cef, ts)
    }

    #[test]
    fn resolves_default_template() {
        let resolved = resolve_template(DEFAULT_TEMPLATE, &ctx());
        assert_eq!(
            resolved,
            "tenant=default/source=app_events/format=cef/year=2026/month=06/day=18/hour=15"
        );
    }

    #[test]
    fn supports_future_fields() {
        let c = ctx()
            .with("retention_class", "standard")
            .with("region", "us-east-1");
        let tpl = "tenant={tenant}/retention={retention_class}/region={region}/";
        let resolved = resolve_template(tpl, &c);
        assert_eq!(
            resolved,
            "tenant=default/retention=standard/region=us-east-1"
        );
    }

    #[test]
    fn object_uri_has_compression_extension() {
        let resolved = resolve_template(DEFAULT_TEMPLATE, &ctx());
        let uri = object_uri(
            "telemetry-data",
            "telemetry-data",
            &resolved,
            "vtop-b1",
            TelemetryFormat::Cef,
            CompressionType::Gzip,
        );
        assert!(uri.ends_with("vtop-b1.cef.gz"));
        assert!(uri.starts_with("s3://telemetry-data/telemetry-data/tenant=default/"));
        assert!(!uri.contains("//tenant")); // no doubled slash
    }

    #[test]
    fn manifest_uri_matches_object_prefix() {
        let resolved = resolve_template(DEFAULT_TEMPLATE, &ctx());
        let m = manifest_uri("telemetry-data", "telemetry-data", &resolved, "vtop-b1");
        assert!(m.ends_with("vtop-b1.manifest.json"));
    }

    #[test]
    fn resolves_per_format_bucket() {
        assert_eq!(
            resolve_bucket("telemetry-{format}", &ctx()),
            "telemetry-cef"
        );
        assert_eq!(
            resolve_bucket("{tenant}-{format}-archive", &ctx()),
            "default-cef-archive"
        );
        // plain name passes through (lowercased)
        assert_eq!(resolve_bucket("telemetry-data", &ctx()), "telemetry-data");
    }

    #[test]
    fn sanitizes_bucket_names() {
        assert_eq!(sanitize_bucket("Telemetry_CEF!!"), "telemetry-cef");
        assert_eq!(sanitize_bucket("__weird__"), "weird");
        assert_eq!(sanitize_bucket(""), "vtop");
    }

    #[test]
    fn none_compression_omits_extension() {
        assert_eq!(
            object_file_name("b1", TelemetryFormat::Jsonl, CompressionType::None),
            "b1.jsonl"
        );
    }
}
