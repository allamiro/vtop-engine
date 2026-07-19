//! Snapshot tests pinning the manifest's on-disk contract.
//!
//! The manifest is the **consumer-facing artifact**: downstream tooling reads it
//! to learn the object URI, the checksum to verify against, the source range the
//! object covers, and whether the batch was verified. Nothing pinned its shape,
//! so a field rename, reorder, or removal could ship silently and break every
//! consumer while the whole test suite stayed green.
//!
//! These snapshots make any change to that contract an explicit, reviewable diff
//! rather than an accident. If a snapshot fails, the question to ask is "did we
//! *mean* to change the manifest format?" — and if so, `cargo insta review`.
//!
//! Field order matters too, not just field names: `manifest.sha256` is computed
//! over the canonical serialization, so reordering fields changes the hash that
//! consumers verify against.

use insta::assert_snapshot;
use vtop_core::manifest::{ManifestBuilder, ManifestMacKey};
use vtop_core::types::{CompressionType, ProgressMarker, SourceType, TelemetryFormat};

/// Every value here is fixed. The manifest must be a pure function of its
/// inputs — no clocks, no randomness — or snapshots (and the self-hash) could
/// not be stable.
fn kafka_builder() -> ManifestBuilder {
    ManifestBuilder {
        batch_id: "vtop-20260101T000000Z-app_events-p0-0-99-deadbeef".into(),
        tenant: "default".into(),
        source_type: SourceType::Kafka,
        source_name: "app_events".into(),
        format: TelemetryFormat::Cef,
        compression: CompressionType::Gzip,
        record_count: 100,
        first_timestamp: Some("2026-01-01T00:00:00Z".into()),
        last_timestamp: Some("2026-01-01T00:00:59Z".into()),
        source_progress: ProgressMarker::Kafka {
            topic: "app_events".into(),
            partition: 0,
            start_offset: 0,
            end_offset: 99,
            consumer_group: "vtop-engine".into(),
        },
        object_uri: "s3://telemetry-cef/tenant=default/source=app_events/format=cef/year=2026/month=01/day=01/hour=00/vtop-20260101T000000Z-app_events-p0-0-99-deadbeef.cef.gz".into(),
        object_size: 4096,
        object_checksum_algorithm: "sha256".into(),
        object_checksum: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
        manifest_uri: "s3://telemetry-cef/tenant=default/source=app_events/format=cef/year=2026/month=01/day=01/hour=00/vtop-20260101T000000Z-app_events-p0-0-99-deadbeef.manifest.json".into(),
        path_template: "tenant={tenant}/source={source}/format={format}/year={yyyy}/month={mm}/day={dd}/hour={hh}/".into(),
        resolved_prefix: "tenant=default/source=app_events/format=cef/year=2026/month=01/day=01/hour=00".into(),
        upload_backend: "s3_native".into(),
        created_at: "2026-01-01T00:01:00Z".into(),
    }
}

fn file_builder() -> ManifestBuilder {
    ManifestBuilder {
        batch_id: "vtop-20260101T000000Z-_data_input_app_log-b0-2048-cafebabe".into(),
        source_type: SourceType::File,
        source_name: "/data/input/app.log".into(),
        format: TelemetryFormat::Jsonl,
        source_progress: ProgressMarker::File {
            path: "/data/input/app.log".into(),
            inode: Some(877),
            start_byte: 0,
            end_byte: 2048,
            file_size: 2048,
            mtime: "2026-01-01T00:00:30Z".into(),
        },
        object_uri: "s3://telemetry-jsonl/tenant=default/source=_data_input_app.log/format=jsonl/year=2026/month=01/day=01/hour=00/obj.jsonl.gz".into(),
        manifest_uri: "s3://telemetry-jsonl/tenant=default/source=_data_input_app.log/format=jsonl/year=2026/month=01/day=01/hour=00/obj.manifest.json".into(),
        resolved_prefix: "tenant=default/source=_data_input_app.log/format=jsonl/year=2026/month=01/day=01/hour=00".into(),
        ..kafka_builder()
    }
}

/// The full Kafka-sourced manifest as consumers see it on disk.
#[test]
fn kafka_manifest_json_contract() {
    let m = kafka_builder().build().unwrap();
    let json = String::from_utf8(m.to_json_bytes().unwrap()).unwrap();
    assert_snapshot!("kafka_manifest", json);
}

/// The authenticated v0.2 shape is a separate contract: the MAC is nested
/// beside the self-hash, deterministic for a fixed key, and never contains the
/// key itself.
#[test]
fn authenticated_manifest_json_contract() {
    let key = ManifestMacKey::from_hex(&"42".repeat(32)).unwrap();
    let m = kafka_builder().build_with_mac(Some(&key)).unwrap();
    let json = String::from_utf8(m.to_json_bytes().unwrap()).unwrap();
    assert_snapshot!("authenticated_kafka_manifest", json);
}

/// The file-sourced variant: proves the source_progress marker is serialized
/// per-source-type (inode/byte range, not offsets) — the field downstream replay
/// tooling depends on.
#[test]
fn file_manifest_json_contract() {
    let m = file_builder().build().unwrap();
    let json = String::from_utf8(m.to_json_bytes().unwrap()).unwrap();
    assert_snapshot!("file_manifest", json);
}

/// The self-hash is a *published* value: consumers verify the manifest against
/// it. It is computed over the canonical serialization, so ANY change to field
/// names, order, or values changes it. Pinning the literal turns a silent
/// serialization change into a failing test.
#[test]
fn manifest_self_hash_is_pinned() {
    let m = kafka_builder().build().unwrap();
    assert_snapshot!("kafka_manifest_self_hash", m.manifest.sha256);
    // And it must actually validate, not merely be stable.
    m.verify_self_hash()
        .expect("the pinned hash must verify against the manifest itself");
}

/// A v0.1 document has no `manifest.mac`. Adding the optional v0.2 field must
/// not change canonicalization when reading and verifying historical bytes.
#[test]
fn legacy_v01_self_hash_still_verifies() {
    let mut legacy = kafka_builder().build().unwrap();
    legacy.version = "0.1".into();
    legacy.manifest.mac = None;
    legacy.manifest.sha256 =
        "9e2c72ecc55fdb0dc623f93611a6195eaa055f8bab993f8fb4238a6d985e80d4".into();
    legacy
        .verify_self_hash()
        .expect("the published v0.1 canonical hash must remain readable");
}

/// A manifest built twice from identical inputs must be byte-identical.
/// If this fails, something non-deterministic (a clock, a map iteration order)
/// leaked into serialization, and the self-hash would be unreproducible.
#[test]
fn manifest_serialization_is_deterministic() {
    let a = kafka_builder().build().unwrap().to_json_bytes().unwrap();
    let b = kafka_builder().build().unwrap().to_json_bytes().unwrap();
    assert_eq!(a, b, "manifest serialization must be deterministic");
}
