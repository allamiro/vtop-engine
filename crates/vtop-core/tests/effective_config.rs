use vtop_core::config::{StreamsConfig, VtopConfig};
use vtop_core::effective_config::effective_streams;

#[test]
fn explicitly_configured_default_values_keep_global_origin() {
    let config = VtopConfig::from_yaml_str("engine: {name: test, tenant: default, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nchecksum: {algorithm: sha256}\nsources: {}\nupload: {bucket: b}\n").unwrap();
    let effective = serde_json::to_value(effective_streams(&config, None)).unwrap();
    assert_eq!(effective["default"]["tenant"]["origin"], "global");
    assert_eq!(
        effective["default"]["checksum.algorithm"]["origin"],
        "global"
    );
    assert_eq!(
        effective["default"]["upload.backend"]["origin"],
        "built-in default"
    );
}
#[test]
fn stream_tenant_defaults_follow_runtime_without_invented_inheritance() {
    let config = VtopConfig::from_yaml_str("engine: {name: test, tenant: global-tenant, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\nupload: {bucket: b}\n").unwrap();
    let streams = StreamsConfig::from_yaml_str("streams:\n- {source_name: one, source_type: file, format: raw, tenant: default}\n- {source_name: two, source_type: kafka, format: raw}\n").unwrap();
    let effective = serde_json::to_value(effective_streams(&config, Some(&streams))).unwrap();
    assert_eq!(effective["streams"][0]["tenant"]["origin"], "stream");
    assert_eq!(
        effective["streams"][1]["tenant"]["origin"],
        "built-in default"
    );
    assert_eq!(effective["streams"][1]["tenant"]["value"], "default");
}

#[test]
fn duplicate_configuration_keys_are_refused_before_effective_resolution() {
    let base = "engine: {name: test, tenant: default, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\nupload: {bucket: b}\n";
    for yaml in [
        base.replace("tenant: default", "tenant: one, tenant: two"),
        format!("{base}checksum: {{algorithm: sha256, algorithm: blake3}}\n"),
    ] {
        assert!(
            serde_yaml::from_str::<VtopConfig>(&yaml).is_err(),
            "duplicate global key accepted: {yaml}"
        );
    }
    assert!(serde_yaml::from_str::<StreamsConfig>("streams:\n- {source_name: one, source_type: file, format: raw, tenant: one, tenant: two}\n").is_err(), "duplicate stream key accepted");
}

/// The resolved upload width is `batching.max_concurrent_batches` unless an
/// active transport tuning narrows it; only then is its origin the transport
/// block. `max_concurrency_ceiling` is a refusal bound, never the width, and a
/// non-S3 backend ignores transport tuning entirely.
#[test]
fn resolved_upload_width_origin_names_what_actually_set_it() {
    let base = "engine: {name: test, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\n";
    let width = |upload: &str| {
        let config = VtopConfig::from_yaml_str(&format!("{base}{upload}")).unwrap();
        let effective = serde_json::to_value(effective_streams(&config, None)).unwrap();
        (
            effective["default"]["resolved_upload_width"]["value"].clone(),
            effective["default"]["resolved_upload_width"]["origin"].clone(),
        )
    };
    let tuning = "  transports:\n    tcp_tls:\n      max_concurrency: 2\n";
    assert_eq!(
        width(&format!("upload:\n  bucket: b\n{tuning}")),
        (2.into(), "global".into()),
        "s3_native narrowed by its transport tuning"
    );
    assert_eq!(
        width(&format!(
            "upload:\n  bucket: b\n  backend: localfs\n  local_path: /x\n{tuning}"
        )),
        (8.into(), "built-in default".into()),
        "localfs ignores transport tuning; the width is the batching default"
    );
    assert_eq!(
        width("upload:\n  bucket: b\n  max_concurrency_ceiling: 16\n"),
        (8.into(), "built-in default".into()),
        "a ceiling bounds the width; it does not set it"
    );
}
