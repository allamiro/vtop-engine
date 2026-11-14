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

/// A key written with no value (`tenant:`) or `~` is still the operator's:
/// the typed parse reads it as the literal text "" or "~" for a string field,
/// so its origin must not claim the built-in default.
#[test]
fn a_key_set_to_an_empty_or_null_scalar_is_reported_as_set() {
    let config = VtopConfig::from_yaml_str("engine: {name: test, tenant: ~, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\nupload: {bucket: b}\n").unwrap();
    let streams = StreamsConfig::from_yaml_str(
        "streams:\n- source_name: a\n  source_type: file\n  format: raw\n  tenant:\n",
    )
    .unwrap();
    let effective = serde_json::to_value(effective_streams(&config, Some(&streams))).unwrap();
    assert_eq!(effective["default"]["tenant"]["value"], "~");
    assert_eq!(effective["default"]["tenant"]["origin"], "global");
    assert_eq!(effective["streams"][0]["tenant"]["value"], "");
    assert_eq!(effective["streams"][0]["tenant"]["origin"], "stream");
}

/// A root key spelled with dots (`"upload.backend": localfs`) is not the
/// nested `upload.backend`: the typed parse ignores it, so the explanation
/// must not claim the operator set that setting.
#[test]
fn a_dotted_key_is_not_mistaken_for_the_nested_setting() {
    let config = VtopConfig::from_yaml_str("engine: {name: test, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\nupload: {bucket: b}\n\"upload.backend\": localfs\n\"upload.require_strong_verification\": false\n").unwrap();
    let effective = serde_json::to_value(effective_streams(&config, None)).unwrap();
    assert_eq!(effective["default"]["upload.backend"]["value"], "s3_native");
    assert_eq!(
        effective["default"]["upload.backend"]["origin"],
        "built-in default"
    );
    assert_eq!(
        effective["default"]["upload.require_strong_verification"]["origin"],
        "built-in default"
    );
    let streams = StreamsConfig::from_yaml_str(
        "streams:\n- {source_name: a, source_type: file, format: raw}\n\"streams.0.tenant\": x\n",
    )
    .unwrap();
    let effective = serde_json::to_value(effective_streams(&config, Some(&streams))).unwrap();
    assert_eq!(
        effective["streams"][0]["tenant"]["origin"],
        "built-in default"
    );
}

/// The engine applies the FIRST stream entry with a given source_name
/// (`StreamsConfig::lookup`); a later entry with the same name is never used,
/// so it is listed as shadowed instead of printed as if it were in effect.
#[test]
fn a_repeated_source_name_is_shadowed_not_shown_as_effective() {
    let config = VtopConfig::from_yaml_str("engine: {name: test, state_store: 'sqlite::memory:', work_dir: /unused}\nbatching: {}\ncompression: {}\nsources: {}\nupload: {bucket: b}\n").unwrap();
    let streams = StreamsConfig::from_yaml_str("streams:\n- {source_name: app, source_type: file, format: raw, tenant: a}\n- {source_name: other, source_type: file, format: raw}\n- {source_name: app, source_type: file, format: raw, tenant: b}\n").unwrap();
    assert_eq!(streams.lookup("app").unwrap().tenant, "a");
    let effective = serde_json::to_value(effective_streams(&config, Some(&streams))).unwrap();
    let names: Vec<_> = effective["streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["source_name"]["value"].clone())
        .collect();
    assert_eq!(names, ["app", "other"]);
    assert_eq!(effective["streams"][0]["tenant"]["value"], "a");
    assert_eq!(
        effective["shadowed_streams"],
        serde_json::json!([{"index": 2, "source_name": "app"}])
    );
}
