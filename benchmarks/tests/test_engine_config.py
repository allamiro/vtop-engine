"""What the harness writes into the engine config it drives the binary with.

The config is the harness's only channel into the engine, so a line that lands
wrong (or silently fails to land) changes what the run actually measured — or,
for `create_bucket`, what the run does to the storage it points at.
"""

from lib.engine import write_engine_config


def write(tmp_path, scenario, key_prefix=""):
    config = str(tmp_path / "engine.yaml")
    write_engine_config(scenario, str(tmp_path / "work"),
                        str(tmp_path / "state.db"),
                        str(tmp_path / "seed" / "*"), config,
                        key_prefix=key_prefix)
    with open(config, encoding="utf-8") as fh:
        return fh.read()


# --------------------------------------------------------------------------
# Bucket provisioning is a scenario's explicit choice
# --------------------------------------------------------------------------


def test_s3_native_with_an_endpoint_does_not_provision_unless_the_scenario_says_so(
        tmp_path, monkeypatch):
    monkeypatch.delenv("VTOP_S3_ENDPOINT_URL", raising=False)
    text = write(tmp_path, {"backend": "s3_native",
                            "endpoint_url": "http://localhost:9000"})
    assert "  create_bucket: false" in text, (
        "an endpoint must not be read as consent to provision: inferred true "
        "asks a real endpoint for a CreateBucket the runtime identity should "
        "not hold (SECURITY_MODEL §5.2) — AccessDenied under least privilege, "
        "a real bucket under broad credentials"
    )


def test_a_scenario_that_opts_in_gets_a_provisioning_config(tmp_path):
    text = write(tmp_path, {"backend": "s3_native",
                            "endpoint_url": "http://localhost:9000",
                            "create_bucket": True})
    assert "  create_bucket: true" in text, (
        "the lab soak (scenario 12) relies on the opt-in: its bucket is not "
        "among the telemetry-* set the compose stack provisions"
    )


def test_the_minio_backend_still_derives_provisioning(tmp_path):
    # Inert in practice — the mc-based backend's ensure_bucket is the base
    # no-op — but the derived value is part of the config's contract and must
    # not change out from under existing minio scenarios.
    text = write(tmp_path, {"backend": "minio"})
    assert "  create_bucket: true" in text


def test_an_explicit_string_false_is_not_read_as_an_opt_in(tmp_path):
    # A value that skipped the loader arrives as a string, and the string
    # "false" is truthy: coercion must go by spelling, not by truthiness.
    text = write(tmp_path, {"backend": "minio", "create_bucket": "false"})
    assert "  create_bucket: false" in text


def test_an_explicit_string_true_is_read_as_an_opt_in(tmp_path):
    # The loader-skipping path in the opt-in direction: the spelling "true"
    # must provision even where an inferred default would not.
    text = write(tmp_path, {"backend": "s3_native",
                            "endpoint_url": "http://localhost:9000",
                            "create_bucket": "true"})
    assert "  create_bucket: true" in text, (
        "a string opt-in must count: the scenario said to provision, and "
        "arriving unparsed is not a reason to ignore it"
    )


# --------------------------------------------------------------------------
# Pipeline width (#87 knob, #102 grid)
# --------------------------------------------------------------------------


def test_an_explicit_zero_width_is_written_through_for_the_engine_to_refuse(
        tmp_path):
    text = write(tmp_path, {"max_concurrent_batches": 0})
    assert "  max_concurrent_batches: 0" in text, (
        "zero must reach the engine so its own validation refuses it loudly "
        "(batching.max_concurrent_batches must be > 0); swallowing it ran the "
        "default width of 8 while summary.json recorded 0, mislabeling the "
        "very concurrency comparison the #102 grid exists to make"
    )


def test_an_unset_width_emits_no_line_and_keeps_the_engine_default(tmp_path):
    text = write(tmp_path, {})
    assert "max_concurrent_batches" not in text, (
        "absent must stay absent so a plain run measures the width that ships"
    )


# --------------------------------------------------------------------------
# Run-scoped object prefix
# --------------------------------------------------------------------------


def test_each_run_namespaces_its_objects_under_its_run_id(tmp_path):
    text = write(tmp_path, {"backend": "minio"},
                 key_prefix="soak-20260903T000000Z-abc123")
    assert '  prefix: "soak-20260903T000000Z-abc123"' in text, (
        "the bench bucket outlives the run (named volume), so an un-prefixed "
        "run's objects are unattributable and only deletable wholesale"
    )


def test_the_prefix_defaults_to_empty(tmp_path):
    text = write(tmp_path, {})
    assert '  prefix: ""' in text
