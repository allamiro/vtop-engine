"""The containerized sender (#476): the second launch mode must not move the
first one.

The pins here are ordered by what they protect. The host invocation is
compared against today's literal command line, because the acceptance bar is
that the default path CANNOT drift while the second mode exists. The
container invocation is then pinned to cross the boundary with exactly the
environment the engine's resolution consumes, with the lab endpoint
translated for the network it actually runs in — and only that endpoint.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib import engine  # noqa: E402
from lib.metrics import CSV_HEADERS  # noqa: E402
from lib.scenario import DEFAULTS, Scenario  # noqa: E402


def scenario(**overrides):
    values = dict(DEFAULTS)
    values.update(overrides)
    return Scenario(values)


def test_host_mode_is_todays_command_line_unchanged():
    argv, env = engine.invocation(
        "/repo/target/release/vtopctl",
        ["--json", "process-once", "--source", "file", "--config", "/tmp/x/_engine.yaml"],
        scenario())
    assert argv == [
        "/repo/target/release/vtopctl", "--json", "process-once",
        "--source", "file", "--config", "/tmp/x/_engine.yaml",
    ], "host mode must produce today's invocation byte-for-byte"
    # And its environment still flows through the backend resolver — the
    # mock scenario adds nothing, so it is the process environment.
    assert "PATH" in env


def test_host_is_the_default_when_the_scenario_says_nothing():
    values = dict(DEFAULTS)
    values.pop("runner_mode", None)
    assert engine.runner_mode(Scenario(values)) == "host"


def test_an_unknown_mode_is_refused_not_defaulted():
    try:
        engine.runner_mode(scenario(runner_mode="containerised"))
    except ValueError as bad:
        assert "containerised" in str(bad)
    else:
        raise AssertionError(
            "a typo'd runner_mode fell back to some mode silently; the run "
            "would be recorded as something it was not")


def test_container_mode_execs_the_compose_service():
    argv, _ = engine.invocation(
        "/repo/target/release/vtopctl",
        ["--json", "process-once", "--source", "file", "--config", "/tmp/x/_engine.yaml"],
        scenario(runner_mode="container"))
    assert argv[:6] == [
        "docker", "compose", "-f", engine.COMPOSE_FILE, "exec", "-T"]
    service_at = argv.index(engine.CONTAINER_SERVICE)
    assert argv[service_at + 1] == "vtopctl", (
        "the container runs the MOUNTED binary by its in-container name — "
        "never the host path, which does not exist in that namespace")
    assert argv[service_at + 2:] == [
        "--json", "process-once", "--source", "file", "--config", "/tmp/x/_engine.yaml"]


def test_container_mode_translates_only_the_lab_endpoint(monkeypatch):
    monkeypatch.delenv("VTOP_S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    lab = scenario(runner_mode="container", backend="s3_native",
                   endpoint_url="http://localhost:9000")
    argv, env = engine.invocation("/x/vtopctl", ["replay"], lab)
    assert "VTOP_S3_ENDPOINT_URL" in argv and "-e" in argv, (
        "the key crosses as a NAME on the command line; its value travels "
        "in the environment, never in /proc/*/cmdline")
    assert env["VTOP_S3_ENDPOINT_URL"] == "http://minio:9000", (
        "loopback inside the engine container is the engine container; the "
        "lab store's in-network name is the compose service")
    assert not any("=" in a for a in argv if a.startswith("AWS_")), (
        "secrets never ride the command line")
    # The lab credentials still follow the store: the decision is made on
    # the HOST-view endpoint, before translation.
    assert "AWS_ACCESS_KEY_ID" in argv and env.get("AWS_ACCESS_KEY_ID")

    external = scenario(runner_mode="container", backend="s3_native",
                        endpoint_url="https://storage.example.net:9000")
    argv, env = engine.invocation("/x/vtopctl", ["replay"], external)
    assert env["VTOP_S3_ENDPOINT_URL"] == "https://storage.example.net:9000", (
        "an operator-supplied endpoint is the operator's topology; rewriting "
        "it would point the engine somewhere never named")
    assert "AWS_ACCESS_KEY_ID" not in argv, (
        "the lab fallbacks must not be handed to somebody else's store")


def test_temporary_credentials_cross_the_boundary_whole(monkeypatch):
    # Key + secret without their session token is a DIFFERENT identity to
    # the SDK's credential chain (review): valid temporary credentials
    # failed auth in container mode only.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-TEST")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token-123")
    external = scenario(runner_mode="container", backend="s3_native",
                        endpoint_url="https://storage.example.net:9000")
    argv, env = engine.invocation("/x/vtopctl", ["replay"], external)
    assert "AWS_SESSION_TOKEN" in argv and env["AWS_SESSION_TOKEN"] == "token-123", (
        "the token crosses by name with its value in the environment")


def test_a_shaped_scenario_goes_through_the_proxy_service_not_around_it():
    # 9100 is the lab store BEHIND toxiproxy (review): translating it to
    # the store's own name would send the engine around the toxics while
    # the summary said shaped.
    assert engine.container_wire_endpoint(
        "http://localhost:9100", shaped=True) == "http://toxiproxy:9100"
    assert engine.container_wire_endpoint(
        "http://localhost:9000", shaped=True) == "http://minio:9000"
    # Unshaped, 9100 is somebody else's service and passes through.
    assert engine.container_wire_endpoint(
        "http://localhost:9100") == "http://localhost:9100"


def test_container_mode_writes_the_wire_endpoint_into_the_config(tmp_path, monkeypatch):
    # The resolver prefers the environment (review): an ambient override on
    # the machine running the tests would silently retarget this pin.
    monkeypatch.delenv("VTOP_S3_ENDPOINT_URL", raising=False)
    lab = scenario(runner_mode="container", backend="s3_native",
                   endpoint_url="http://127.0.0.1:9000")
    config = tmp_path / "engine.yaml"
    engine.write_engine_config(lab, str(tmp_path), str(tmp_path / "s.db"),
                               str(tmp_path / "*"), str(config))
    text = config.read_text()
    assert "endpoint_url: http://minio:9000" in text
    assert "127.0.0.1" not in text

    host = scenario(runner_mode="host", backend="s3_native",
                    endpoint_url="http://127.0.0.1:9000")
    engine.write_engine_config(host, str(tmp_path), str(tmp_path / "s.db"),
                               str(tmp_path / "*"), str(config))
    assert "endpoint_url: http://127.0.0.1:9000" in config.read_text(), (
        "host mode's config must be unchanged from today")


def test_runner_mode_is_a_recorded_column_everywhere():
    assert "runner_mode" in CSV_HEADERS["metrics.csv"], (
        "a number read without knowing which namespace produced it is a "
        "number compared against the wrong baseline")
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
    from benchmarks.run_matrix import COMPARE_COLS
    assert "runner_mode" in COMPARE_COLS


def test_the_engine_service_keeps_the_hardening_baseline():
    """The compose lint the acceptance names: the new service cannot relax
    the baseline, asserted beside the same assertion for MinIO."""
    # The suite also runs on the no-PyYAML fallback lap, which exists to
    # prove the SCENARIO parser's fallback — compose files are not in that
    # contract, and linting one requires a real YAML parser.
    import pytest
    yaml = pytest.importorskip(
        "yaml", reason="the compose lint needs a real YAML parser")
    compose_path = os.path.join(os.path.dirname(__file__), "..",
                                "docker-compose.benchmark.yml")
    with open(compose_path, encoding="utf-8") as fh:
        compose = yaml.safe_load(fh)
    for name in ("vtop-engine", "minio"):
        service = compose["services"][name]
        assert service.get("cap_drop") == ["ALL"], name
        assert "no-new-privileges:true" in service.get("security_opt", []), name
        assert service.get("read_only") is True, name
        assert service.get("pids_limit"), name
        assert service.get("mem_limit"), name
    vtop = compose["services"]["vtop-engine"]
    assert vtop.get("profiles") == ["containerized"], (
        "the engine container must never join the default profile: host "
        "mode is the default and pays nothing for the second mode")
    # The binary mount is the long bind form (a dict), chosen so a missing
    # source fails `up` instead of being mkdir'd as an empty directory.
    binary_mount = [
        v for v in vtop["volumes"]
        if isinstance(v, dict) and "target/release/vtopctl" in str(v.get("source", ""))
    ]
    assert binary_mount, (
        "the measured artifact is the HOST-built binary, bind-mounted from "
        "the host — an image rebuild would measure a different compiler's output")
    mount = binary_mount[0]
    assert mount.get("read_only") is True, (
        "the mounted binary must be read-only — the container never rewrites "
        "the artifact both modes measure")
    assert mount.get("bind", {}).get("create_host_path") is False, (
        "create_host_path must be false so a missing binary fails `up` loudly "
        "rather than mounting an empty directory the runner would try to exec")
