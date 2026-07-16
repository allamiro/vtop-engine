"""Thin driver around the compiled `vtopctl` binary.

The benchmark never imports engine code — it only builds and runs the binary and
parses its JSON output, keeping benchmark logic fully separate from the engine.
"""
from __future__ import annotations

import json
import os
import subprocess


def repo_root() -> str:
    # benchmarks/lib/engine.py -> repo root is two levels up.
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def vtopctl_path(build_if_missing: bool = True) -> str:
    env = os.environ.get("VTOPCTL_BIN")
    if env and os.path.exists(env):
        return env
    root = repo_root()
    path = os.path.join(root, "target", "release", "vtopctl")
    if not os.path.exists(path) and build_if_missing:
        subprocess.run(["cargo", "build", "--release", "--bin", "vtopctl"],
                       cwd=root, check=True)
    return path


def write_engine_config(scenario, work_dir: str, state_db: str,
                        input_glob: str, config_path: str) -> str:
    backend = scenario.get("backend", "mock")
    bucket = scenario.get("bucket", "telemetry-data")
    create_bucket = "true" if backend == "minio" else "false"
    endpoint = scenario.get("endpoint_url", "") or os.environ.get("VTOP_S3_ENDPOINT_URL", "")
    whole_file = "true" if scenario.get("whole_file") or scenario.get("format") == "binary" else "false"
    checksum = scenario.get("checksum", "sha256")
    # The engine implements sha256 / blake3 / none; record the request as-is.
    if checksum not in ("sha256", "blake3", "none", "disabled"):
        checksum = "sha256"
    lines = [
        "engine:",
        "  name: vtop-bench",
        "  tenant: default",
        f'  state_store: "sqlite://{state_db}"',
        f"  work_dir: {work_dir}",
        "  log_level: warn",
        "batching:",
        f"  max_records: {scenario.get('batch_max_records', 10000)}",
        f"  max_bytes: {scenario.get('batch_max_bytes', 104857600)}",
        f"  max_batch_age_seconds: {scenario.get('batch_max_age_seconds', 60)}",
        "compression:",
        f"  type: {scenario.get('compression', 'gzip')}",
        f"  level: {scenario.get('compression_level', 6)}",
        "checksum:",
        f"  algorithm: {checksum}",
        "sources:",
        "  file:",
        "    enabled: true",
        f"    whole_file: {whole_file}",
        "    paths:",
        f'      - "{input_glob}"',
        "upload:",
        f"  backend: {backend}",
        f'  bucket: "{bucket}"',
        '  prefix: ""',
        f"  create_bucket: {create_bucket}",
        "  region: us-east-1",
        "  force_path_style: true",
        "  verify_tls: false",
    ]
    if backend == "localfs":
        root = scenario.get("local_path", "") or os.path.join(os.path.dirname(state_db), "objects")
        lines.append(f'  local_path: "{root}"')
    if endpoint:
        lines.append(f"  endpoint_url: {endpoint}")
    with open(config_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return config_path


def _backend_env(scenario) -> dict[str, str]:
    env = dict(os.environ)
    if scenario.get("backend") == "minio":
        env.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
        env.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin")
        env.setdefault("AWS_REGION", "us-east-1")
        env.setdefault("VTOP_S3_FORCE_PATH_STYLE", "true")
        env.setdefault("VTOP_S3_VERIFY_TLS", "false")
        if scenario.get("endpoint_url"):
            env.setdefault("VTOP_S3_ENDPOINT_URL", scenario.get("endpoint_url"))
    return env


def process_once(binary: str, config_path: str, scenario,
                 source: str = "file") -> tuple[int, list[dict], str]:
    """Run `vtopctl process-once --json` and parse the batch outcomes."""
    proc = subprocess.run(
        [binary, "--json", "process-once", "--source", source, "--config", config_path],
        capture_output=True, text=True, env=_backend_env(scenario))
    outcomes: list[dict] = []
    try:
        outcomes = json.loads(proc.stdout) if proc.stdout.strip() else []
    except json.JSONDecodeError:
        outcomes = []
    return proc.returncode, outcomes, proc.stderr


def replay(binary: str, config_path: str, scenario) -> tuple[int, str]:
    proc = subprocess.run(
        [binary, "replay", "--config", config_path],
        capture_output=True, text=True, env=_backend_env(scenario))
    return proc.returncode, proc.stdout + proc.stderr
