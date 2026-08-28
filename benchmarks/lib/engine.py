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
    endpoint = scenario.get("endpoint_url", "") or os.environ.get("VTOP_S3_ENDPOINT_URL", "")
    # An explicit endpoint means the lab stack, whichever backend speaks to
    # it — s3_native against MinIO needs its bucket created exactly as the
    # mc-based backend does. Real S3 (s3_native, no endpoint) keeps bucket
    # creation out of the runtime identity (SECURITY_MODEL §5).
    create_bucket = "true" if (backend == "minio" or (backend == "s3_native" and endpoint)) else "false"
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
    ]
    # The #87 pipeline-width knob, surfaced so a scenario grid can vary it
    # (#102: object_upload p95 vs concurrency is the signal that decides
    # whether an adaptive controller has anything to react to). Absent =
    # the engine default, exactly as before.
    if scenario.get("max_concurrent_batches"):
        lines.append(
            f"  max_concurrent_batches: {scenario.get('max_concurrent_batches')}")
    lines += [
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


# The .env the BENCHMARK STACK actually honors. Compose resolves a -f file's
# project directory to that file's own directory, so it is benchmarks/.env —
# and deliberately NOT the repository root's — that interpolates the stack's
# credentials. The runner must read the same file, or a filed override would
# reach one side of the authentication and not the other, in either
# direction.
_ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".env")


def _dotenv_overrides() -> dict[str, str]:
    """KEY=VALUE pairs from the .env compose reads for the benchmark stack.

    Deliberately minimal (comments and blank lines skipped, one layer of
    matching quotes stripped, no interpolation): the harness is
    dependency-light, and the lab's .env holds simple assignments.
    """
    values: dict[str, str] = {}
    try:
        with open(_ENV_FILE, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] \
                        and value[0] in "\"'":
                    value = value[1:-1]
                values[key.strip()] = value
    except OSError:
        pass
    return values


def _backend_env(scenario) -> dict[str, str]:
    env = dict(os.environ)
    # s3_native pointed at the lab endpoint needs the same credentials the
    # mc-based backend does; setdefault keeps real AWS credentials (already
    # in the environment) winning over the lab fallbacks.
    if scenario.get("backend") == "minio" or (
            scenario.get("backend") == "s3_native" and scenario.get("endpoint_url")):
        # The benchmark compose lets an operator override the SERVER's
        # credentials via MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (issue #81).
        # The client must follow the same variables THROUGH THE SAME
        # CHANNELS: a literal default here made every upload against an
        # overridden stack fail authentication, and consulting only the
        # process environment repeats that failure for an operator who set
        # the override in .env — which compose auto-loads and a separately
        # launched Python process does not. A blank value falls back to the
        # lab default, exactly as compose's own ${VAR:-default} would.
        dotenv = _dotenv_overrides()

        def _credential(name: str) -> str:
            # A PRESENT shell variable masks the filed one even when blank,
            # exactly as compose's interpolation resolves it: the server
            # sees ${VAR:-minioadmin} with the blank shell value and uses
            # the default, so falling through to .env here would hand the
            # client a credential the server never saw.
            if name in env:
                return env[name] or "minioadmin"
            return dotenv.get(name) or "minioadmin"

        env.setdefault("AWS_ACCESS_KEY_ID", _credential("MINIO_ROOT_USER"))
        env.setdefault("AWS_SECRET_ACCESS_KEY",
                       _credential("MINIO_ROOT_PASSWORD"))
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
