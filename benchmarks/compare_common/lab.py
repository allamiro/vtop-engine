"""Docker plumbing every comparison harness shares: the VTOP image, compose projects, pins, run ids.

The VTOP image is built from `docker/Dockerfile` and tagged by what it is built
from (the `crates/` tree and `Cargo.lock`), so the three comparisons on one
commit share one image. A build input that differs from HEAD is refused: the
image would carry code no commit names.

Every tool and helper image is used by digest (`products.toml`, or the pins
below), so a tag that moves cannot change a run.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
from collections.abc import Sequence
from typing import Any

BENCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(BENCH)

# The harness container (producer, tap, checker, sampler): the Python the chart job pins.
HARNESS_IMAGE = "python:3.12.16-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b"
# The lab MinIO, built from source by docker/minio/Dockerfile (it carries `mc`).
MINIO_IMAGE = "vtop-minio:RELEASE.2025-09-07T16-13-09Z"
MINIO_BUILD = os.path.join(REPO, "docker", "minio")
LAB_ACCESS_KEY = "minioadmin"
LAB_SECRET_KEY = "minioadmin"

_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def log(prefix: str, message: str) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
    print(f"[{prefix} {stamp}] {message}", flush=True)


def sh(args: Sequence[str], *, check: bool = True, timeout: float | None = None, input_text: str | None = None,
       env: dict[str, str] | None = None, cwd: str | None = None) -> subprocess.CompletedProcess:
    merged = dict(os.environ)
    merged.update(env or {})
    result = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout, input=input_text,
                            env=merged, cwd=cwd)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(list(args)[:6])} ... exited {result.returncode}: "
                           f"{(result.stderr or result.stdout).strip()[-800:]}")
    return result


def new_run_id(kind: str, now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    return f"{kind}-{now.strftime('%Y%m%dT%H%M%SZ')}"


def check_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id) or run_id in (".", ".."):
        raise SystemExit(f"--run-id {run_id!r} must be one path component of letters, digits, '.', '_' or '-'")
    return run_id


def run_tag(run_id: str) -> str:
    """The 8-character tag embedded in every record id of this run (checker.make_id)."""
    return hashlib.sha256(run_id.encode()).hexdigest()[:8]


def git(*args: str) -> str:
    return sh(["git", "-C", REPO, *args]).stdout.strip()


def engine_tree() -> str:
    """What the VTOP image is built from: the crates tree and the lock file."""
    return hashlib.sha256((git("rev-parse", "HEAD:crates") + git("rev-parse", "HEAD:Cargo.lock")).encode()
                          ).hexdigest()[:12]


def ensure_vtop_image(rebuild: bool = False) -> str:
    from kafka_compare import run as kafka_run

    dirty = kafka_run.dirty_build_inputs()
    if dirty:
        raise SystemExit(f"the engine's build inputs differ from HEAD ({'; '.join(dirty[:5])}): commit or stash "
                         "them, so the image benchmarked is the commit recorded")
    image = f"vtop-compare:{engine_tree()}"
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log("lab", f"building {image} from docker/Dockerfile")
        sh(["docker", "build", "-f", os.path.join(REPO, "docker", "Dockerfile"), "-t", image, REPO], timeout=7200)
    return image


def ensure_minio_image() -> str:
    if sh(["docker", "image", "inspect", MINIO_IMAGE], check=False).returncode != 0:
        log("lab", f"building {MINIO_IMAGE} from docker/minio")
        sh(["docker", "build", "-t", MINIO_IMAGE, MINIO_BUILD], timeout=3600)
    return MINIO_IMAGE


def ensure_image(ref: str) -> None:
    if sh(["docker", "image", "inspect", ref], check=False).returncode != 0:
        sh(["docker", "pull", ref], timeout=1800)


def image_id(ref: str) -> str:
    return json.loads(sh(["docker", "image", "inspect", ref]).stdout)[0]["Id"]


def compose(project: str, files: Sequence[str], *args: str, env: dict[str, str] | None = None,
            timeout: float = 900, check: bool = True) -> subprocess.CompletedProcess:
    command = ["docker", "compose", "-p", project]
    for path in files:
        command += ["-f", path]
    return sh(command + list(args), env=env, timeout=timeout, check=check)


def container_exit(name: str) -> dict[str, Any]:
    state = json.loads(sh(["docker", "inspect", name]).stdout)[0]["State"]
    return {"running": state.get("Running"), "exit_code": state.get("ExitCode"), "error": state.get("Error", "")}


def logs_tail(name: str, lines: int = 20) -> str:
    out = sh(["docker", "logs", "--tail", str(lines), name], check=False)
    return (out.stdout + out.stderr).strip()


def last_log_line(name: str) -> str:
    text = logs_tail(name, 1)
    return text.splitlines()[-1][:300] if text else ""


def compose_limits(text: str) -> dict[str, dict[str, str]]:
    """{service: {cpus, mem_limit, memswap_limit}} from a compose file's text, for services that declare any.

    A regex reader, not YAML: PyYAML is optional in the harness, and the
    limits are one-line scalars by convention here."""
    out: dict[str, dict[str, str]] = {}
    service = None
    in_services = False
    for line in text.splitlines():
        if re.match(r"^services:\s*$", line):
            in_services = True
            continue
        if re.match(r"^\S", line):
            in_services = False
            service = None
            continue
        if not in_services:
            continue
        match = re.match(r"^  ([A-Za-z0-9_.-]+):\s*$", line)
        if match:
            service = match.group(1)
            continue
        match = re.match(r"^    (cpus|mem_limit|memswap_limit):\s*\"?([^\"\s#]+)\"?", line)
        if match and service:
            out.setdefault(service, {})[match.group(1)] = match.group(2)
    return out
