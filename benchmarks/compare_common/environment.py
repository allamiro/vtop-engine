"""environment.json for a cross-product run (#744 section 4.2): what was measured, where, with what.

The `comparable` block is what makes two runs comparable: host, Docker VM,
limits, every image and connector pin, the store build, the VTOP commit and the
host class. It is hashed into `env_fingerprint`, which every CSV row carries,
so rows from two environments can never be drawn as one (rule 9). Each tool's
configuration file is recorded verbatim with its sha256 under `configs`
(rule 13), and the telemetry setting each one carries under `telemetry`
(rule 19).

A configuration file that differs from HEAD is refused before a run starts
(`config_drift`): the CSV records the hash of what ran, and a reader must be
able to find that file in the commit the run names.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import platform
import subprocess
from collections.abc import Iterable
from typing import Any

from compare_common import schema

BENCH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPO = os.path.dirname(BENCH)
STORE_LABEL = "MinIO (lab build, upstream archived)"


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def git(*args: str, repo: str = REPO) -> str:
    return subprocess.run(["git", "-C", repo, *args], check=True, capture_output=True, text=True).stdout.strip()


def config_drift(paths: Iterable[str], repo: str = REPO) -> list[str]:
    """The configuration files (repo-relative) whose working copy is not HEAD's, staged or not, or untracked."""
    paths = sorted(set(paths))
    if not paths:
        return []
    out = subprocess.run(["git", "-C", repo, "status", "--porcelain", "--untracked-files=all", "--", *paths],
                         check=True, capture_output=True, text=True).stdout
    return [line[3:] for line in out.splitlines() if line.strip()]


def record_configs(files: dict[str, dict[str, str]], repo: str = REPO) -> dict[str, dict[str, dict[str, str]]]:
    """{system: {profile: {path, sha256, text}}} from {system: {profile: repo-relative path}}."""
    out: dict[str, dict[str, dict[str, str]]] = {}
    for system, profiles in sorted(files.items()):
        for profile, rel in sorted(profiles.items()):
            with open(os.path.join(repo, rel), encoding="utf-8") as handle:
                text = handle.read()
            out.setdefault(system, {})[profile] = {"path": rel, "sha256": schema.sha256_text(text), "text": text}
    return out


def host_facts(probe_disk: bool = True) -> dict[str, Any]:
    """The host and Docker VM, as kafka_compare records them (#600), so the footers read alike."""
    from kafka_compare import run as kafka_run

    def docker_json(*args: str) -> dict[str, Any]:
        try:
            return json.loads(subprocess.run(["docker", *args, "--format", "{{json .}}"], check=True,
                                             capture_output=True, text=True).stdout)
        except (OSError, subprocess.CalledProcessError, ValueError):
            return {}

    info = docker_json("info")
    version = docker_json("version")
    return {
        "host_cpu": kafka_run._sysctl("machdep.cpu.brand_string") or _linux_cpu() or platform.processor(),
        "host_logical_cpus": kafka_run._sysctl("hw.ncpu") or str(os.cpu_count()),
        "host_memory_bytes": kafka_run.host_memory_bytes(),
        "host_os": f"macOS {platform.mac_ver()[0]}" if platform.mac_ver()[0] else
                   f"{platform.system()} {platform.release()}",
        "host_disk": kafka_run.probe_host_disk() if probe_disk else "not probed",
        "docker_platform": (version.get("Server") or {}).get("Platform", {}).get("Name", ""),
        "docker_server_version": info.get("ServerVersion", ""),
        "docker_vm_kernel": info.get("KernelVersion", ""),
        "docker_vm_arch": info.get("Architecture", ""),
        "docker_vm_cpus": info.get("NCPU", ""),
        "docker_vm_memory_bytes": info.get("MemTotal", ""),
        "docker_storage_driver": info.get("Driver", ""),
        "docker_cgroup_version": info.get("CgroupVersion", ""),
    }


def _linux_cpu() -> str:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as handle:
            for line in handle:
                if line.lower().startswith(("model name", "hardware")):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return ""


def new_environment(kind: str, run_id: str, plan: dict[str, Any], comparable: dict[str, Any],
                    configs: dict[str, Any], *, telemetry: dict[str, str], private: bool,
                    extra: dict[str, Any] | None = None) -> dict[str, Any]:
    for key in ("host_class", "vtop_commit"):
        if not comparable.get(key):
            raise ValueError(f"comparable block lacks {key}")
    env = {
        "schema_version": schema.SCHEMA_VERSION,
        "kind": kind,
        "run_id": run_id,
        "run_state": "running",
        "started_at": utc_now(),
        "private": private,
        "store": STORE_LABEL,
        "comparable": comparable,
        "plan": plan,
        "configs": configs,
        "telemetry": telemetry,
        "events": [],
    }
    env.update(extra or {})
    return env


def save(run_dir: str, env: dict[str, Any]) -> None:
    path = os.path.join(run_dir, "environment.json")
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(env, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
