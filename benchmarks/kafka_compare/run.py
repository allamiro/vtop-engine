#!/usr/bin/env python3
"""Run the phase-1 VTOP vs Apache Kafka matrix (#595, #596) on this Docker host.

One command, both systems, one at a time:

    python3 benchmarks/kafka_compare/run.py

For every cell of the matrix, Kafka then VTOP is brought up fresh (empty named
volumes), proven correct by a byte-exact smoke read-back, warmed up, and timed
with the SAME client container - the JVM perf tools from the pinned
`apache/kafka` image - at saturation, then at fixed rates derived from Kafka's
saturation for that cell, then for consume throughput. Each measurement is one
CSV row; a cell a system could not run is a row saying why. See README.md.

`--group-commit off,on` (#752) runs VTOP once per setting of the node's
`group_commit.enabled`, so the report shows both. The harness writes the
setting into the leader's config, only if the engine under test accepts a
`group_commit:` section, and checks the running node's own gauge says the
setting took; otherwise it refuses, before any Docker work or before the first
measurement, rather than run without it.

Schema 4 (#600) adds two measurements to the same run. Every measured run is
bracketed by the three nodes' cgroup v2 counters (CPU seconds, bytes written
and read; resources.py), and an end-to-end stage per record size, after both
systems' cells, offers both systems - and Kafka under `fsync-parity` as well -
one `common` rate (a share of the slower system's median saturation) while a
consumer stamps each record's receive time against its CreateTime
(client/E2eLatency.java, e2e.py). The overhead of both is stated in README.md.
"""
from __future__ import annotations

import argparse
import datetime as dt
import functools
import hashlib
import json
import math
import os
import platform
import plistlib
import re
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from typing import Any, NamedTuple

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from compare_common import hostmeta  # noqa: E402
from kafka_compare import e2e, perftool, plan, resources, schema, stats  # noqa: E402

REPO = os.path.dirname(os.path.dirname(HERE))
KAFKA_IMAGE = "apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7"
TOPIC = "bench"
PROFILE = "defaults"
# Where vtop-node's config is defined, and the section the ablation sets (#752). The
# harness reads this file at HEAD (the commit the image is built from) to learn which
# keys the node accepts, so it never writes one the binary would refuse or ignore.
NODE_CONFIG = "crates/vtop-node/src/config.rs"
GROUP_COMMIT_GAUGE = "vtop_broker_group_commit_enabled"
LEADER_CONFIG = os.path.join(HERE, "vtop", "leader.yaml")
# The client container's limits: identical for both systems, and larger than
# a node's so the client is not the bottleneck it measures.
CLIENT_CPUS = "4"
CLIENT_MEMORY = "4g"
CLIENT_HEAP = "-Xms1g -Xmx1g"
# Explicit on the command line for both systems; every other producer setting
# is the client's default, and the client's own dump of them is recorded.
PRODUCER_PROPS = ("acks=all", "compression.type=none")
# Offset commits are off for both: the gateway without a metadata plane
# refuses OffsetCommit (a listed gap), and a consumer measurement should not
# hinge on it either way.
CONSUMER_PROPS = ("enable.auto.commit=false",)

# The end-to-end consumer's container: smaller than the producer's. The pinned Kafka image's JRE has no
# jdk.compiler, so it cannot launch E2eLatency.java as a source file: client/Dockerfile compiles it once with this
# JDK (the one #599's crash/client/Dockerfile uses) against the image's own kafka-clients jars, onto that image.
CLIENT_DIR = os.path.join(HERE, "client")
JDK_IMAGE = "eclipse-temurin:21.0.11_10-jdk@sha256:98155457fcdbeb0987472e8abf721657b698ede407aad52ac24ae42618c0931c"
E2E_CLASSES = "/opt/e2e-latency"
E2E_CLASSPATH = f"/opt/kafka/libs/*:{E2E_CLASSES}"
E2E_CPUS = "2"
E2E_MEMORY = "2g"
E2E_HEAP = "-Xms512m -Xmx512m"
E2E_LOAD_PCT = 50
E2E_EXTRA_PROFILES = ["fsync-parity"]
# A consumer that has seen its last record waits this long for another before it reports what it has.
E2E_IDLE_MS = 30_000
E2E_READY_TIMEOUT = 120.0
KAFKA_PROFILES = {"defaults": "Kafka as shipped: log.flush.interval.messages unset, so an acknowledgement does "
                              "not wait for an fsync",
                  "fsync-parity": "flush.messages=1 on every broker and on the topic: every append is flushed "
                                  "before it is acknowledged"}

SYSTEM_SPEC = {
    "kafka": {"compose": "compose.kafka.yml", "network": "kc-kafka-bench",
              "bootstrap": "kafka-1:9092", "leader": "kc-kafka-kafka-1-1"},
    "vtop": {"compose": "compose.vtop.yml", "network": "kc-vtop-bench",
             "bootstrap": "vtop-1:9092", "leader": "kc-vtop-vtop-1-1"},
}


def log(message: str) -> None:
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%H:%M:%S")
    print(f"[kafka-compare {stamp}] {message}", flush=True)


def sh(args: list[str], *, check: bool = True, timeout: float | None = None, input_text: str | None = None,
       env: dict[str, str] | None = None, cwd: str | None = None) -> subprocess.CompletedProcess:
    merged = dict(os.environ)
    if env:
        merged.update(env)
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, input=input_text,
                            env=merged, cwd=cwd)
    if check and result.returncode != 0:
        raise RuntimeError(f"{' '.join(args[:6])} ... exited {result.returncode}: "
                           f"{(result.stderr or result.stdout).strip()[-800:]}")
    return result


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


# ------------------------------------------------------------------ group commit (#752)


class GroupCommitNotApplied(Exception):
    """The node that came up is not running the group-commit setting the run asked for.

    Deliberately not a RuntimeError: a cell that fails with one of those is written as `refused` rows
    and the run goes on to the next cell, but a setting that did not take would repeat in every cell and
    leave a table that looks complete and compares nothing. It stops the run."""


def parse_group_commits(text: str) -> list[str]:
    """`--group-commit off,on`: the settings to run, in the one order the matrix records them."""
    items = text.split(",")
    unknown = [item for item in items if item not in plan.GROUP_COMMITS]
    if unknown or len(set(items)) != len(items):
        raise SystemExit(f"--group-commit {text!r}: give distinct settings from {list(plan.GROUP_COMMITS)}, "
                         "comma-separated (off = `group_commit.enabled: false`, on = `enabled: true`)")
    return [setting for setting in plan.GROUP_COMMITS if setting in items]


def _struct_body(source: str, name: str) -> str | None:
    match = re.search(rf"^pub struct {name}\b[^{{]*\{{(.*?)^\}}", source, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else None


def node_group_commit_keys(source: str) -> set[str] | None:
    """The keys of the `group_commit:` section a vtop-node config.rs accepts, or None when the data node's
    config has no such section (an engine from before #752 wired group commit in, whose config refuses an
    unknown key at startup). Read from the source of the commit under test, so it is the binary's own schema."""
    node = _struct_body(source, "DataNodeConfig")
    if node is None or not re.search(r"^\s*pub group_commit\s*:", node, re.MULTILINE):
        return None
    section = _struct_body(source, "GroupCommitSection")
    if section is None:
        raise SystemExit(f"{NODE_CONFIG} gives DataNodeConfig a `group_commit` but defines no GroupCommitSection "
                         "this harness can read: update the harness to the config's new shape")
    return set(re.findall(r"^\s*pub (\w+)\s*:", section, re.MULTILINE))


def require_group_commit_support(source: str, settings: list[str]) -> set[str]:
    """The keys the node accepts, or a refusal that says what is missing and what to do."""
    keys = node_group_commit_keys(source)
    asked = ", ".join(settings)
    if keys is None:
        raise SystemExit(
            f"--group-commit {asked} was asked for, but the engine at HEAD does not accept a `group_commit:` "
            f"section ({NODE_CONFIG} has none; it lands with #832, issue #752). A node build without it would "
            "refuse the key at startup, or, with the key left out, run without group commit while the results "
            "said it was on or off. Nothing was started. Build a commit that has the section, or run "
            "without --group-commit.")
    if "enabled" not in keys:
        raise SystemExit(f"--group-commit {asked} was asked for, but the node's `group_commit:` section has no "
                         f"`enabled` key (it accepts {sorted(keys)}): the harness cannot switch group commit on "
                         "or off. Nothing was started; update the harness to the node's config.")
    return keys


def check_group_commit(args: argparse.Namespace) -> tuple[list[str], set[str]]:
    """The settings asked for and the keys the node accepts, refused before any Docker work."""
    if getattr(args, "group_commit", None) is None:
        return [], set()
    settings = parse_group_commits(args.group_commit)
    return settings, require_group_commit_support(git("show", f"HEAD:{NODE_CONFIG}"), settings)


def leader_config(base: str, setting: str, accepted: set[str]) -> str:
    """The leader's node.yaml for one setting: the file the harness ships plus an explicit `group_commit:`
    section. `off` is written as `enabled: false`, not left out, so a later change of the node's default
    cannot change what the run's "off" measured. Only `enabled` is written, and only because `accepted`
    says the node takes it."""
    if re.search(r"^group_commit\s*:", base, re.MULTILINE):
        raise SystemExit(f"{os.path.relpath(LEADER_CONFIG, REPO)} already sets group_commit: the harness "
                         "writes it per run, and a second value would be a guess about which wins")
    if "enabled" not in accepted:
        raise SystemExit("the node's group_commit section has no `enabled` key: nothing to write")
    value = "true" if setting == "on" else "false"
    return (base if base.endswith("\n") else base + "\n") + (
        f"# Written by run.py for the group-commit ablation (#752): group commit {setting}.\n"
        f"group_commit:\n  enabled: {value}\n")


def scrape_leader_metrics(system: str) -> str:
    """The leader's /metrics text, from inside its own container (the node image carries curl, as its
    healthcheck uses), so no port is published and no client container is needed."""
    return sh(["docker", "exec", SYSTEM_SPEC[system]["leader"], "curl", "-sf",
               "http://127.0.0.1:9600/metrics"], timeout=60).stdout


def check_group_commit_applied(setting: str, metrics: str) -> int:
    """The node's own gauge says what the node is running. Returns it; raises GroupCommitNotApplied when it
    is missing or is not what `setting` asked for."""
    match = re.search(rf"^{GROUP_COMMIT_GAUGE}(?:\{{[^}}]*\}})?\s+(\S+)\s*$", metrics, re.MULTILINE)
    if match is None:
        raise GroupCommitNotApplied(f"the leader exports no {GROUP_COMMIT_GAUGE} gauge, so group commit "
                                    f"{setting} cannot be confirmed; the node build is not the one the "
                                    "harness expects")
    want = 1 if setting == "on" else 0
    try:
        have = int(float(match.group(1)))
    except ValueError:
        have = None
    if have != want:
        raise GroupCommitNotApplied(f"group commit {setting} was asked for, but the leader's {GROUP_COMMIT_GAUGE} "
                                    f"is {match.group(1)} (expected {want}): the node did not take the setting, "
                                    "so nothing measured under this label would be what it says")
    return have


# ----------------------------------------------------------------------- environment


def _compose_limits(path: str) -> dict[str, str]:
    """The per-node limits a compose file declares (its first cpus/mem_limit/memswap_limit)."""
    with open(path) as handle:
        text = handle.read()
    limits = {}
    for key in ("cpus", "mem_limit", "memswap_limit"):
        match = re.search(rf"^\s+{key}:\s*(\S+)\s*$", text, re.MULTILINE)
        if not match:
            raise SystemExit(f"{path} declares no {key}: refusing an unlimited node (rule 1)")
        limits[key] = match.group(1)
    return limits


def check_equal_limits() -> dict[str, str]:
    kafka = _compose_limits(os.path.join(HERE, "compose.kafka.yml"))
    vtop = _compose_limits(os.path.join(HERE, "compose.vtop.yml"))
    if kafka != vtop:
        raise SystemExit(f"per-node limits differ (rule 1): kafka {kafka} vs vtop {vtop}")
    return kafka


def _sysctl(name: str) -> str:
    try:
        return sh(["sysctl", "-n", name]).stdout.strip()
    except (RuntimeError, FileNotFoundError):
        return ""


def host_memory_bytes() -> str:
    """Physical RAM in bytes: the macOS sysctl, else POSIX sysconf (Linux), else ""."""
    value = _sysctl("hw.memsize")
    if value:
        return value
    try:
        pages, page_size = os.sysconf("SC_PHYS_PAGES"), os.sysconf("SC_PAGE_SIZE")
    except (AttributeError, ValueError, OSError):
        return ""
    return str(pages * page_size) if pages > 0 and page_size > 0 else ""


def volume_filesystem_command() -> list[str]:
    """`df -T` on a fresh ANONYMOUS volume. `docker run --rm` removes an
    anonymous volume with its container, so the probe never names, and never
    deletes, a volume that this run did not create."""
    return ["docker", "run", "--rm", "--mount", "type=volume,destination=/probe", "--entrypoint", "df",
            KAFKA_IMAGE, "-T", "/probe"]


class HostDiskUnknown(Exception):
    """The disk behind Docker's data could not be named; the message says what the probe saw."""


def describe_macos_disk(path: str, info: Callable[[str], dict[str, Any]]) -> str:
    """"<model>, SSD|HDD, <bus>, <filesystem>" of the physical disk under the volume holding `path`.
    `info(device)` is `diskutil info -plist <device>` as a dict. Raises HostDiskUnknown, naming
    what diskutil did or did not say, rather than record a guess."""
    try:
        volume = info(path)
        stores = volume.get("APFSPhysicalStores") or []
        device = stores[0]["APFSPhysicalStore"] if stores else volume["ParentWholeDisk"]
        whole = info(device).get("ParentWholeDisk") or device
        disk = info(whole)
    except Exception as exc:
        raise HostDiskUnknown(f"diskutil info for {path}: {exc}") from exc
    model, solid = disk.get("MediaName"), disk.get("SolidState")
    if not model or solid is None:
        raise HostDiskUnknown(f"diskutil names no model or solid-state flag for {whole} (MediaName={model!r}, "
                              f"SolidState={solid!r})")
    return ", ".join((model, "SSD" if solid else "HDD", disk.get("BusProtocol") or "unknown bus",
                      volume.get("FilesystemType") or "unknown filesystem"))


def describe_linux_disk(findmnt: str, lsblk: Callable[[str], str]) -> str:
    """The same for Linux. `findmnt` is `findmnt -no SOURCE,FSTYPE -T <Docker's data root>`; `lsblk(device)`
    runs `lsblk -nsPo NAME,TYPE,MODEL,ROTA,TRAN <device>`, the device and every device under it, so the
    walk reaches the disk through partitions, LVM and LUKS. The kernel's rotational flag is quoted as it
    is (virtio and some RAID controllers report it wrongly), not turned into SSD or HDD."""
    fields = findmnt.split()
    if len(fields) < 2:
        raise HostDiskUnknown(f"findmnt named no device and filesystem (it printed {findmnt.strip()!r})")
    # btrfs prints the subvolume after the device: /dev/nvme0n1p3[/root].
    source, fstype = re.sub(r"\[.*\]$", "", fields[0]), fields[1]
    try:
        listing = lsblk(source)
    except Exception as exc:
        raise HostDiskUnknown(f"lsblk {source}: {exc}") from exc
    devices = [dict(re.findall(r'(\w+)="([^"]*)"', line)) for line in listing.splitlines() if line.strip()]
    disks = [device for device in devices if device.get("TYPE") == "disk"]
    if not disks:
        raise HostDiskUnknown(f"lsblk shows no disk under {source} (types: {[d.get('TYPE') for d in devices]})")
    disk = disks[0]
    model = disk.get("MODEL", "").strip()
    if not model:
        raise HostDiskUnknown(f"lsblk names no model for {disk.get('NAME')} under {source}")
    rota = {"0": "non-rotational (kernel flag)", "1": "rotational (kernel flag)"}.get(disk.get("ROTA", ""),
                                                                                      "unknown rotation")
    return ", ".join((model, rota, disk.get("TRAN") or "unknown bus", fstype))


def docker_desktop_data_folder() -> str | None:
    """Where Docker Desktop for Mac keeps its VM disk, if its settings say (the "Disk image location")."""
    base = os.path.expanduser("~/Library/Group Containers/group.com.docker")
    for name, key in (("settings-store.json", "DataFolder"), ("settings.json", "dataFolder")):
        try:
            with open(os.path.join(base, name), encoding="utf-8") as handle:
                folder = json.load(handle).get(key)
        except (OSError, ValueError, AttributeError):
            continue
        if folder:
            return folder
    return None


def host_disk(docker_root: str) -> str:
    """The disk behind Docker's data (schema 2, #600: every image's footer names it), or HostDiskUnknown."""
    system = platform.system()
    if system == "Darwin":
        def info(device: str) -> dict[str, Any]:
            return plistlib.loads(sh(["diskutil", "info", "-plist", device]).stdout.encode())
        return describe_macos_disk(docker_desktop_data_folder() or "/", info)
    if system == "Linux":
        try:
            findmnt = sh(["findmnt", "-no", "SOURCE,FSTYPE", "-T", docker_root]).stdout
        except (RuntimeError, FileNotFoundError) as exc:
            raise HostDiskUnknown(f"findmnt -T {docker_root}: {exc}") from exc
        return describe_linux_disk(findmnt, lambda device: sh(["lsblk", "-nsPo", "NAME,TYPE,MODEL,ROTA,TRAN",
                                                                device]).stdout)
    raise HostDiskUnknown(f"no disk probe for {system}")


def probe_host_disk() -> str:
    """The host disk, before any image is pulled or built or any directory is made: a run whose disk
    cannot be named would be refused by the report, so it is refused before it starts."""
    try:
        root = json.loads(sh(["docker", "info", "--format", "{{json .}}"]).stdout).get("DockerRootDir") or "/"
        return host_disk(root)
    except HostDiskUnknown as exc:
        raise SystemExit(f"could not name the host disk behind Docker's data: {exc}. Every image's footer names "
                         "it (environment.json schema 2), so the run is refused before it starts; fix the probe "
                         "or the permission it needs and run again.") from exc


def checked_host_disk(given: str | None) -> str | None:
    """`--host-disk` validated, or None when the flag was not given.

    For a host whose probe cannot name the disk (an EBS NVMe volume that reports no model, a virtio VM), the
    operator names it. The text goes into every image's footer, whose fields are `label: value` joined by
    ' | ' on lines, so a value holding the separator or a line break is refused here, at argument parsing,
    not when the report is drawn hours later."""
    if given is None:
        return None
    text = given.strip()
    if not text or "|" in text or "\n" in text or "\r" in text:
        raise SystemExit(f"--host-disk {given!r}: must be a non-empty single line without '|' (the footer's "
                         "field separator), for example --host-disk \"EBS gp3 100 GiB, SSD, NVMe, ext4\"")
    return text


def resolve_host_disk(given: str | None) -> str:
    """The host disk to record: `--host-disk` as given, else the probe's answer."""
    text = checked_host_disk(given)
    return probe_host_disk() if text is None else text


def git(*args: str) -> str:
    return sh(["git", "-C", REPO, *args]).stdout.strip()


# What docker/Dockerfile compiles the image from. A change here that is not
# committed would be built into an image tagged, and recorded, as HEAD.
BUILD_INPUTS = ("Cargo.toml", "Cargo.lock", ".cargo", "crates", "docker")


def dirty_build_inputs() -> list[str]:
    status = git("status", "--porcelain", "--untracked-files=all", "--", *BUILD_INPUTS)
    return [line for line in status.splitlines() if line.strip()]


def capture_environment(run_id: str, matrix: dict[str, Any], vtop_image: str, limits: dict[str, str],
                        disk: str, group_commit_configs: dict[str, str] | None = None) -> dict:
    info = json.loads(sh(["docker", "info", "--format", "{{json .}}"]).stdout)
    version = json.loads(sh(["docker", "version", "--format", "{{json .}}"]).stdout)
    kafka_inspect = json.loads(sh(["docker", "image", "inspect", KAFKA_IMAGE]).stdout)[0]
    vtop_inspect = json.loads(sh(["docker", "image", "inspect", vtop_image]).stdout)[0]
    java = sh(["docker", "run", "--rm", "--entrypoint", "java", KAFKA_IMAGE, "-version"]).stderr.strip()
    volume_fs = sh(volume_filesystem_command(), check=False).stdout.strip().splitlines()
    others = [name for name in sh(["docker", "ps", "--format", "{{.Names}}"]).stdout.split() if name]
    host_mem = host_memory_bytes()
    comparable = {
        "host_cpu": _sysctl("machdep.cpu.brand_string") or hostmeta.cpu_model(),
        "host_model": _sysctl("hw.model"),
        "host_logical_cpus": _sysctl("hw.ncpu") or str(os.cpu_count()),
        "host_memory_bytes": host_mem,
        "host_os": f"macOS {platform.mac_ver()[0]}" if platform.mac_ver()[0] else
                   f"{platform.system()} {platform.release()}",
        "docker_platform": (version.get("Server") or {}).get("Platform", {}).get("Name", ""),
        "docker_server_version": info.get("ServerVersion"),
        "docker_vm_os": info.get("OperatingSystem"),
        "docker_vm_kernel": info.get("KernelVersion"),
        "docker_vm_arch": info.get("Architecture"),
        "docker_vm_cpus": info.get("NCPU"),
        "docker_vm_memory_bytes": info.get("MemTotal"),
        "docker_storage_driver": info.get("Driver"),
        # Device and type only: the usage columns change run to run and would
        # make every run's fingerprint different.
        "volume_filesystem": " ".join(volume_fs[-1].split()[:2]) if volume_fs else "unknown",
        # The host's disk under Docker's data (schema 2): model, SSD or HDD, bus, filesystem.
        "host_disk": disk,
        "node_limits": limits,
        "client_limits": {"cpus": CLIENT_CPUS, "memory": CLIENT_MEMORY, "heap": CLIENT_HEAP},
        "kafka_image": KAFKA_IMAGE,
        "kafka_image_id": kafka_inspect["Id"],
        "vtop_commit": git("rev-parse", "HEAD"),
        "vtop_engine_tree": git("rev-parse", "HEAD:crates"),
        "vtop_image": vtop_image,
        "vtop_image_id": vtop_inspect["Id"],
        "client": client_name(),
        "client_java": java.splitlines()[0] if java else "",
        "profile": PROFILE,
        # The group-commit settings this run measures (#752) and the `enabled` each was written as: part of
        # the fingerprinted environment, so a run that varied it cannot pass as one that did not. Empty
        # when the run left VTOP at the node's own default.
        "vtop_group_commit": {"settings": {name: {"enabled": name == "on"} for name in group_commit_configs or {}}},
        "producer_props": list(PRODUCER_PROPS),
        "consumer_props": list(CONSUMER_PROPS),
    }
    if matrix.get("resources"):
        comparable["resources_source"] = resources.COUNTER_SOURCE
        comparable["resources_window_close"] = resources.CLOSE_MODE
    if matrix.get("e2e"):
        comparable["e2e_consumer"] = ("client/E2eLatency.java, compiled by client/Dockerfile against the Kafka "
                                      "image's kafka-clients and run by that image's JVM: assign, seek to the end, "
                                      "latency = poll() return time minus the record's CreateTime, 1 ms buckets")
        comparable["e2e_client_image"] = e2e_image_name()
        comparable["e2e_jdk_image"] = JDK_IMAGE
        comparable["e2e_consumer_limits"] = {"cpus": E2E_CPUS, "memory": E2E_MEMORY, "heap": E2E_HEAP}
        comparable["kafka_profiles"] = dict(KAFKA_PROFILES)
    return {
        "schema_version": schema.SCHEMA_VERSION,
        # Which generator draws it (failover.py overwrites it). Recorded from schema 2 on, so a
        # schema-2 run never needs its kind guessed from the files in its directory.
        "kind": "throughput",
        "run_id": run_id,
        "run_state": "running",
        "started_at": utc_now(),
        "label": "single host, Docker Desktop; indicative, not a production benchmark",
        "comparable": comparable,
        # Which machine this was (#737): the instance type, the CPU model, the kernel. Beside `comparable`, not in
        # it, so it is not part of the fingerprint and a run recorded without it loads as before.
        "host_metadata": hostmeta.collect(),
        "matrix": matrix,
        "worktree_dirty": bool(git("status", "--porcelain")),
        # A count, not names: the names are other people's projects and do not
        # belong in a committed result; the count says the VM was not idle.
        "other_containers_at_start_count": len(others),
        "configs": {
            "kafka_compose": _read(os.path.join(HERE, "compose.kafka.yml")),
            "vtop_compose": _read(os.path.join(HERE, "compose.vtop.yml")),
            "vtop_leader": _read(LEADER_CONFIG),
            **{f"vtop_leader_group_commit_{name}": text for name, text in (group_commit_configs or {}).items()},
            "vtop_follower_1": _read(os.path.join(HERE, "vtop", "follower-1.yaml")),
            "vtop_follower_2": _read(os.path.join(HERE, "vtop", "follower-2.yaml")),
        },
        "observed": {},
        "events": [],
    }


def _read(path: str) -> str:
    with open(path) as handle:
        return handle.read()


def client_name() -> str:
    tag = KAFKA_IMAGE.split("@")[0].split(":")[-1]
    return f"kafka-producer-perf-test/kafka-consumer-perf-test {tag} (JVM)"


def save_environment(run_dir: str, environment: dict) -> None:
    path = os.path.join(run_dir, "environment.json")
    tmp = path + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(environment, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- clusters


def compose_files(system: str, profile: str = "defaults") -> list[str]:
    """The `-f` arguments for a system's Compose project. Kafka's `fsync-parity` profile is a second file
    layered after the base one; every other combination is the base file alone."""
    files = ["-f", os.path.join(HERE, SYSTEM_SPEC[system]["compose"])]
    if system == "kafka" and profile == "fsync-parity":
        files += ["-f", os.path.join(HERE, "compose.kafka.fsync-parity.yml")]
    return files


def compose(system: str, *args: str, vtop_image: str, timeout: float = 600, profile: str = "defaults",
            leader_config: str | None = None) -> subprocess.CompletedProcess:
    """`leader_config` is an absolute path to the leader's node.yaml for this run (compose.vtop.yml
    mounts it in place of the shipped one); None keeps the shipped file."""
    env = {"VTOP_IMAGE": vtop_image, "KAFKA_IMAGE": KAFKA_IMAGE}
    if leader_config is not None:
        env["VTOP_LEADER_CONFIG"] = leader_config
    return sh(["docker", "compose", *compose_files(system, profile), *args], env=env, timeout=timeout, cwd=HERE)


def cluster_down(system: str, vtop_image: str) -> None:
    compose(system, "down", "-v", "--remove-orphans", vtop_image=vtop_image)


def cluster_up(system: str, vtop_image: str, profile: str = "defaults", leader_config: str | None = None) -> None:
    cluster_down(system, vtop_image)
    compose(system, "up", "-d", "--wait", "--wait-timeout", "300", vtop_image=vtop_image, timeout=400,
            profile=profile, leader_config=leader_config)
    if system == "kafka":
        command = ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", SYSTEM_SPEC[system]["bootstrap"],
                   "--create", "--topic", TOPIC, "--partitions", "1", "--replication-factor", "3",
                   "--config", "min.insync.replicas=2"]
        if profile == "fsync-parity":
            command += ["--config", "flush.messages=1"]
        client(system, command, timeout=120)


_client_seq = 0


def client(system: str, command: list[str], *, timeout: float, input_text: str | None = None,
           info_logging: bool = False, check: bool = True) -> subprocess.CompletedProcess:
    """One client invocation in its own container on the system's network."""
    global _client_seq
    _client_seq += 1
    name = f"kc-client-{os.getpid()}-{_client_seq}"
    env_args = ["-e", f"KAFKA_HEAP_OPTS={CLIENT_HEAP}"]
    if info_logging:
        env_args += ["-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/log4j2.yaml"]
    args = ["docker", "run", "--rm", "--name", name, "--network", SYSTEM_SPEC[system]["network"],
            "--cpus", CLIENT_CPUS, "--memory", CLIENT_MEMORY, *env_args]
    if input_text is not None:
        args.append("-i")
    args += [KAFKA_IMAGE, *command]
    try:
        return sh(args, timeout=timeout, input_text=input_text, check=check)
    except subprocess.TimeoutExpired:
        sh(["docker", "rm", "-f", name], check=False)
        raise


# ------------------------------------------------------------------------------ smoke


def smoke_records(count: int, size: int) -> list[str]:
    """`count` distinct printable records of exactly `size` bytes, reproducible."""
    records = []
    for index in range(count):
        head = f"smoke-{index:08d}-"
        body = hashlib.sha256(f"kc-smoke-{index}".encode()).hexdigest()
        while len(head) + len(body) < size:
            body += hashlib.sha256(body.encode()).hexdigest()
        records.append((head + body)[:size])
    return records


def smoke(system: str, count: int, size: int) -> None:
    """Produce `count` records with acks=all and read every one back, byte-exact
    and in order, through the same client; raise on any difference (#596)."""
    records = smoke_records(count, size)
    bootstrap = SYSTEM_SPEC[system]["bootstrap"]
    client(system, ["/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", bootstrap, "--topic", TOPIC,
                    "--command-property", "acks=all"], timeout=300, input_text="\n".join(records) + "\n")
    read = client(system, ["/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", bootstrap,
                           "--topic", TOPIC, "--partition", "0", "--offset", "earliest",
                           "--max-messages", str(count), "--timeout-ms", "60000"], timeout=300, check=False)
    got = read.stdout.split("\n")
    if got and got[-1] == "":
        got.pop()
    if len(got) != count:
        raise RuntimeError(f"smoke read back {len(got)} records, produced {count}: {read.stderr.strip()[-400:]}")
    for index, (want, have) in enumerate(zip(records, got)):
        if want != have:
            raise RuntimeError(f"smoke record {index} differs: produced {want[:40]!r}..., read {have[:40]!r}...")


# ------------------------------------------------------------------------------- runs


def producer_command(system: str, records: int, size: int, rate: int, warmup: int) -> list[str]:
    return ["/opt/kafka/bin/kafka-producer-perf-test.sh", "--bootstrap-server", SYSTEM_SPEC[system]["bootstrap"],
            "--topic", TOPIC, "--num-records", str(records), "--record-size", str(size), "--throughput", str(rate),
            "--warmup-records", str(warmup), "--command-property", *PRODUCER_PROPS]


TIMED_OUT = "timed out after"


def _captured(text: str | bytes | None) -> str:
    """What a timed-out child printed: subprocess.run hands it back as bytes even in text mode."""
    if text is None:
        return ""
    return text.decode(errors="replace") if isinstance(text, bytes) else text


def run_producer(system: str, records: int, size: int, rate: int, timeout: float,
                 info_logging: bool = False) -> tuple[perftool.ProducerResult | None, str, float, str]:
    """(result or None, note, wall seconds, raw output).

    A run that hits `timeout` is a failed run (its container is removed by `client`). Its note names the
    achieved rate of the last progress line the tool printed before it was cut off, when there is one, so a
    leg that ran to its wall limit still says how fast the system was going."""
    warmup = max(1, records // 5)
    started = time.monotonic()
    try:
        done = client(system, producer_command(system, records, size, rate, warmup), timeout=timeout,
                      check=False, info_logging=info_logging)
    except subprocess.TimeoutExpired as exc:
        output = _captured(exc.stdout) + "\n" + _captured(exc.stderr)
        last = perftool.last_progress_rate(output)
        seen = (f"last achieved rate {last} records/s (the tool's last progress line)" if last is not None
                else "no progress line to read an achieved rate from")
        return None, f"{TIMED_OUT} {int(timeout)} s; {seen}", time.monotonic() - started, output
    wall = time.monotonic() - started
    output = done.stdout + "\n" + done.stderr
    errors = sorted({line.strip()[:200] for line in output.splitlines()
                     if "Exception" in line and not line.strip().startswith("at ")})
    try:
        result = perftool.parse_producer(done.stdout, expect_steady_state=True)
    except perftool.PerfParseError as exc:
        detail = "; ".join(errors[:2]) or output.strip()[-300:]
        return None, f"{exc}; exit {done.returncode}; {detail}", wall, output
    if done.returncode != 0 or errors:
        return None, f"exit {done.returncode}; {'; '.join(errors[:2])}", wall, output
    return result, "", wall, output


def base_row(ctx: dict, system: str, mode: str, size: int, rep: int, profile: str = PROFILE,
             group_commit: str = "") -> dict[str, Any]:
    basis = {"fixed_rate": "kafka", "e2e": "common"}.get(mode, "")
    return {
        "run_id": ctx["run_id"], "env_fingerprint": ctx["fingerprint"],
        "vtop_commit": ctx["env"]["comparable"]["vtop_commit"], "kafka_image": KAFKA_IMAGE,
        "client": client_name(), "profile": profile, "system": system, "mode": mode,
        "record_size": size, "partitions": 1, "producers": 1, "compression": "none", "acks": "all",
        "group_commit": group_commit, "repetition": rep, "load_basis": basis, "started_at": utc_now(),
    }


def producer_row(row: dict, result: perftool.ProducerResult | None, note: str, wall: float) -> dict:
    row["wall_seconds"] = round(wall, 1)
    if result is None:
        row.update(status="failed", note=note)
        return row
    row.update(status="ok", records=result.records, records_per_sec=result.records_per_sec,
               mib_per_sec=result.mib_per_sec, lat_avg_ms=result.lat_avg_ms, lat_max_ms=result.lat_max_ms,
               lat_p50_ms=result.lat_p50_ms, lat_p95_ms=result.lat_p95_ms, lat_p99_ms=result.lat_p99_ms,
               lat_p999_ms=result.lat_p999_ms, lat_basis="single", note=note)
    return row


# ------------------------------------------------------------------------ resources (metric 4)


def epoch_ms() -> int:
    return int(time.time() * 1000)


def node_names(system: str) -> list[str]:
    """The system's three node containers, as Compose names them (project kc-<system>, one per service)."""
    return [f"kc-{system}-{system}-{index}-1" for index in (1, 2, 3)]


def _cgroup_file(node: str, name: str) -> str:
    return sh(["docker", "exec", node, "cat", f"{resources.CGROUP_ROOT}/{name}"], timeout=60).stdout


def make_closer() -> resources.Closer:
    """`sync` in each node and the cgroup counters read from inside it, with `docker exec`."""
    return resources.Closer(
        sync=lambda node: sh(["docker", "exec", node, "sync"], timeout=120),
        sleep=time.sleep,
        snap=lambda node: resources.snapshot(lambda name: _cgroup_file(node, name)),
    )


class Window(NamedTuple):
    start_ms: int
    end_ms: int
    total: resources.Window


_WINDOW_ERRORS = (resources.CgroupRefused, RuntimeError, subprocess.TimeoutExpired)


def measure(ctx: dict, system: str, action: Callable[[], Any]) -> tuple[Any, Window | None, str]:
    """`action()` between an opened and a closed cgroup window on the system's nodes: (its value, the window,
    "") or, when a counter could not be read, (its value, None, why). The action runs either way, so a
    counter fault costs the row its resources (and the row says so), not the run its measurement."""
    if not ctx.get("resources"):
        return action(), None, ""
    nodes, closer = node_names(system), ctx["closer"]
    try:
        begin = resources.open_window(nodes, closer)
        start_ms = epoch_ms()
    except _WINDOW_ERRORS as exc:
        return action(), None, f"opening the resource window: {exc}"[:300]
    value = action()
    end_ms = epoch_ms()
    try:
        finish = resources.close_window(nodes, closer)
        total = resources.total({node: resources.window(begin[node], finish[node]) for node in nodes})
    except _WINDOW_ERRORS as exc:
        return value, None, f"closing the resource window: {exc}"[:300]
    return value, Window(start_ms, end_ms, total), ""


def attach_resources(ctx: dict, row: dict, window: Window | None, error: str, delivered_bytes: int) -> dict:
    """The window's figures on an ok row; an ok row whose window could not be read becomes failed, saying so
    (the report refuses an ok row without its resources when the matrix asked for them)."""
    if not ctx.get("resources") or row.get("status") != "ok":
        return row
    if window is None:
        row.update(status="failed", note=f"resources: {error}")
        return row
    row.update(window_start_ms=window.start_ms, window_end_ms=window.end_ms, delivered_bytes=delivered_bytes,
               cpu_seconds=round(window.total.cpu_seconds, 3), disk_write_bytes=window.total.disk_write_bytes,
               disk_read_bytes=window.total.disk_read_bytes)
    return row


# ------------------------------------------------------------- end-to-end latency (metric 3)

_e2e_seq = 0


def e2e_image_name() -> str:
    """kc-e2e-client:<12 hex>, named for the source, the Dockerfile and both pinned images it is built from."""
    digest = hashlib.sha256()
    for name in ("Dockerfile", "E2eLatency.java"):
        with open(os.path.join(CLIENT_DIR, name), "rb") as handle:
            digest.update(name.encode() + b"\0" + handle.read() + b"\0")
    digest.update(KAFKA_IMAGE.encode() + b"\0" + JDK_IMAGE.encode())
    return f"kc-e2e-client:{digest.hexdigest()[:12]}"


def ensure_e2e_image(rebuild: bool = False) -> str:
    """The end-to-end consumer's image (client/Dockerfile), built when it is not there."""
    image = e2e_image_name()
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} (the end-to-end consumer, compiled against the pinned image's kafka-clients)")
        sh(["docker", "build", "-t", image, "--build-arg", f"KAFKA_IMAGE={KAFKA_IMAGE}",
            "--build-arg", f"JDK_IMAGE={JDK_IMAGE}", CLIENT_DIR], timeout=1800)
    return image


def e2e_main_command(*args: str) -> list[str]:
    """The compiled consumer's class run by the image's JVM, with the image's kafka-clients jars."""
    return ["java", "-cp", E2E_CLASSPATH, "E2eLatency", *args]


def e2e_consumer_command(system: str, expected: int) -> list[str]:
    return e2e_main_command(SYSTEM_SPEC[system]["bootstrap"], TOPIC, str(expected), str(E2E_IDLE_MS))


class E2eConsumer:
    """The end-to-end consumer's container, started before the producer and read until it exits."""

    def __init__(self, system: str, expected: int, image: str):
        global _e2e_seq
        _e2e_seq += 1
        self.name = f"kc-e2e-{os.getpid()}-{_e2e_seq}"
        args = ["docker", "run", "--rm", "--name", self.name, "--network", SYSTEM_SPEC[system]["network"],
                "--cpus", E2E_CPUS, "--memory", E2E_MEMORY, "-e", f"JAVA_TOOL_OPTIONS={E2E_HEAP}",
                image, *e2e_consumer_command(system, expected)]
        self.proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.out: list[str] = []
        self.err: list[str] = []
        self.ready = threading.Event()
        self.threads = [threading.Thread(target=self._drain, args=(self.proc.stdout, self.out, True), daemon=True),
                        threading.Thread(target=self._drain, args=(self.proc.stderr, self.err, False), daemon=True)]
        for thread in self.threads:
            thread.start()

    def _drain(self, stream, lines: list[str], watch: bool) -> None:
        for line in stream:
            lines.append(line.rstrip("\n"))
            if watch and line.strip() == "READY":
                self.ready.set()

    def wait_ready(self, timeout: float) -> None:
        if not self.ready.wait(timeout):
            raise RuntimeError(f"the end-to-end consumer was not ready after {int(timeout)} s: "
                               f"{' '.join(self.err)[-400:] or 'no output'}")

    def finish(self, timeout: float) -> tuple[int | None, str, str]:
        try:
            code = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.kill()
            code = None
        for thread in self.threads:
            thread.join(timeout=10)
        return code, "\n".join(self.out), "\n".join(self.err)

    def kill(self) -> None:
        if self.proc.poll() is None:
            sh(["docker", "rm", "-f", self.name], check=False)


def run_e2e_once(system: str, size: int, rate: int, records: int, timeout: float, image: str) -> tuple:
    """One end-to-end repetition: the consumer (in `image`, see `ensure_e2e_image`) is ready at the end of the
    log, then the producer offers `records` at `rate`, then the consumer is read until it has them all or has
    waited E2E_IDLE_MS for one.
    Returns (producer result, note, wall seconds, producer output, consumer exit code, stdout, stderr)."""
    consumer = E2eConsumer(system, records, image)
    try:
        consumer.wait_ready(E2E_READY_TIMEOUT)
        result, note, wall, output = run_producer(system, records, size, rate, timeout)
        code, out, err = consumer.finish(E2E_IDLE_MS / 1000 + 120)
    finally:
        consumer.kill()
    return result, note, wall, output, code, out, err


def finish_e2e_row(row: dict, result: perftool.ProducerResult | None, note: str, wall: float, code: int | None,
                   out: str, err: str, rate: int, records: int) -> tuple[dict, dict[int, int] | None]:
    """The e2e row of one repetition, and its histogram when the row is ok. The percentiles are the
    histogram's, so the CSV and the CDF agree by construction."""
    row.update(offered_rate=rate, load_pct=E2E_LOAD_PCT, wall_seconds=round(wall, 1))
    if result is None:
        row.update(status="failed", note=note)
        return row, None
    text = "\n".join(line for line in out.splitlines() if line.strip() != "READY")
    try:
        _, hist = e2e.parse_output(text)
        outcome = e2e.judge(hist, records, rate, result.records_per_sec)
    except e2e.E2eRefused as exc:
        detail = f" (consumer exit {code}: {err.strip()[-200:]})" if code not in (0, None) else ""
        row.update(status="failed", note=f"{exc}{detail}"[:600])
        return row, None
    row.update(status=outcome.status, records=outcome.records, records_produced=records,
               sustained=outcome.sustained, note=outcome.note)
    if outcome.status != "ok":
        return row, None
    for column, (num, den) in e2e.PERCENTILES.items():
        row[column] = e2e.percentile_ms(hist, num, den)
    row.update(lat_max_ms=float(max(hist)), lat_basis="single")
    return row, hist


def write_e2e_result(ctx: dict, row: dict, hist: dict[int, int] | None) -> None:
    """The histogram first, then the row that vouches for it."""
    if hist is not None and row["status"] == "ok":
        key = {column: row[column] for column in schema.E2E_HIST_KEY}
        for bucket, count in sorted(hist.items()):
            schema.append_row(ctx["hist_csv"], {**key, "bucket_ms": bucket, "count": count}, schema.E2E_HIST_COLUMNS)
    schema.append_row(ctx["csv"], row)


def write_leader_config(ctx: dict, group_commit: str) -> str | None:
    """The leader's node.yaml for a group-commit setting, written under the run's directory (#752), or None
    (the shipped file) when the run does not vary the setting."""
    if not group_commit:
        return None
    configs = os.path.join(ctx["run_dir"], "configs")
    os.makedirs(configs, exist_ok=True)
    path = os.path.join(configs, f"vtop-leader-group-commit-{group_commit}.yaml")
    with open(path, "w") as handle:
        handle.write(ctx["group_commit_configs"][group_commit])
    return path


def confirm_group_commit(ctx: dict, system: str, size: int, group_commit: str, logs: str, tag: str) -> None:
    """The leader's own gauge says the setting took, recorded as an event; GroupCommitNotApplied otherwise."""
    if not group_commit:
        return
    try:
        have = check_group_commit_applied(group_commit, scrape_leader_metrics(system))
    except GroupCommitNotApplied as exc:
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size,
                                     "group_commit": group_commit, "event": str(exc)[:900]})
        _save_leader_log(system, logs, tag, ctx["vtop_image"])
        raise
    ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size,
                                 "group_commit": group_commit,
                                 "event": f"group commit {group_commit} confirmed: {GROUP_COMMIT_GAUGE} {have}"})


def e2e_targets(matrix: dict[str, Any]) -> list[tuple[str, str, str]]:
    """(system, profile, group commit) the end-to-end stage measures: Kafka as shipped, VTOP as shipped once per
    group-commit setting the matrix asks for (#752; "" when it asks for none), and Kafka under each extra
    profile."""
    return ([("kafka", PROFILE, "")] + [("vtop", PROFILE, setting) for setting in matrix.get("group_commits") or [""]]
            + [("kafka", extra, "") for extra in plan.extra_profiles(matrix)])


def refuse_e2e(ctx: dict, size: int, reason: str, targets: list[tuple[str, str, str]]) -> None:
    for system, profile, group_commit in targets:
        for rep in range(1, ctx["reps"] + 1):
            row = base_row(ctx, system, "e2e", size, rep, profile, group_commit)
            row.update(status="refused", note=reason, load_pct=E2E_LOAD_PCT)
            schema.append_row(ctx["csv"], row)


def run_e2e_cell(ctx: dict, system: str, profile: str, size: int, rate: int, group_commit: str = "") -> None:
    """One end-to-end target of one record size: a fresh cluster in the profile (and, for VTOP, the group-commit
    setting, confirmed by the leader's gauge as in `run_cell`), the smoke read-back, a short unrecorded warm-up
    at the same rate, then every repetition."""
    logs = os.path.join(ctx["run_dir"], "logs")
    os.makedirs(logs, exist_ok=True)
    tag = f"{system}-{profile}-{size}B-e2e" + (f"-gc-{group_commit}" if group_commit else "")
    records = stats.records_for_duration(rate, ctx["fixed_seconds"])
    targets = [(system, profile, group_commit)]
    leader_config = write_leader_config(ctx, group_commit)
    try:
        log(f"{tag}: bringing the cluster up fresh")
        cluster_up(system, ctx["vtop_image"], profile, leader_config=leader_config)
        confirm_group_commit(ctx, system, size, group_commit, logs, tag)
        log(f"{tag}: smoke - {ctx['smoke_records']} records of {size} B, produced and read back byte-exact")
        smoke(system, ctx["smoke_records"], size)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        reason = f"cluster refused before timing: {exc}"[:900]
        log(f"{tag}: {reason}")
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "profile": profile, "record_size": size,
                                     "event": reason})
        _save_leader_log(system, logs, tag, ctx["vtop_image"])
        refuse_e2e(ctx, size, reason, targets)
        cluster_down(system, ctx["vtop_image"])
        return
    observed_key = "kafka_topic" if profile == PROFILE else f"kafka_topic_{profile}"
    if system == "kafka" and observed_key not in ctx["env"]["observed"]:
        _observe_kafka(ctx, profile)
    log(f"{tag}: warm-up run at {rate} records/s (not recorded)")
    _, _, _, output = run_producer(system, max(rate * 10, 1000), size, rate, ctx["sat_timeout"])
    _save_output(logs, f"{tag}-warmup.log", output)
    for rep in range(1, ctx["reps"] + 1):
        log(f"{tag}: end-to-end rep {rep}/{ctx['reps']} ({records} records at {rate}/s)")
        row = base_row(ctx, system, "e2e", size, rep, profile, group_commit)
        timeout = max(ctx["sat_timeout"], 6 * records / rate)
        try:
            value, window, werr = measure(
                ctx, system, functools.partial(run_e2e_once, system, size, rate, records, timeout,
                                               ctx["e2e_image"]))
            result, note, wall, output, code, out, err = value
        except (RuntimeError, subprocess.TimeoutExpired) as exc:
            row.update(offered_rate=rate, load_pct=E2E_LOAD_PCT, status="failed", note=str(exc)[:600])
            schema.append_row(ctx["csv"], row)
            continue
        _save_output(logs, f"{tag}-{rep}-producer.log", output)
        _save_output(logs, f"{tag}-{rep}-consumer.log", out + "\n" + err)
        row, hist = finish_e2e_row(row, result, note, wall, code, out, err, rate, records)
        attach_resources(ctx, row, window, werr, records * size)
        write_e2e_result(ctx, row, hist)
        log(f"{tag}: -> p50 {row.get('lat_p50_ms')} ms p99 {row.get('lat_p99_ms')} ms "
            f"sustained={row.get('sustained')} status={row['status']}")
    _save_leader_log(system, logs, tag, ctx["vtop_image"])
    cluster_down(system, ctx["vtop_image"])


def run_e2e_stage(ctx: dict, size: int) -> None:
    """After both systems' cells of a record size: the common rate, then every end-to-end target."""
    targets = e2e_targets(ctx["matrix"])
    saturation = ctx["saturation"]
    # With group commit varied (#752), VTOP is the slower of its settings: every target is offered one rate.
    vtop = [saturation.get(("vtop", size, setting), []) for setting in ctx["matrix"].get("group_commits") or [""]]
    vtop_saturation = [] if any(not reps for reps in vtop) else min(vtop, key=stats.median)
    try:
        rate = e2e.common_rate(saturation.get(("kafka", size, ""), []), vtop_saturation, E2E_LOAD_PCT)
    except e2e.E2eRefused as exc:
        reason = f"no common offered rate for this cell: {exc}"
        log(f"e2e {size} B: {reason}")
        ctx["env"]["events"].append({"at": utc_now(), "record_size": size, "event": reason})
        refuse_e2e(ctx, size, reason, targets)
        return
    ctx["env"]["observed"].setdefault("e2e_rates", {})[str(size)] = rate
    for system, profile, group_commit in targets:
        run_e2e_cell(ctx, system, profile, size, rate, group_commit)


def fixed_rate_records(rate: int, own_saturation: float | None, fixed_seconds: float) -> tuple[int, bool]:
    """(records to send, whether they were sized from the system's own saturation) for one fixed-rate leg.

    The producer is throttled at the offered `rate` either way. A system whose own saturation median is below
    that rate cannot send `rate x fixed_seconds` records in `fixed_seconds`, and used to run for as long as
    its shortfall made it (lesson 1 of the v0.11.1 AWS round); it is sent `own median x fixed_seconds`, so
    the leg lasts about the target and the row says it did not hold the rate. With no saturation measured the
    leg is sized from the rate, and the wall limit bounds it."""
    if own_saturation is not None and own_saturation < rate:
        return stats.records_for_duration(max(1, int(own_saturation)), fixed_seconds), True
    return stats.records_for_duration(rate, fixed_seconds), False


def sized_note(rate: int, achieved: float, own_saturation: float, fixed_seconds: float, records: int,
               note: str) -> str:
    said = (f"offered {rate} records/s, achieved {achieved:.0f}; the leg was sized from this system's saturation "
            f"median ({own_saturation:.0f} records/s x {fixed_seconds:g} s = {records} records), not from the "
            "offered rate")
    return f"{said}; {note}" if note else said


def refuse_rows(ctx: dict, system: str, size: int, reason: str, load_pcts: list[int],
                group_commit: str = "") -> None:
    """A row for every measurement of this system's cell, saying why it did not run."""
    for rep in range(1, ctx["reps"] + 1):
        modes = [("saturation", None)] + [("fixed_rate", pct) for pct in load_pcts]
        if ctx["consume"]:
            modes.append(("consume", None))
        for mode, pct in modes:
            row = base_row(ctx, system, mode, size, rep, group_commit=group_commit)
            row.update(status="refused", note=reason, load_pct=pct)
            schema.append_row(ctx["csv"], row)


def run_cell(ctx: dict, system: str, size: int, kafka_rates: dict[int, int] | None,
             group_commit: str = "") -> dict[int, int] | None:
    """Every measurement of one system in one cell. Returns Kafka's derived rates.

    `group_commit` (`off` or `on`, VTOP only) is the node's `group_commit.enabled` for this cell: its config
    is written under the run's directory, the cluster comes up with it, and the leader's own gauge must say
    the setting took before anything is timed (GroupCommitNotApplied stops the run otherwise)."""
    sat_records = ctx["sat_records"][size]
    load_pcts = list(stats.LOAD_FRACTIONS)
    logs = os.path.join(ctx["run_dir"], "logs")
    os.makedirs(logs, exist_ok=True)
    tag = f"{system}-{size}B" + (f"-gc-{group_commit}" if group_commit else "")
    leader_config = write_leader_config(ctx, group_commit)
    try:
        log(f"{tag}: bringing the cluster up fresh")
        cluster_up(system, ctx["vtop_image"], leader_config=leader_config)
        confirm_group_commit(ctx, system, size, group_commit, logs, tag)
        log(f"{tag}: smoke - {ctx['smoke_records']} records of {size} B, produced and read back byte-exact")
        smoke(system, ctx["smoke_records"], size)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        reason = f"cluster refused before timing: {exc}"[:900]
        log(f"{tag}: {reason}")
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size, "event": reason})
        _save_leader_log(system, logs, tag, ctx["vtop_image"])
        refuse_rows(ctx, system, size, reason, load_pcts, group_commit)
        cluster_down(system, ctx["vtop_image"])
        return None
    ctx["env"]["events"].append({"at": utc_now(), "system": system, "record_size": size,
                                 "event": f"smoke passed: {ctx['smoke_records']} records byte-exact"})
    if system == "kafka" and "kafka_topic" not in ctx["env"]["observed"]:
        _observe_kafka(ctx)

    # Unrecorded warm-up run: JIT, page cache, segment creation on both sides.
    log(f"{tag}: warm-up run (not recorded)")
    result, note, _, output = run_producer(system, max(sat_records // 4, 1000), size, -1, ctx["sat_timeout"],
                                           info_logging=True)
    try:
        ctx["env"]["observed"].setdefault("producer_config", {})[
            f"{system}-{size}" + (f"-gc-{group_commit}" if group_commit else "")] = \
            perftool.parse_client_config(output)
    except perftool.PerfParseError as exc:
        ctx["env"]["events"].append({"at": utc_now(), "system": system, "event": f"producer config: {exc}"})
    with open(os.path.join(logs, f"{tag}-warmup.log"), "w") as handle:
        handle.write(output)

    saturation = []
    for rep in range(1, ctx["reps"] + 1):
        log(f"{tag}: saturation rep {rep}/{ctx['reps']} ({sat_records} records)")
        row = base_row(ctx, system, "saturation", size, rep, group_commit=group_commit)
        (result, note, wall, output), window, werr = measure(
            ctx, system, lambda: run_producer(system, sat_records, size, -1, ctx["sat_timeout"]))
        _save_output(logs, f"{tag}-saturation-{rep}.log", output)
        if result is not None:
            saturation.append(result.records_per_sec)
        row = attach_resources(ctx, producer_row(row, result, note, wall), window, werr, sat_records * size)
        schema.append_row(ctx["csv"], row)
        log(f"{tag}: -> {row.get('records_per_sec', row.get('note'))} records/s")

    ctx.setdefault("saturation", {})[(system, size, group_commit)] = saturation
    own = stats.median(saturation) if saturation else None
    if system == "kafka":
        try:
            kafka_rates = stats.derive_offered_rates(saturation)
        except ValueError as exc:
            kafka_rates = None
            ctx["env"]["events"].append({"at": utc_now(), "event": f"no offered rates for {size} B: {exc}"})
        ctx["env"]["observed"].setdefault("offered_rates", {})[str(size)] = kafka_rates
    for rep in range(1, ctx["reps"] + 1):
        for pct in load_pcts:
            row = base_row(ctx, system, "fixed_rate", size, rep, group_commit=group_commit)
            row["load_pct"] = pct
            if not kafka_rates:
                row.update(status="refused", note="no Kafka saturation for this cell to derive the offered rate from")
                schema.append_row(ctx["csv"], row)
                continue
            rate = kafka_rates[pct]
            records, sized = fixed_rate_records(rate, own, ctx["fixed_seconds"])
            row["offered_rate"] = rate
            log(f"{tag}: fixed rate {pct}% = {rate} records/s, rep {rep}/{ctx['reps']}"
                + (f" ({records} records, sized from this system's saturation)" if sized else ""))
            timeout = plan.fixed_rate_wall_limit(ctx["fixed_seconds"])
            (result, note, wall, output), window, werr = measure(
                ctx, system, functools.partial(run_producer, system, records, size, rate, timeout))
            _save_output(logs, f"{tag}-fixed-{pct}-{rep}.log", output)
            if result is not None:
                row["sustained"] = stats.sustained(rate, result.records_per_sec)
                if sized:
                    note = sized_note(rate, result.records_per_sec, own, ctx["fixed_seconds"], records, note)
            elif note.startswith(TIMED_OUT):
                # The leg ran to its wall limit: whatever it achieved, it did not hold the offered rate.
                row["sustained"] = False
            row = attach_resources(ctx, producer_row(row, result, note, wall), window, werr, records * size)
            schema.append_row(ctx["csv"], row)
            log(f"{tag}: -> p50 {row.get('lat_p50_ms')} ms p99 {row.get('lat_p99_ms')} ms "
                f"achieved {row.get('records_per_sec')} sustained={row.get('sustained')}")

    if ctx["consume"]:
        for rep in range(1, ctx["reps"] + 1):
            row = base_row(ctx, system, "consume", size, rep, group_commit=group_commit)
            log(f"{tag}: consume rep {rep}/{ctx['reps']} ({sat_records} records from the start of the log)")
            started = time.monotonic()
            command = ["/opt/kafka/bin/kafka-consumer-perf-test.sh", "--bootstrap-server",
                       SYSTEM_SPEC[system]["bootstrap"], "--topic", TOPIC, "--num-records", str(sat_records),
                       "--group", f"kc-{ctx['run_id']}-{system}-{size}"
                       + (f"-gc-{group_commit}" if group_commit else "") + f"-{rep}", "--timeout", "60000",
                       "--command-property", *CONSUMER_PROPS]
            try:
                done, window, werr = measure(
                    ctx, system, functools.partial(client, system, command, timeout=ctx["sat_timeout"], check=False))
                _save_output(logs, f"{tag}-consume-{rep}.log", done.stdout + done.stderr)
                # A summary printed by a tool that then failed is not a
                # measurement, as on the producer path.
                if done.returncode != 0:
                    raise perftool.PerfParseError("the consumer perf tool exited nonzero")
                result = perftool.parse_consumer(done.stdout, sat_records)
                row.update(status="ok", records=result.records, records_per_sec=result.records_per_sec,
                           mib_per_sec=result.mib_per_sec,
                           note=f"rebalance {result.rebalance_ms:.0f} ms excluded; fetch {result.fetch_ms:.0f} ms")
                attach_resources(ctx, row, window, werr, result.records * size)
            except subprocess.TimeoutExpired:
                row.update(status="failed", note=f"timed out after {ctx['sat_timeout']} s")
            except perftool.PerfParseError as exc:
                row.update(status="failed", note=f"{exc}; exit {done.returncode}")
            row["wall_seconds"] = round(time.monotonic() - started, 1)
            schema.append_row(ctx["csv"], row)
            log(f"{tag}: -> {row.get('records_per_sec', row.get('note'))} records/s consumed")

    _save_leader_log(system, logs, tag, ctx["vtop_image"])
    cluster_down(system, ctx["vtop_image"])
    return kafka_rates


def _save_output(logs: str, name: str, output: str) -> None:
    with open(os.path.join(logs, name), "w") as handle:
        handle.write(output)


def _save_leader_log(system: str, logs: str, tag: str, vtop_image: str) -> None:
    done = sh(["docker", "logs", SYSTEM_SPEC[system]["leader"]], check=False)
    text = done.stdout + done.stderr
    # Kafka's broker log is long and routine; keep its tail. VTOP's leader log
    # is where gateway refusals are named, so keep it whole.
    if system == "kafka":
        text = "\n".join(text.splitlines()[-400:])
    _save_output(logs, f"{tag}-node1.log", text)


def _observe_kafka(ctx: dict, profile: str = PROFILE) -> None:
    bootstrap = SYSTEM_SPEC["kafka"]["bootstrap"]
    topic = client("kafka", ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", bootstrap, "--describe",
                             "--topic", TOPIC], timeout=120, check=False).stdout.strip()
    broker = client("kafka", ["/opt/kafka/bin/kafka-configs.sh", "--bootstrap-server", bootstrap, "--describe",
                              "--all", "--entity-type", "brokers", "--entity-name", "1"],
                    timeout=120, check=False).stdout
    wanted = re.compile(r"^\s+(log\.flush\.[a-z.]+|flush\.[a-z.]+|min\.insync\.replicas|default\.replication\.factor|"
                        r"replica\.lag\.time\.max\.ms|num\.(network|io|replica\.fetchers)[a-z.]*|"
                        r"log\.segment\.bytes|message\.max\.bytes|unclean\.leader\.election\.enable|"
                        r"broker\.session\.timeout\.ms|group\.initial\.rebalance\.delay\.ms|"
                        r"socket\.[a-z.]+|replica\.fetch\.[a-z.]+|log\.message\.timestamp\.type)=(\S*)")
    settings = {}
    for line in broker.splitlines():
        match = wanted.match(line)
        if match:
            settings[match.group(1)] = match.group(3)
    suffix = "" if profile == PROFILE else f"_{profile}"
    ctx["env"]["observed"][f"kafka_topic{suffix}"] = topic
    ctx["env"]["observed"][f"kafka_broker_1_config{suffix}"] = settings


# ------------------------------------------------------------------------------- main


def ensure_vtop_image(rebuild: bool) -> str:
    """The image of HEAD's engine, built if absent or asked for.

    Refuses when the build inputs differ from HEAD: an existing image would be
    stale, and a rebuilt one would put uncommitted code under HEAD's tag and
    in the results as HEAD's commit. Commit (or stash) first.
    """
    dirty = dirty_build_inputs()
    if dirty:
        raise SystemExit(f"the engine's build inputs differ from HEAD ({'; '.join(dirty[:5])}): "
                         "commit or stash them, so the image benchmarked is the commit recorded")
    sha = git("rev-parse", "--short=12", "HEAD")
    image = f"vtop-bench:{sha}"
    exists = sh(["docker", "image", "inspect", image], check=False).returncode == 0
    if rebuild or not exists:
        log(f"building {image} from docker/Dockerfile (both binaries)")
        sh(["docker", "build", "-f", os.path.join(REPO, "docker", "Dockerfile"), "-t", image, REPO], timeout=3600)
    return image


def parse_sat_records(text: str, sizes: list[int]) -> dict[int, int]:
    """Records per saturation and consume run, for each of `sizes`.

    A size given twice is refused (which count would run is a guess). A size
    not in `sizes` is dropped, not refused, so `--sizes 100` works with the
    default table; either way the recorded matrix holds only what runs.
    """
    table = {}
    for item in text.split(","):
        size, _, count = item.partition(":")
        try:
            size_n, count_n = int(size), int(count)
        except ValueError:
            raise SystemExit(f"--sat-records entry {item!r} is not SIZE:COUNT") from None
        if count_n < 1:
            raise SystemExit(f"--sat-records gives {count_n} records for {size_n} B: must be positive")
        if size_n in table:
            raise SystemExit(f"--sat-records gives record size {size_n} B twice")
        table[size_n] = count_n
    missing = [size for size in sizes if size not in table]
    if missing:
        raise SystemExit(f"--sat-records names no count for record sizes {missing}")
    return {size: table[size] for size in sizes}


def parse_sizes(text: str) -> list[int]:
    try:
        sizes = [int(size) for size in text.split(",")]
    except ValueError:
        raise SystemExit(f"--sizes {text!r} is not a comma-separated list of byte counts") from None
    if any(size < 1 for size in sizes) or len(set(sizes)) != len(sizes):
        raise SystemExit(f"--sizes {text!r}: record sizes must be positive and distinct")
    return sizes


# One path component: a run id names a directory under results/, never a path.
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def check_run_id(run_id: str) -> str:
    if not _RUN_ID.match(run_id) or ".." in run_id:
        raise SystemExit(f"--run-id {run_id!r} must be one path component of letters, digits, '.', '_' or '-'")
    return run_id


def check_args(args: argparse.Namespace) -> tuple[list[int], dict[int, int]]:
    """Every matrix argument, refused up front before any Docker work."""
    sizes = parse_sizes(args.sizes)
    sat_records = parse_sat_records(args.sat_records, sizes)
    if args.reps < 1:
        raise SystemExit(f"--reps {args.reps}: at least one repetition is needed")
    for name, value in (("--fixed-seconds", args.fixed_seconds), ("--sat-timeout", args.sat_timeout)):
        if not math.isfinite(value) or value <= 0:
            raise SystemExit(f"{name} {value}: must be a positive, finite number of seconds")
    if args.smoke_records < 1:
        raise SystemExit(f"--smoke-records {args.smoke_records}: the smoke must read back at least one record")
    if args.run_id is not None:
        check_run_id(args.run_id)
    check_group_commit(args)
    return sizes, sat_records


def build_matrix(args: argparse.Namespace, sizes: list[int], sat_records: dict[int, int]) -> dict[str, Any]:
    """The matrix this invocation runs, as environment.json records it. The
    schema-2 axes say what the phase-1 shape does: one compression, one
    profile, Kafka-relative rates. Schema 4 adds the resource windows and the
    end-to-end stage (at the common rate, with Kafka also under `fsync-parity`),
    each switched off by its own flag."""
    load_pcts = list(stats.LOAD_FRACTIONS)
    matrix = {"record_sizes": sizes, "partitions": [1], "producers": [1], "compressions": ["none"],
              "acks": ["all"], "profile": PROFILE, "profiles": [PROFILE], "repetitions": args.reps,
              "load_pcts": load_pcts, "load_bases": {"kafka": load_pcts}, "consume": not args.no_consume,
              "sat_records": {str(k): v for k, v in sat_records.items()},
              "fixed_seconds": args.fixed_seconds, "warmup_fraction": 0.2, "smoke_records": args.smoke_records,
              "resources": not getattr(args, "no_resources", False)}
    if not getattr(args, "no_e2e", False):
        matrix["e2e"] = {"load_basis": "common", "load_pct": E2E_LOAD_PCT}
        if not getattr(args, "no_fsync_parity", False):
            matrix["e2e"]["extra_profiles"] = list(E2E_EXTRA_PROFILES)
    if getattr(args, "group_commit", None) is not None:
        # VTOP's `group_commit.enabled` is an axis of the matrix (#752): a run that asked for both
        # settings owes both, which `--check` and the report hold it to.
        matrix["group_commits"] = parse_group_commits(args.group_commit)
    return matrix


def preflight_clients(matrix: dict[str, Any], rebuild: bool = False) -> str | None:
    """Before the VTOP image is built or any cluster started: what the resource windows and the end-to-end
    consumer need from this host and from the pinned images, or a refusal that names the flag that turns it off.
    The end-to-end consumer's image is built here and its class run once in check mode, the way every
    repetition runs it. Returns that image, or None when the matrix has no end-to-end stage."""
    if matrix.get("resources"):
        try:
            resources.check_v2(sh(["docker", "run", "--rm", KAFKA_IMAGE, "cat",
                                   f"{resources.CGROUP_ROOT}/cgroup.controllers"], timeout=300).stdout)
        except (resources.CgroupRefused, RuntimeError, subprocess.TimeoutExpired) as exc:
            raise SystemExit(f"the CPU and bytes-written windows cannot be read on this host: {exc}. Fix it, or "
                             "run with --no-resources to leave metric 4 out.") from exc
    if not matrix.get("e2e"):
        return None
    try:
        image = ensure_e2e_image(rebuild)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"the end-to-end consumer's image (client/Dockerfile: {JDK_IMAGE.split('@')[0]} compiles "
                         f"E2eLatency.java onto {KAFKA_IMAGE.split('@')[0]}) could not be built: {exc}. Fix it, or "
                         "run with --no-e2e to leave metric 3 out.") from exc
    try:
        done = sh(["docker", "run", "--rm", image, *e2e_main_command("--check")], timeout=300)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"the end-to-end consumer cannot run in {image}: {exc}. Fix it, or run with --no-e2e "
                         "to leave metric 3 out.") from exc
    if done.stdout.strip() != "ok":
        raise SystemExit(f"the end-to-end consumer's check in {image} printed {done.stdout.strip()[:80]!r}, not ok. "
                         "Fix it, or run with --no-e2e to leave metric 3 out.")
    return image


def check_plan(matrix: dict[str, Any], max_hours: float) -> float:
    """The projected hours, or a refusal before any Docker work."""
    if not math.isfinite(max_hours) or max_hours <= 0:
        raise SystemExit(f"--max-hours {max_hours}: must be a positive, finite number of hours")
    try:
        plan.check_matrix(matrix)
        return plan.check_budget(matrix, max_hours)
    except plan.PlanRefused as exc:
        raise SystemExit(f"refused before any Docker work: {exc}") from None


def check(run_dir: str) -> int:
    """`--check`: what a run directory still lacks. 0 when every expected row
    is there once and nothing else is; 2 otherwise, listing the missing keys."""
    try:
        found = schema.check_run(run_dir)
        with open(os.path.join(run_dir, "environment.json")) as handle:
            state = json.load(handle).get("run_state")
    except schema.ReportRefused as exc:
        print(f"check refused: {exc}", file=sys.stderr)
        return 2
    if found.duplicates or found.extra:
        print(f"{len(found.duplicates)} duplicated and {len(found.extra)} unexpected row keys, "
              f"first {(found.duplicates + found.extra)[:3]}", file=sys.stderr)
        return 2
    present = len(found.expected) - len(found.missing)
    print(f"{run_dir}: {present} of {len(found.expected)} expected rows present; run_state {state}")
    if not found.missing and state != "complete":
        # Every row is there but the run never said it finished: the report
        # refuses it until environment.json says `complete` (review).
        print(f"every row is present but run_state is {state!r}; the report reads only a complete run",
              file=sys.stderr)
        return 2
    if found.missing:
        print(f"missing {len(found.missing)} (key {', '.join(schema.KEY)}):", file=sys.stderr)
        for key in found.missing:
            print("  " + " ".join(str(part) if part != "" else "-" for part in key), file=sys.stderr)
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--sizes", default="100,1024", help="record sizes in bytes")
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--sat-records", default="100:3000000,1024:1000000",
                        help="records per saturation run, per record size")
    parser.add_argument("--fixed-seconds", type=float, default=30.0, help="target length of a fixed-rate run")
    parser.add_argument("--smoke-records", type=int, default=10000)
    parser.add_argument("--sat-timeout", type=float, default=1800.0,
                        help="seconds before a saturation, consume or warm-up run is failed (a fixed-rate leg's "
                             "limit is 3 x --fixed-seconds + 60 s)")
    parser.add_argument("--no-consume", action="store_true")
    parser.add_argument("--no-e2e", action="store_true",
                        help="leave out the end-to-end latency stage (metric 3, and the profile's latency cost)")
    parser.add_argument("--no-fsync-parity", action="store_true",
                        help="end-to-end stage for both systems as shipped only, not Kafka under fsync-parity")
    parser.add_argument("--no-resources", action="store_true",
                        help="do not read the cgroup counters around each run (metric 4)")
    parser.add_argument("--rebuild", action="store_true", help="rebuild the vtop image even if it exists")
    parser.add_argument("--max-hours", type=float, default=30.0,
                        help="refuse a matrix projected to take longer, before any Docker work")
    parser.add_argument("--host-disk", default=None, metavar="TEXT",
                        help="name the disk behind Docker's data yourself, for a host whose probe cannot (an EBS "
                             "NVMe volume that reports no model); recorded as given, one line, no '|'")
    parser.add_argument("--check", metavar="RUN_DIR", default=None,
                        help="report which rows a run still lacks; exit 0 when complete, 2 otherwise")
    parser.add_argument("--group-commit", metavar="SETTINGS", default=None,
                        help="VTOP's group_commit.enabled, as a comma-separated list of off and/or on (#752): "
                             "VTOP runs once per setting, Kafka once. Refused when the engine at HEAD has no "
                             "`group_commit:` section. Left out, VTOP runs at the node's own default")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    checked_host_disk(args.host_disk)

    if args.check is not None:
        return check(args.check)
    sizes, sat_records = check_args(args)
    matrix = build_matrix(args, sizes, sat_records)
    settings, node_keys = check_group_commit(args)
    leader_base = _read(LEADER_CONFIG)
    group_commit_configs = {setting: leader_config(leader_base, setting, node_keys) for setting in settings}
    hours = check_plan(matrix, args.max_hours)
    log(f"{len(schema.expected_keys(matrix))} result rows, projected {hours:.1f} h at most, "
        f"{plan.on_time_hours(matrix):.1f} h if every fixed-rate leg is on time (--max-hours {args.max_hours:g})")
    if args.reps < 3:
        log(f"warning: {args.reps} repetitions is below #595 rule 7's minimum of 3")
    limits = check_equal_limits()
    for system in SYSTEM_SPEC:
        running = sh(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project=kc-{system}"]).stdout
        if running.strip():
            raise SystemExit(f"kc-{system} is already running; stop it first (one system at a time, rule 1)")
    disk = resolve_host_disk(args.host_disk)
    sh(["docker", "pull", "-q", KAFKA_IMAGE], timeout=1800)
    e2e_image = preflight_clients(matrix, args.rebuild)
    vtop_image = ensure_vtop_image(args.rebuild)

    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = os.path.join(HERE, "results", run_id)
    if os.path.exists(run_dir):
        raise SystemExit(f"{run_dir} exists; runs never overwrite each other")
    os.makedirs(run_dir)
    env = capture_environment(run_id, matrix, vtop_image, limits, disk, group_commit_configs)
    if settings:
        env["observed"]["vtop_group_commit_keys_accepted"] = sorted(node_keys)
    ctx = {"run_id": run_id, "run_dir": run_dir, "csv": os.path.join(run_dir, "results.csv"), "env": env,
           "fingerprint": schema.fingerprint(env), "vtop_image": vtop_image, "reps": args.reps,
           "sat_records": sat_records, "fixed_seconds": args.fixed_seconds,
           "smoke_records": args.smoke_records, "sat_timeout": args.sat_timeout, "consume": not args.no_consume,
           "group_commit_configs": group_commit_configs,
           "matrix": matrix, "resources": bool(matrix.get("resources")), "e2e": bool(matrix.get("e2e")),
           "closer": make_closer(), "saturation": {}, "hist_csv": os.path.join(run_dir, schema.E2E_HIST),
           "e2e_image": e2e_image}
    schema.write_header(ctx["csv"])
    if ctx["e2e"]:
        schema.write_header(ctx["hist_csv"], schema.E2E_HIST_COLUMNS)
    save_environment(run_dir, env)
    log(f"run {run_id} -> {run_dir} (fingerprint {ctx['fingerprint']})")
    try:
        for size in sizes:
            rates = run_cell(ctx, "kafka", size, None)
            save_environment(run_dir, env)
            for setting in settings or [""]:
                run_cell(ctx, "vtop", size, rates, group_commit=setting)
                save_environment(run_dir, env)
            if ctx["e2e"]:
                run_e2e_stage(ctx, size)
                save_environment(run_dir, env)
    except BaseException as exc:
        env["run_state"] = "interrupted"
        env["events"].append({"at": utc_now(), "event": f"run interrupted: {exc!r}"[:900]})
        save_environment(run_dir, env)
        for system in SYSTEM_SPEC:
            try:
                cluster_down(system, vtop_image)
            except Exception:  # best effort on the way out
                pass
        raise
    env["run_state"] = "complete"
    env["finished_at"] = utc_now()
    save_environment(run_dir, env)
    log(f"done: {ctx['csv']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
