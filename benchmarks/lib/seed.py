"""Seed-data generation for benchmarks.

Generates N input files of a chosen format and size class. Pure Python (fast,
no subprocess), so it scales to many files. Source names/paths are never
hardcoded — the caller chooses the output directory.
"""
from __future__ import annotations

import json
import os
import random
import string
from collections.abc import Callable

# Size classes in bytes (min, max).
SIZE_CLASSES: dict[str, tuple[int, int]] = {
    "small": (1 * 1024, 64 * 1024),
    "medium": (1 * 1024 * 1024, 10 * 1024 * 1024),
    "large": (100 * 1024 * 1024, 1024 * 1024 * 1024),
}

VENDORS = ["VTOP", "Acme", "Globex", "Initech", "Umbrella"]
PRODUCTS = ["Engine", "Gateway", "Sensor", "Proxy", "Firewall"]
EVENTS = ["login", "logout", "file_access", "firewall_deny", "priv_esc",
          "malware", "port_scan", "dns_query", "config_change"]
USERS = ["alice", "bob", "carol", "dave", "eve", "root", "svc-account"]
ACTIONS = ["allow", "deny", "block", "login", "sudo", "read", "write"]
OUTCOMES = ["success", "failure", "unknown"]


def _ip() -> str:
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"


def _ts() -> str:
    return (
        f"2026-06-18T{random.randint(0, 23):02d}"
        f":{random.randint(0, 59):02d}"
        f":{random.randint(0, 59):02d}Z"
    )


def line_jsonl() -> str:
    return json.dumps({
        "ts": _ts(), "event": random.choice(EVENTS), "user": random.choice(USERS),
        "src": _ip(), "dst": _ip(), "port": random.randint(0, 65535),
        "action": random.choice(ACTIONS), "severity": random.randint(1, 10),
        "outcome": random.choice(OUTCOMES), "bytes": random.randint(0, 1_000_000),
    })


def line_csv() -> str:
    return ",".join([_ts(), random.choice(EVENTS), random.choice(USERS), _ip(),
                     _ip(), str(random.randint(0, 65535)), random.choice(ACTIONS),
                     str(random.randint(1, 10)), random.choice(OUTCOMES)])


def line_txt() -> str:
    return f"{_ts()} [{random.choice(['INFO','WARN','ERROR'])}] {random.choice(EVENTS)} by {random.choice(USERS)} from {_ip()} -> {random.choice(OUTCOMES)}"


def line_cef() -> str:
    return (f"CEF:0|{random.choice(VENDORS)}|{random.choice(PRODUCTS)}|1.0|"
            f"{random.randint(100,999)}|{random.choice(EVENTS)}|{random.randint(1,10)}|"
            f"src={_ip()} dst={_ip()} suser={random.choice(USERS)} act={random.choice(ACTIONS)} outcome={random.choice(OUTCOMES)}")


def line_leef() -> str:
    return (f"<{random.randint(1,191)}>{_ts()} host LEEF:1.0|{random.choice(VENDORS)}|"
            f"{random.choice(PRODUCTS)}|1.0|{random.choice(EVENTS)}|src={_ip()}\tdst={_ip()}\t"
            f"usrName={random.choice(USERS)}\tsev={random.randint(1,10)}\toutcome={random.choice(OUTCOMES)}")


def line_syslog() -> str:
    return (f"<{random.randint(1,191)}>1 {_ts()} host {random.choice(PRODUCTS)} {random.randint(1,9999)} - - "
            f"event={random.choice(EVENTS)} user={random.choice(USERS)} src={_ip()} outcome={random.choice(OUTCOMES)}")


LINE_GENERATORS: dict[str, Callable[[], str]] = {
    "jsonl": line_jsonl, "json": line_jsonl, "csv": line_csv, "txt": line_txt,
    "log": line_txt, "cef": line_cef, "leef": line_leef, "syslog": line_syslog,
}


def _ext(fmt: str) -> str:
    return {"jsonl": "jsonl", "json": "json", "csv": "csv", "txt": "log",
            "log": "log", "cef": "cef", "leef": "leef", "syslog": "syslog",
            "mixed": "log", "binary": "bin"}.get(fmt, "log")


def _write_text_file(path: str, target_bytes: int, fmt: str) -> int:
    gens: list[Callable[[], str]]
    if fmt == "mixed":
        gens = [line_jsonl, line_csv, line_txt, line_cef, line_leef, line_syslog]
    else:
        gens = [LINE_GENERATORS.get(fmt, line_txt)]
    written = 0
    # "x", never "w" (#481, review): an existing path is refused, not
    # truncated. See generate_dataset.
    with open(path, "x", encoding="utf-8") as fh:
        while written < target_bytes:
            line = random.choice(gens)() + "\n"
            fh.write(line)
            written += len(line)
    return written


def _write_binary_file(path: str, target_bytes: int) -> int:
    # Note: the engine is line-oriented; binary is supported for I/O load
    # measurement but archives as raw and yields few records. See README.
    # Write in bounded chunks so a large (up to 1 GiB) file never allocates the
    # whole payload in memory at once.
    chunk = 8 * 1024 * 1024
    remaining = target_bytes
    with open(path, "xb") as fh:
        while remaining > 0:
            n = min(chunk, remaining)
            fh.write(os.urandom(n))
            remaining -= n
    return target_bytes


def _pick_size(size_class: str | int) -> int:
    # An exact size (#663): a workload of identical objects, e.g. the 20 MiB
    # objects the #483 baseline and scenario 24 measure, instead of a range.
    # A bool is an int to Python and is never a size.
    if isinstance(size_class, int) and not isinstance(size_class, bool):
        if size_class <= 0:
            raise ValueError(f"file_size must be a positive number of bytes, got {size_class}")
        return size_class
    if isinstance(size_class, str) and size_class.isdigit():
        return _pick_size(int(size_class))
    if size_class == "mixed":
        cls = random.choices(["small", "medium", "large"], weights=[70, 25, 5])[0]
    else:
        cls = size_class
    lo, hi = SIZE_CLASSES.get(cls, SIZE_CLASSES["small"])
    return random.randint(lo, hi)


def generate_dataset(out_dir: str, fmt: str, volume: int, size_class: str,
                     seed: int = 0, prefix: str = "evt") -> dict[str, int]:
    """Generate `volume` files into `out_dir`. Returns totals.

    `seed`: pass a NON-ZERO value for reproducible data (same bytes every run);
    the default `0` means "seed from OS entropy" (fresh, non-reproducible data).

    `prefix`: the filename namespace. Rounds of a sustained run MUST pass
    distinct prefixes. Names are `{prefix}-{index}-{4 random chars}`, so two
    rounds sharing a prefix draw from only 456,976 suffixes per index and
    collide readily over a long soak — and a collision opens the existing path
    with "w", truncating a file the engine has already recorded a cursor for.
    The engine keeps that path's committed byte offset, so the shorter
    replacement is skipped and its records never arrive: silent loss inside the
    very measurement meant to observe a backlog (review).

    A file is NEVER overwritten: it is created exclusively, and an existing
    path raises FileExistsError (#481, review). With a pinned `seed`, names
    repeat exactly from run to run, so a rerun into a caller-supplied
    --seed-dir would otherwise rewrite the previous run's files - or a
    caller's own data that happens to share a name - in a directory the
    runner promises to leave intact. The runner gives every run its own
    prefix, so a refusal here means two generations in one run asked for the
    same namespace, which is a bug to surface rather than paper over.

    ALL OR NOTHING (#481, review): if any file of the call fails - a name
    collision, a full disk, an interrupt - every file THIS call created is
    removed before the error propagates, so an aborted generation leaves no
    orphans for the next run to count as pre-existing input. Only this call's
    files are ever removed: each is created exclusively, so a path that
    existed beforehand fails at open and is never on the list. (Under
    concurrent seeding the engine may already have read an earlier file of
    the failed call; the runner treats a seeder failure as a failed run.)
    """
    random.seed(seed if seed else None)
    os.makedirs(out_dir, exist_ok=True)
    ext = _ext(fmt)
    total_bytes = 0
    pad = len(str(volume))
    created: list[str] = []
    try:
        for i in range(volume):
            name = f"{prefix}-{str(i).zfill(pad)}-{''.join(random.choices(string.ascii_lowercase, k=4))}.{ext}"
            path = os.path.join(out_dir, name)
            target = _pick_size(size_class)
            try:
                if fmt == "binary":
                    written = _write_binary_file(path, target)
                else:
                    written = _write_text_file(path, target, fmt)
            except FileExistsError:
                raise  # not ours: the exclusive open refused it
            except BaseException:
                created.append(path)  # ours if it exists: opened exclusively
                raise
            created.append(path)
            total_bytes += written
    except BaseException:
        for path in created:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        raise
    return {"files": volume, "bytes": total_bytes}
