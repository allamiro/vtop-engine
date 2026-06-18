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
from typing import Callable, Dict, List, Tuple

# Size classes in bytes (min, max).
SIZE_CLASSES: Dict[str, Tuple[int, int]] = {
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
    return "2026-06-18T%02d:%02d:%02dZ" % (
        random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))


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


LINE_GENERATORS: Dict[str, Callable[[], str]] = {
    "jsonl": line_jsonl, "json": line_jsonl, "csv": line_csv, "txt": line_txt,
    "log": line_txt, "cef": line_cef, "leef": line_leef, "syslog": line_syslog,
}


def _ext(fmt: str) -> str:
    return {"jsonl": "jsonl", "json": "json", "csv": "csv", "txt": "log",
            "log": "log", "cef": "cef", "leef": "leef", "syslog": "syslog",
            "mixed": "log", "binary": "bin"}.get(fmt, "log")


def _write_text_file(path: str, target_bytes: int, fmt: str) -> int:
    gens: List[Callable[[], str]]
    if fmt == "mixed":
        gens = [line_jsonl, line_csv, line_txt, line_cef, line_leef, line_syslog]
    else:
        gens = [LINE_GENERATORS.get(fmt, line_txt)]
    written = 0
    with open(path, "w", encoding="utf-8") as fh:
        while written < target_bytes:
            line = random.choice(gens)() + "\n"
            fh.write(line)
            written += len(line)
    return written


def _write_binary_file(path: str, target_bytes: int) -> int:
    # Note: the engine is line-oriented; binary is supported for I/O load
    # measurement but archives as raw and yields few records. See README.
    with open(path, "wb") as fh:
        fh.write(os.urandom(target_bytes))
    return target_bytes


def _pick_size(size_class: str) -> int:
    if size_class == "mixed":
        cls = random.choices(["small", "medium", "large"], weights=[70, 25, 5])[0]
    else:
        cls = size_class
    lo, hi = SIZE_CLASSES.get(cls, SIZE_CLASSES["small"])
    return random.randint(lo, hi)


def generate_dataset(out_dir: str, fmt: str, volume: int, size_class: str,
                     seed: int = 0) -> Dict[str, int]:
    """Generate `volume` files into `out_dir`. Returns totals."""
    random.seed(seed or None)
    os.makedirs(out_dir, exist_ok=True)
    ext = _ext(fmt)
    total_bytes = 0
    pad = len(str(volume))
    for i in range(volume):
        name = f"evt-{str(i).zfill(pad)}-{''.join(random.choices(string.ascii_lowercase, k=4))}.{ext}"
        path = os.path.join(out_dir, name)
        target = _pick_size(size_class)
        if fmt == "binary":
            total_bytes += _write_binary_file(path, target)
        else:
            total_bytes += _write_text_file(path, target, fmt)
    return {"files": volume, "bytes": total_bytes}
