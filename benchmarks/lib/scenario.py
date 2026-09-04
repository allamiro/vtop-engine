"""Scenario loading.

A scenario is a small, flat YAML document. We use PyYAML when available and
otherwise fall back to a minimal parser for the subset the bundled scenarios
use — flat `key: value` lines plus folded/literal block scalars — so the
framework runs with no third-party deps.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

# Defaults for every tunable knob. Scenario files override a subset.
DEFAULTS: dict[str, Any] = {
    "name": "scenario",
    "description": "",
    "volume": 1000,                 # number of input files
    "file_size": "small",           # small | medium | large | mixed
    "format": "jsonl",              # jsonl|csv|txt|cef|leef|syslog|mixed|binary
    "batch_max_records": 10000,
    "batch_max_bytes": 104857600,
    "batch_max_age_seconds": 60,
    "compression": "gzip",          # none | gzip | zstd
    "compression_level": 6,
    "checksum": "sha256",           # sha256 | blake3 | disabled (engine: sha256)
    "backend": "mock",              # mock | mock_fail | mock_limited | minio
    "duration_seconds": 0,          # 0 = drain once; >0 = sustained load for N s
    # How much work to add per cycle, as a multiple of the ORIGINAL volume.
    # The default re-seeds a quarter of it, which the engine drains within the
    # cycle — load, but never a deficit. Above ~1.0 the seeder outruns the
    # engine and a real backlog accumulates, which is the only condition the
    # sustained-backpressure hypotheses (#98) are about. See
    # scenarios/11-backpressure-soak.yaml.
    "backlog_multiplier": 0.25,
    # Seed WHILE the engine works, instead of between cycles.
    #
    # The serial loop cannot produce a backlog at any volume or multiplier:
    # `process-once` drains everything it can see before returning, and only
    # then is more work added, so the engine is caught up by construction at
    # the end of every cycle. That is why the original 1M rec/s framing of #98
    # would not have measured what it wanted either — the deficit came from
    # Kafka producing continuously, which is a property of the SOURCE, not of
    # the volume. A background seeder restores that property at any scale.
    "seed_concurrently": False,
    # Seconds between background seeding rounds, when seeding concurrently.
    "seed_interval_seconds": 1.0,
    "fault": "none",                # none | verify_fail | replay
    "sys_sample_interval": 1.0,     # seconds between system-metric samples
    "bucket": "telemetry-data",
    "endpoint_url": "",             # for backend=minio
    # "" = derive from the backend (true only for the lab minio backend, false
    # for everything else — see lib/engine.py). Provisioning a bucket against
    # a real endpoint is a scenario's explicit opt-in, never an inference.
    "create_bucket": "",
}


def reseed_count(volume: int, multiplier: float) -> int:
    """How many files to add per sustained-load cycle.

    Extracted from the runner so the arithmetic that decides whether a soak
    builds a backlog can be tested without running one. Always at least one
    file: a multiplier rounding to zero would silently turn a sustained-load
    scenario into a single-cycle drain, which looks like a passing run of
    something it never did (#98).
    """
    # ROUNDED, not truncated. `int()` floors, and a multiplier whose binary
    # representation lands a hair under the integer (1.15, 0.29) then seeds one
    # file fewer every round — permanently understating both bytes_seeded and
    # the very deficit this knob exists to create (review).
    return max(1, round(volume * multiplier))


@dataclass
class Scenario:
    values: dict[str, Any] = field(default_factory=dict)

    def __getattr__(self, key: str) -> Any:
        try:
            return self.values[key]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(key) from exc

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)


def _coerce(value: str) -> Any:
    v = value.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        return v[1:-1]
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(v)
    except ValueError:
        pass
    try:
        return float(v)
    except ValueError:
        pass
    return v


# The two YAML block-scalar styles, each bare or with a chomping indicator.
# The bundled scenarios write their descriptions as `>-` blocks, and the old
# line-at-a-time parser stored the literal ">-" and dropped the prose — while
# the module promised it could read every bundled scenario.
_BLOCK_INDICATORS = (">", ">-", ">+", "|", "|-", "|+")


def _fallback_parse(text: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        raw = lines[i]
        i += 1
        # Block-scalar detection looks at the RAW post-colon value, before any
        # comment stripping, and the block body is consumed verbatim: the "#98"
        # and "#102" in the descriptions are citations inside prose, and a
        # comment strip would truncate every line at the first one.
        if ":" in raw and not raw.lstrip().startswith("#"):
            key_part, _, val_part = raw.partition(":")
            indicator = val_part.strip()
            if indicator in _BLOCK_INDICATORS:
                key_indent = len(raw) - len(raw.lstrip())
                block: list[str] = []
                while i < len(lines):
                    nxt = lines[i]
                    if nxt.strip() and len(nxt) - len(nxt.lstrip()) <= key_indent:
                        break
                    block.append(nxt)
                    i += 1
                # A deliberate approximation of YAML folding, kept only as
                # large as the bundled scenarios need: folded (>) joins lines
                # with spaces, literal (|) with newlines, interior blank
                # lines are dropped rather than becoming paragraph breaks,
                # and every chomping variant (bare, -, +) is treated as
                # strip — trailing newlines never survive.
                joiner = " " if indicator.startswith(">") else "\n"
                out[key_part.strip()] = joiner.join(
                    ln.strip() for ln in block if ln.strip())
                # `i` already rests on the terminating line, so flat parsing
                # resumes there and keys after the block still parse.
                continue
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        out[key.strip()] = _coerce(val) if val.strip() else ""
    return out


def load_scenario(path: str) -> Scenario:
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    parsed: dict[str, Any]
    try:
        import yaml  # type: ignore
    except ImportError:
        # PyYAML not installed — use the minimal flat-subset parser.
        parsed = _fallback_parse(text)
    else:
        # PyYAML is available: a real syntax error should surface, not be
        # silently mis-read by the fallback parser.
        parsed = yaml.safe_load(text) or {}

    values = dict(DEFAULTS)
    values.update({k: v for k, v in parsed.items() if v is not None})
    # Derive the name from the FILE, not from `values`: DEFAULTS already carries
    # name="scenario", so checking values here was always truthy and this branch
    # was dead. Every unnamed scenario was therefore called "scenario", which
    # collides in run ids and makes matrix results indistinguishable.
    if not parsed.get("name"):
        values["name"] = os.path.splitext(os.path.basename(path))[0]
    return Scenario(values)
