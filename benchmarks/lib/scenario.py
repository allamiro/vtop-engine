"""Scenario loading.

A scenario is a small, flat YAML document. We use PyYAML when available and
otherwise fall back to a minimal parser for the flat `key: value` subset used by
the bundled scenarios — so the framework runs with no third-party deps.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Dict

# Defaults for every tunable knob. Scenario files override a subset.
DEFAULTS: Dict[str, Any] = {
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
    "fault": "none",                # none | verify_fail | replay
    "sys_sample_interval": 1.0,     # seconds between system-metric samples
    "bucket": "telemetry-data",
    "endpoint_url": "",             # for backend=minio
}


@dataclass
class Scenario:
    values: Dict[str, Any] = field(default_factory=dict)

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


def _fallback_parse(text: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for raw in text.splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if ":" not in line:
            continue
        key, _, val = line.partition(":")
        out[key.strip()] = _coerce(val) if val.strip() else ""
    return out


def load_scenario(path: str) -> Scenario:
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    parsed: Dict[str, Any]
    try:
        import yaml  # type: ignore

        parsed = yaml.safe_load(text) or {}
    except Exception:
        parsed = _fallback_parse(text)

    values = dict(DEFAULTS)
    values.update({k: v for k, v in parsed.items() if v is not None})
    if not values.get("name"):
        values["name"] = os.path.splitext(os.path.basename(path))[0]
    return Scenario(values)
