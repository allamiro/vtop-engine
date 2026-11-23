"""The footer every Kafka-comparison image carries (#600), from one source.

#600 asks every image to name host CPU, RAM and disk, the Kafka version, the
VTOP commit, the client, the profile, the repetitions and the date. report.py,
failover_report.py and readme_charts.py all draw it from `footer_text`, which
reads only the run's environment.json, and all write the same text into the
image's metadata (`metadata`), because the glyphs are paths and a test or a
screen reader cannot read them. The Docker volumes' filesystem (inside the
Docker VM) is named beside the host's disk, since both decide what an fsync
costs.

The text is `label: value` segments joined by " | ", on two lines; `parse`
reads it back. A run recorded before the harness probed the host's disk
(schema 1: both committed runs) says the disk was not recorded; raw data is
never edited to fill it in.
"""
from __future__ import annotations

from typing import Any

# Every image's footer has a non-empty value for each of these labels.
REQUIRED = ("host", "RAM", "disk", "Kafka", "VTOP", "client", "profile", "repetitions", "date")

CAVEAT = "single host, indicative, not a production benchmark"
SEPARATOR = " | "


def gib(value: Any) -> str:
    """Bytes as whole GiB, or `unknown` when the run could not read them
    (e.g. `host_memory_bytes` is empty on a host without the macOS sysctl)."""
    try:
        size = int(value)
    except (TypeError, ValueError):
        return "unknown"
    return f"{size / 2**30:.0f} GiB" if size > 0 else "unknown"


def disk(comparable: dict[str, Any]) -> str:
    recorded = comparable.get("host_disk")
    if recorded:
        return str(recorded)
    return "not recorded (the run predates the disk probe)"


def fields(env: dict[str, Any]) -> list[list[tuple[str, str]]]:
    """The footer's (label, value) pairs, line by line."""
    c = env["comparable"]
    m = env["matrix"]
    limits = c["node_limits"]
    failover = env.get("kind") == "failover"
    kafka = c["kafka_image"].split("@")[0] + (" (KRaft, 3 combined nodes)" if failover else " (KRaft, 3 brokers)")
    vtop = c["vtop_commit"][:12] + (" (3 co-located candidates)" if failover else " (3 nodes, Kafka gateway)")
    client = f"{c['client_kafka']} vs {c['client_vtop']}" if failover else c["client"]
    profile = c["profile"] + (" (shipped timeouts)" if failover and c["profile"] == "defaults" else "")
    first = [
        ("host", f"{c['host_cpu']}, {c['host_logical_cpus']} logical CPUs, {c['host_os']}"),
        ("RAM", gib(c.get("host_memory_bytes"))),
        ("disk", disk(c)),
        ("Docker", f"{c['docker_platform']}, VM {c['docker_vm_cpus']} CPUs / "
                   f"{gib(c.get('docker_vm_memory_bytes'))}, kernel {c['docker_vm_kernel']}, "
                   f"volumes {c['volume_filesystem']}"),
        ("per node", f"{limits['cpus']} CPUs, {limits['mem_limit']} RAM"),
    ]
    second = [("Kafka", kafka), ("VTOP", vtop), ("client", client), ("profile", profile)]
    # A run that varied VTOP's group commit (#752) says so; Kafka has no such setting.
    settings = list((c.get("vtop_group_commit") or {}).get("settings") or {})
    if settings:
        second.append(("group commit", f"{', '.join(settings)} (VTOP only)"))
    if failover:
        second.append(("offered", f"{m['offered_rate']:,} records/s"))
    second += [("repetitions", str(m["repetitions"])), ("run", env["run_id"]), ("date", env["started_at"][:10])]
    return [first, second]


def footer_text(env: dict[str, Any]) -> str:
    """The footer for any image drawn from the run whose environment.json is `env`.

    Refuses a value that holds the separator or a line break: read back, it would cut the
    value short or inject a label (the disk model and the client are free text)."""
    lines = fields(env)
    for line in lines:
        for label, value in line:
            if SEPARATOR.strip() in value or "\n" in value:
                raise ValueError(f"footer field {label!r} is {value!r}, which holds a separator or a line break")
    text = [SEPARATOR.join(f"{label}: {value}" for label, value in line) for line in lines]
    text[-1] += SEPARATOR + CAVEAT
    return "\n".join(text)


def parse(text: str) -> dict[str, str]:
    """{label: value} from a footer's text; segments with no label (the caveat) are skipped,
    and a label that appears twice is refused rather than the later one winning."""
    out = {}
    for line in text.split("\n"):
        for segment in line.split(SEPARATOR):
            label, sep, value = segment.partition(": ")
            if sep:
                if label.strip() in out:
                    raise ValueError(f"footer label {label.strip()!r} appears twice")
                out[label.strip()] = value.strip()
    return out


def wrap(text: str, width: int) -> str:
    """Line breaks between segments, never inside one that fits a line, so no word is left alone."""
    import textwrap

    out = []
    for line in text.split("\n"):
        current = ""
        for segment in line.split(SEPARATOR):
            joined = f"{current}{SEPARATOR}{segment}" if current else segment
            if len(joined) <= width or not current:
                current = joined
            else:
                out.append(current + SEPARATOR.rstrip())
                current = segment
        out.extend(textwrap.wrap(current, width) if len(current) > width else [current])
    return "\n".join(out)


def metadata(text: str, ext: str) -> dict[str, Any]:
    """savefig metadata for one format: the footer in the image's description, and nothing
    that changes from run to run (no date, no software banner)."""
    if ext == "svg":
        return {"Date": None, "Creator": None, "Description": text}
    if ext == "png":
        return {"Software": None, "Description": text}
    raise ValueError(f"no metadata rule for .{ext}")
