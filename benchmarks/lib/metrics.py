"""Metrics collection + summary generation.

Writes the six required CSV files plus summary.json / summary.md into
`results/<run_id>/`. Never overwrites a prior run (unique run_id; refuses an
existing directory).
"""
from __future__ import annotations

import csv
import json
import os
import random
import string
from datetime import datetime, timezone

CSV_HEADERS: dict[str, list[str]] = {
    "metrics.csv": [
        "run_id", "scenario_name", "start_time", "end_time", "duration_seconds",
        "total_input_files", "total_input_bytes", "total_output_objects",
        "total_output_bytes", "successful_files", "failed_files", "replayed_files",
        "throughput_files_per_sec", "throughput_mb_per_sec", "avg_latency_ms",
        "p50_latency_ms", "p95_latency_ms", "p99_latency_ms", "cpu_avg_percent",
        "cpu_max_percent", "memory_avg_mb", "memory_max_mb", "disk_read_mb",
        "disk_write_mb", "network_tx_mb", "network_rx_mb", "error_count",
    ],
    "batch_metrics.csv": [
        "run_id", "batch_id", "scenario_name", "batch_start_time", "batch_end_time",
        "batch_duration_ms", "input_files", "input_bytes", "compressed_bytes",
        "compression_ratio", "checksum_algorithm", "checksum_duration_ms",
        "upload_duration_ms", "manifest_upload_duration_ms", "verify_duration_ms",
        "total_batch_duration_ms", "batch_status", "error_message",
    ],
    "state_transition_metrics.csv": [
        "run_id", "batch_id", "file_id", "from_state", "to_state",
        "transition_time", "duration_since_previous_state_ms", "status",
        "error_message",
    ],
    "upload_metrics.csv": [
        "run_id", "batch_id", "object_key", "backend", "bucket",
        "object_size_bytes", "upload_start_time", "upload_end_time",
        "upload_duration_ms", "upload_speed_mb_per_sec", "retry_count", "status",
        "error_message",
    ],
    "replay_metrics.csv": [
        "run_id", "batch_id", "failed_state", "replay_start_time",
        "replay_end_time", "replay_duration_ms", "replay_attempt_number",
        "replay_success", "error_message",
    ],
    "system_metrics.csv": [
        "run_id", "timestamp", "cpu_percent", "memory_mb", "disk_read_mb",
        "disk_write_mb", "network_tx_mb", "network_rx_mb", "open_files",
        "active_threads", "queue_depth",
    ],
}


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_run_id(scenario_name: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rnd = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in scenario_name)
    return f"{safe}-{stamp}-{rnd}"


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return round(s[lo] + (s[hi] - s[lo]) * frac, 3)


class ResultsWriter:
    def __init__(self, results_root: str, run_id: str) -> None:
        self.run_id = run_id
        self.dir = os.path.join(results_root, run_id)
        if os.path.exists(self.dir):
            raise FileExistsError(f"results dir already exists (refusing to overwrite): {self.dir}")
        os.makedirs(self.dir)
        self._files = {}
        self._writers = {}
        for fname, header in CSV_HEADERS.items():
            fh = open(os.path.join(self.dir, fname), "w", newline="", encoding="utf-8")
            w = csv.writer(fh)
            w.writerow(header)
            self._files[fname] = fh
            self._writers[fname] = w

    def row(self, fname: str, data: dict) -> None:
        header = CSV_HEADERS[fname]
        self._writers[fname].writerow([data.get(col, "") for col in header])
        self._files[fname].flush()

    def write_summary(self, summary: dict) -> None:
        with open(os.path.join(self.dir, "summary.json"), "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, default=str)
        with open(os.path.join(self.dir, "summary.md"), "w", encoding="utf-8") as fh:
            fh.write(_summary_md(summary))

    def close(self) -> None:
        for fh in self._files.values():
            try:
                fh.close()
            except Exception:
                pass


def _md_cell(v) -> str:
    """Escape a value for safe inclusion in a Markdown table cell."""
    return (
        str(v)
        .replace("\\", "\\\\")
        .replace("|", "\\|")
        .replace("\n", " ")
        .replace("\r", " ")
    )


def _summary_md(s: dict) -> str:
    def g(k):
        return _md_cell(s.get(k, ""))
    lines = [
        f"# Benchmark summary — {g('scenario_name')}",
        "",
        f"- **Run ID:** `{g('run_id')}`",
        f"- **Start / end:** {g('start_time')} → {g('end_time')}",
        f"- **Duration:** {g('duration_seconds')} s",
        "",
        "## Scenario",
        "",
        "| Knob | Value |",
        "|------|-------|",
    ]
    for k in ("format", "file_size", "volume", "compression", "checksum",
              "backend", "batch_max_records", "batch_max_bytes",
              "batch_max_age_seconds", "duration_seconds", "fault"):
        lines.append(f"| {k} | {_md_cell(s.get('scenario', {}).get(k, ''))} |")
    lines += [
        "",
        "## Results",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Files processed | {g('total_input_files')} |",
        f"| Input bytes | {g('total_input_bytes')} |",
        f"| Output objects | {g('total_output_objects')} |",
        f"| Output bytes | {g('total_output_bytes')} |",
        f"| Successful / failed | {g('successful_files')} / {g('failed_files')} |",
        f"| Replayed | {g('replayed_files')} |",
        f"| Throughput | {g('throughput_files_per_sec')} files/s, {g('throughput_mb_per_sec')} MB/s |",
        f"| Compression ratio (avg) | {g('compression_ratio_avg')} |",
        f"| Avg batch duration | {g('avg_batch_duration_ms')} ms |",
        f"| Latency p50 / p95 / p99 | {g('p50_latency_ms')} / {g('p95_latency_ms')} / {g('p99_latency_ms')} ms |",
        f"| Errors | {g('error_count')} |",
        f"| Failed / successful batches | {g('failed_batches')} / {g('successful_batches')} |",
        f"| CPU avg / max | {g('cpu_avg_percent')}% / {g('cpu_max_percent')}% |",
        f"| Memory avg / max | {g('memory_avg_mb')} / {g('memory_max_mb')} MB |",
        f"| Upload backend | {g('backend')} |",
        "",
        "## Bottleneck observations",
        "",
        g("bottleneck_observations") or "_n/a_",
        "",
    ]
    return "\n".join(lines) + "\n"
