#!/usr/bin/env python3
"""Run a single benchmark scenario and collect metrics.

Usage:
  python3 benchmarks/run_benchmark.py benchmarks/scenarios/<scenario>.yaml \
      [--results-dir DIR] [--seed-dir DIR] [--keep-seed]

Outputs results/<run_id>/ with the six CSV files + summary.json + summary.md.
Never overwrites a prior run.
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import engine, metrics, seed  # noqa: E402
from lib.metrics import ResultsWriter, iso_now, new_run_id, percentile  # noqa: E402
from lib.scenario import load_scenario  # noqa: E402
from lib.sysmon import SystemMonitor  # noqa: E402

STAGES = [
    ("batching", "sealed", None),
    ("sealed", "compressed", "compress_ms"),
    ("compressed", "checksummed", "checksum_ms"),
    ("checksummed", "object_uploaded", "object_upload_ms"),
    ("object_uploaded", "manifest_uploaded", "manifest_upload_ms"),
    ("manifest_uploaded", "verified", "verify_ms"),
    ("verified", "source_committed", "commit_ms"),
]
STATE_RANK = {s: i for i, s in enumerate(
    ["discovered", "batching", "sealed", "compressed", "checksummed",
     "object_uploaded", "manifest_uploaded", "verified", "source_committed"])}


def parse_bucket_key(uri):
    if not uri or not uri.startswith("s3://"):
        return "", ""
    rest = uri[5:]
    b, _, k = rest.partition("/")
    return b, k


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("scenario")
    ap.add_argument("--results-dir", default=None)
    ap.add_argument("--seed-dir", default=None)
    ap.add_argument("--keep-seed", action="store_true")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    results_root = args.results_dir or os.path.join(here, "results")
    os.makedirs(results_root, exist_ok=True)

    sc = load_scenario(args.scenario)
    run_id = new_run_id(sc.name)
    writer = ResultsWriter(results_root, run_id)
    print(f"[bench] scenario={sc.name} run_id={run_id}")
    print(f"[bench] results -> {writer.dir}")

    binary = engine.vtopctl_path(build_if_missing=True)

    seed_dir = args.seed_dir or tempfile.mkdtemp(prefix=f"vtop-seed-{sc.name}-")
    work_dir = tempfile.mkdtemp(prefix="vtop-work-")
    state_db = os.path.join(tempfile.mkdtemp(prefix="vtop-state-"), "state.db")
    input_glob = os.path.join(seed_dir, "*")
    # Keep the engine config OUT of the seed glob.
    config_path = os.path.join(os.path.dirname(state_db), "_engine.yaml")
    engine.write_engine_config(sc, work_dir, state_db, input_glob, config_path)

    start = time.time()
    start_iso = iso_now()
    batch_total_ms = []
    out_objects = 0
    out_bytes = 0
    in_bytes = 0
    success = failed = replayed = errors = 0
    comp_ratios = []
    files_seen = 0

    def emit_sys(sample):
        row = {"run_id": run_id}
        row.update(sample)
        writer.row("system_metrics.csv", row)

    with SystemMonitor(emit_sys, interval=float(sc.get("sys_sample_interval", 1.0))):
        # initial seed
        totals = seed.generate_dataset(seed_dir, sc.format, int(sc.volume), sc.file_size)
        files_seen += totals["files"]
        print(f"[bench] seeded {totals['files']} files ({totals['bytes']} bytes) "
              f"format={sc.format} size={sc.file_size}")

        duration = float(sc.get("duration_seconds", 0) or 0)
        cycle = 0
        while True:
            rc, outcomes, stderr = engine.process_once(binary, config_path, sc)
            if rc != 0 and not outcomes:
                errors += 1
            produced = 0
            cycle_success = 0
            cycle_fail = 0
            for o in outcomes:
                if not o.get("batch_id"):
                    continue
                produced += 1
                m = o.get("metrics") or {}
                state = o.get("final_state", "")
                status = "committed" if o.get("committed") else state
                bid = o["batch_id"]
                cbytes = m.get("compressed_bytes", 0)
                ubytes = m.get("uncompressed_bytes", 0)
                total_ms = m.get("total_ms", 0)
                if o.get("committed"):
                    success += 1
                    cycle_success += 1
                    out_objects += 1
                    out_bytes += cbytes
                    batch_total_ms.append(total_ms)
                    if m.get("compression_ratio"):
                        comp_ratios.append(m["compression_ratio"])
                elif state == "failed":
                    failed += 1
                    cycle_fail += 1
                in_bytes += ubytes

                writer.row("batch_metrics.csv", {
                    "run_id": run_id, "batch_id": bid, "scenario_name": sc.name,
                    "batch_start_time": "", "batch_end_time": "",
                    "batch_duration_ms": total_ms, "input_files": 1,
                    "input_bytes": ubytes, "compressed_bytes": cbytes,
                    "compression_ratio": m.get("compression_ratio", ""),
                    "checksum_algorithm": sc.get("checksum", "sha256"),
                    "checksum_duration_ms": m.get("checksum_ms", ""),
                    "upload_duration_ms": m.get("object_upload_ms", ""),
                    "manifest_upload_duration_ms": m.get("manifest_upload_ms", ""),
                    "verify_duration_ms": m.get("verify_ms", ""),
                    "total_batch_duration_ms": total_ms,
                    "batch_status": status,
                    "error_message": "" if o.get("committed") else stderr[:200],
                })

                # upload metrics
                b, k = parse_bucket_key(o.get("object_uri"))
                up_ms = m.get("object_upload_ms", 0) or 0
                speed = (cbytes / 1e6) / (up_ms / 1000.0) if up_ms else 0.0
                writer.row("upload_metrics.csv", {
                    "run_id": run_id, "batch_id": bid, "object_key": k,
                    "backend": sc.get("backend", "mock"), "bucket": b,
                    "object_size_bytes": cbytes, "upload_start_time": "",
                    "upload_end_time": "", "upload_duration_ms": up_ms,
                    "upload_speed_mb_per_sec": round(speed, 3), "retry_count": 0,
                    "status": status, "error_message": "",
                })

                # state transitions derived from per-stage timing
                reached = STATE_RANK.get(state, 0)
                for frm, to, key in STAGES:
                    if STATE_RANK.get(to, 99) > reached:
                        break
                    writer.row("state_transition_metrics.csv", {
                        "run_id": run_id, "batch_id": bid, "file_id": "",
                        "from_state": frm, "to_state": to,
                        "transition_time": iso_now(),
                        "duration_since_previous_state_ms": m.get(key, 0) if key else 0,
                        "status": "ok", "error_message": "",
                    })
                if state == "failed":
                    writer.row("state_transition_metrics.csv", {
                        "run_id": run_id, "batch_id": bid, "file_id": "",
                        "from_state": "batching", "to_state": "failed",
                        "transition_time": iso_now(),
                        "duration_since_previous_state_ms": 0,
                        "status": "failed", "error_message": stderr[:200],
                    })

            cycle += 1
            elapsed = time.time() - start
            if duration > 0:
                if elapsed >= duration:
                    break
                # sustain load: add more files
                more = seed.generate_dataset(seed_dir, sc.format,
                                             max(1, int(sc.volume) // 4), sc.file_size,
                                             seed=cycle + 1)
                files_seen += more["files"]
            else:
                # Drained, or no forward progress (e.g. mock_fail keeps failing
                # the same files since nothing commits) — stop and let replay run.
                if produced == 0:
                    break
                if cycle_success == 0 and cycle_fail > 0:
                    break
                if cycle > 100000:  # safety backstop
                    break

        # failure / replay measurement
        if failed > 0 or sc.get("fault") in ("verify_fail", "replay"):
            rstart = iso_now()
            t0 = time.time()
            rc, out = engine.replay(binary, config_path, sc)
            rms = int((time.time() - t0) * 1000)
            replayed = failed
            writer.row("replay_metrics.csv", {
                "run_id": run_id, "batch_id": "*", "failed_state": "failed",
                "replay_start_time": rstart, "replay_end_time": iso_now(),
                "replay_duration_ms": rms, "replay_attempt_number": 1,
                "replay_success": rc == 0, "error_message": "" if rc == 0 else out[:200],
            })

    end = time.time()
    duration_s = round(end - start, 3)
    in_mb = in_bytes / 1e6
    summary = {
        "run_id": run_id, "scenario_name": sc.name, "scenario": sc.values,
        "start_time": start_iso, "end_time": iso_now(),
        "duration_seconds": duration_s,
        "total_input_files": files_seen, "total_input_bytes": in_bytes,
        "total_output_objects": out_objects, "total_output_bytes": out_bytes,
        "successful_files": success, "failed_files": failed,
        "replayed_files": replayed,
        "throughput_files_per_sec": round(success / duration_s, 3) if duration_s else 0,
        "throughput_mb_per_sec": round(in_mb / duration_s, 3) if duration_s else 0,
        "avg_latency_ms": round(sum(batch_total_ms) / len(batch_total_ms), 3) if batch_total_ms else 0,
        "avg_batch_duration_ms": round(sum(batch_total_ms) / len(batch_total_ms), 3) if batch_total_ms else 0,
        "p50_latency_ms": percentile(batch_total_ms, 50),
        "p95_latency_ms": percentile(batch_total_ms, 95),
        "p99_latency_ms": percentile(batch_total_ms, 99),
        "compression_ratio_avg": round(sum(comp_ratios) / len(comp_ratios), 3) if comp_ratios else 0,
        "error_count": errors, "failed_batches": failed, "successful_batches": success,
        "backend": sc.get("backend", "mock"),
    }
    # CPU/mem summary from system samples
    cpu = [s["cpu_percent"] for s in []]  # filled below from writer? recompute
    summary.update(_sys_summary(writer.dir))
    summary["bottleneck_observations"] = _bottleneck(summary)

    writer.row("metrics.csv", {**summary,
                               "cpu_avg_percent": summary.get("cpu_avg_percent", 0),
                               "cpu_max_percent": summary.get("cpu_max_percent", 0),
                               "memory_avg_mb": summary.get("memory_avg_mb", 0),
                               "memory_max_mb": summary.get("memory_max_mb", 0),
                               "disk_read_mb": summary.get("disk_read_mb", 0),
                               "disk_write_mb": summary.get("disk_write_mb", 0),
                               "network_tx_mb": summary.get("network_tx_mb", 0),
                               "network_rx_mb": summary.get("network_rx_mb", 0)})
    writer.write_summary(summary)
    writer.close()

    if not args.keep_seed:
        shutil.rmtree(seed_dir, ignore_errors=True)
    shutil.rmtree(work_dir, ignore_errors=True)

    print(f"[bench] done: {success} ok, {failed} failed, {replayed} replayed in {duration_s}s")
    print(f"[bench] summary: {os.path.join(writer.dir, 'summary.md')}")
    return 0


def _sys_summary(result_dir):
    import csv as _csv
    cpu, mem, dr, dw, ntx, nrx = [], [], [], [], [], []
    path = os.path.join(result_dir, "system_metrics.csv")
    try:
        with open(path) as fh:
            for r in _csv.DictReader(fh):
                cpu.append(float(r.get("cpu_percent") or 0))
                mem.append(float(r.get("memory_mb") or 0))
                dr.append(float(r.get("disk_read_mb") or 0))
                dw.append(float(r.get("disk_write_mb") or 0))
                ntx.append(float(r.get("network_tx_mb") or 0))
                nrx.append(float(r.get("network_rx_mb") or 0))
    except FileNotFoundError:
        pass
    avg = lambda xs: round(sum(xs) / len(xs), 2) if xs else 0
    mx = lambda xs: round(max(xs), 2) if xs else 0
    return {
        "cpu_avg_percent": avg(cpu), "cpu_max_percent": mx(cpu),
        "memory_avg_mb": avg(mem), "memory_max_mb": mx(mem),
        "disk_read_mb": mx(dr), "disk_write_mb": mx(dw),
        "network_tx_mb": mx(ntx), "network_rx_mb": mx(nrx),
    }


def _bottleneck(s):
    obs = []
    if s.get("failed_batches"):
        obs.append(f"{s['failed_batches']} batches failed (fault injection / verification).")
    if s.get("compression_ratio_avg", 0) and s["compression_ratio_avg"] < 1.2:
        obs.append("Low compression ratio — data may be high-entropy or already compressed.")
    if s.get("cpu_max_percent", 0) > 90:
        obs.append("CPU-bound (max CPU > 90%).")
    if s.get("p99_latency_ms", 0) and s.get("avg_latency_ms", 0) and \
            s["p99_latency_ms"] > 3 * max(1.0, s["avg_latency_ms"]):
        obs.append("Tail latency (p99) >> average — investigate stragglers / GC / IO stalls.")
    return " ".join(obs) if obs else "No obvious bottleneck in this run."


if __name__ == "__main__":
    raise SystemExit(main())
