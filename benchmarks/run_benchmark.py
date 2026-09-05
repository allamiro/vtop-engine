#!/usr/bin/env python3
"""Run a single benchmark scenario and collect metrics.

Usage:
  python3 benchmarks/run_benchmark.py benchmarks/scenarios/<scenario>.yaml \
      [--results-dir DIR] [--seed-dir DIR] [--keep-seed]

A generated seed directory is removed on exit unless --keep-seed is given.
A seed directory passed with --seed-dir is NEVER removed - it may contain
real data the benchmark did not create.

Outputs results/<run_id>/ with the six CSV files + summary.json + summary.md.
Never overwrites a prior run.
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import engine, seed, shaping  # noqa: E402
from lib.metrics import ResultsWriter, iso_now, new_run_id, percentile  # noqa: E402
from lib.scenario import load_scenario, reseed_count  # noqa: E402
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


def should_remove_seed_dir(seed_dir_is_ours: bool, keep_seed: bool) -> bool:
    """Whether the benchmark may recursively delete the seed directory.

    A directory the benchmark GENERATED is temporary scratch and is removed
    unless --keep-seed asks to inspect it. A directory supplied by the caller
    with --seed-dir is NEVER removed: it may hold real data the benchmark did
    not create, and `shutil.rmtree` on it would be silent data loss.
    """
    return seed_dir_is_ours and not keep_seed


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
    # The shape is judged HERE, before a seed byte exists (#403): a bad knob
    # fails the run before it costs anything, and a shaped scenario never
    # runs unshaped under its own name.
    shape = shaping.Shape.from_scenario(sc)
    if shape is not None:
        # The engine must go THROUGH the proxy the shape is on: an endpoint
        # override (VTOP_S3_ENDPOINT_URL outranks the scenario) would send it
        # around the toxics while the summary said shaped.
        shaping.require_endpoint_through_proxy(sc, engine.effective_endpoint(sc))
    run_id = new_run_id(sc.name)
    writer = ResultsWriter(results_root, run_id)
    print(f"[bench] scenario={sc.name} run_id={run_id}")
    print(f"[bench] results -> {writer.dir}")

    binary = engine.vtopctl_path(build_if_missing=True)

    # Only a seed directory the benchmark CREATED may be deleted afterwards.
    # A caller-supplied --seed-dir can point at real data, so it is never
    # recursively removed no matter what --keep-seed says.
    #
    # Ownership is decided by the SAME truthiness test that allocates the path,
    # so the two can never disagree: `--seed-dir ""` (easy to produce with
    # `--seed-dir "$UNSET_VAR"`) falls through to mkdtemp, and must therefore be
    # owned by us - otherwise we would create a directory and then leak it.
    seed_dir_is_ours = not args.seed_dir
    seed_dir = args.seed_dir or tempfile.mkdtemp(prefix=f"vtop-seed-{sc.name}-")
    work_dir = tempfile.mkdtemp(prefix="vtop-work-")
    state_db = os.path.join(tempfile.mkdtemp(prefix="vtop-state-"), "state.db")
    input_glob = os.path.join(seed_dir, "*")
    # Keep the engine config OUT of the seed glob.
    config_path = os.path.join(os.path.dirname(state_db), "_engine.yaml")
    engine.write_engine_config(sc, work_dir, state_db, input_glob, config_path,
                               key_prefix=run_id)

    start = time.time()
    start_iso = iso_now()
    batch_total_ms = []
    out_objects = 0
    out_bytes = 0
    in_bytes = 0
    success = failed = replayed = errors = 0
    comp_ratios = []
    files_seen = 0
    # Seeded bytes, tracked alongside the archived ones so the backlog is a
    # like-for-like subtraction. The first version compared seeded FILES with
    # `success`, which counts committed BATCHES — with small batches a run
    # archives more batches than it was ever given files, so the deficit came
    # out negative and clamped to a permanent zero. A metric that reads zero
    # whatever happens is worse than no metric.
    seeded_bytes = 0
    seed_lock = threading.Lock()

    def emit_sys(sample):
        row = {"run_id": run_id}
        row.update(sample)
        writer.row("system_metrics.csv", row)

    # The pipe is shaped for exactly the block the measurements come from,
    # and unshaped on every way out of it (#403).
    with SystemMonitor(emit_sys, interval=float(sc.get("sys_sample_interval", 1.0))), \
            shaping.shaped(sc, endpoint=engine.effective_endpoint(sc)):
        # initial seed
        # A --seed-dir the caller supplied may already hold input. Those bytes
        # reach `bytes_archived`, so they must reach `bytes_seeded` too or the
        # deficit is computed between two different populations and clamps to
        # zero (review).
        for existing in glob.glob(os.path.join(seed_dir, "*")):
            if os.path.isfile(existing):
                seeded_bytes += os.path.getsize(existing)
                files_seen += 1
        if files_seen:
            print(f"[bench] seed dir already held {files_seen} file(s); counted "
                  "into the seeded baseline")
        totals = seed.generate_dataset(seed_dir, sc.format, int(sc.volume), sc.file_size)
        files_seen += totals["files"]
        seeded_bytes += totals["bytes"]
        print(f"[bench] seeded {totals['files']} files ({totals['bytes']} bytes) "
              f"format={sc.format} size={sc.file_size}")

        duration = float(sc.get("duration_seconds", 0) or 0)
        cycle = 0

        # THE SEEDER RUNS BESIDE THE ENGINE, not between its cycles.
        #
        # `process-once` drains everything it can see before returning, so
        # work added after it returns is work the engine was never behind on:
        # serially, the deficit is zero at the end of every cycle by
        # construction, at any volume and any multiplier. Kafka's producers do
        # not wait, and that — not the record rate — is what put the engine
        # hopelessly behind in #98. A thread seeding on a wall-clock interval
        # restores the property at whatever scale the disk can afford.
        stop_seeding = threading.Event()
        seeder = None
        seeder_error: list[BaseException] = []
        if duration > 0 and sc.get("seed_concurrently", False):
            per_round = reseed_count(int(sc.volume),
                                     float(sc.get("backlog_multiplier", 0.25)))
            interval = float(sc.get("seed_interval_seconds", 1.0))
            # REFUSED, not clamped: a non-positive interval turns the seeder
            # into a tight loop that fills the disk instead of pacing anything,
            # and a scenario asking for it has a mistake in it that a silent
            # correction would hide (review).
            if interval <= 0:
                print("[bench] seed_interval_seconds must be > 0 when seeding "
                      f"concurrently; got {interval}", file=sys.stderr)
                return 2
            # Whole-file formats are refused for the same reason a partially
            # written file is not a record: the seeder writes to the final path,
            # so the engine can discover and commit a half-written object and
            # then archive the rest as a second one. Line formats tolerate this
            # because the reader stops at the last newline (review).
            if sc.get("whole_file", False) or sc.format == "binary":
                print("[bench] seed_concurrently cannot be used with whole-file "
                      "input: the engine may commit a partially written file",
                      file=sys.stderr)
                return 2

            def _seed_loop():
                nonlocal files_seen, seeded_bytes
                round_no = 0
                try:
                    while not stop_seeding.wait(interval):
                        round_no += 1
                        # ITS OWN NAMESPACE. Sharing one across rounds makes
                        # filename collisions likely over a long soak, and a
                        # collision truncates a file the engine already holds a
                        # cursor for — silent loss inside the measurement.
                        more = seed.generate_dataset(seed_dir, sc.format, per_round,
                                                     sc.file_size, seed=10_000 + round_no,
                                                     prefix=f"evt{round_no}")
                        with seed_lock:
                            files_seen += more["files"]
                            seeded_bytes += more["bytes"]
                except BaseException as exc:  # noqa: BLE001 - reported, not swallowed
                    seeder_error.append(exc)

            seeder = threading.Thread(target=_seed_loop, name="seeder", daemon=True)
            seeder.start()
            print(f"[bench] seeding concurrently: {per_round} files every {interval}s")
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
                    # Count input bytes only for committed batches so failed and
                    # re-read (sustained-mode) batches don't inflate throughput.
                    in_bytes += ubytes
                    batch_total_ms.append(total_ms)
                    if m.get("compression_ratio"):
                        comp_ratios.append(m["compression_ratio"])
                elif state == "failed":
                    failed += 1
                    cycle_fail += 1

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
                # THE DEFICIT, recorded per cycle. Lag is the observable the
                # sustained-backpressure hypotheses (#98) are read off: whether
                # it plateaus or climbs is the difference between an engine
                # that holds a steady deficit and one that degrades as it falls
                # behind. Sampling it per cycle is what makes that a shape
                # rather than a single end-of-run number.
                with seed_lock:
                    seeded_now, files_now = seeded_bytes, files_seen
                writer.row("backlog_metrics.csv", {
                    "run_id": run_id, "cycle": cycle,
                    "elapsed_seconds": round(elapsed, 3),
                    "files_seeded": files_now,
                    "bytes_seeded": seeded_now,
                    "bytes_archived": in_bytes,
                    "backlog_bytes": max(0, seeded_now - in_bytes),
                    "cycle_batches": produced,
                })
                if elapsed >= duration:
                    break
                if seeder is not None:
                    # The seeder is already supplying work; adding more here
                    # would double-count the rate the scenario asked for. But
                    # a cycle that saw nothing must WAIT for the next round
                    # rather than immediately launching another `process-once`
                    # — a 300-second soak would otherwise spin subprocesses and
                    # bill their startup to the engine's CPU (review).
                    if produced == 0:
                        time.sleep(float(sc.get("seed_interval_seconds", 1.0)))
                    continue
                # SUSTAIN, and optionally OUTRUN. The re-seed happens after the
                # cycle drained, so at the default multiplier the engine is
                # never actually behind — it is caught up by construction, and
                # no amount of volume changes that. A multiplier above what a
                # cycle can drain is what creates a real backlog, which is the
                # condition the hypotheses need and the reason this knob exists.
                per_cycle = reseed_count(int(sc.volume),
                                         float(sc.get("backlog_multiplier", 0.25)))
                more = seed.generate_dataset(seed_dir, sc.format,
                                             per_cycle, sc.file_size,
                                             seed=cycle + 1)
                files_seen += more["files"]
                seeded_bytes += more["bytes"]
            else:
                # Drained, or no forward progress (e.g. mock_fail keeps failing
                # the same files since nothing commits) — stop and let replay run.
                if produced == 0:
                    break
                if cycle_success == 0 and cycle_fail > 0:
                    break
                if cycle > 100000:  # safety backstop
                    break

        # THE SEEDER STOPS BEFORE ANYTHING ELSE IS MEASURED. It used to be
        # signalled after replay, so a run that replayed kept accumulating
        # post-window files and inflated both total_seeded_bytes and the
        # backlog (review). Joined without a timeout, too: a bounded join left
        # a daemon thread writing into a seed directory the cleanup was about
        # to remove.
        stop_seeding.set()
        if seeder is not None:
            seeder.join()
        if seeder_error:
            # Reported, not swallowed. A run whose load generation died
            # half-way is not a shorter run, it is a different experiment, and
            # returning success would file it as the one that was asked for.
            print(f"[bench] concurrent seeding failed: {seeder_error[0]}",
                  file=sys.stderr)
            return 3

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


    # --- the ledger, and what it costs to open (#98 hypotheses 2 and 3) -----
    # Both are about BATCH count rather than record count, which is why they
    # are testable at small scale: every batch writes its transitions, so a
    # scenario with a low batch_max_records produces more ledger rows per byte
    # than a flood does. #77 notes startup loads the whole ledger into memory,
    # so its size is an operational limit rather than a curiosity.
    ledger_bytes = 0
    ledger_rows = 0
    try:
        ledger_bytes = os.path.getsize(state_db)
        con = sqlite3.connect(state_db)
        try:
            for (table,) in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                ledger_rows += con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        finally:
            con.close()
    except (OSError, sqlite3.Error):
        # A missing or unreadable ledger is reported as zero rather than
        # aborting a run whose real measurements already succeeded.
        pass

    # RECOVERY, timed against the ledger the soak just built — and against an
    # EMPTY INPUT DIRECTORY, which is the whole point.
    #
    # The first version pointed `process-once` at the real input glob. That
    # command runs recover() and then a full source cycle, so in any run that
    # ended with a backlog — the intended state of this scenario — it ingested
    # and uploaded the leftovers and billed all of it to `recovery_ms`, while
    # mutating the ledger it had just measured and discarding the outcomes
    # (review). It measured the opposite of what it claimed: the more backlog
    # the soak built, the less the number had to do with recovery.
    #
    # A second config over the SAME state store, the SAME run_id object
    # prefix, and an empty input directory isolates it: the recovery pass
    # opens the very ledger world the soak built, with nothing to process.
    recovery_ms = 0
    if ledger_rows:
        empty_dir = tempfile.mkdtemp(prefix="vtop-recovery-empty-")
        recovery_config = os.path.join(os.path.dirname(state_db), "_recovery.yaml")
        engine.write_engine_config(
            sc, work_dir, state_db, os.path.join(empty_dir, "*"), recovery_config,
            key_prefix=run_id)
        t0 = time.time()
        engine.process_once(binary, recovery_config, sc)
        recovery_ms = int((time.time() - t0) * 1000)
        shutil.rmtree(empty_dir, ignore_errors=True)

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
        "ledger_bytes": ledger_bytes,
        "ledger_rows": ledger_rows,
        "ledger_bytes_per_batch": round(ledger_bytes / out_objects, 1) if out_objects else 0,
        "recovery_ms": recovery_ms,
        "final_backlog_bytes": max(0, seeded_bytes - in_bytes),
        "total_seeded_bytes": seeded_bytes,
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
        # The pipe the numbers were measured through, or None (#403): a p95
        # is never read without it.
        "shaping": shape.describe() if shape else None,
    }
    # CPU/mem summary from the system-metrics samples written during the run.
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

    if should_remove_seed_dir(seed_dir_is_ours, args.keep_seed):
        shutil.rmtree(seed_dir, ignore_errors=True)
    elif not seed_dir_is_ours:
        print(f"[bench] leaving caller-supplied seed dir untouched: {seed_dir}")
    shutil.rmtree(work_dir, ignore_errors=True)
    # The state dir is mkdtemp'd exactly like work_dir and was the one of the
    # three the cleanup forgot — every run leaked a vtop-state-* directory
    # into the temp dir (found as three strays after a three-run grid).
    shutil.rmtree(os.path.dirname(state_db), ignore_errors=True)

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
    def avg(xs):
        return round(sum(xs) / len(xs), 2) if xs else 0

    def mx(xs):
        return round(max(xs), 2) if xs else 0

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
    try:
        raise SystemExit(main())
    except shaping.ShapingError as exc:
        # The pipe could not be shaped (#403): one line naming what to start,
        # and a non-zero exit — not a traceback for a stack that is not up.
        print(f"[bench] {exc}", file=sys.stderr)
        raise SystemExit(2) from None
