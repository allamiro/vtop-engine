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

from lib import competitor, engine, seed, shaping  # noqa: E402
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


def measured_nothing(success: int, failed: int, errors: int,
                     files_seeded: int = 0) -> bool:
    """Whether a run's engine produced no batch at all from real input (#499).

    Extracted so the refuse-loudly decision can be tested without driving a
    real engine. A run where nothing committed and nothing failed-at-verify
    measured nothing when EITHER the engine exited nonzero (config error) OR
    input was actually seeded yet no batch came out — the latter covers the
    case where every seeded file fails the adapter read, which the engine logs
    and skips while still exiting 0, so a nonzero subprocess exit cannot be
    required (review). Returning success in either case would file a non-run as
    a fast, all-zeros success (exactly how scenario 07 hid at HEAD). A genuinely
    empty run — no input seeded and no error — is NOT this case and must pass.
    """
    return success == 0 and failed == 0 and (errors > 0 or files_seeded > 0)


def uncounted_batches(rc: int, outcomes: list[dict]) -> int:
    """How many things one `process_once` did that its committed bytes cannot
    account for on the wire (#478, review).

    The engine's JSON carries a size only for a batch that reached VERIFIED; a
    batch that failed — including after its object was uploaded, at
    verification — carries `metrics: null`, and a call that exits nonzero may
    have uploaded any number of batches before it stopped and printed nothing.
    Each uncommitted batch counts once and a nonzero exit counts once more. An
    outcome with no `batch_id` is the engine's "nothing to read" marker and put
    nothing on the link, so it counts for nothing.

    Deliberately blind to WHICH stage a batch failed at: the output does not
    say, and a batch that failed at compression uploading nothing is
    indistinguishable here from one that failed at verify having uploaded
    everything. Unknown is counted as unknown.
    """
    uncommitted = sum(1 for o in outcomes if o.get("batch_id") and not o.get("committed"))
    return uncommitted + (1 if rc != 0 else 0)


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
    # The launch mode is judged with the same before-it-costs-anything
    # timing as the shape (#476): a typo'd runner_mode must refuse here,
    # not fall back to host and record a run as something it was not.
    try:
        mode = engine.runner_mode(sc)
    except ValueError as bad:
        print(f"[bench] {bad}", file=sys.stderr)
        return 2
    if mode == "container":
        print("[bench] runner_mode=container: the engine runs inside the "
              "compose stack (profile `containerized` must be up)")
    # The proxy hop is a TOPOLOGY claim (#484), judged with the same
    # before-it-costs-anything timing: a run that declares the h3 proxy and
    # then dials the store directly would still be filed as the
    # topology-controlled baseline a later transport comparison reads from,
    # and nothing in the numbers would give it away.
    try:
        engine.require_endpoint_through_h3_proxy(sc)
    except ValueError as bad:
        print(f"[bench] {bad}", file=sys.stderr)
        return 2
    if sc.get("h3_proxy"):
        print("[bench] h3_proxy: the upload path runs through the lab's "
              f"HTTP/3-terminating proxy at {engine.effective_endpoint(sc)} "
              "(profile `h3` must be up)")
    # The shape is judged HERE, before a seed byte exists (#403): a bad knob
    # fails the run before it costs anything, and a shaped scenario never
    # runs unshaped under its own name. Which SHAPER is the scenario's own
    # choice (#477); the dispatch answers for both.
    shape = shaping.shape_from_scenario(sc)
    # The flow beside the upload (#478), judged with the same
    # before-it-costs-anything timing as the shape — and BEFORE the calibration
    # probe below, because a typo in a scenario key should not first cost a
    # shape install and a thirteen-second probe. None means no competitor and
    # no columns.
    contention_spec = competitor.CompetitorSpec.from_scenario(sc, shape)
    emulator_validation_mbps = ""
    if shape is not None:
        # The engine must go THROUGH whatever is shaping it: an endpoint
        # override (VTOP_S3_ENDPOINT_URL outranks the scenario) would send it
        # around the toxics, or around the middlebox, while the summary said
        # shaped.
        shaping.require_endpoint_reaches_the_shape(sc, engine.effective_endpoint(sc))
        # And the emulator itself is measured before it is trusted (#477): an
        # emulator that does not produce the link it was configured for files
        # numbers against a link that does not exist. netem only — toxiproxy
        # shapes each connection inside the proxy, where an iperf3 run through
        # the same proxy would measure a different thing than the engine does.
        if getattr(shape, "driver", "") == "netem":
            from lib import netem
            emulator_validation_mbps = netem.calibrate(shape)
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
    # ABSOLUTE from the start (review): a relative --seed-dir means one
    # thing against the runner's cwd and another against the container's,
    # and the generated config carries whichever spelling it was given.
    if args.seed_dir:
        seed_dir = os.path.abspath(args.seed_dir)
    else:
        seed_dir = tempfile.mkdtemp(prefix=f"vtop-seed-{sc.name}-")
    work_dir = tempfile.mkdtemp(prefix="vtop-work-")
    state_db = os.path.join(tempfile.mkdtemp(prefix="vtop-state-"), "state.db")

    # EVERY WAY OUT removes the run's OWNED scratch, not only the ones that
    # reach the end of the run (review). A refusal that RAISES — two solo
    # windows that disagree, an iperf3 report that cannot be read, a contended
    # window with no whole engine cycle in it, a shape that would not install —
    # used to leave main() through the exception, past the cleanup at its
    # bottom, and leak the seed, work and state directories on every refused
    # run; so did the seeder's own failure, which returns early. The finally is
    # what makes a new refusal unable to forget any of the three. The
    # caller-supplied seed dir is still never deleted, and the writer's
    # directory stays as the record of the run, refused or not.
    try:
        return _measure(args, sc, mode, shape, contention_spec, emulator_validation_mbps,
                        run_id, writer, binary, seed_dir, work_dir, state_db)
    finally:
        writer.close()
        if should_remove_seed_dir(seed_dir_is_ours, args.keep_seed):
            shutil.rmtree(seed_dir, ignore_errors=True)
        elif not seed_dir_is_ours:
            print(f"[bench] leaving caller-supplied seed dir untouched: {seed_dir}")
        shutil.rmtree(work_dir, ignore_errors=True)
        # The state dir is mkdtemp'd exactly like work_dir and was the one of
        # the three the cleanup forgot — every run leaked a vtop-state-*
        # directory into the temp dir (found as three strays after a three-run
        # grid).
        shutil.rmtree(os.path.dirname(state_db), ignore_errors=True)


def _measure(args, sc, mode, shape, contention_spec, emulator_validation_mbps, run_id,
             writer, binary, seed_dir, work_dir, state_db) -> int:
    """Everything a run does once its scratch exists: the engine config, the
    preflight refusals, the measured block and the results.

    Split out of `main` only so `main` can hold the run's scratch in a
    try/finally around it. Returns the exit code; its refusals return or raise
    and leave the cleanup to that finally.
    """
    input_glob = os.path.join(seed_dir, "*")
    # Keep the engine config OUT of the seed glob.
    config_path = os.path.join(os.path.dirname(state_db), "_engine.yaml")
    engine.write_engine_config(sc, work_dir, state_db, input_glob, config_path,
                               key_prefix=run_id)

    if mode == "container":
        # The command backends need external CLI tools (mc, aws, s3cmd)
        # the hardened bench image does not carry (review): refuse them up
        # front with the in-process alternative named, rather than failing
        # every cycle on a missing binary.
        if sc.get("backend") in ("minio", "awscli", "s3cmd"):
            print(
                f"[bench] runner_mode=container cannot use backend "
                f"{sc.get('backend')!r}: it shells out to a CLI tool the "
                "engine container does not carry. Use s3_native (in-process "
                "S3) or localfs for container runs.",
                file=sys.stderr)
            return 2
        # Refused HERE, before a seed byte exists — the same
        # before-it-costs-anything timing as the shape and mode checks: a
        # mis-mounted run root otherwise soaks for the full duration
        # counting buried errors (#476, found live). The seed directory is
        # created first (review): a caller-supplied --seed-dir that does
        # not exist yet is the generator's to make, not the preflight's to
        # refuse. And a refusal cleans up what this run already made
        # (review): the writer's directory stays as the record of the
        # refused run, but the owned scratch does not leak.
        os.makedirs(seed_dir, exist_ok=True)
        problem = engine.preflight_container(config_path, seed_dir, binary, scenario=sc)
        if problem is not None:
            print(f"[bench] {problem}", file=sys.stderr)
            return 2

    start = time.time()
    start_iso = iso_now()
    # Read at the engine's two boundaries on a contended run; None elsewhere,
    # where the rows' own first and last samples bound the monitored period.
    counter_base = None
    counter_end = None
    batch_total_ms = []
    batch_upload_ms = []  # the store's share of each batch (#403)
    out_objects = 0
    out_bytes = 0
    in_bytes = 0
    success = failed = replayed = errors = 0
    # The first no-outcome cycle's stderr, kept so a run that measured nothing
    # can surface WHY in the results dir instead of leaving an all-zeros summary
    # with the reason on a discarded stderr (#499).
    engine_stderr = ""
    fail_stderr_set = False
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

    # Set only by a contended run, where the engine's window ends before the
    # closing solo one; None leaves the resource summary unbounded, which is
    # right for every other run because its window IS the monitored period.
    engine_window_end_iso = None
    harness_close_seconds = 0.0

    def emit_sys(sample):
        row = {"run_id": run_id}
        row.update(sample)
        writer.row("system_metrics.csv", row)

    # The pipe is shaped for exactly the block the measurements come from,
    # and unshaped on every way out of it (#403).
    # Resource accounting names the SAME subject in both modes (review):
    # container mode reads the engine container's own accounting, and host
    # mode is scoped to just the engine's process subtree rather than the
    # whole runner tree — the generator and the concurrent seeder are the
    # harness, not the measured program, and counting them made host and
    # container cpu/memory incomparable. The subtree filter names the
    # engine binary; a sample that cannot find it reads 0 rather than the
    # harness's load.
    # RESOLVED before its basename (review): psutil reads the executable
    # through Process.exe(), which follows a VTOPCTL_BIN symlink to its
    # target, so matching on the link's own basename would exclude every
    # engine process and report zero. realpath makes both sides name the
    # resolved file.
    engine_proc_name = os.path.basename(os.path.realpath(binary))
    # The competitor is entered LAST and left FIRST (#478), which is what puts
    # all three of its windows inside one installation of one shape: a shape
    # reinstalled between the brackets would measure how reproducible `apply()`
    # is rather than how steady the link was. It samples the engine's own
    # committed-bytes counter to get VTOP's share of the contended window;
    # reading an int under the GIL needs no lock, and the counter is the
    # runner's own accounting rather than a second measurement of the wire.
    with SystemMonitor(emit_sys, interval=float(sc.get("sys_sample_interval", 1.0)),
                       container="vtop-bench-engine" if mode == "container" else None,
                       proc_name=None if mode == "container" else engine_proc_name) as monitor, \
            shaping.shaped_run(sc, shape=shape,
                               endpoint=engine.effective_endpoint(sc)), \
            competitor.contended(contention_spec,
                                 vtop_bytes=lambda: out_bytes) as contention:
        # A CONTENDED RUN'S SOLO WINDOWS BELONG TO THE HARNESS, NOT TO THE
        # ENGINE (#478). The competitor measures the empty link twice — once as
        # this block opens, once as it closes — and the engine is deliberately
        # idle for both. Billing those seconds to the engine would understate
        # every throughput column by the length of a window in which it was
        # asked not to work, and the understatement would grow with the window
        # the scenario chose. So the clock restarts here, after the first one,
        # and `duration_seconds` subtracts the second below. An uncontended run
        # keeps the clock it has always been measured on.
        if contention is not None:
            start = time.time()
            start_iso = iso_now()
            # The counters' baseline is read AT the boundary, not inferred from
            # the last sample before it (review): with a two-second sampling
            # interval that row can predate the engine by two seconds of the
            # solo competitor's traffic.
            counter_base = monitor.counters_now()
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

        # THE CONTENDED WINDOW OPENS HERE (#478): after the seed exists, after
        # the seeder is running, and after the last refusal this block can
        # still make. Not at the top of the block — a competitor started before
        # there was anything to upload would spend its opening seconds alone on
        # the link and record the average as contended — and not before those
        # refusals, whose `return` would otherwise leave a window open with
        # nobody to close it. `close()` refuses if this was never reached.
        if contention is not None:
            contention.start_with_vtop()
        while True:
            # WHERE THE CYCLE BEGAN (#478, review). The engine's committed-byte
            # total moves in one step when this call returns, so the contended
            # window can only be charged with a cycle whose WHOLE interval it
            # contains — and that needs the near edge as well as the far one.
            # One clock read per cycle, taken unconditionally so the two edges
            # can never come from different passes of this line.
            cycle_started_at = time.monotonic()
            rc, outcomes, stderr = engine.process_once(binary, config_path, sc)
            # AND WHERE IT ENDED, read the instant the call returns (review).
            # Everything below until `note_progress` is the harness's own
            # bookkeeping — parsing outcomes, and three flushed CSV rows per
            # batch — during which the engine uploads nothing. Stamped any later,
            # that time is charged to the cycle: it can carry a cycle that
            # returned inside the contended window across its closing edge, and
            # it widens the span the attributed bytes are divided by.
            cycle_returned_at = time.monotonic()
            if not outcomes:
                if rc != 0:
                    errors += 1
                    # A GENUINE failure's diagnostic takes priority and wins
                    # over any provisional one (review): the first nonzero-exit
                    # cycle is the real reason the run went nowhere, so a benign
                    # non-empty stderr from an earlier idle poll must not bury
                    # it, and a later failure must not replace the first.
                    # Only a NON-EMPTY failure diagnostic latches, though: an
                    # empty one carries no reason, so it must neither lock out a
                    # later failure that DOES have a message nor erase a
                    # provisional rc-0 read-error message we are still holding
                    # (review). We keep counting the error either way.
                    if stderr and not fail_stderr_set:
                        engine_stderr = stderr
                        fail_stderr_set = True
                elif not fail_stderr_set and not engine_stderr and stderr:
                    # No genuine failure yet: hold the first non-empty stderr as
                    # a PROVISIONAL diagnostic, for the case where every seeded
                    # file fails the adapter read and the engine still exits 0
                    # (its read-error log is all we have). A real failure later
                    # overrides this; if none comes and it stays empty, the
                    # empty-file fallback below still records the refusal.
                    engine_stderr = stderr
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
                if up_ms:
                    batch_upload_ms.append(up_ms)
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
            # NOTED WHERE THE TOTAL HAS JUST MOVED (#478, review). The engine's
            # committed-byte total is only updated as a cycle's outcomes are
            # parsed, just above, so the cycle that closes here is the finest
            # grain VTOP's share of the contended window can be measured at.
            # Noted HERE because the total has only now moved; stamped with the
            # instant the call returned, because that is when the cycle ended.
            # Called after EVERY cycle and never conditionally: each cycle's
            # bytes are a difference against the last one counted, so a skipped
            # call would fold two cycles into one interval — likely to straddle
            # an edge of the window and be dropped from the share entirely.
            if contention is not None:
                contention.note_progress(cycle_started_at, cycle_returned_at,
                                         uncounted_batches(rc, outcomes))
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

        # THE BRACKET CLOSES HERE (#478): the engine's last work is done and
        # the shape is still installed, which is the only moment the second
        # solo window can measure the same link the first one did. Closing is
        # the block's own act rather than the context manager's exit, because
        # this block can also stop with a plain `return` — and a solo window
        # measured on the way out of a refusal would spend a minute on it and
        # could then raise a drift complaint that buries the reason the run
        # actually stopped.
        if contention is not None:
            # The engine's window ends HERE, before the closing solo one
            # (review). SystemMonitor is the outermost context, so it keeps
            # sampling through both solo windows — during which the engine is
            # deliberately idle and the competitor is saturating the link. Left
            # unbounded, cpu_avg_percent was diluted by two windows of
            # enforced idleness and the host-global network counters carried
            # the competitor's own traffic, while duration_seconds excluded
            # exactly those seconds. Recording the boundary lets the resource
            # summary cover the same interval the duration reports.
            engine_window_end_iso = iso_now()
            # And the counters are read AT it, as they are at the opening
            # boundary (review): the last sample before this line can be a
            # whole interval old, and filtering out every later row would drop
            # the engine's final second or two of disk traffic from the delta.
            counter_end = monitor.counters_now()
            # Everything close() spends is the harness's, and all of it is
            # timed (review): waiting out the flow's closing exchange and
            # docker exec's teardown, placing the window, and the closing solo
            # window. Subtracting only the solo window's own length left the
            # collection tail billed to an engine that had already stopped.
            close_started = time.time()
            contention.close()
            harness_close_seconds = time.time() - close_started


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
        # Its own finally, for the same reason main() holds the run's scratch
        # in one: a recovery pass that raises must not leak the directory.
        try:
            recovery_config = os.path.join(os.path.dirname(state_db), "_recovery.yaml")
            engine.write_engine_config(
                sc, work_dir, state_db, os.path.join(empty_dir, "*"), recovery_config,
                key_prefix=run_id)
            t0 = time.time()
            engine.process_once(binary, recovery_config, sc)
            recovery_ms = int((time.time() - t0) * 1000)
        finally:
            shutil.rmtree(empty_dir, ignore_errors=True)

    end = time.time()
    # Closing the contention ran with the engine already idle (#478) — the
    # competitor's collection tail, the window's placement and the second solo
    # window — so all of it is subtracted rather than billed to the engine: it
    # would otherwise divide every throughput column by seconds in which
    # nothing was uploaded. The first solo window is excluded by the clock
    # restarting inside the block. The ledger measurement and recovery pass
    # after the block stay in, as they do for every run, so a contended run and
    # an uncontended one are timed the same way. Zero for a run with no
    # competitor, whose duration is the one it has always had.
    duration_s = round(end - start - harness_close_seconds, 3)
    in_mb = in_bytes / 1e6
    # The wire the run used and its tuning (#479, #480), recorded together so a
    # number is read with both. Only s3_native routes through the seam; the
    # effective transport follows the engine's precedence (VTOP_S3_TRANSPORT
    # over the scenario value).
    if sc.get("backend") == "s3_native":
        eff_transport = (os.environ.get("VTOP_S3_TRANSPORT", "").strip()
                         or sc.get("transport", "tcp_tls"))
        transport_tuning = engine.resolved_transport_tuning(sc, eff_transport)
    else:
        eff_transport = ""
        transport_tuning = {}
    # Flat, for the comparison tables (review). The nested dict survives in
    # summary.json and disappears from matrix.csv and metrics.csv, so a matrix
    # that varies ONLY the tuning presented every row with identical visible
    # conditions — the reader cannot see the one thing the runs differ in.
    transport_tuning_flat = engine.format_transport_tuning(transport_tuning)
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
        # The upload leg alone (#403): the number a shaped pipe moves first,
        # and the raw signal #102's width controller consumes.
        "upload_p50_ms": percentile(batch_upload_ms, 50),
        "upload_p95_ms": percentile(batch_upload_ms, 95),
        "compression_ratio_avg": round(sum(comp_ratios) / len(comp_ratios), 3) if comp_ratios else 0,
        "error_count": errors, "failed_batches": failed, "successful_batches": success,
        "backend": sc.get("backend", "mock"),
        # The pipe the numbers were measured through, or None (#403): a p95
        # is never read without it.
        "shaping": shape.describe() if shape else None,
        # What the emulator actually produced, alone, through the shaped path
        # (#477). Blank for an unshaped run and for the toxiproxy driver;
        # recorded beside every netem number so a result is never read
        # without the evidence that its link was the configured one.
        "emulator_validation_mbps": emulator_validation_mbps,
        # What the run cost the flow beside it (#478): every phase, its own
        # window, and the arithmetic between them, so a reader can rebuild the
        # fairness index rather than take it. None when the scenario named no
        # competitor.
        "competitor": contention.describe() if contention else None,
        # And flat, for the CSV, the summary table and the matrix — blank when
        # there was no competitor, for the same reason the shaping columns are
        # blank rather than absent on an unshaped run.
        **(contention.flat_columns() if contention else competitor.blank_columns()),
        # And flat, for the CSV, the summary table and the matrix (review).
        # An UNSHAPED run states its columns blank rather than omitting them
        # (#477): the matrix fills a missing column from the scenario, and
        # every scenario carries the loader's default `shaping_driver:
        # toxiproxy` — so an omitted column made an unshaped baseline claim a
        # driver it never used, and the mixed-driver refusal then fired on the
        # one comparison it exists to permit. Blank is a measurement; absent
        # is a hole somebody else fills in.
        **(shape.flat_columns() if shape
           else {column: "" for column in shaping.SHAPING_COLUMNS}),
        # Which way the sender ran (#476), on every run including host-mode
        # ones: a container's veth and its bridge hop are part of the
        # measurement, and a number read without knowing the namespace is a
        # number compared against the wrong baseline.
        "runner_mode": engine.runner_mode(sc),
        # The wire that carried the bytes (#479): recorded so a throughput
        # number is never read without knowing its transport. Only the
        # s3_native backend routes through the EgressTransport seam, so the
        # value is blank for backends that use no such wire (mock, localfs, and
        # the command backends that shell out) rather than mislabeling a run
        # that no transport carried. The RESOLVED value is recorded, matching
        # the engine's own precedence (config_from_upload): a non-empty
        # VTOP_S3_TRANSPORT override outranks the scenario value, so recording
        # the scenario value would mislabel an overridden run.
        "transport": eff_transport,
        # The active transport's resolved egress tuning (#480), so a comparison
        # knows not just the wire but how it was tuned. Empty when no tuning is
        # configured or the backend routes through no seam.
        "transport_tuning": transport_tuning,
        "transport_tuning_flat": transport_tuning_flat,
        # Whether the lab's HTTP/3-terminating proxy sat between the engine
        # and the store (#484), by the name of the service that did. The
        # topology is part of the measurement exactly as the namespace and the
        # wire are: the proxied baseline exists precisely because it is NOT
        # comparable to the direct one, and a row that does not say which it
        # was invites the comparison it was built to prevent. The declaration
        # is what is recorded, and the runner has already refused any run whose
        # endpoint did not match it.
        "h3_proxy": engine.H3_PROXY_SERVICE if sc.get("h3_proxy") else "",
    }
    # CPU/mem summary from the system-metrics samples written during the run.
    # Bounded to the engine's own window on a contended run, so the resource
    # metrics correspond to duration_seconds and stay comparable with an
    # uncontended run's (#478, review). Unbounded otherwise: an ordinary run's
    # window IS the whole monitored period.
    summary.update(_sys_summary(writer.dir,
                                since_iso=start_iso if contention_spec else None,
                                until_iso=engine_window_end_iso,
                                counter_base=counter_base, counter_end=counter_end))
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

    # A run whose engine produced no batch from real input measured NOTHING
    # (#499): a config error (nonzero exit) OR every seeded file failing the
    # adapter read (logged, skipped, exit 0). Returning 0 with an all-zeros
    # summary files a non-run as a fast success — exactly how scenario 07 hid
    # at HEAD.
    refuse = measured_nothing(success, failed, errors, files_seen)

    # Surface the engine's own diagnostics in the results dir whenever there is
    # a diagnostic to surface or a refusal to explain (#499): the reason a run
    # went nowhere used to live only on a stderr the runner discarded, leaving
    # nothing on disk but a zero row. The file is written even for an empty
    # diagnostic so the refusal message below never points at a log that is not
    # there (review); an empty file honestly records that the engine said
    # nothing.
    stderr_log = os.path.join(writer.dir, "engine-stderr.log")
    if refuse or errors > 0:
        with open(stderr_log, "w", encoding="utf-8") as fh:
            fh.write(engine_stderr)

    print(f"[bench] done: {success} ok, {failed} failed, {replayed} replayed in {duration_s}s")
    print(f"[bench] summary: {os.path.join(writer.dir, 'summary.md')}")

    if refuse:
        print(f"[bench] REFUSED: the engine produced no batch from "
              f"{files_seen} seeded file(s) ({errors} error(s)); nothing was "
              f"measured. Engine stderr: {stderr_log}", file=sys.stderr)
        return 4
    return 0


def _sys_summary(result_dir, since_iso=None, until_iso=None, counter_base=None,
                 counter_end=None):
    """Summarise the system samples, optionally over one window only.

    A contended run's two solo windows belong to the harness, not to the engine
    (#478): it is deliberately idle for both while the competitor saturates the
    link, so samples from them dilute cpu_avg_percent and put the competitor's
    traffic into host-global network counters. `duration_seconds` already
    excludes those seconds; without the same bound here the resource columns
    described a different interval from the one beside them.

    The timestamps are the sampler's own ISO strings, which sort lexically, so
    the comparison needs no parsing. A row without one is kept: dropping a
    sample because its timestamp is missing would silently narrow the sample
    set on exactly the runs where the sampler is already misbehaving.
    """
    import csv as _csv
    cpu, mem, dr, dw, ntx, nrx = [], [], [], [], [], []
    path = os.path.join(result_dir, "system_metrics.csv")

    def in_window(row) -> bool:
        stamp = row.get("timestamp") or ""
        if not stamp:
            return True
        if since_iso and stamp < since_iso:
            return False
        return not (until_iso and stamp > until_iso)

    # The cumulative counters need REBASING, not just filtering (review).
    # SystemMonitor takes its baseline when the monitor starts — before the
    # first solo window — and every row after that is cumulative from it. So
    # dropping the pre-window rows leaves all of the first solo phase's disk
    # and network traffic embedded in the maxima that remain, even though the
    # CPU average is now correct. The last value seen BEFORE the window is
    # subtracted from the ones inside it, which rebases the counter on the
    # engine's own boundary. Gauges (cpu, memory) are instantaneous and are
    # filtered only.
    #
    # When the caller read the counters AT the boundary (`counter_base`, from
    # SystemMonitor.counters_now), that reading is the baseline and the rows
    # before the window only get filtered: the last row before the boundary can
    # be a whole sampling interval stale, and the traffic in that gap — the
    # solo competitor's, on a contended run — is exactly what the rebase is for.
    cumulative = ("disk_read_mb", "disk_write_mb", "network_tx_mb", "network_rx_mb")
    base = dict.fromkeys(cumulative, 0.0)
    if counter_base is not None:
        base.update({key: float(counter_base.get(key, 0.0)) for key in cumulative})
    try:
        with open(path) as fh:
            for r in _csv.DictReader(fh):
                if not in_window(r):
                    # Remember where the counters stood on the way in; a row
                    # AFTER the window cannot move the baseline backwards.
                    stamp = r.get("timestamp") or ""
                    if counter_base is None and (not since_iso or not stamp or stamp < since_iso):
                        for key in cumulative:
                            base[key] = float(r.get(key) or 0)
                    continue
                cpu.append(float(r.get("cpu_percent") or 0))
                mem.append(float(r.get("memory_mb") or 0))
                dr.append(max(0.0, float(r.get("disk_read_mb") or 0) - base["disk_read_mb"]))
                dw.append(max(0.0, float(r.get("disk_write_mb") or 0) - base["disk_write_mb"]))
                ntx.append(max(0.0, float(r.get("network_tx_mb") or 0) - base["network_tx_mb"]))
                nrx.append(max(0.0, float(r.get("network_rx_mb") or 0) - base["network_rx_mb"]))
    except FileNotFoundError:
        pass
    def avg(xs):
        return round(sum(xs) / len(xs), 2) if xs else 0

    def mx(xs):
        return round(max(xs), 2) if xs else 0

    # A reading taken at the closing boundary is the delta's end when there is
    # one (see counter_end at the call site); the rows can only under-state it.
    if counter_end is not None:
        for key, series in (("disk_read_mb", dr), ("disk_write_mb", dw),
                            ("network_tx_mb", ntx), ("network_rx_mb", nrx)):
            series.append(max(0.0, float(counter_end.get(key, 0.0)) - base[key]))

    summary = {
        "cpu_avg_percent": avg(cpu), "cpu_max_percent": mx(cpu),
        "memory_avg_mb": avg(mem), "memory_max_mb": mx(mem),
        "disk_read_mb": mx(dr), "disk_write_mb": mx(dw),
        "network_tx_mb": mx(ntx), "network_rx_mb": mx(nrx),
    }
    # WITHHELD, not rebased, on a contended run (review). psutil's network
    # counters are host-wide, and the engine's window contains the competing
    # flow BY DESIGN — about 75 MB over the bundled 60 s, 10 Mbit/s window — so
    # no boundary reading can separate the engine's bytes from the neighbour's.
    # A number that is mostly the competitor would sit in the engine's resource
    # columns and compare against uncontended runs as if it were the engine's.
    # Unknown is not zero: the columns are left empty and the reason recorded.
    if counter_base is not None:
        summary["network_tx_mb"] = ""
        summary["network_rx_mb"] = ""
        summary["network_counters_withheld"] = (
            "host-wide counters include the competing flow inside the engine's window")
    return summary


def _bottleneck(s):
    obs = []
    # What the run cost its neighbour, stated first (#478): it is the finding a
    # contended scenario exists to produce, and the one a reader of this line
    # is looking for. A MEASUREMENT and never a verdict — this issue builds the
    # instrument, and no threshold on it is a shipping gate yet, so the
    # sentence reports the share and stops.
    comp = s.get("competitor") or {}
    if comp.get("with_vtop"):
        # "in the same window" was a claim this run could not make (review):
        # VTOP's rate is over the engine cycles the window wholly contained, not
        # over the window. Both of the index's rates are over THAT span, so the
        # sentence names the span once and gives both numbers on it.
        obs.append(
            f"The competing flow kept {comp.get('share_of_solo_pct')}% of its solo goodput "
            f"while the engine ran ({comp['with_vtop'].get('goodput_mbps')} vs "
            f"{comp.get('solo_mean_mbps')} Mbit/s). Over the "
            f"{comp.get('jain_span_seconds')}s the index is taken on it held "
            f"{comp.get('competitor_goodput_mbps_over_vtop_window')} Mbit/s against VTOP's "
            f"{comp.get('vtop_goodput_mbps')} (Jain {comp.get('jain_index')}).")
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
