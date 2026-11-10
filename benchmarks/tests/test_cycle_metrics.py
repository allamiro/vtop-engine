"""Each `process-once` cycle is timed on the engine's own clock (#483).

The run-level throughput divides by a wall clock that also holds the seeding,
the ledger read and the recovery pass. A goodput compared against a link's
rate has to divide by the time the engine was actually working, and only the
cycles know it — so each one leaves a row saying when it started, how long the
call took and what it committed.
"""
import csv
import glob
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import run_benchmark  # noqa: E402

CYCLE_SECONDS = 0.3


def _scenario(tmp_path):
    path = tmp_path / "cycles.yaml"
    path.write_text(
        "name: cycle-timing\n"
        "backend: s3_native\n"
        "endpoint_url: http://localhost:9000\n"
        "volume: 2\n"
        "duration_seconds: 0\n"
        "sys_sample_interval: 0.1\n",
        encoding="utf-8",
    )
    return str(path)


def _committed(batch_id, compressed):
    return {"batch_id": batch_id, "committed": True, "final_state": "source_committed",
            "metrics": {"compressed_bytes": compressed, "uncompressed_bytes": compressed,
                        "total_ms": 1, "object_upload_ms": 1}}


def test_each_cycle_records_its_own_duration_and_committed_bytes(tmp_path, monkeypatch):
    calls = []

    def process_once(*_a, **_k):
        calls.append(1)
        if len(calls) == 1:
            time.sleep(CYCLE_SECONDS)
            return 0, [_committed("b1", 1000), _committed("b2", 234)], ""
        return 0, [], ""

    monkeypatch.setattr(run_benchmark.engine, "vtopctl_path", lambda *a, **k: "/bin/true")
    monkeypatch.setattr(run_benchmark.engine, "write_engine_config", lambda *a, **k: None)
    monkeypatch.setattr(run_benchmark.engine, "effective_endpoint", lambda *a, **k: "")
    monkeypatch.setattr(run_benchmark.engine, "process_once", process_once)
    monkeypatch.setattr(run_benchmark.seed, "generate_dataset",
                        lambda *a, **k: {"files": 2, "bytes": 1234})
    results = tmp_path / "results"
    monkeypatch.setattr(sys, "argv", ["run_benchmark.py", _scenario(tmp_path),
                                      "--results-dir", str(results)])

    assert run_benchmark.main() == 0
    (run_dir,) = glob.glob(str(results / "*"))
    with open(os.path.join(run_dir, "cycle_metrics.csv"), encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    # The drained cycle and the empty one after it that ends the loop.
    assert [r["cycle"] for r in rows] == ["1", "2"], rows
    first, second = rows
    assert first["committed_batches"] == "2" and first["committed_bytes"] == "1234", (
        f"the cycle that committed two batches of 1000 + 234 bytes recorded {first}")
    assert second["committed_batches"] == "0" and second["committed_bytes"] == "0", second
    # Timed around the call itself: at least the engine's own 0.3 s, and not
    # stretched by anything near another 0.3 s of harness work.
    assert CYCLE_SECONDS <= float(first["cycle_seconds"]) < 2 * CYCLE_SECONDS, (
        f"a {CYCLE_SECONDS}s process_once was recorded as {first['cycle_seconds']}s")
    assert float(second["started_seconds"]) >= (
        float(first["started_seconds"]) + float(first["cycle_seconds"])), (
        f"the second cycle cannot start before the first returned: {rows}")
