"""A SIGTERM ends a run the way Ctrl-C does (the v0.11.1 AWS round's lesson 5).

Only SIGINT reached the `except BaseException` in `run.py` and `crash.py` that writes `run_state: interrupted`,
records why and takes the clusters down. A SIGTERM (a stopped instance, a CI timeout, `kill`) ended the process
with `run_state: running` and the clusters left up. Each runner now installs a handler for the length of its
`main` that raises `run.Terminated`, a BaseException, so the same path runs; the signal is named in `events`.

Each test runs a stubbed runner in a subprocess (Docker replaced by `kafka_compare_stub`, or by a few lambdas
for crash.py), waits until it is inside a measurement, sends SIGTERM, and reads what it left on disk.
"""
import json
import os
import signal
import subprocess
import sys
import textwrap

import pytest

from kafka_compare import run

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM is a POSIX signal")

PRELUDE = f"""
import json, os, subprocess, sys, time
sys.path.insert(0, {BENCH!r})
sys.path.insert(0, {HERE!r})
os.environ["VTOP_BENCH_NO_IMDS"] = "1"
root = sys.argv[1]
def ready():
    print("READY", flush=True)
    time.sleep(120)
"""

RUN_SCRIPT = PRELUDE + textwrap.dedent("""
    import kafka_compare_stub as stub
    from kafka_compare import run
    downs = []
    calls = stub.install(run, root, hooks={"producer": lambda system, records, rate: rate > 0 and ready()})
    run.cluster_down = lambda system, image: downs.append(system)
    try:
        code = run.entry(stub.ARGS + ["--run-id", "sigterm"])
    finally:
        with open(os.path.join(root, "downs.json"), "w") as handle:
            json.dump(downs, handle)
    sys.exit(code)
""")

CRASH_SCRIPT = PRELUDE + textwrap.dedent("""
    from kafka_compare import crash, run
    crash.HERE = root
    downs = []

    class Host:
        def __init__(self, *args):
            pass
        def call(self, name, *args, **kwargs):
            return "flakey"

    class Backend:
        def __init__(self, ctx):
            pass
        def down(self, volumes=False):
            downs.append(volumes)

    def environment(run_id, matrix, *args):
        return {"schema_version": 1, "kind": "crash", "run_id": run_id, "run_state": "running", "started_at": "x",
                "matrix": matrix, "comparable": {"vtop_commit": "a" * 40}, "observed": {}, "events": []}

    crash.Host = Host
    crash.DockerBackend = Backend
    crash.capture_environment = environment
    crash.sh = lambda args, **kw: subprocess.CompletedProcess(args, 0, "", "")
    crash.ensure_image = lambda rebuild: "vtop-bench:stub"
    crash.ensure_client_image = lambda rebuild: "kc-crash-client:stub"
    crash.shipped_group_commit = lambda: True
    crash.run_matrix = lambda *args, **kwargs: ready()
    run.check_equal_limits = lambda: {"cpus": "2", "mem_limit": "2g", "memswap_limit": "2g"}
    try:
        code = crash.entry(["--smoke", "--run-id", "crash-sigterm", "--host-disk", "Stub NVMe, SSD, NVMe, ext4"])
    finally:
        with open(os.path.join(root, "downs.json"), "w") as handle:
            json.dump(downs, handle)
    sys.exit(code)
""")


def terminate(tmp_path, script):
    path = tmp_path / "runner.py"
    path.write_text(script)
    root = tmp_path / "root"
    root.mkdir()
    proc = subprocess.Popen([sys.executable, str(path), str(root)], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True)
    try:
        for line in proc.stdout:
            if line.strip() == "READY":
                break
        else:
            pytest.fail(f"the stubbed runner never reached a measurement: {proc.stderr.read()[-2000:]}")
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=60)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    return proc.returncode, root


def environment(directory):
    with open(directory / "environment.json", encoding="utf-8") as handle:
        return json.load(handle)


def test_run_py_records_a_sigterm_as_an_interruption_and_takes_the_clusters_down(tmp_path):
    code, root = terminate(tmp_path, RUN_SCRIPT)
    env = environment(root / "results" / "sigterm")
    assert env["run_state"] == "interrupted"
    assert any(event.get("signal") == "SIGTERM" and "SIGTERM" in event["event"] for event in env["events"])
    assert sorted(json.loads((root / "downs.json").read_text())) == ["kafka", "vtop"]
    assert code == 128 + signal.SIGTERM


def test_crash_py_records_a_sigterm_as_an_interruption_and_takes_the_clusters_down(tmp_path):
    code, root = terminate(tmp_path, CRASH_SCRIPT)
    env = environment(root / "results" / "crash-sigterm")
    assert env["run_state"] == "interrupted"
    assert any(event.get("signal") == "SIGTERM" and "SIGTERM" in event["event"] for event in env["events"])
    assert json.loads((root / "downs.json").read_text()) == [True]
    assert code == 128 + signal.SIGTERM


def test_the_handler_is_the_caller_s_again_once_main_returns():
    before = signal.getsignal(signal.SIGTERM)
    with run.sigterm_raises():
        assert signal.getsignal(signal.SIGTERM) is not before
    assert signal.getsignal(signal.SIGTERM) is before
