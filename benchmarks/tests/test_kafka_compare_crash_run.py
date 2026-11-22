"""The crash benchmark's runner (#599), driven by a fake host and a fake cluster.

No Docker and no root: `FakeBackend` plays the cluster, the producer, the consumer and the
data disk, records every call in order, and loses what a power loss would lose. What is
pinned here is the order of the steps (the trigger is the kill, then the drop, and never a
sync between them), what each outcome writes in the row, and what the loop does when the
method looks broken or the clock runs out.
"""
import json
import os
import subprocess
import sys

import pytest

from kafka_compare import crash
from kafka_compare import crash_analysis as ca
from kafka_compare import crash_schema as cs

HERE = os.path.dirname(os.path.abspath(__file__))
KC = os.path.join(os.path.dirname(HERE), "kafka_compare")


class FakeBackend:
    """A cluster that acknowledges `rate` records a second and, in power-loss mode, loses
    everything acknowledged in the `loss_window_s` seconds before the kill."""

    def __init__(self, rate=1000, loss_window_s=12.0, canary=None, restart_fails=False, sustain=1.0,
                 sentinel_needed=False, no_acks=False):
        self.calls = []
        self.rate = rate
        self.loss_window_s = loss_window_s
        self.canary = canary
        self.restart_fails = restart_fails
        self.sustain = sustain
        self.sentinel_needed = sentinel_needed
        self.no_acks = no_acks
        self.now = 1_000_000.0  # seconds
        self.mode = None
        self.prefix = None
        self.trigger_after = None
        self.killed_at = None
        self.sentinel_done = False
        self.acks = []

    # --- time
    def wall(self):
        return self.now

    def wait_until(self, t):
        self.calls.append(("wait_until",))
        self.now = max(self.now, t)

    def now_ms(self):
        return self.now * 1000

    # --- steps
    def begin(self, config, mode, crash_no):
        self.calls.append(("begin", config, mode, crash_no))
        self.mode, self.crash_no = mode, crash_no
        self.sentinel_done = False

    def reset(self, config):
        self.calls.append(("reset", config))
        self.config = config

    def bring_up(self, config):
        self.calls.append(("bring_up", config))

    def sync(self):
        self.calls.append(("sync",))

    def canary_synced(self):
        self.calls.append(("canary_synced",))

    def canary_unsynced(self):
        self.calls.append(("canary_unsynced",))

    def start_producer(self, config, prefix, max_messages):
        self.calls.append(("start_producer", prefix))
        self.prefix = prefix
        self.started = self.now

    def wait_producer_started(self):
        self.calls.append(("wait_producer_started",))
        return self.now

    def clock_probe(self):
        self.calls.append(("clock_probe",))
        return 0.0, 1.0

    def kill_all(self):
        self.calls.append(("kill_all",))
        self.killed_at = self.now
        return self.now * 1000

    def drop(self):
        self.calls.append(("drop",))
        self.now += 0.12

    def stop_producer(self):
        self.calls.append(("stop_producer",))

    def release(self):
        self.calls.append(("release",))

    def cleanup(self):
        self.calls.append(("cleanup",))

    def restore(self):
        self.calls.append(("restore",))

    def canary_check(self):
        self.calls.append(("canary_check",))
        if self.canary:
            return self.canary
        lost = self.mode == "flakey-drop"
        return ca.Canary(1048576, 1048576, 0 if lost else 1048576, 1048576)

    def producer_lines(self):
        self.calls.append(("producer_lines",))
        if self.no_acks:
            return [json.dumps({"timestamp": 1, "name": "startup_complete"})]
        elapsed = self.killed_at - self.started
        n = int(self.rate * elapsed * self.sustain)
        lines = [json.dumps({"timestamp": self.started * 1000, "name": "startup_complete"})]
        for i in range(n):
            t = self.started * 1000 + i * 1000 / self.rate
            lines.append(json.dumps({"timestamp": t, "name": "producer_send_success", "value": f"{self.prefix}.{i}",
                                     "offset": i, "partition": 0, "topic": "bench"}))
        self.acks = n
        return lines

    def restart(self, config):
        self.calls.append(("restart", config))
        if self.restart_fails:
            raise crash.Refused("the cluster did not serve within 240 s of the restart")

    def wait_served(self, at_least=0):
        self.calls.append(("wait_served", at_least))
        end = self.survivors() + (1 if self.sentinel_done else 0)
        if end < at_least:
            raise crash.Refused(f"the end offset stayed at {end}, below the sentinel's {at_least}")
        return end

    def survivors(self):
        if self.mode == "flakey-drop":
            lost = int(self.rate * self.loss_window_s)
            kept = max(0, self.acks - lost)
            if self.sentinel_needed and not self.sentinel_done:
                kept = max(0, kept - 5)
            return kept
        return self.acks

    def read_all(self, end):
        self.calls.append(("read_all", self.sentinel_done))
        count = end - (1 if self.sentinel_done else 0)
        lines = [f"Offset:{i}\t{self.prefix}.{i}" for i in range(count)]
        if self.sentinel_done:
            lines.append(f"Offset:{count}\t{ca.sentinel_value(self.crash_no)}")
        return "\n".join(lines) + "\n"

    def produce_sentinel(self, value, end_before):
        self.calls.append(("produce_sentinel", value, end_before))
        self.sentinel_done = True

    def save_logs(self, config, mode, crash_no, summary):
        self.calls.append(("save_logs", config, mode, crash_no))


def params(**over):
    base = {"rate": 1000, "seed": 599, "idempotent": True, "sustain_floor": 0.85}
    base.update(over)
    return crash.Params(**base)


def one(config="kafka-defaults", mode="flakey-drop", crash_no=1, trigger=20.0, backend=None, **over):
    b = backend or FakeBackend(**over)
    row = crash.run_crash(b, params(), config, mode, crash_no, trigger)
    return b, row


def names(b):
    return [c[0] for c in b.calls]


# ------------------------------------------------------------------------------ order


def test_the_trigger_is_the_kill_then_the_drop_and_never_a_sync_between():
    b, row = one()
    order = names(b)
    assert order.index("kill_all") + 1 == order.index("drop")
    assert order.count("sync") == 1 and order.index("sync") < order.index("start_producer")
    assert order.index("canary_synced") < order.index("start_producer")
    assert order.index("canary_unsynced") < order.index("kill_all")
    # the cluster's containers are released before the filesystem is remounted, and the canary is
    # read after the remount
    assert order.index("release") < order.index("restore") < order.index("canary_check") < order.index("restart")
    assert row["status"] == "ok"


def test_the_producer_is_stopped_before_the_cluster_comes_back():
    b, _ = one()
    order = names(b)
    assert order.index("stop_producer") < order.index("restart")
    assert order.index("producer_lines") < order.index("restart")


def test_a_process_kill_never_drops_or_restores_the_filesystem():
    b, row = one(mode="sigkill")
    assert "drop" not in names(b) and "restore" not in names(b)
    assert row["status"] == "ok" and row["acked_lost"] == 0 and row["kill_to_drop_ms"] is None
    assert row["canary_unsynced_lost"] is False


def test_the_sentinel_is_produced_before_the_read_back_that_decides_the_loss():
    b, row = one(sentinel_needed=True)
    order = [c for c in b.calls if c[0] in ("read_all", "produce_sentinel")]
    assert order == [("read_all", False),
                     ("produce_sentinel", "sentinel-c001", row["served_end_before_sentinel"]),
                     ("read_all", True)]
    # both columns are written, and the sentinel was what made the reads whole
    assert row["read_missing_before_sentinel"] == row["acked_lost"] + 5
    assert row["sentinel_needed"] is True


def test_without_the_sentinel_effect_both_missing_columns_agree():
    _, row = one()
    assert row["read_missing_before_sentinel"] == row["acked_lost"] and row["acked_lost"] > 0
    assert row["sentinel_needed"] is False


def test_the_read_that_decides_the_loss_waits_for_the_sentinel_to_be_served():
    b, row = one()
    served = [c for c in b.calls if c[0] == "wait_served"]
    assert served[0] == ("wait_served", 0)
    # the end offset must move past what it was before the sentinel, or the read does not happen
    assert served[1] == ("wait_served", row["served_end_before_sentinel"] + 1)
    assert row["served_end_after_sentinel"] == row["served_end_before_sentinel"] + 1


def test_a_sentinel_that_never_becomes_readable_is_a_failed_row_not_a_total_loss():
    b = FakeBackend()

    def never(value, end_before):
        raise crash.Refused("no write became readable within 120 s of the restart")

    b.produce_sentinel = never
    row = crash.run_crash(b, params(), "vtop-shipped", "flakey-drop", 1, 20.0)
    assert row["status"] == "failed" and row["needed_manual_repair"] is True
    assert "no write became readable" in row["note"]
    assert "acked_lost" not in row  # no loss is claimed from a read that never happened


# ------------------------------------------------------------------------------ the row


def test_a_power_loss_row_counts_what_the_window_lost():
    _, row = one(trigger=20.0, loss_window_s=12.0)
    assert row["acked"] == 20_000 and row["acked_lost"] == 12_000
    assert row["lost_first_seq"] == 8_000 and row["lost_last_seq"] == 19_999 and row["lost_runs"] == 1
    assert row["lost_window_s"] == pytest.approx(12.0, abs=0.1)
    assert row["duplicates"] == 0 and row["reordered"] == 0 and row["offset_mismatch"] == 0
    assert row["canary_synced_ok"] is True and row["canary_unsynced_lost"] is True
    assert row["kill_to_drop_ms"] == pytest.approx(120, abs=1)
    assert row["config"] == "kafka-defaults" and row["profile"] == "defaults" and row["system"] == "kafka"
    assert row["trigger_after_s"] == 20.0 and row["seed"] == 599 and row["offered_rate"] == 1000
    assert row["acked_in_last_10s"] == pytest.approx(10_000, abs=5)
    assert row["note"] == ""
    # every column of the row is one the CSV has, and the row is writable
    assert set(row) <= set(cs.COLUMNS)


def test_a_failed_canary_row_is_refused_with_its_reason_and_keeps_its_numbers():
    bad = ca.Canary(4096, 1048576, 0, 1048576)
    _, row = one(canary=bad)
    assert row["status"] == "refused"
    assert "fsynced" in row["note"] and "4096" in row["note"]
    assert row["acked"] == 20_000  # kept, so the row says what happened, but not ok


def test_a_power_loss_that_dropped_nothing_is_refused_as_a_process_kill():
    whole = ca.Canary(1048576, 1048576, 1048576, 1048576)
    _, row = one(canary=whole)
    assert row["status"] == "refused" and "process kill" in row["note"]


def test_an_unsustained_rate_is_a_refused_row():
    _, row = one(sustain=0.5)
    assert row["status"] == "refused"
    assert "did not sustain" in row["note"] and "10000 of 20000" in row["note"]


def test_a_producer_that_acknowledged_nothing_is_a_failed_row():
    _, row = one(no_acks=True)
    assert row["status"] == "failed" and "acknowledged nothing" in row["note"]


def test_a_restart_that_never_serves_is_a_failed_row_that_needed_repair():
    b, row = one(restart_fails=True)
    assert row["status"] == "failed" and row["needed_manual_repair"] is True
    assert "did not serve" in row["note"]
    assert row["canary_synced_ok"] is True  # what was learned before the restart is kept
    assert "save_logs" in names(b)


def test_a_cluster_that_will_not_come_up_before_the_crash_is_a_refused_row():
    b = FakeBackend()

    def broken(config):
        raise crash.Refused("the brokers never became healthy")

    b.bring_up = broken
    row = crash.run_crash(b, params(), "kafka-defaults", "flakey-drop", 1, 20.0)
    assert row["status"] == "refused" and "never became healthy" in row["note"]
    assert "kill_all" not in names(b) and "cleanup" in names(b)


def test_the_logs_are_saved_even_when_the_read_back_raises():
    b = FakeBackend()

    def boom(end):
        raise RuntimeError("consumer exited 1")

    b.read_all = boom
    row = crash.run_crash(b, params(), "vtop-shipped", "flakey-drop", 1, 20.0)
    assert row["status"] == "failed" and "consumer exited 1" in row["note"]
    assert ("save_logs", "vtop-shipped", "flakey-drop", 1) in b.calls


# ------------------------------------------------------------------------------- the loop


def small_matrix(crashes=2, modes=("flakey-drop",), configs=("kafka-defaults", "kafka-fsync-parity", "vtop-shipped")):
    return {"configs": list(configs), "crash_modes": list(modes), "crashes": crashes, "rate": 1000,
            "seed": 599, "trigger_window_s": [10.0, 20.0]}


class RecordingBackend(FakeBackend):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.order = []

    def reset(self, config):
        super().reset(config)
        self.order.append(config)


def run_loop(b, tmp_path, matrix, **kw):
    cs.write_header(os.path.join(str(tmp_path), "crash.csv"))
    return crash.run_matrix(b, params(), matrix, str(tmp_path), base_row={"run_id": "r"}, **kw)


def test_every_configuration_runs_crash_i_before_any_runs_crash_i_plus_one(tmp_path):
    rows = run_loop(RecordingBackend(), tmp_path, small_matrix(crashes=2, modes=("sigkill", "flakey-drop")))
    keys = [(r["config"], r["crash_mode"], r["crash"]) for r in rows]
    assert [k[2] for k in keys] == [1] * 6 + [2] * 6
    assert len(set(keys)) == 12
    # the same trigger time for every configuration of one crash number (a paired design)
    by_crash = {}
    for r in rows:
        by_crash.setdefault(r["crash"], set()).add(r["trigger_after_s"])
    assert all(len(v) == 1 for v in by_crash.values())


def test_the_loop_writes_each_row_to_the_csv_as_it_goes(tmp_path):
    run_loop(RecordingBackend(), tmp_path, small_matrix(crashes=1))
    rows = cs.read_rows(os.path.join(str(tmp_path), "crash.csv"))
    assert len(rows) == 3 and {r["status"] for r in rows} == {"ok"}
    assert {r["run_id"] for r in rows} == {"r"}


def test_a_resumed_run_skips_the_rows_it_already_has(tmp_path):
    b = RecordingBackend()
    done = {("kafka-defaults", "flakey-drop", 1), ("vtop-shipped", "flakey-drop", 1)}
    run_loop(b, tmp_path, small_matrix(crashes=1), skip=done)
    assert b.order == ["kafka-fsync-parity"]


def test_the_loop_stops_at_the_cap_before_a_crash_that_would_not_fit(tmp_path):
    b = RecordingBackend()
    ticks = iter([0.0, 0.0, 5000.0])
    with pytest.raises(crash.CapReached, match="--max-hours"):
        run_loop(b, tmp_path, small_matrix(crashes=2), deadline_s=1000.0, clock=lambda: next(ticks),
                 crash_estimate_s=300.0)
    rows = cs.read_rows(os.path.join(str(tmp_path), "crash.csv"))
    assert len(rows) == 1 and b.order == ["kafka-defaults"]


def test_a_run_where_kafka_defaults_loses_nothing_stops_before_any_chart(tmp_path):
    b = RecordingBackend(loss_window_s=0.0)
    with pytest.raises(crash.SuspectMethod, match="lost nothing in 5 crashes"):
        run_loop(b, tmp_path, small_matrix(crashes=6), suspect_after=5)
    rows = cs.read_rows(os.path.join(str(tmp_path), "crash.csv"))
    assert max(int(r["crash"]) for r in rows) == 5


# ------------------------------------------------------------- commands and refusals


def test_every_node_is_killed_by_one_sigkill_command():
    command = crash.kill_command(["kc-crash-kafka-kafka-1-1", "kc-crash-kafka-kafka-2-1", "kc-crash-kafka-kafka-3-1"])
    assert command == ["docker", "kill", "--signal", "KILL", "kc-crash-kafka-kafka-1-1",
                       "kc-crash-kafka-kafka-2-1", "kc-crash-kafka-kafka-3-1"]


def test_the_host_commands_are_the_script_with_a_prefix_and_nothing_else():
    host = crash.Host("/data", ["sudo", "-n"], runner=lambda cmd, **kw: subprocess.CompletedProcess(cmd, 0, "", ""))
    assert host.command("drop") == ["sudo", "-n", "bash", os.path.join(KC, "crash", "powercut.sh"), "drop", "/data"]
    bare = crash.Host("/data", [], runner=None).command("sync")
    assert bare[:2] == ["bash", os.path.join(KC, "crash", "powercut.sh")]


def test_the_drop_is_the_flakey_drop_and_is_not_preceded_by_a_sync():
    seen = []

    def runner(cmd, **kw):
        seen.append(cmd[-2:])
        return subprocess.CompletedProcess(cmd, 0, "", "")

    host = crash.Host("/data", [], runner=runner)
    host.call("drop")
    host.call("restore")
    assert seen == [["drop", "/data"], ["restore", "/data"]]


def test_the_default_host_prefix_is_sudo_unless_root():
    assert crash.default_host_prefix(euid=0) == []
    assert crash.default_host_prefix(euid=1000) == ["sudo", "-n"]


def test_the_flush_override_is_layered_for_kafka_fsync_parity_only():
    parity = os.path.join("crash", "compose.kafka.fsync-parity.yml")
    assert parity in crash.compose_files("kafka", "kafka-fsync-parity")
    assert parity not in crash.compose_files("kafka", "kafka-defaults")
    # teardown runs both systems with the crash's configuration; VTOP has no flush override
    assert crash.compose_files("vtop", "kafka-fsync-parity") == list(crash.SPEC["vtop"]["files"])
    assert all(isinstance(name, str) for name in crash.compose_files("vtop", None))


def test_a_cleanup_that_fails_does_not_end_the_run_or_change_the_row():
    b = FakeBackend()

    def broken():
        raise RuntimeError("docker is gone")

    b.cleanup = broken
    row = crash.run_crash(b, params(), "kafka-defaults", "sigkill", 1, 20.0)
    assert row["status"] == "ok"


def test_kafka_topic_creation_carries_the_profile():
    defaults = crash.topic_create_args("kafka-defaults")
    parity = crash.topic_create_args("kafka-fsync-parity")
    assert "min.insync.replicas=2" in defaults and "flush.messages=1" not in defaults
    assert "flush.messages=1" in parity and "min.insync.replicas=2" in parity
    assert "--replication-factor" in defaults and defaults[defaults.index("--replication-factor") + 1] == "3"


def test_the_flush_setting_the_broker_reports_must_match_the_profile():
    off = "log.flush.interval.messages=9223372036854775807 sensitive=false synonyms={DEFAULT_CONFIG:x}"
    on = "log.flush.interval.messages=1 sensitive=false synonyms={STATIC_BROKER_CONFIG:x}"
    crash.verify_flush("kafka-defaults", off)
    crash.verify_flush("kafka-fsync-parity", on)
    with pytest.raises(crash.Refused, match="fsync-parity.*log.flush.interval.messages=1"):
        crash.verify_flush("kafka-fsync-parity", off)
    with pytest.raises(crash.Refused, match="defaults.*unset"):
        crash.verify_flush("kafka-defaults", on)
    with pytest.raises(crash.Refused, match="does not report"):
        crash.verify_flush("kafka-defaults", "nothing here")


def test_the_smoke_profile_is_small_and_covers_every_configuration_in_both_modes():
    args = crash.parse_args(["--smoke"])
    matrix = crash.build_matrix(args)
    assert matrix["crashes"] == 2 and set(matrix["configs"]) == set(ca.CONFIGS)
    assert set(matrix["crash_modes"]) == set(ca.CRASH_MODES)
    assert matrix["trigger_window_s"][1] <= 15
    assert cs.projected_seconds(matrix) / 3600 < 1.5


def test_the_full_profile_defaults_to_the_issues_30_crashes_in_power_loss_mode():
    matrix = crash.build_matrix(crash.parse_args([]))
    assert matrix["crashes"] == 30 and matrix["crash_modes"] == ["flakey-drop"]
    assert matrix["rate"] == 1000 and matrix["trigger_window_s"] == [25.0, 55.0]


def test_a_matrix_over_max_hours_is_refused_before_any_docker_work(monkeypatch):
    def no_docker(*a, **k):
        raise AssertionError("docker was called before the plan was checked")

    monkeypatch.setattr(crash, "sh", no_docker)
    with pytest.raises(SystemExit, match="over --max-hours"):
        crash.main(["--crashes", "30", "--max-hours", "1"])


@pytest.mark.parametrize("argv", [["--crashes", "0"], ["--rate", "0"], ["--trigger-window", "30,10"],
                                  ["--trigger-window", "x"], ["--modes", "power"], ["--configs", "redpanda"],
                                  ["--run-id", "../x"], ["--max-hours", "0"], ["--node-cpus", "0"]])
def test_bad_arguments_are_refused_before_any_docker_work(monkeypatch, argv):
    def no_docker(*a, **k):
        raise AssertionError("docker was called before the arguments were checked")

    monkeypatch.setattr(crash, "sh", no_docker)
    with pytest.raises(SystemExit):
        crash.main(argv)


def test_check_on_a_missing_directory_refuses(tmp_path, capsys):
    assert crash.main(["--check", str(tmp_path / "nothing")]) == 2
    assert "check refused" in capsys.readouterr().err


# -------------------------------------------------------------- the files around it


def read(*parts):
    with open(os.path.join(KC, *parts)) as handle:
        return handle.read()


def test_powercut_is_valid_bash_and_refuses_an_unknown_command(tmp_path):
    script = os.path.join(KC, "crash", "powercut.sh")
    assert subprocess.run(["bash", "-n", script]).returncode == 0
    done = subprocess.run(["bash", script, "frobnicate", str(tmp_path / "d")], capture_output=True, text=True)
    assert done.returncode != 0 and "unknown command" in done.stderr
    # the drop needs a setup first, and says so instead of touching anything
    done = subprocess.run(["bash", script, "drop", str(tmp_path / "d")], capture_output=True, text=True)
    assert done.returncode != 0 and "run 'setup' first" in done.stderr


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="the canary uses GNU dd and sync -f")
def test_the_canaries_are_written_and_read_back_by_the_script(tmp_path):
    script = os.path.join(KC, "crash", "powercut.sh")
    root = str(tmp_path / "data")
    os.makedirs(root)

    def call(command):
        return subprocess.run(["bash", script, command, root], capture_output=True, text=True, check=True).stdout

    call("sync")
    call("canary-synced")
    call("canary-unsynced")
    assert call("canary-check").strip() == "synced=1048576 unsynced=1048576"
    assert call("canary-check").strip() == "synced=0 unsynced=0"  # read once, then removed
    assert call("status").strip() == "plain"


def test_the_override_files_change_only_the_data_volumes_the_cpus_the_network_and_the_flush():
    kafka = read("crash", "compose.kafka.crash.yml")
    vtop = read("crash", "compose.vtop.crash.yml")
    parity = read("crash", "compose.kafka.fsync-parity.yml")
    base_kafka, base_vtop = read("compose.kafka.yml"), read("compose.vtop.yml")
    for n in (1, 2, 3):
        assert f"kafka-{n}-data:" in base_kafka and f"kafka-{n}-data:" in kafka
        assert f"vtop-{n}-data:" in base_vtop and f"vtop-{n}-data:" in vtop
        assert 'KAFKA_LOG_FLUSH_INTERVAL_MESSAGES: "1"' in parity
    assert "CRASH_DATA_ROOT" in kafka and "CRASH_DATA_ROOT" in vtop
    assert "cpus: ${CRASH_NODE_CPUS:-2}" in kafka and "cpus: ${CRASH_NODE_CPUS:-2}" in vtop
    assert "cpus: 2" in base_kafka and "cpus: 2" in base_vtop  # the default is the base files' value
    # nothing about memory, flushing (outside the parity file) or the engine's settings is overridden
    assert "mem_limit" not in kafka + vtop and "KAFKA_" not in kafka + vtop
    assert "name: kc-crash-kafka-bench" in kafka and "name: kc-crash-vtop-bench" in vtop


def readme_commands():
    text = read("README.md")
    section = text[text.index("## Whole-replica-set crash (#599)"):text.index("## Known gaps")]
    block = section[section.index("```bash"):]
    block = block[:block.index("```", 3)].replace("\\\n", " ")
    commands = []
    for line in block.splitlines():
        line = line.strip()
        if line.startswith("python3 benchmarks/kafka_compare/crash.py"):
            commands.append(shlex_split(line)[2:])
    return commands


def shlex_split(line):
    import shlex

    return shlex.split(line)


def test_every_command_the_readme_shows_parses_and_the_full_run_fits_its_own_cap():
    commands = readme_commands()
    assert len(commands) == 4
    runs = []
    for argv in commands:
        args = crash.parse_args(argv)
        if args.check is not None:
            continue
        matrix = crash.build_matrix(args)
        hours = crash.check_args(args, matrix)  # the plan is within its own --max-hours
        runs.append((args, matrix, hours))
    smoke, full, resume = runs
    assert smoke[1]["smoke"] and not full[1]["smoke"]
    # the documented one-hour command: 12 crashes of three configurations in power-loss mode, about an hour
    assert full[1]["crashes"] == 12 and full[1]["crash_modes"] == ["flakey-drop"]
    assert 0.8 <= full[2] <= 1.25, full[2]
    assert resume[0].resume == "<run-id>"
