"""The crash harness's gaps, closed before the v0.11.0 AWS round (#599).

1. A second VTOP configuration: group commit (the node's `group_commit:` section, #752/#832) against the shipped
   default. The shipped default is read from the node's source, and the leader's own gauge is checked against what
   each configuration says it is.
2. Record size: #599 asks for 1 KiB records, `kafka-verifiable-producer` has no size flag, so the producer is a
   small JVM class on the same pinned image that prints the same lines with a padded value.
3. The host: instance type, CPU model and kernel (compare_common/hostmeta.py), and `--host-disk`.
"""
import json
import os
import re
import subprocess

import pytest

from compare_common import hostmeta
from kafka_compare import crash
from kafka_compare import crash_analysis as ca
from kafka_compare import crash_schema as cs
from kafka_compare import run as phase1

HERE = os.path.dirname(os.path.abspath(__file__))
KC = os.path.join(os.path.dirname(HERE), "kafka_compare")
REPO = os.path.dirname(os.path.dirname(HERE))


def read(*parts):
    with open(os.path.join(KC, *parts)) as handle:
        return handle.read()


# ------------------------------------------------------------ 1. the VTOP configurations


def test_there_are_three_vtop_configurations_and_each_says_what_group_commit_is():
    assert ca.CONFIGS["vtop-shipped"] == ("vtop", "defaults")
    assert ca.CONFIGS["vtop-group-commit-on"] == ("vtop", "group-commit-on")
    assert ca.CONFIGS["vtop-group-commit-off"] == ("vtop", "group-commit-off")
    assert ca.VTOP_GROUP_COMMIT == {"vtop-shipped": None, "vtop-group-commit-on": True,
                                    "vtop-group-commit-off": False}
    assert set(ca.VTOP_GROUP_COMMIT) == {c for c, (system, _) in ca.CONFIGS.items() if system == "vtop"}


NODE_CONFIG = """
/// Whether group commit is on when the section does not say. Off until the
/// evidence gates in #752 hold; turning it on by default is this one line.
pub const GROUP_COMMIT_ON_BY_DEFAULT: bool = %s;
"""


@pytest.mark.parametrize("word,expected", [("false", False), ("true", True)])
def test_the_shipped_group_commit_default_is_read_from_the_nodes_source(word, expected):
    assert ca.shipped_group_commit_default(NODE_CONFIG % word) is expected


def test_a_node_source_without_the_constant_is_refused_not_guessed():
    with pytest.raises(ValueError, match="GROUP_COMMIT_ON_BY_DEFAULT"):
        ca.shipped_group_commit_default("pub const SOMETHING_ELSE: bool = true;")


def test_the_repository_node_source_names_its_default():
    with open(os.path.join(REPO, "crates", "vtop-node", "src", "config.rs")) as handle:
        assert isinstance(ca.shipped_group_commit_default(handle.read()), bool)


def test_the_default_matrix_runs_the_shipped_vtop_and_the_group_commit_state_that_differs_from_it():
    both_kafka = ("kafka-defaults", "kafka-fsync-parity")
    assert ca.default_configs(False) == (*both_kafka, "vtop-shipped", "vtop-group-commit-on")
    assert ca.default_configs(True) == (*both_kafka, "vtop-shipped", "vtop-group-commit-off")
    assert ca.default_configs(None) is None


def test_the_effective_group_commit_of_a_configuration():
    assert ca.effective_group_commit("vtop-group-commit-on", shipped=False) is True
    assert ca.effective_group_commit("vtop-group-commit-off", shipped=True) is False
    assert ca.effective_group_commit("vtop-shipped", shipped=False) is False
    assert ca.effective_group_commit("vtop-shipped", shipped=True) is True
    assert ca.effective_group_commit("kafka-defaults", shipped=True) is None


def test_the_default_configs_come_from_the_repositorys_own_default(monkeypatch):
    monkeypatch.setattr(crash, "shipped_group_commit", lambda: False)
    assert crash.build_matrix(crash.parse_args([]))["configs"] == [
        "kafka-defaults", "kafka-fsync-parity", "vtop-shipped", "vtop-group-commit-on"]
    monkeypatch.setattr(crash, "shipped_group_commit", lambda: True)
    assert crash.build_matrix(crash.parse_args([]))["configs"][-1] == "vtop-group-commit-off"


def test_configs_named_on_the_command_line_are_taken_as_given(monkeypatch):
    monkeypatch.setattr(crash, "shipped_group_commit", lambda: None)  # unreadable: only --configs can proceed
    args = crash.parse_args(["--configs", "vtop-shipped,vtop-group-commit-on,vtop-group-commit-off"])
    assert crash.build_matrix(args)["configs"] == ["vtop-shipped", "vtop-group-commit-on", "vtop-group-commit-off"]
    with pytest.raises(SystemExit, match="--configs"):
        crash.build_matrix(crash.parse_args([]))


def test_a_pair_of_configurations_with_the_same_effective_setting_is_said_so(monkeypatch):
    # With group commit off in the shipped node, `vtop-group-commit-off` is the same thing twice.
    notes = crash.redundant_configs(["vtop-shipped", "vtop-group-commit-off"], shipped=False)
    assert notes and "vtop-group-commit-off" in notes[0] and "vtop-shipped" in notes[0]
    assert crash.redundant_configs(["vtop-shipped", "vtop-group-commit-on"], shipped=False) == []
    assert crash.redundant_configs(["kafka-defaults", "vtop-shipped"], shipped=False) == []


def test_the_leaders_own_gauge_must_match_the_configuration():
    on, off = "vtop_broker_group_commit_enabled 1\n", "# HELP x\nvtop_broker_group_commit_enabled 0\n"
    crash.verify_group_commit("vtop-group-commit-on", on, shipped=False)
    crash.verify_group_commit("vtop-group-commit-off", off, shipped=True)
    crash.verify_group_commit("vtop-shipped", off, shipped=False)
    crash.verify_group_commit("vtop-shipped", on, shipped=True)
    with pytest.raises(crash.Refused, match="group-commit-on.*reports 0"):
        crash.verify_group_commit("vtop-group-commit-on", off, shipped=False)
    with pytest.raises(crash.Refused, match="group-commit-off.*reports 1"):
        crash.verify_group_commit("vtop-group-commit-off", on, shipped=True)
    with pytest.raises(crash.Refused, match="vtop-shipped.*reports 1.*shipped default is off"):
        crash.verify_group_commit("vtop-shipped", on, shipped=False)
    with pytest.raises(crash.Refused, match="does not report"):
        crash.verify_group_commit("vtop-shipped", "nothing", shipped=False)


def test_the_leader_file_of_each_configuration():
    assert crash.vtop_leader_file("vtop-shipped") == os.path.join("vtop", "leader.yaml")
    assert crash.vtop_leader_file("vtop-group-commit-on") == os.path.join("crash", "leader-group-commit-on.yaml")
    assert crash.vtop_leader_file("vtop-group-commit-off") == os.path.join("crash", "leader-group-commit-off.yaml")
    assert crash.vtop_leader_file("kafka-defaults") == os.path.join("vtop", "leader.yaml")


@pytest.mark.parametrize("state,word", [("on", "true"), ("off", "false")])
def test_a_group_commit_leader_is_the_shipped_leader_plus_the_section_and_nothing_else(state, word):
    variant = read("crash", f"leader-group-commit-{state}.yaml")
    shipped = read("vtop", "leader.yaml")
    assert variant.startswith(shipped.rstrip("\n") + "\n")
    added = variant[len(shipped.rstrip("\n") + "\n"):]
    assert re.sub(r"(?m)^#.*\n", "", added).strip() == f"group_commit:\n  enabled: {word}"


def test_the_followers_are_never_given_a_group_commit_section():
    # an enabled section on a follower is refused by the node (config.rs refuse_group_commit_misuse)
    for name in ("follower-1.yaml", "follower-2.yaml"):
        assert "group_commit" not in read("vtop", name)


def test_the_leader_mount_reads_the_file_the_harness_names_and_defaults_to_the_shipped_one():
    base = read("compose.vtop.yml")
    assert base.count("${CRASH_VTOP_LEADER:-./vtop/leader.yaml}:/etc/vtop/node.yaml:ro") == 1
    assert "follower-1.yaml:/etc/vtop/node.yaml:ro" in base and "follower-2.yaml:/etc/vtop/node.yaml:ro" in base


class Capture:
    def __init__(self):
        self.calls = []

    def __call__(self, args, **kwargs):
        self.calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 0, "", "")


def backend(tmp_path, monkeypatch, config, **ctx):
    capture = Capture()
    monkeypatch.setattr(crash, "sh", capture)
    full = {"run_dir": str(tmp_path), "env": {"observed": {}}, "host": None, "vtop_image": "vtop-bench:x",
            "data_root": "/d", "node_cpus": 2, "client_cpus": 4, "client_memory": "4g",
            "params": crash.Params(rate=1000, seed=1), "save_env": lambda: None, "record_size": 1024,
            "client_image": "kc-crash-client:abc"}
    full.update(ctx)
    b = crash.DockerBackend(full)
    b.begin(config, "flakey-drop", 1)
    return b, capture


@pytest.mark.parametrize("config,expected", [("vtop-shipped", os.path.join("vtop", "leader.yaml")),
                                             ("vtop-group-commit-on", os.path.join("crash",
                                                                                    "leader-group-commit-on.yaml"))])
def test_compose_is_given_the_leader_file_of_the_configuration(tmp_path, monkeypatch, config, expected):
    b, capture = backend(tmp_path, monkeypatch, config)
    b.compose("vtop", "up", "-d", config=config)
    env = capture.calls[-1][1]["env"]
    assert env["CRASH_VTOP_LEADER"] == "./" + expected.replace(os.sep, "/")


def test_bring_up_checks_the_gauge_after_the_cluster_is_healthy(tmp_path, monkeypatch):
    b, capture = backend(tmp_path, monkeypatch, "vtop-group-commit-on", shipped_group_commit=False)

    def sh(args, **kwargs):
        capture(args, **kwargs)
        if args[:2] == ["docker", "exec"]:
            return subprocess.CompletedProcess(args, 0, "vtop_broker_group_commit_enabled 0\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(crash, "sh", sh)
    with pytest.raises(crash.Refused, match="group-commit-on.*reports 0"):
        b.bring_up("vtop-group-commit-on")
    exec_calls = [a for a, _ in capture.calls if a[:2] == ["docker", "exec"]]
    assert exec_calls and "kc-crash-vtop-vtop-1-1" in exec_calls[0]
    assert "127.0.0.1:9600/metrics" in " ".join(exec_calls[0])


def test_the_gauge_is_recorded_per_configuration(tmp_path, monkeypatch):
    env = {"observed": {}}
    b, capture = backend(tmp_path, monkeypatch, "vtop-group-commit-on", shipped_group_commit=False, env=env)
    monkeypatch.setattr(crash, "sh", lambda args, **kw: subprocess.CompletedProcess(
        args, 0, "vtop_broker_group_commit_enabled 1\n" if args[:2] == ["docker", "exec"] else "", ""))
    b.bring_up("vtop-group-commit-on")
    assert env["observed"]["vtop_group_commit"]["vtop-group-commit-on"] == 1


def test_the_environment_records_the_shipped_default_and_every_leader_file(monkeypatch):
    env = capture_env(monkeypatch)
    assert env["comparable"]["vtop_group_commit_shipped_default"] is False
    assert env["configs"]["vtop_leader_group_commit_on"] == read("crash", "leader-group-commit-on.yaml")
    assert env["configs"]["vtop_leader_group_commit_off"] == read("crash", "leader-group-commit-off.yaml")
    assert "vtop-group-commit-on" in env["comparable"]["profiles"]
    assert "vtop-group-commit-off" in env["comparable"]["profiles"]


# ------------------------------------------------------------ 2. the record size


def test_a_padded_value_has_exactly_the_size_asked_and_still_names_its_crash_and_number():
    value = ca.record_value("7", 12, 1024)
    assert len(value) == 1024 and value.startswith("7.12~x") and set(value[6:]) == {"x"}
    assert ca.record_value("7", 12, 0) == "7.12"
    assert ca.record_value("7", 12, 3) == "7.12", "a size below the text's own length pads nothing"
    assert ca.record_value("30", 0, 1024).startswith("30.0~")


def test_acknowledgements_of_padded_records_parse_to_the_same_ids():
    lines = [json.dumps({"timestamp": 1, "name": "startup_complete"})]
    for i in range(3):
        lines.append(json.dumps({"timestamp": 10 + i, "name": "producer_send_success",
                                 "value": ca.record_value("7", i, 1024), "offset": i}))
    run = ca.parse_acks(lines, "7")
    assert [a.seq for a in run.acks] == [0, 1, 2] and [a.offset for a in run.acks] == [0, 1, 2]


def test_a_padded_record_of_another_crash_is_not_this_crashs():
    lines = [json.dumps({"timestamp": 1, "name": "startup_complete"}),
             json.dumps({"timestamp": 2, "name": "producer_send_success",
                         "value": ca.record_value("8", 0, 1024), "offset": 0})]
    with pytest.raises(ca.ParseError, match="not a record this crash sent"):
        ca.parse_acks(lines, "7")


@pytest.mark.parametrize("bad", ["7.x~xxxx", "7.01~xxxx", "7.~xxxx", "7.1~y", "7.1~xx.y", "7.1~xx\tyy"])
def test_a_value_that_is_not_one_of_ours_is_not_a_record(bad):
    lines = [json.dumps({"timestamp": 1, "name": "startup_complete"}),
             json.dumps({"timestamp": 2, "name": "producer_send_success", "value": bad, "offset": 0})]
    with pytest.raises(ca.ParseError):
        ca.parse_acks(lines, "7")


def test_the_read_back_recognises_padded_records_the_sentinel_and_strangers():
    text = (f"Offset:0\t{ca.record_value('7', 0, 1024)}\nOffset:1\t{ca.record_value('7', 1, 1024)}\n"
            f"Offset:2\t7.2\nOffset:3\t{ca.sentinel_value(7)}\nOffset:4\t{ca.record_value('9', 5, 1024)}\n")
    got = ca.parse_consumed(text, "7", ca.sentinel_value(7))
    assert [(c.offset, c.kind, c.seq) for c in got] == [(0, "record", 0), (1, "record", 1), (2, "record", 2),
                                                          (3, "sentinel", None), (4, "other", None)]


def producer_command(tmp_path, monkeypatch, record_size, rate=1000):
    popen = []

    class FakePopen:
        def __init__(self, command, **kwargs):
            popen.append(command)

    b, _ = backend(tmp_path, monkeypatch, "kafka-defaults", record_size=record_size,
                   params=crash.Params(rate=rate, seed=1))
    monkeypatch.setattr(crash.subprocess, "Popen", FakePopen)
    b.start_producer("kafka-defaults", "7", 5000)
    return popen[0]


def test_the_producer_is_the_padding_client_for_both_systems(tmp_path, monkeypatch):
    command = producer_command(tmp_path, monkeypatch, 1024)
    assert "kc-crash-client:abc" in command and crash.KAFKA_IMAGE not in command
    text = " ".join(command)
    assert "AckProducer" in text and "--value-size 1024" in text and "--value-prefix 7" in text
    assert "--acks -1" in text and "--throughput 1000" in text and "--max-messages 5000" in text
    assert "--command-config /out/producer.properties" in text


def test_record_size_zero_is_the_stock_verifiable_producer_a_few_bytes_a_record(tmp_path, monkeypatch):
    command = producer_command(tmp_path, monkeypatch, 0)
    assert crash.KAFKA_IMAGE in command and "kc-crash-client:abc" not in command
    text = " ".join(command)
    assert "kafka-verifiable-producer.sh" in text and "--value-size" not in text and "AckProducer" not in text


def test_the_client_image_is_built_once_and_named_for_what_is_in_it(monkeypatch):
    capture = Capture()
    state = {"exists": False}

    def sh(args, **kwargs):
        capture(args, **kwargs)
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0 if state["exists"] else 1, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(crash, "sh", sh)
    image = crash.ensure_client_image(rebuild=False)
    builds = [a for a, _ in capture.calls if a[:2] == ["docker", "build"]]
    assert len(builds) == 1 and image in builds[0]
    flat = " ".join(builds[0])
    assert f"KAFKA_IMAGE={crash.KAFKA_IMAGE}" in flat and f"JDK_IMAGE={crash.JDK_IMAGE}" in flat
    assert re.fullmatch(r"kc-crash-client:[0-9a-f]{12}", image)
    state["exists"] = True
    capture.calls.clear()
    assert crash.ensure_client_image(rebuild=False) == image
    assert not [a for a, _ in capture.calls if a[:2] == ["docker", "build"]]
    crash.ensure_client_image(rebuild=True)
    assert [a for a, _ in capture.calls if a[:2] == ["docker", "build"]]


def test_the_client_image_name_changes_with_its_source(monkeypatch, tmp_path):
    first = crash.client_image_name()
    other = tmp_path / "client"
    other.mkdir()
    for name in ("Dockerfile", "AckProducer.java"):
        (other / name).write_text(read("crash", "client", name) + "// changed\n")
    monkeypatch.setattr(crash, "CLIENT_DIR", str(other))
    assert crash.client_image_name() != first


def test_the_dockerfile_pins_the_same_two_images_crash_py_does():
    text = read("crash", "client", "Dockerfile")
    assert f"ARG KAFKA_IMAGE={crash.KAFKA_IMAGE}\n" in text
    assert f"ARG JDK_IMAGE={crash.JDK_IMAGE}\n" in text
    assert "@sha256:" in crash.JDK_IMAGE and "@sha256:" in crash.KAFKA_IMAGE


def test_the_java_class_prints_the_lines_the_parser_reads_and_pads_the_value_the_parser_accepts():
    java = read("crash", "client", "AckProducer.java")
    for name in ("startup_complete", "producer_send_success", "producer_send_error", "tool_data"):
        assert name in java
    assert "text.append('~')" in java and "append(prefix).append('.').append(seq)" in java
    assert "--value-size" in java and "--command-config" in java and "--throughput" in java
    assert ca.PAD_MARK == "~" and ca.PAD_FILL == "x"


def test_the_rows_say_the_record_size_and_the_client(monkeypatch):
    b, row = run_one_crash(record_size=1024)
    assert row["record_size"] == 1024 and "AckProducer" in row["client"]
    b, row = run_one_crash(record_size=0)
    assert row["record_size"] == 0 and row["client"] == "kafka-verifiable-producer"


def test_the_csv_has_a_record_size_column_and_an_ok_row_needs_it(tmp_path):
    assert "record_size" in cs.COLUMNS
    assert "record_size" in cs._NUMERIC_OK


def run_one_crash(record_size):
    from test_kafka_compare_crash_run import FakeBackend

    fake = FakeBackend()
    row = crash.run_crash(fake, crash.Params(rate=1000, seed=599, record_size=record_size), "kafka-defaults",
                          "flakey-drop", 1, 20.0)
    return fake, row


@pytest.mark.parametrize("argv", [["--record-size", "-1"], ["--record-size", "x"], ["--record-size", "2000000"]])
def test_a_bad_record_size_is_refused_before_any_docker_work(monkeypatch, argv):
    def no_docker(*a, **k):
        raise AssertionError("docker was called before the arguments were checked")

    monkeypatch.setattr(crash, "sh", no_docker)
    with pytest.raises(SystemExit):
        crash.main(argv)


def test_the_default_record_size_is_the_issues_one_kib():
    assert crash.parse_args([]).record_size == 1024
    assert crash.build_matrix(crash.parse_args([]))["record_size"] == 1024
    assert crash.build_matrix(crash.parse_args(["--record-size", "0"]))["record_size"] == 0


def test_record_size_belongs_to_the_matrix_so_a_resume_cannot_change_it():
    matrix = crash.build_matrix(crash.parse_args([]))
    cs.check_matrix(matrix)
    bad = dict(matrix, record_size=-5)
    with pytest.raises(cs.PlanRefused, match="record_size"):
        cs.check_matrix(bad)


# ------------------------------------------------------------ 3. the host


def capture_env(monkeypatch, host_disk=None):
    facts = {"instance_type": "c6i.4xlarge", "cpu_model": "Intel(R) Xeon(R) Platinum 8375C", "kernel": "Linux 6.8"}

    def fake_phase1(run_id, matrix, vtop_image, limits, disk):
        return {"comparable": {"host_cpu": "x86_64", "host_disk": disk, "profile": "defaults",
                               "consumer_props": [], "vtop_commit": "c" * 40}, "host_metadata": dict(facts),
                "observed": {}}

    class FakeHost:
        def call(self, name, *args, **kwargs):
            return "fstype=ext4 options=rw" if name == "describe" else ""

    monkeypatch.setattr(phase1, "capture_environment", fake_phase1)
    monkeypatch.setattr(phase1, "git", lambda *a: "c" * 40)
    args = crash.parse_args([])
    matrix = {"configs": ["vtop-shipped"], "crash_modes": ["flakey-drop"],
                                                     "crashes": 1, "rate": 1000, "seed": 1,
                                                     "trigger_window_s": [25.0, 55.0], "record_size": 1024}
    monkeypatch.setattr(crash, "shipped_group_commit", lambda: False)
    return crash.capture_environment("r", matrix, "vtop-bench:x", {"cpus": "2"}, host_disk or "EBS", args, FakeHost())


def test_the_crash_environment_carries_the_shared_host_metadata_and_names_the_client(monkeypatch):
    env = capture_env(monkeypatch)
    assert env["host_metadata"] == {"instance_type": "c6i.4xlarge",
                                    "cpu_model": "Intel(R) Xeon(R) Platinum 8375C", "kernel": "Linux 6.8"}
    assert "host_metadata" not in env["comparable"]
    assert "AckProducer" in env["comparable"]["client"] and env["comparable"]["record_size"] == 1024


def test_the_harness_has_no_cpu_probe_of_its_own_any_more():
    assert not hasattr(crash, "cpu_model"), "the CPU model comes from compare_common.hostmeta, as in every harness"
    assert hostmeta.cpu_model.__module__ == "compare_common.hostmeta"


def test_a_host_disk_given_to_crash_py_is_checked_like_the_other_harnesses(monkeypatch):
    def no_docker(*a, **k):
        raise AssertionError("docker was called before the arguments were checked")

    monkeypatch.setattr(crash, "sh", no_docker)
    with pytest.raises(SystemExit, match="--host-disk"):
        crash.main(["--host-disk", "EBS | gp3"])
