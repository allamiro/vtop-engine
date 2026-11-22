"""The lab's parsers and the calls it makes, with no Docker (#726)."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import soak_lab  # noqa: E402

DESCRIBE = """
Consumer group 'vtop-soak' has no active members.

GROUP      TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST  CLIENT-ID
vtop-soak  soak-topic   0          1200            1500            300  -            -     -
vtop-soak  soak-topic   1          -               900             -    -            -     -
vtop-soak  other-topic  0          7               9               2    -            -     -
"""


def test_parse_probe_reads_the_engine_process():
    text = "comm vtopctl\nrss_kb 204800\nfds 61\nwork_kb 1024\nledger_bytes 262144\n"
    assert soak_lab.parse_probe(text) == {
        "rss_bytes": 204800 * 1024,
        "open_fds": 61,
        "work_dir_bytes": 1024 * 1024,
        "ledger_bytes": 262144,
    }


def test_parse_probe_refuses_a_pid_one_that_is_not_the_engine():
    with pytest.raises(ValueError, match="not vtopctl"):
        soak_lab.parse_probe("comm sh\nrss_kb 1\nfds 1\nwork_kb 1\nledger_bytes 1\n")


def test_parse_probe_names_what_is_missing():
    with pytest.raises(ValueError, match="rss_kb"):
        soak_lab.parse_probe("comm vtopctl\nfds 1\n")


def test_parse_describe_reads_committed_and_end_per_partition_of_the_topic():
    rows = soak_lab.parse_describe(DESCRIBE, "vtop-soak", "soak-topic")
    assert rows == {0: {"committed": 1200, "end": 1500}, 1: {"committed": None, "end": 900}}


def test_parse_describe_treats_a_body_without_the_table_as_an_error_not_an_empty_answer():
    with pytest.raises(ValueError, match="no offsets table"):
        soak_lab.parse_describe(
            "Error: Executing consumer group command failed due to timeout", "vtop-soak", "soak-topic"
        )


def test_lag_totals_sums_and_requires_every_partition():
    rows = {0: {"committed": 10, "end": 20}, 1: {"committed": None, "end": 5}}
    assert soak_lab.lag_totals(rows, 2) == {"end": 25, "committed": 10}
    with pytest.raises(ValueError, match="expected 3 partitions"):
        soak_lab.lag_totals(rows, 3)


def test_parse_json_report_finds_the_object_after_log_lines():
    text = 'INFO starting\n{\n  "checked": 4,\n  "passed": true,\n  "findings": []\n}\n'
    assert soak_lab.parse_json_report(text)["checked"] == 4
    with pytest.raises(ValueError, match="no JSON report"):
        soak_lab.parse_json_report("just logs")


def test_scrub_summary_counts_findings():
    s = soak_lab.scrub_summary(
        {"mode": "all", "checked": 12, "committed_rows": 12, "passed": False, "findings": [{}, {}]}
    )
    assert s == {"report_mode": "all", "batches_checked": 12, "committed_rows": 12, "findings": 2, "passed": False}


def test_audit_gaps_finds_the_stretches_the_audit_printed_without_failing_over():
    text = (
        "audited 12 ledger row(s) and 0 digested row(s) from 0 digest file(s); 2 source unit(s)\n"
        "ok   kafka soak-topic[0] group vtop-soak: 6 committed range(s) over [0, 10875), 0 duplicated\n"
        "     gap [4000, 6000): no records archived (compaction or transaction markers can leave such gaps)\n"
        "ok   kafka soak-topic[1] group vtop-soak: 6 committed range(s) over [0, 10000), 0 duplicated\n"
        "     gap [10, 12): no records archived (compaction or transaction markers can leave such gaps)\n"
        "coverage audit PASSED\n"
    )
    assert soak_lab.audit_gaps(text) == [
        "gap [4000, 6000): no records archived (compaction or transaction markers can leave such gaps)",
        "gap [10, 12): no records archived (compaction or transaction markers can leave such gaps)",
    ]
    clean = (
        "ok   kafka soak-topic[0] group vtop-soak: 6 committed range(s) over [0, 10875), 0 dup\ncoverage audit PASSED\n"
    )
    assert soak_lab.audit_gaps(clean) == []


REPORT = (
    "audited 19 ledger row(s) and 105 digested row(s) from 8 digest file(s); 2 source unit(s)\n"
    "ok   kafka soak-topic[0] group vtop-soak: 46 committed range(s) over [0, 81348), 0 duplicated\n"
    "ok   kafka soak-topic[1] group vtop-soak: 46 committed range(s) over [7, 79943), 3 duplicated\n"
    "ok   kafka other-topic[0] group vtop-soak: 1 committed range(s) over [0, 5), 0 duplicated\n"
    "coverage audit PASSED\n"
)


def test_audit_ranges_reads_the_span_judged_for_each_partition_of_the_topic():
    assert soak_lab.audit_ranges(REPORT, "soak-topic") == {0: (0, 81348), 1: (7, 79943)}


def test_coverage_problems_names_a_prefix_the_audit_did_not_judge_and_a_short_end():
    spans = soak_lab.audit_ranges(REPORT, "soak-topic")
    assert soak_lab.coverage_problems(spans, 2, None) == [
        "partition 1: the archive starts at offset 7, not 0; [0, 7) is unjudged"
    ]
    problems = soak_lab.coverage_problems({0: (0, 100), 1: (0, 90)}, 2, {0: 100, 1: 95})
    assert problems == ["partition 1: the archive ends at offset 90, the partition at 95"]
    assert soak_lab.coverage_problems({0: (0, 100)}, 2, None) == ["partition 1: the audit reported no committed range"]
    assert soak_lab.coverage_problems({0: (0, 100), 1: (0, 90)}, 2, {0: 100, 1: 90}) == []


def test_parse_pruned_reads_the_status_or_the_sentence():
    assert soak_lab.parse_pruned('{"status": "ok", "deleted": 17, "digest": null}') == 17
    assert soak_lab.parse_pruned("pruned 3 committed ledger row(s); digest /x; audit record a") == 3
    assert soak_lab.parse_pruned("garbage") is None


def test_the_shim_source_is_the_one_live_chaos_scenario_07_embeds():
    source = soak_lab.extract_skew_shim_source()
    assert "VTOP_SKEW_SECONDS" in source and "clock_gettime" in source and "VTOP_SKEW_LOADED_MARKER" in source
    scenario = soak_lab.SKEW_SCENARIO.read_text()
    assert source.strip() in scenario


def test_a_scenario_without_the_shim_is_an_error(tmp_path):
    other = tmp_path / "x.sh"
    other.write_text("echo nothing here\n")
    with pytest.raises(soak_lab.LabError, match="cannot find the clock-skew shim"):
        soak_lab.extract_skew_shim_source(other)


def test_shim_names_match_what_the_dockerfile_builds():
    assert soak_lab.shim_name(3600) == "skew+3600.so"
    assert soak_lab.shim_name(-3600) == "skew-3600.so"
    dockerfile = (soak_lab.SOAK_DIR / "Dockerfile.engine").read_text()
    assert "skew$(printf '%+d'" in dockerfile


class Recorder:
    def __init__(self, replies=None):
        self.calls = []
        self.replies = replies or {}

    def __call__(self, argv, timeout):
        self.calls.append(list(argv))
        for key, reply in self.replies.items():
            if key in " ".join(map(str, argv)):
                return reply
        return soak_lab.Result(0, "", "")


def make_lab(tmp_path, runner=None, http=None, data_dir=None):
    (tmp_path / "ctl").mkdir(exist_ok=True)
    return soak_lab.Lab(
        "vtop-t",
        tmp_path,
        "img:1",
        toxiproxy_port=18999,
        data_dir=data_dir,
        load={"SOAK_RATE": 300},
        runner=runner or Recorder(),
        sleep=lambda s: None,
        http=http,
    )


def test_compose_calls_carry_the_project_the_run_directory_and_the_load(tmp_path):
    runner = Recorder()
    lab = make_lab(tmp_path, runner)
    lab.up("kafka", "minio")
    argv = runner.calls[0]
    assert argv[0] == "env"
    assert "SOAK_PROJECT=vtop-t" in argv and f"SOAK_RUN_DIR={tmp_path}" in argv and "SOAK_RATE=300" in argv
    assert argv[argv.index("-p") + 1] == "vtop-t"
    assert argv[-4:] == ["up", "-d", "--wait", "kafka"] or argv[-2:] == ["kafka", "minio"]


def test_the_engines_data_directories_are_handed_to_the_user_the_image_runs_as(tmp_path):
    runner = Recorder()
    make_lab(tmp_path, runner).prepare_data_dirs()
    argv = runner.calls[0]
    assert "run" in argv and "chown" in argv and "10001:10001" in argv
    assert argv[-2:] == ["/data/state", "/data/work"]


def test_a_subnet_is_given_to_the_lab_network_by_an_override_file(tmp_path):
    runner = Recorder()
    lab = soak_lab.Lab("vtop-t", tmp_path, "img:1", toxiproxy_port=1, subnet="10.99.1.0/24", runner=runner)
    lab.up("kafka")
    argv = runner.calls[0]
    override = str(tmp_path / "network.override.yml")
    assert argv.count("-f") == 2 and override in argv
    assert "subnet: 10.99.1.0/24" in (tmp_path / "network.override.yml").read_text()
    runner2 = Recorder()
    soak_lab.Lab("vtop-t", tmp_path, "img:1", toxiproxy_port=1, runner=runner2).up("kafka")
    assert runner2.calls[0].count("-f") == 1


def test_a_data_dir_turns_the_volumes_into_bind_mounts(tmp_path):
    env = make_lab(tmp_path, data_dir=tmp_path / "d").compose_env
    assert env["SOAK_MINIO_DATA"] == str(tmp_path / "d" / "minio")
    assert "SOAK_MINIO_DATA" not in make_lab(tmp_path).compose_env


def test_state_parses_inspect_output(tmp_path):
    runner = Recorder({"inspect": soak_lab.Result(0, "false|137|2026-01-01T00:00:00Z||false\n", "")})
    state = make_lab(tmp_path, runner).state("engine")
    assert state == {
        "running": False,
        "exit_code": 137,
        "started_at": "2026-01-01T00:00:00Z",
        "health": "",
        "oom_killed": False,
    }


def test_state_is_empty_for_a_container_that_does_not_exist(tmp_path):
    runner = Recorder({"inspect": soak_lab.Result(1, "", "No such object")})
    assert make_lab(tmp_path, runner).state("engine") == {}


def test_set_preload_writes_and_clears_the_file_the_entrypoint_reads(tmp_path):
    lab = make_lab(tmp_path)
    lab.set_preload(-3600)
    assert (tmp_path / "ctl" / "ld_preload").read_text().strip() == "/opt/soak/shims/skew-3600.so"
    lab.set_preload(None)
    assert not (tmp_path / "ctl" / "ld_preload").exists()


def test_skew_marker_is_read_inside_the_engine_container_as_the_user_that_wrote_it(tmp_path):
    # The shim makes the file 0600 as the image's user; on a Linux host the
    # driver's user cannot read it, so the host never opens it.
    runner = Recorder({"cat": soak_lab.Result(0, "1700003600\n", "")})
    lab = make_lab(tmp_path, runner)
    (tmp_path / "ctl" / "skew-loaded").write_text("999")
    (tmp_path / "ctl" / "skew-loaded").chmod(0)
    assert lab.skew_marker() == 1700003600
    assert runner.calls[-1][:3] == ["docker", "exec", "vtop-t-engine"]
    assert runner.calls[-1][3:] == ["cat", "/soak/ctl/skew-loaded"]


def test_skew_marker_is_none_when_no_shim_loaded_or_the_file_is_unreadable(tmp_path):
    absent = Recorder({"cat": soak_lab.Result(1, "", "cat: /soak/ctl/skew-loaded: No such file or directory")})
    assert make_lab(tmp_path, absent).skew_marker() is None
    garbled = Recorder({"cat": soak_lab.Result(0, "not a number", "")})
    assert make_lab(tmp_path, garbled).skew_marker() is None


def test_clearing_the_marker_tolerates_a_file_the_host_user_may_not_remove(tmp_path):
    lab = make_lab(tmp_path)
    (tmp_path / "ctl" / "skew-loaded").write_text("1")
    lab.clear_skew_marker()
    assert not (tmp_path / "ctl" / "skew-loaded").exists()
    lab.clear_skew_marker()  # absent: no error


def test_toxiproxy_calls_go_to_the_proxy_api_and_a_refusal_is_an_error(tmp_path):
    seen = []

    def http(method, path, body):
        seen.append((method, path, body))
        return (200, json.dumps({"enabled": False})) if method == "GET" else (200, "{}")

    lab = make_lab(tmp_path, http=http)
    lab.set_proxy("kafka", False)
    assert seen[0] == ("POST", "/proxies/kafka", b'{"enabled": false}')
    assert lab.proxy_enabled("kafka") is False
    refusing = make_lab(tmp_path, http=lambda m, p, b: (404, "proxy not found"))
    with pytest.raises(soak_lab.LabError, match="proxy not found"):
        refusing.set_proxy("kafka", False)


def test_engine_reachability_is_a_tcp_connect_from_inside_the_engine_container(tmp_path):
    runner = Recorder()
    assert make_lab(tmp_path, runner).engine_can_reach("minio") is True
    assert runner.calls[0][:3] == ["docker", "exec", "vtop-t-engine"]
    assert runner.calls[0][3:5] == ["nc", "-z"] and runner.calls[0][-1] == "19000"
    failing = Recorder({"nc": soak_lab.Result(1, "", "")})
    assert make_lab(tmp_path, failing).engine_can_reach("minio") is False


def test_a_failed_lab_call_says_what_failed_and_what_it_printed(tmp_path):
    runner = Recorder({"start": soak_lab.Result(1, "", "Error response from daemon: boom")})
    with pytest.raises(soak_lab.LabError, match="docker start engine failed.*boom"):
        make_lab(tmp_path, runner).start_engine()


def test_the_subprocess_runner_reports_a_timeout_as_124_and_a_missing_binary_as_127():
    timed_out = soak_lab.subprocess_runner(["sleep", "5"], 0.2)
    assert timed_out.code == 124 and "timed out" in timed_out.err
    assert soak_lab.subprocess_runner(["definitely-not-a-binary-xyz"], 5).code == 127


def test_the_tmpfs_goes_over_the_work_directory_in_the_engines_mount_namespace_only(tmp_path):
    runner = Recorder()
    make_lab(tmp_path, runner).mount_work_tmpfs()
    argv = runner.calls[0]
    assert argv[:3] == ["docker", "run", "--rm"]
    assert "--privileged" in argv and "--network" in argv  # the helper, not the engine, holds the privilege
    assert argv[argv.index("--pid") + 1] == "container:vtop-t-engine"
    assert argv[argv.index("--entrypoint") + 1] == "nsenter" and "img:1" in argv
    assert argv[argv.index("--") + 1 :] == [
        "mount",
        "-t",
        "tmpfs",
        "-o",
        f"size={soak_lab.ENOSPC_TMPFS_BYTES},uid=10001,gid=10001,mode=0700",
        "tmpfs",
        "/data/work",
    ]


def test_lifting_the_tmpfs_is_a_lazy_unmount_in_the_same_namespace(tmp_path):
    runner = Recorder()
    make_lab(tmp_path, runner).unmount_work_tmpfs()
    argv = runner.calls[0]
    assert argv[argv.index("--") + 1 :] == ["umount", "-l", "/data/work"]
    assert argv[argv.index("-t") + 1 :][:2] == ["1", "-m"]


def test_a_refused_mount_is_an_error_that_says_what_it_printed(tmp_path):
    runner = Recorder({"nsenter": soak_lab.Result(1, "", "mount: permission denied")})
    with pytest.raises(soak_lab.LabError, match="permission denied"):
        make_lab(tmp_path, runner).mount_work_tmpfs()


def test_the_mount_depth_counts_the_mounts_on_the_work_directory_in_the_engine(tmp_path):
    runner = Recorder({"mountinfo": soak_lab.Result(0, "2\n", "")})
    assert make_lab(tmp_path, runner).work_mount_depth() == 2
    assert runner.calls[0][:3] == ["docker", "exec", "vtop-t-engine"]
    assert '"/data/work"' in runner.calls[0][-1] and "/proc/self/mountinfo" in runner.calls[0][-1]


def test_the_filler_and_the_probe_run_inside_the_engine_container(tmp_path):
    runner = Recorder()
    lab = make_lab(tmp_path, runner)
    lab.fill_work()
    lab.probe_work_write()
    assert runner.calls[0][:4] == ["docker", "exec", "vtop-t-engine", "dd"]
    assert "of=/data/work/.soak-filler" in runner.calls[0]
    assert runner.calls[1][:4] == ["docker", "exec", "vtop-t-engine", "sh"]
    assert "dd if=/dev/zero of=/data/work/.soak-probe" in runner.calls[1][-1]  # dash's printf says "I/O error"


def test_the_engines_log_is_read_from_the_daemons_own_clock(tmp_path):
    runner = Recorder(
        {"info": soak_lab.Result(0, "2026-01-01T00:00:00.5Z\n", ""), "logs": soak_lab.Result(0, "out", "err")}
    )
    lab = make_lab(tmp_path, runner)
    since = lab.daemon_time()
    assert since == "2026-01-01T00:00:00.5Z"
    assert lab.engine_log_since(since) == "out\nerr"
    assert runner.calls[-1] == ["docker", "logs", "--since", since, "vtop-t-engine"]
