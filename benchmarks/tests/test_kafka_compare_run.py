"""The runner's guards that need no Docker (#596): what it refuses before it
starts, and what it never does to a Docker host it shares."""
import subprocess

import pytest

from kafka_compare import perftool, run


def test_the_filesystem_probe_uses_an_anonymous_volume_it_cannot_share():
    command = run.volume_filesystem_command()
    assert command[:3] == ["docker", "run", "--rm"]
    mount = command[command.index("--mount") + 1]
    assert mount == "type=volume,destination=/probe"  # no source: anonymous, removed by --rm
    assert "-v" not in command and "volume" not in command[1:2]


def test_capturing_the_environment_never_removes_a_volume(monkeypatch):
    calls = []

    def fake_sh(args, **_kwargs):
        calls.append(args)
        out = {"info": '{"NCPU": 8}', "version": '{"Server": {}}', "inspect": '[{"Id": "sha256:00"}]'}
        if args[:2] == ["docker", "info"]:
            return subprocess.CompletedProcess(args, 0, out["info"], "")
        if args[:2] == ["docker", "version"]:
            return subprocess.CompletedProcess(args, 0, out["version"], "")
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0, out["inspect"], "")
        if args[0] == "git":
            return subprocess.CompletedProcess(args, 0, "", "")
        return subprocess.CompletedProcess(args, 0, "Filesystem Type\n/dev/vda1 ext4 1 1 1 1% /probe\n", "")

    monkeypatch.setattr(run, "sh", fake_sh)
    env = run.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"})
    assert env["comparable"]["volume_filesystem"] == "/dev/vda1 ext4"
    assert not [args for args in calls if args[:3] == ["docker", "volume", "rm"]]
    assert not [args for args in calls if any("kc-fsprobe" in part for part in args)]


def test_host_memory_falls_back_to_sysconf_without_the_macos_sysctl(monkeypatch):
    monkeypatch.setattr(run, "_sysctl", lambda _name: "")
    monkeypatch.setattr(run.os, "sysconf", lambda name: {"SC_PHYS_PAGES": 1000, "SC_PAGE_SIZE": 4096}[name])
    assert run.host_memory_bytes() == str(1000 * 4096)


def test_host_memory_is_empty_rather_than_wrong_when_nothing_answers(monkeypatch):
    def no_sysconf(_name):
        raise ValueError("unsupported")

    monkeypatch.setattr(run, "_sysctl", lambda _name: "")
    monkeypatch.setattr(run.os, "sysconf", no_sysconf)
    assert run.host_memory_bytes() == ""


@pytest.mark.parametrize("run_id", ["../outside", "/tmp/outside", "a/b", "..", ".hidden", "", "x" * 65])
def test_a_run_id_must_be_one_path_component(run_id):
    with pytest.raises(SystemExit, match="one path component"):
        run.check_run_id(run_id)


def test_a_timestamp_run_id_is_accepted():
    assert run.check_run_id("20261109T232328Z") == "20261109T232328Z"


@pytest.mark.parametrize("argv, match", [
    (["--reps", "0"], "--reps"),
    (["--sizes", "100,100"], "distinct"),
    (["--sizes", "0"], "positive"),
    (["--sizes", "big"], "--sizes"),
    (["--sat-records", "100:0,1024:5"], "must be positive"),
    (["--sat-records", "100:many,1024:5"], "SIZE:COUNT"),
    (["--sat-records", "100:5"], "no count for record sizes"),
    (["--fixed-seconds", "nan"], "--fixed-seconds"),
    (["--fixed-seconds", "inf"], "--fixed-seconds"),
    (["--fixed-seconds", "0"], "--fixed-seconds"),
    (["--sat-timeout", "-1"], "--sat-timeout"),
    (["--smoke-records", "0"], "--smoke-records"),
    (["--run-id", "../escape"], "one path component"),
])
def test_matrix_arguments_are_refused_before_any_docker_work(monkeypatch, argv, match):
    def no_docker(args, **_kwargs):
        raise AssertionError(f"ran {args[:3]} before refusing the arguments")

    monkeypatch.setattr(run, "sh", no_docker)
    with pytest.raises(SystemExit, match=match):
        run.main(argv)


def test_a_dirty_engine_tree_is_refused_not_benchmarked_as_head(monkeypatch):
    def fake_git(*args):
        if args[0] == "status":
            assert "--" in args and "crates" in args
            return " M crates/vtop-node/src/client.rs"
        raise AssertionError(f"git {args} after the tree was found dirty")

    monkeypatch.setattr(run, "git", fake_git)
    monkeypatch.setattr(run, "sh", lambda *_a, **_k: pytest.fail("built or inspected an image"))
    with pytest.raises(SystemExit, match="build inputs differ from HEAD"):
        run.ensure_vtop_image(rebuild=False)


def test_a_clean_engine_tree_uses_the_head_image(monkeypatch):
    monkeypatch.setattr(run, "git", lambda *args: "" if args[0] == "status" else "abcdef123456")
    monkeypatch.setattr(run, "sh", lambda args, **_k: subprocess.CompletedProcess(args, 0, "", ""))
    assert run.ensure_vtop_image(rebuild=False) == "vtop-bench:abcdef123456"


CONSUMER_OUTPUT = """\
start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg, nMsg.sec, rebalance.time.ms, \
fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
2026-11-09 23:14:25:820, 2026-11-09 23:14:26:927, 14.3477, 12.9609, 150447, 135905.1491, 774, 333, 43.0863, \
451792.7928
"""


@pytest.mark.parametrize("returncode, status", [(0, "ok"), (1, "failed")])
def test_a_consumer_that_exits_nonzero_is_failed_even_with_a_summary(monkeypatch, tmp_path, returncode, status):
    rows = []
    produced = perftool.ProducerResult(1000, 1000.0, 1.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, True)
    monkeypatch.setattr(run, "cluster_up", lambda *_a: None)
    monkeypatch.setattr(run, "cluster_down", lambda *_a: None)
    monkeypatch.setattr(run, "smoke", lambda *_a: None)
    monkeypatch.setattr(run, "_save_leader_log", lambda *_a: None)
    monkeypatch.setattr(run, "run_producer", lambda *_a, **_k: (produced, "", 1.0, ""))
    monkeypatch.setattr(run, "client", lambda *_a, **_k: subprocess.CompletedProcess([], returncode,
                                                                                    CONSUMER_OUTPUT, ""))
    monkeypatch.setattr(run.schema, "append_row", lambda _path, row: rows.append(dict(row)))
    ctx = {"run_id": "r", "run_dir": str(tmp_path), "csv": str(tmp_path / "results.csv"), "fingerprint": "f",
           "env": {"comparable": {"vtop_commit": "a" * 40}, "observed": {}, "events": []},
           "vtop_image": "vtop-bench:x", "reps": 1, "sat_records": {100: 150447}, "fixed_seconds": 1.0,
           "smoke_records": 1, "sat_timeout": 10.0, "consume": True}
    run.run_cell(ctx, "vtop", 100, {25: 100, 50: 200, 75: 300})
    consume = [row for row in rows if row["mode"] == "consume"]
    assert [row["status"] for row in consume] == [status]
    if status == "failed":
        assert "exited nonzero" in consume[0]["note"]
