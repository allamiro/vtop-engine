"""The driver's pure parts and the lab's compose file (#726)."""

import hashlib
import re
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import soak_driver  # noqa: E402
import soak_lab  # noqa: E402
import soak_profile  # noqa: E402
from soak_record import Recorder, RunState  # noqa: E402

try:  # PyYAML is optional in this harness; the compose tests need it
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

EXPOSITION = """# HELP vtop_commits_total commits
# TYPE vtop_commits_total counter
vtop_commits_total{tenant="soak",source_type="kafka",format="raw"} 7
vtop_commits_total{tenant="soak",source_type="kafka",format="json"} 3
# TYPE vtop_verified_total counter
vtop_verified_total{tenant="soak",source_type="kafka",format="raw"} 10
# TYPE vtop_batch_duration_seconds histogram
vtop_batch_duration_seconds_bucket{tenant="soak",source_type="kafka",format="raw",le="1"} 2
vtop_batch_duration_seconds_sum{tenant="soak",source_type="kafka",format="raw"} 33.5
vtop_batch_duration_seconds_count{tenant="soak",source_type="kafka",format="raw"} 10
# TYPE vtop_something_else_total counter
vtop_something_else_total 99
"""


def test_sampled_families_are_summed_over_labels_and_others_are_left_out():
    totals = soak_driver.summed_families(EXPOSITION)
    assert totals["vtop_commits_total"] == 10
    assert totals["vtop_verified_total"] == 10
    assert totals["vtop_batch_duration_seconds_sum"] == 33.5
    assert totals["vtop_batch_duration_seconds_count"] == 10
    assert "vtop_something_else_total" not in totals
    assert "vtop_failed_total" not in totals  # not emitted yet: absent, not zero


def test_the_evaluator_reads_the_names_the_driver_samples():
    import soak_eval

    for name in (
        soak_eval.M_COMMITS,
        soak_eval.M_VERIFIED,
        soak_eval.M_VERIFY_FAILURES,
        soak_eval.M_BATCH_SUM,
        soak_eval.M_BATCH_COUNT,
    ):
        assert name in soak_driver.SAMPLED_FAMILIES


def test_the_engine_config_follows_the_profile():
    p = soak_profile.load("72h")
    text = soak_driver.render_engine_config(p)
    assert "max_records: 20000" in text and f'consumer_group: "{soak_lab.GROUP}"' in text
    assert "toxiproxy:19094" in text and "http://toxiproxy:19000" in text
    assert "require_strong_verification: true" in text  # the commit rule's strongest form
    if yaml:
        cfg = yaml.safe_load(text)
        assert cfg["batching"]["max_batch_age_seconds"] == 30
        assert cfg["sources"]["kafka"]["topic_include_regex"] == f"^{soak_lab.TOPIC}$"


def test_the_engine_reaches_kafka_and_the_store_only_through_the_proxy():
    text = soak_driver.render_engine_config(soak_profile.load("ci-smoke"))
    hosts = set(re.findall(r"(?:kafka|minio):\d+", text))
    assert not hosts, f"the engine config names a backend directly: {hosts}"


def make_driver(tmp_path, profile):
    rec = Recorder(tmp_path)
    lab = soak_lab.Lab("vtop-t", tmp_path, "img", toxiproxy_port=1)
    return soak_driver.Driver(profile, lab, tmp_path, rec, RunState(), tmp_path), rec


def test_the_schedule_holds_every_planned_fault_and_every_periodic_run(tmp_path):
    p = soak_profile.load("ci-smoke")
    driver, rec = make_driver(tmp_path, p)
    items = driver.schedule()
    rec.close()
    faults = [i for i in items if i[2] == "fault"]
    assert [f[3][0].id for f in faults] == list(range(1, len(faults) + 1))
    assert len(faults) == len(soak_profile.build_plan(p))
    assert sum(1 for i in items if i[2] == "prune") == (p["duration_s"] - 1) // p["prune_interval_s"]
    assert [i[0] for i in items] == sorted(i[0] for i in items)


def test_a_fault_comes_before_periodic_work_due_at_the_same_second(tmp_path):
    p = soak_profile.load("ci-smoke")
    driver, rec = make_driver(tmp_path, p)
    items = driver.schedule()
    rec.close()
    at_60 = [i[2] for i in items if i[0] == 60.0]
    assert at_60 == ["fault", "prune"]


def test_occurrences_count_per_kind_so_the_clock_alternates(tmp_path):
    p = soak_profile.load("72h")
    driver, rec = make_driver(tmp_path, p)
    clock = [i[3] for i in driver.schedule() if i[2] == "fault" and i[3][0].kind == "clock_step"]
    rec.close()
    steps = [soak_profile.clock_step_for(p, occurrence) for _, occurrence in clock]
    assert steps[:4] == [3600, -3600, 3600, -3600]


def test_a_commands_detail_keeps_what_it_said_and_drops_routine_log_lines():
    text = (
        "pruned 3 committed ledger row(s)\n"
        '{"timestamp":"2026-01-01T00:00:00Z","level":"INFO","fields":{"message":"otel disabled"}}\n'
        '{"timestamp":"2026-01-01T00:00:00Z","level":"WARN","fields":{"message":"plaintext endpoint"}}\n'
        '{"timestamp":"2026-01-01T00:00:01Z","level":"ERROR","fields":{"message":"store refused the request"}}\n'
        "error: scrub FAILED: 2 finding(s)"
    )
    kept = soak_driver.tail(text).splitlines()
    assert kept[0] == "pruned 3 committed ledger row(s)" and kept[-1] == "error: scrub FAILED: 2 finding(s)"
    assert len(kept) == 3 and "store refused the request" in kept[1]


def test_the_evidence_tarball_lists_a_checksum_for_every_file(tmp_path):
    run = tmp_path / "soak-x"
    (run / "raw").mkdir(parents=True)
    (run / "samples.jsonl").write_text('{"elapsed_s": 1}\n')
    (run / "raw" / "metrics.txt.gz").write_bytes(b"\x1f\x8b abc")
    out = soak_driver.write_evidence(run, tmp_path / "soak-x.tar.gz")
    with tarfile.open(out) as tar:
        names = tar.getnames()
        sums = tar.extractfile("soak-x/SHA256SUMS").read().decode().splitlines()
    assert "soak-x/samples.jsonl" in names and "soak-x/raw/metrics.txt.gz" in names
    listed = {line.split("  ", 1)[1]: line.split("  ", 1)[0] for line in sums}
    assert set(listed) == {"samples.jsonl", "raw/metrics.txt.gz"}
    assert listed["samples.jsonl"] == hashlib.sha256(b'{"elapsed_s": 1}\n').hexdigest()


def test_preflight_refuses_a_disk_without_the_profiles_free_space(tmp_path, monkeypatch):
    import collections

    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(soak_driver.shutil, "disk_usage", lambda path: usage(500 * 2**30, 400 * 2**30, 100 * 2**30))
    p = soak_profile.load("72h")
    lab = soak_lab.Lab("vtop-t", tmp_path, "img", toxiproxy_port=1, runner=lambda argv, t: soak_lab.Result(0, "x", ""))
    with pytest.raises(soak_lab.LabError, match="needs at least 180 GiB"):
        soak_driver.preflight(p, lab, tmp_path)


def test_preflight_passes_with_the_space_and_the_image(tmp_path, monkeypatch):
    import collections

    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(soak_driver.shutil, "disk_usage", lambda path: usage(500 * 2**30, 100 * 2**30, 400 * 2**30))
    lab = soak_lab.Lab("vtop-t", tmp_path, "img", toxiproxy_port=1, runner=lambda argv, t: soak_lab.Result(0, "x", ""))
    soak_driver.preflight(soak_profile.load("72h"), lab, tmp_path)


def test_preflight_names_the_missing_engine_image(tmp_path):
    p = soak_profile.load("ci-smoke")

    def runner(argv, timeout):
        return soak_lab.Result(1 if "inspect" in argv else 0, "", "no such image")

    lab = soak_lab.Lab("vtop-t", tmp_path, "vtop-engine:soak", toxiproxy_port=1, runner=runner)
    with pytest.raises(soak_lab.LabError, match="vtop-engine:soak does not exist"):
        soak_driver.preflight(p, lab, tmp_path)


@pytest.fixture(scope="module")
def compose():
    if yaml is None:
        pytest.skip("PyYAML is optional in the benchmark harness")
    return yaml.safe_load(soak_lab.COMPOSE_FILE.read_text())


@pytest.mark.skipif(yaml is None, reason="PyYAML is optional in the benchmark harness")
class TestCompose:
    def test_nothing_is_published_except_toxiproxys_admin_port_on_loopback(self, compose):
        published = {name: s["ports"] for name, s in compose["services"].items() if "ports" in s}
        assert list(published) == ["toxiproxy"]
        assert all(p.startswith("127.0.0.1:") for p in published["toxiproxy"])

    def test_the_engine_is_not_restarted_by_docker(self, compose):
        assert compose["services"]["engine"]["restart"] == "no"

    def test_container_names_derive_from_the_project(self, compose):
        for name, service in compose["services"].items():
            assert service["container_name"] == f"${{SOAK_PROJECT:-vtop-soak}}-{name}"

    def test_the_container_names_match_what_the_lab_calls_them(self):
        lab = soak_lab.Lab("p", Path("."), "i", toxiproxy_port=1)
        assert lab.container("engine") == "p-engine"

    def test_the_engine_is_told_the_broker_by_its_proxied_address(self, compose):
        env = compose["services"]["kafka"]["environment"]
        assert "PROXIED://toxiproxy:19094" in env["KAFKA_ADVERTISED_LISTENERS"]

    def test_the_proxy_ports_agree_with_toxiproxy_json_and_the_lab(self):
        import json

        proxies = json.loads((soak_lab.SOAK_DIR / "toxiproxy.json").read_text())
        ports = {p["name"]: int(p["listen"].rsplit(":", 1)[1]) for p in proxies}
        assert ports == soak_lab.PROXY_PORTS

    def test_the_engine_mounts_its_config_and_the_shim_control_directory(self, compose):
        mounts = " ".join(compose["services"]["engine"]["volumes"])
        assert "/soak/config:ro" in mounts and ":/soak/ctl" in mounts

    def test_every_soak_variable_the_compose_file_reads_is_one_the_lab_sets(self):
        text = soak_lab.COMPOSE_FILE.read_text()
        # SOAK_CPUS_* are the operator's own knobs: read from the environment
        # the driver inherits, never set by it.
        used = {v for v in re.findall(r"\$\{(SOAK_[A-Z_]+)", text) if not v.startswith("SOAK_CPUS_")}
        lab = soak_lab.Lab(
            "p",
            Path("/r"),
            "i",
            toxiproxy_port=1,
            data_dir=Path("/d"),
            load={"SOAK_RATE": 1, "SOAK_RECORD_BYTES": 1, "SOAK_ENGINE_MEM": "1g"},
        )
        assert used <= set(lab.compose_env), used - set(lab.compose_env)
