"""The host's instance type, CPU model and kernel in every harness's environment.json, and `--host-disk`.

The v0.11.0 AWS round runs the harnesses on named EC2 instances; a result has to say which one. The block is
`host_metadata`, beside `comparable` and outside the environment fingerprint, so a run committed before it
existed loads and regenerates as it did, and a resume never refuses because the metadata service answered
differently the second time.
"""
import importlib
import io
import json
import os
import subprocess
import urllib.error

import pytest
from test_compare_common import make_env

from compare_common import environment, hostmeta, runner, schema
from kafka_compare import regenerate, run
from kafka_compare import schema as kschema


@pytest.fixture
def imds_enabled(monkeypatch):
    monkeypatch.delenv("VTOP_BENCH_NO_IMDS", raising=False)


class Answer(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def imds(token=b"TOKEN", itype=b"c6i.4xlarge", log=None):
    """An opener that answers like IMDSv2: a PUT for a token, then a GET that needs it."""
    def opener(request, timeout=None):
        if log is not None:
            log.append((request.get_method(), request.full_url, dict(request.header_items()), timeout))
        if request.get_method() == "PUT":
            return Answer(token)
        return Answer(itype)
    return opener


# ------------------------------------------------------------------ the instance type


def test_the_instance_type_is_read_with_imdsv2_a_token_first(imds_enabled):
    log = []
    assert hostmeta.instance_type(imds(log=log), system="Linux") == "c6i.4xlarge"
    (put, put_url, put_headers, put_timeout), (get, get_url, get_headers, get_timeout) = log
    assert (put, put_url) == ("PUT", "http://169.254.169.254/latest/api/token")
    assert put_headers["X-aws-ec2-metadata-token-ttl-seconds"] == "60"
    assert (get, get_url) == ("GET", "http://169.254.169.254/latest/meta-data/instance-type")
    assert get_headers["X-aws-ec2-metadata-token"] == "TOKEN"
    assert 0 < put_timeout <= 2 and 0 < get_timeout <= 2, "the lookup must not hold a run up"


@pytest.mark.parametrize("failure", [urllib.error.URLError("no route"), TimeoutError("timed out"),
                                     ConnectionRefusedError("refused"), OSError("unreachable")])
def test_an_unreachable_metadata_service_is_unknown_not_an_error(imds_enabled, failure):
    def opener(request, timeout=None):
        raise failure
    assert hostmeta.instance_type(opener, system="Linux") == "unknown"


def test_a_metadata_service_that_fails_the_second_request_is_unknown(imds_enabled):
    calls = []

    def opener(request, timeout=None):
        calls.append(request.get_method())
        if request.get_method() == "GET":
            raise urllib.error.HTTPError(request.full_url, 401, "Unauthorized", {}, None)
        return Answer(b"TOKEN")
    assert hostmeta.instance_type(opener, system="Linux") == "unknown"
    assert calls == ["PUT", "GET"]


@pytest.mark.parametrize("answer", [b"", b"<html>captive portal</html>", b"c6i", b"c6i.4xlarge\nextra", b"x" * 200])
def test_an_answer_that_is_not_an_instance_type_is_unknown(imds_enabled, answer):
    assert hostmeta.instance_type(imds(itype=answer), system="Linux") == "unknown"


def test_an_empty_token_is_unknown(imds_enabled):
    assert hostmeta.instance_type(imds(token=b""), system="Linux") == "unknown"


@pytest.mark.parametrize("name", ["c6i.4xlarge", "c6i.8xlarge", "m6i.xlarge", "m7g.metal", "u-6tb1.112xlarge",
                                  "mac2-m2.metal", "t3.nano"])
def test_every_instance_type_shape_is_accepted(imds_enabled, name):
    assert hostmeta.instance_type(imds(itype=name.encode()), system="Linux") == name


def test_a_host_that_is_not_linux_does_not_ask(imds_enabled):
    asked = []

    def opener(request, timeout=None):
        asked.append(request)
        return Answer(b"x")
    assert hostmeta.instance_type(opener, system="Darwin") == "unknown"
    assert not asked


def test_the_lookup_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("VTOP_BENCH_NO_IMDS", "1")
    asked = []
    assert hostmeta.instance_type(lambda *a, **k: asked.append(a), system="Linux") == "unknown"
    assert not asked


# ------------------------------------------------------------------ cpu model and kernel


def test_the_cpu_model_is_the_model_name_in_cpuinfo(tmp_path):
    info = tmp_path / "cpuinfo"
    info.write_text("processor\t: 0\nvendor_id\t: GenuineIntel\n"
                    "model name\t: Intel(R) Xeon(R) Platinum 8375C @ 2.90GHz\n"
                    "processor\t: 1\nmodel name\t: Intel(R) Xeon(R) Platinum 8375C @ 2.90GHz\n")
    assert hostmeta.cpu_model(str(info)) == "Intel(R) Xeon(R) Platinum 8375C @ 2.90GHz"


def test_an_arm_cpuinfo_with_only_a_hardware_line_still_names_the_cpu(tmp_path):
    info = tmp_path / "cpuinfo"
    info.write_text("processor\t: 0\nHardware\t: Graviton3\n")
    assert hostmeta.cpu_model(str(info)) == "Graviton3"


def test_without_cpuinfo_the_cpu_comes_from_sysctl_then_the_platform(tmp_path):
    missing = str(tmp_path / "nope")
    assert hostmeta.cpu_model(missing, sysctl=lambda name: "Apple M5" if "brand" in name else "") == "Apple M5"
    assert hostmeta.cpu_model(missing, sysctl=lambda name: "")  # never empty
    assert hostmeta.cpu_model(missing, sysctl=lambda name: "") != ""


def test_the_kernel_names_the_system_and_release(monkeypatch):
    monkeypatch.setattr(hostmeta.platform, "system", lambda: "Linux")
    monkeypatch.setattr(hostmeta.platform, "release", lambda: "6.8.0-1018-aws")
    assert hostmeta.kernel() == "Linux 6.8.0-1018-aws"


def test_collect_returns_the_three_facts(imds_enabled, monkeypatch):
    monkeypatch.setattr(hostmeta.platform, "system", lambda: "Linux")
    monkeypatch.setattr(hostmeta.platform, "release", lambda: "6.8.0-1018-aws")
    monkeypatch.setattr(hostmeta, "cpu_model", lambda: "Intel(R) Xeon(R)")
    assert hostmeta.collect(imds()) == {"instance_type": "c6i.4xlarge", "cpu_model": "Intel(R) Xeon(R)",
                                        "kernel": "Linux 6.8.0-1018-aws"}


# ------------------------------------------------------------------ in every harness's environment.json


def fake_sh(args, **_kwargs):
    if args[:2] == ["docker", "info"]:
        return subprocess.CompletedProcess(args, 0, '{"NCPU": 8}', "")
    if args[:2] == ["docker", "version"]:
        return subprocess.CompletedProcess(args, 0, '{"Server": {}}', "")
    if args[:3] == ["docker", "image", "inspect"]:
        return subprocess.CompletedProcess(args, 0, '[{"Id": "sha256:00"}]', "")
    if args[0] == "git":
        return subprocess.CompletedProcess(args, 0, "", "")
    return subprocess.CompletedProcess(args, 0, "Filesystem Type\n/dev/vda1 ext4 1 1 1 1% /probe\n", "")


FACTS = {"instance_type": "c6i.4xlarge", "cpu_model": "Intel(R) Xeon(R) Platinum 8375C", "kernel": "Linux 6.8.0-aws"}


def test_the_throughput_harness_records_the_host_metadata(monkeypatch):
    monkeypatch.setattr(run, "sh", fake_sh)
    monkeypatch.setattr(hostmeta, "collect", lambda *a, **k: dict(FACTS))
    env = run.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"}, "Test SSD, SSD, NVMe, ext4")
    assert env["host_metadata"] == FACTS
    assert "host_metadata" not in env["comparable"], "it must not change which runs are comparable"


def test_the_failover_harness_records_it_too(monkeypatch):
    from kafka_compare import failover
    monkeypatch.setattr(run, "sh", fake_sh)
    monkeypatch.setattr(failover, "engine_commit", lambda: "e" * 40)
    monkeypatch.setattr(hostmeta, "collect", lambda *a, **k: dict(FACTS))
    env = failover.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"}, "Test SSD, SSD, NVMe, ext4")
    assert env["host_metadata"] == FACTS and env["kind"] == "failover"


def test_the_linux_cpu_is_the_model_not_the_platform_processor_string(monkeypatch):
    monkeypatch.setattr(run, "sh", fake_sh)
    monkeypatch.setattr(run, "_sysctl", lambda name: "")
    monkeypatch.setattr(hostmeta, "collect", lambda *a, **k: dict(FACTS))
    monkeypatch.setattr(hostmeta, "cpu_model", lambda *a, **k: "Intel(R) Xeon(R) Platinum 8375C")
    monkeypatch.setattr(run.platform, "processor", lambda: "x86_64")
    env = run.capture_environment("r", {}, "vtop-bench:x", {"cpus": "2"}, "d")
    assert env["comparable"]["host_cpu"] == "Intel(R) Xeon(R) Platinum 8375C"


def test_the_shared_environment_of_the_cross_product_harnesses_records_it(monkeypatch):
    monkeypatch.setattr(hostmeta, "collect", lambda *a, **k: dict(FACTS))
    comparable = {"host_class": "laptop", "vtop_commit": "a" * 40}
    env = environment.new_environment("testkind", "r1", {}, comparable, {}, telemetry={}, private=False)
    assert env["host_metadata"] == FACTS
    assert "host_metadata" not in env["comparable"]


def test_a_resume_ignores_the_host_metadata(monkeypatch):
    comparable = make_env()["comparable"]
    recorded = {"comparable": dict(comparable), "host_metadata": dict(FACTS)}
    runner.check_resume(recorded, dict(comparable))  # the same comparable block: resumes, whatever IMDS says now
    assert schema.fingerprint(comparable) == schema.fingerprint(dict(comparable))


def test_the_fingerprint_does_not_see_the_block():
    env = {"comparable": {"host_cpu": "x", "vtop_commit": "a" * 40}}
    before = kschema.fingerprint(env)
    env["host_metadata"] = dict(FACTS)
    assert kschema.fingerprint(env) == before


def test_no_committed_kafka_run_has_a_host_metadata_block_and_all_still_load():
    runs = regenerate.committed_runs()
    assert runs
    for run_dir in runs:
        with open(os.path.join(run_dir, "environment.json")) as handle:
            assert "host_metadata" not in json.load(handle), f"{run_dir} was edited after the fact"


# ------------------------------------------------------------------ --host-disk


def test_a_given_host_disk_is_recorded_as_given_and_not_probed(monkeypatch):
    monkeypatch.setattr(run, "probe_host_disk", lambda: pytest.fail("the probe ran although --host-disk was given"))
    assert run.resolve_host_disk("Amazon Elastic Block Store, SSD, NVMe, ext4") == \
        "Amazon Elastic Block Store, SSD, NVMe, ext4"


def test_without_the_flag_the_probe_names_the_disk(monkeypatch):
    monkeypatch.setattr(run, "probe_host_disk", lambda: "probed disk")
    assert run.resolve_host_disk(None) == "probed disk"


@pytest.mark.parametrize("bad", ["", "   ", "EBS | gp3", "EBS\ngp3", "EBS\r\ngp3"])
def test_a_host_disk_the_footer_cannot_carry_is_refused_up_front(bad):
    with pytest.raises(SystemExit, match="--host-disk"):
        run.resolve_host_disk(bad)


@pytest.mark.parametrize("module_name", ["run", "failover"])
def test_the_harnesses_take_host_disk_and_stop_probing(monkeypatch, tmp_path, module_name):
    module = importlib.import_module(f"kafka_compare.{module_name}")

    class Reached(Exception):
        pass

    def stop_at_pull(args, **_kwargs):
        if args[:2] == ["docker", "ps"]:
            return subprocess.CompletedProcess(args, 0, "", "")
        if args[:2] == ["docker", "pull"]:
            raise Reached
        raise AssertionError(f"ran {args}")

    monkeypatch.setattr(run, "sh", stop_at_pull)
    monkeypatch.setattr(module, "sh", stop_at_pull)
    monkeypatch.setattr(run, "probe_host_disk", lambda: pytest.fail("probed"))
    monkeypatch.setattr(module, "check_equal_limits", lambda: {"cpus": "2", "mem_limit": "2g"})
    monkeypatch.setattr(module, "HERE", str(tmp_path))
    with pytest.raises(Reached):
        module.main(["--host-disk", "EBS gp3 NVMe", "--run-id", "x"])


@pytest.mark.parametrize("module_name", ["run", "failover"])
def test_a_bad_host_disk_is_refused_before_any_docker_work(monkeypatch, module_name):
    module = importlib.import_module(f"kafka_compare.{module_name}")

    def no_docker(args, **_kwargs):
        raise AssertionError(f"ran {args}")

    monkeypatch.setattr(run, "sh", no_docker)
    monkeypatch.setattr(module, "sh", no_docker)
    monkeypatch.setattr(module, "check_equal_limits", lambda: {"cpus": "2", "mem_limit": "2g"})
    with pytest.raises(SystemExit, match="--host-disk"):
        module.main(["--host-disk", "a | b", "--run-id", "x"])
