"""The end-to-end consumer's image (#596, #600), without Docker.

The pinned apache/kafka image's JRE has no jdk.compiler, so `java /client/E2eLatency.java` fails there ("Module
jdk.compiler not in boot Layer", the first AWS run). client/Dockerfile compiles the class once with the pinned JDK,
as #599's crash/client/Dockerfile does for its producer, and the runner runs the class in that built image. These
tests hold the commands to that, and hold the preflight that builds and checks it to running before any
cluster or the VTOP image."""
import os
import re
import subprocess

import pytest
from test_kafka_compare_harness_metrics import fake_args

from kafka_compare import crash, run

HERE = os.path.dirname(os.path.abspath(__file__))


def read(*parts):
    with open(os.path.join(run.HERE, *parts), encoding="utf-8") as handle:
        return handle.read()


def test_the_dockerfile_compiles_the_class_with_the_pinned_jdk_onto_the_pinned_kafka_image():
    text = read("client", "Dockerfile")
    assert f"ARG KAFKA_IMAGE={run.KAFKA_IMAGE}\n" in text
    assert f"ARG JDK_IMAGE={run.JDK_IMAGE}\n" in text
    assert "@sha256:" in run.KAFKA_IMAGE and "@sha256:" in run.JDK_IMAGE
    # The same two images as #599's producer, so one pinned JDK serves both harnesses.
    assert run.JDK_IMAGE == crash.JDK_IMAGE and run.KAFKA_IMAGE == crash.KAFKA_IMAGE
    assert re.search(r"^FROM \$\{JDK_IMAGE\} AS build$", text, re.MULTILINE)
    assert 'javac --release 17 -Xlint:all -Werror -cp "/kafka-libs/*" -d /out /src/E2eLatency.java' in text
    assert "COPY --from=kafka /opt/kafka/libs /kafka-libs" in text
    final = text.rsplit("\nFROM ", 1)[1]
    assert final.startswith("${KAFKA_IMAGE}\n") and f"COPY --from=build /out {run.E2E_CLASSES}" in final


def test_the_consumer_command_runs_the_compiled_class_not_a_source_file():
    command = run.e2e_consumer_command("kafka", 1000)
    assert command[:4] == ["java", "-cp", f"/opt/kafka/libs/*:{run.E2E_CLASSES}", "E2eLatency"]
    assert command[4:] == [run.SYSTEM_SPEC["kafka"]["bootstrap"], run.TOPIC, "1000", str(run.E2E_IDLE_MS)]
    assert not any(part.endswith(".java") for part in command)
    assert run.e2e_main_command("--check")[-2:] == ["E2eLatency", "--check"]


def test_the_image_name_is_its_inputs_hash_and_changes_with_its_source(monkeypatch, tmp_path):
    first = run.e2e_image_name()
    assert re.fullmatch(r"kc-e2e-client:[0-9a-f]{12}", first)
    other = tmp_path / "client"
    other.mkdir()
    for name in ("Dockerfile", "E2eLatency.java"):
        (other / name).write_text(read("client", name) + "// changed\n")
    monkeypatch.setattr(run, "CLIENT_DIR", str(other))
    assert run.e2e_image_name() != first


def test_the_image_is_built_once_with_both_pinned_images_as_build_arguments(monkeypatch):
    calls, state = [], {"exists": False}

    def fake_sh(args, **_k):
        calls.append(args)
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 0 if state["exists"] else 1, "", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(run, "sh", fake_sh)
    image = run.ensure_e2e_image()
    builds = [a for a in calls if a[:2] == ["docker", "build"]]
    assert len(builds) == 1 and builds[0][-1] == run.CLIENT_DIR and image in builds[0]
    flat = " ".join(builds[0])
    assert f"KAFKA_IMAGE={run.KAFKA_IMAGE}" in flat and f"JDK_IMAGE={run.JDK_IMAGE}" in flat
    state["exists"], calls[:] = True, []
    assert run.ensure_e2e_image() == image and not [a for a in calls if a[:2] == ["docker", "build"]]
    run.ensure_e2e_image(rebuild=True)
    assert [a for a in calls if a[:2] == ["docker", "build"]]


def preflight_sh(calls, check_out="ok\n", build_fails=False):
    def fake_sh(args, **_k):
        calls.append(args)
        if args[:3] == ["docker", "image", "inspect"]:
            return subprocess.CompletedProcess(args, 1, "", "")
        if args[:2] == ["docker", "build"] and build_fails:
            raise RuntimeError("exit 1: javac: error")
        if "cat" in args:
            return subprocess.CompletedProcess(args, 0, "cpuset cpu io memory pids", "")
        return subprocess.CompletedProcess(args, 0, check_out, "")
    return fake_sh


def test_the_preflight_builds_the_image_and_runs_the_check_in_it(monkeypatch):
    matrix = run.build_matrix(fake_args(), [100], {100: 2000})
    calls = []
    monkeypatch.setattr(run, "sh", preflight_sh(calls))
    image = run.preflight_clients(matrix)
    assert image == run.e2e_image_name()
    build = next(i for i, a in enumerate(calls) if a[:2] == ["docker", "build"])
    check = next(i for i, a in enumerate(calls) if a[-1] == "--check")
    assert build < check
    assert calls[check] == ["docker", "run", "--rm", image, "java", "-cp", run.E2E_CLASSPATH, "E2eLatency", "--check"]
    assert not any(part.endswith(".java") for a in calls for part in a)


@pytest.mark.parametrize("sh_kw, match", [({"build_fails": True}, "could not be built"),
                                          ({"check_out": "Error: A JNI error\n"}, "not ok")])
def test_a_consumer_that_cannot_start_is_refused_naming_no_e2e(monkeypatch, sh_kw, match):
    matrix = run.build_matrix(fake_args(), [100], {100: 2000})
    monkeypatch.setattr(run, "sh", preflight_sh([], **sh_kw))
    with pytest.raises(SystemExit, match=match) as refused:
        run.preflight_clients(matrix)
    assert "--no-e2e" in str(refused.value)


def test_without_the_end_to_end_stage_nothing_is_built(monkeypatch):
    matrix = run.build_matrix(fake_args(no_e2e=True, no_resources=True), [100], {100: 2000})
    calls = []
    monkeypatch.setattr(run, "sh", preflight_sh(calls))
    assert run.preflight_clients(matrix) is None and calls == []


def test_main_runs_the_preflight_before_the_vtop_image_and_any_cluster(monkeypatch):
    order = []

    class Stop(Exception):
        pass

    def preflight(matrix, rebuild=False):
        order.append(("preflight", bool(matrix.get("e2e")), rebuild))
        raise Stop

    monkeypatch.setattr(run, "sh", lambda args, **_k: subprocess.CompletedProcess(args, 0, "", ""))
    monkeypatch.setattr(run, "check_equal_limits", lambda: {"cpus": "2"})
    monkeypatch.setattr(run, "resolve_host_disk", lambda given: "Test SSD")
    monkeypatch.setattr(run, "preflight_clients", preflight)
    monkeypatch.setattr(run, "ensure_vtop_image", lambda rebuild: order.append("vtop image"))
    monkeypatch.setattr(run, "cluster_up", lambda *_a, **_k: order.append("cluster"))
    monkeypatch.setattr(run, "capture_environment", lambda *_a, **_k: order.append("environment"))
    with pytest.raises(Stop):
        run.main(["--run-id", "preflight-order", "--sizes", "100", "--reps", "1", "--rebuild"])
    assert order == [("preflight", True, True)]
    assert not os.path.exists(os.path.join(run.HERE, "results", "preflight-order"))
