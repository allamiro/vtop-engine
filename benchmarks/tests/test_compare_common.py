"""The shared tests of the cross-product comparisons (#744 section 8), on compare_common itself.

Each kind's own test file runs the kind-specific halves (its compose limits,
its telemetry settings, its truncated run) against its real configuration.
"""
from __future__ import annotations

import asyncio
import gzip
import json
import os
import re
import subprocess

import pytest

from compare_common import cgroup, checker, colours, kills, registry, runner, schema, store, tap
from compare_common import plan as plans
from compare_common import report as creport

# --------------------------------------------------------------- a test kind

KIND = schema.Kind(
    name="testkind",
    family="archive",
    use_cases=("a",),
    variant_columns=("size",),
    metric_columns=("records_per_sec", "gaps", "sustained"),
    cells={"smoke": "correctness", "drain": "performance", "crash-K1": "correctness"},
    profiles={"connect-lenses": ("exactly-once",), "vtop": ("strong", "verify-off"), "vector": ("acks",),
              "confluent-s3": ("exactly-once",)},
    forbidden=lambda system, profile, cell: ("verify-off is never run in a crash cell"
                                             if profile == "verify-off" and cell.startswith("crash") else None),
)

PLAN = {
    "kind": "testkind",
    "name": "t",
    "host_class": "laptop",
    "systems": ["connect-lenses", "vtop", "vector"],
    "profiles": {"connect-lenses": ["exactly-once"], "vtop": ["strong"], "vector": ["acks"]},
    "cells": [
        {"cell": "smoke", "repetitions": 1, "minutes_per_run": 2, "variants": [{"size": 100}]},
        {"cell": "drain", "repetitions": 2, "minutes_per_run": 10, "variants": [{"size": 100}, {"size": 1024}]},
    ],
}

CONFIG_TEXT = {"connect-lenses": "connect: 1\n", "vtop": "vtop: 1\n", "vector": "vector: 1\n"}


def make_env(run_id="r1", host_class="laptop", plan=None, state="complete"):
    comparable = {"host_class": host_class, "vtop_commit": "abc123", "host_cpu": "test"}
    configs = {system: {profile: {"path": f"x/{system}", "sha256": schema.sha256_text(CONFIG_TEXT[system]),
                                  "text": CONFIG_TEXT[system]}
                        for profile in ("exactly-once", "strong", "verify-off", "acks")}
               for system in CONFIG_TEXT}
    env = {"schema_version": 1, "kind": "testkind", "run_id": run_id, "run_state": state,
           "started_at": "2026-11-22T00:00:00+00:00", "private": False, "store": "MinIO",
           "comparable": comparable, "plan": plan or PLAN, "configs": configs, "telemetry": {}, "events": []}
    return env


def write_run(tmp_path, rows_override=None, env=None, drop=None):
    env = env or make_env()
    run_dir = tmp_path / env["run_id"]
    run_dir.mkdir()
    with open(run_dir / "environment.json", "w") as handle:
        json.dump(env, handle)
    csv_path = str(run_dir / "testkind.csv")
    for item in plans.schedule(env["plan"], KIND):
        if drop and drop(item):
            continue
        row = runner.base_row(env, item, system_version="1", image_digest="sha256:x",
                              config_sha256=schema.sha256_text(CONFIG_TEXT[item.system]))
        row.update({"status": "ok", "note": "", "started_at": "t", "wall_seconds": "1",
                    "records_per_sec": {"connect-lenses": 100, "vtop": 50, "vector": 200}[item.system],
                    "gaps": 0, "sustained": "true"})
        if rows_override:
            rows_override(row)
        schema.append_row(csv_path, KIND, row)
    return str(run_dir)


# ---------------------------------------------------------------- 1. checker


def _obj(lines, gz=True):
    data = "".join(line + "\n" for line in lines).encode()
    return gzip.compress(data, mtime=0) if gz else data


def _line(seq, tag="tst1"):
    return json.dumps({"id": checker.make_id(tag, seq), "pad": "x" * 10})


def _fixture(**faults):
    objects = {"a/1.jsonl.gz": _obj([_line(i) for i in range(0, 50)]),
               "a/2.jsonl.gz": _obj([_line(i) for i in range(50, 100)])}
    incomplete = []
    if faults.get("gap"):
        objects["a/2.jsonl.gz"] = _obj([_line(i) for i in range(50, 100) if i != 77])
    if faults.get("duplicate"):
        objects["a/3.jsonl.gz"] = _obj([_line(42)])
    if faults.get("torn"):
        body = _obj([_line(i) for i in range(100, 110)])
        objects["a/4.jsonl.gz"] = body[: len(body) // 2]
    if faults.get("orphan"):
        objects[".temp-upload/a/9"] = b"partial"
        incomplete.append("a/5.jsonl.gz")
    if faults.get("flipped"):
        body = bytearray(objects["a/1.jsonl.gz"])
        body[len(body) // 2] ^= 0x10
        objects["a/1.jsonl.gz"] = bytes(body)
    return checker.Snapshot(objects, {}, incomplete)


def _check(snapshot, expected=range(100)):
    return checker.check(snapshot, expected, "tst1", is_orphan=lambda key: key.startswith(".temp-upload/"))


def test_checker_reports_nothing_on_the_clean_fixture():
    result = _check(_fixture())
    assert (result.gaps, result.duplicates, result.torn_objects, result.orphan_objects, result.unexpected) == \
        (0, 0, 0, 0, 0)
    assert result.present_unique == result.expected == 100


def test_checker_detects_an_injected_gap():
    result = _check(_fixture(gap=True))
    assert (result.gaps, result.duplicates, result.torn_objects, result.orphan_objects) == (1, 0, 0, 0)
    assert result.gap_sample == [77]


def test_checker_detects_an_injected_duplicate():
    result = _check(_fixture(duplicate=True))
    assert (result.gaps, result.duplicates, result.torn_objects, result.orphan_objects) == (0, 1, 0, 0)


def test_checker_detects_an_injected_torn_object():
    result = _check(_fixture(torn=True), expected=range(110))
    assert result.torn_objects == 1 and result.torn_keys == ["a/4.jsonl.gz"]
    assert result.duplicates == 0 and result.orphan_objects == 0


def test_checker_detects_an_injected_orphan_object():
    result = _check(_fixture(orphan=True))
    assert result.orphan_objects == 2
    assert result.orphan_keys == [".temp-upload/a/9", "incomplete-upload:a/5.jsonl.gz"]
    assert (result.gaps, result.duplicates, result.torn_objects) == (0, 0, 0)


def test_checker_detects_an_injected_flipped_byte():
    result = _check(_fixture(flipped=True))
    assert result.torn_objects == 1 and result.torn_keys == ["a/1.jsonl.gz"]
    assert result.gaps == 50  # the object's records cannot be read, so they are missing


def test_checker_counts_a_partial_last_line_as_torn_and_reads_a_wrapped_value():
    wrapped = json.dumps(json.dumps({"id": checker.make_id("tst1", 1)}))  # a JSON string holding JSON
    snapshot = checker.Snapshot({"k": _obj([wrapped]) + b"", "t": b'{"id":"vtb-tst1-00'}, {}, [])
    result = checker.check(snapshot, [1], "tst1")
    assert result.present_unique == 1 and result.torn_keys == ["t"]


def test_checker_counts_another_runs_ids_as_unexpected():
    snapshot = checker.Snapshot({"k": _obj([_line(1, tag="zzz9")])}, {}, [])
    assert checker.check(snapshot, [1], "tst1").unexpected == 1


def test_checker_keeps_the_earliest_last_modified_per_id():
    snapshot = checker.Snapshot({"a": _obj([_line(1)]), "b": _obj([_line(1)])}, {"a": 20.0, "b": 10.0}, [])
    assert checker.check(snapshot, [1], "tst1").first_visible == {1: 10.0}


def test_snapshot_reads_a_mirror_and_mc_timestamps(tmp_path):
    (tmp_path / "p").mkdir()
    (tmp_path / "p" / "x.gz").write_bytes(b"abc")
    listing = [{"type": "file", "key": "p/x.gz", "lastModified": "2026-11-22T10:00:00.123456789Z"}]
    snap = checker.snapshot_from_dir(str(tmp_path), listing, ["u"])
    assert snap.objects == {"p/x.gz": b"abc"}
    assert snap.last_modified["p/x.gz"] == pytest.approx(1795341600.123457, abs=1e-5)
    for text, frac in (("2026-11-22T10:00:00.25Z", 0.25), ("2026-11-22T10:00:00Z", 0.0),
                       ("2026-11-22T10:00:00.5+00:00", 0.5), ("2026-11-22T10:00:00.1234Z", 0.1234)):
        assert checker.parse_time(text) == pytest.approx(1795341600 + frac, abs=1e-6), text


# --------------------------------------------------------------- 2-3 registry


def test_the_committed_registry_passes_its_checks():
    registry.check(registry.load())


def test_the_subset_parser_reads_the_committed_registry_as_tomllib_does():
    tomllib = pytest.importorskip("tomllib")
    with open(registry.PRODUCTS, "rb") as handle:
        data = handle.read()
    assert registry.parse_subset(data.decode()) == tomllib.loads(data.decode())


def test_a_product_with_publish_ok_false_is_refused_without_private_and_never_charted():
    reg = registry.load()
    with pytest.raises(registry.RegistryError, match="publish_ok is false"):
        registry.require_runnable(reg, "confluent-s3", private=False)
    registry.require_runnable(reg, "confluent-s3", private=True)
    with pytest.raises(registry.RegistryError, match="never draws"):
        registry.require_chartable(reg, "confluent-s3")
    with pytest.raises(registry.RegistryError, match="bar running it at all"):
        registry.require_runnable(reg, "logstash", private=True)


def test_a_private_or_unpublishable_run_is_refused_by_the_loader(tmp_path):
    env = make_env()
    env["private"] = True
    with pytest.raises(schema.RunRefused, match="--private"):
        schema.load_run(write_run(tmp_path, env=env), KIND)


def _broken(**change):
    reg = registry.load()
    entry = dict(reg["products"]["vector"])
    entry.update(change)
    reg["products"]["vector"] = entry
    return reg


@pytest.mark.parametrize("image,needle", [
    ("timberio/vector:0.61.0-debian", "no @sha256 digest"),
    ("timberio/vector:latest@sha256:" + "a" * 64, "tag 'latest'"),
    ("timberio/vector@sha256:" + "a" * 64, "tag '(none)'"),
])
def test_every_image_has_a_digest_and_no_tag_is_latest(image, needle):
    with pytest.raises(registry.RegistryError, match=re.escape(needle)):
        registry.check(_broken(image=image))


def test_a_licence_read_more_than_90_days_before_the_pin_is_refused():
    with pytest.raises(registry.RegistryError, match="read it again"):
        registry.check(_broken(retrieved="2026-01-01", pinned="2026-11-14"))


def test_a_clause_whose_hash_differs_is_refused():
    with pytest.raises(registry.RegistryError, match="clause_sha256"):
        registry.check(_broken(clause="something else"))


# ----------------------------------------------------- 4. configuration hashes


def test_config_hash_in_csv_equals_the_committed_config(tmp_path):
    run = write_run(tmp_path)
    rows, _env = schema.load_run(run, KIND)
    assert {row["config_sha256"] for row in rows} == {schema.sha256_text(t) for t in CONFIG_TEXT.values()}


def test_a_row_whose_config_hash_is_not_the_recorded_config_is_refused(tmp_path):
    def tamper(row):
        if row["system"] == "vector":
            row["config_sha256"] = "0" * 64
    with pytest.raises(schema.RunRefused, match="config_sha256"):
        schema.load_run(write_run(tmp_path, tamper), KIND)


def test_a_run_with_a_config_that_differs_from_the_committed_one_is_refused(tmp_path):
    from compare_common import environment

    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / "tool.yaml").write_text("a: 1\n")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", "c"],
                   check=True)
    assert environment.config_drift(["tool.yaml"], repo=str(repo)) == []
    (repo / "tool.yaml").write_text("a: 2\n")
    assert environment.config_drift(["tool.yaml"], repo=str(repo)) == ["tool.yaml"]


# ------------------------------------------------------------ 5. host classes


def test_hosted_host_class_refuses_performance_rows(tmp_path):
    env = make_env(host_class="hosted")
    with pytest.raises(schema.RunRefused, match="performance row from a hosted runner"):
        schema.load_run(write_run(tmp_path, env=env), KIND)


def test_a_run_mixing_host_classes_is_refused(tmp_path):
    def other(row):
        if row["system"] == "vector":
            row["host_class"] = "dedicated"
    with pytest.raises(schema.RunRefused, match="host_class"):
        schema.load_run(write_run(tmp_path, other), KIND)


# ------------------------------------------------------- 6. partial or truncated


def test_a_complete_run_loads(tmp_path):
    rows, env = schema.load_run(write_run(tmp_path), KIND)
    assert len(rows) == len(plans.expected_keys(PLAN, KIND)) == 15


def test_a_missing_row_is_refused_and_named(tmp_path):
    run = write_run(tmp_path, drop=lambda item: item.system == "vector" and item.cell == "drain"
                    and item.repetition == 2 and item.variant["size"] == 1024)
    with pytest.raises(schema.RunRefused, match="vector/acks/drain/2/1024"):
        schema.load_run(run, KIND)


def test_a_truncated_csv_is_refused(tmp_path):
    run = write_run(tmp_path)
    path = os.path.join(run, "testkind.csv")
    with open(path) as handle:
        text = handle.read()
    with open(path, "w") as handle:
        handle.write(text[: len(text) - 20])
    with pytest.raises(schema.RunRefused):
        schema.load_run(run, KIND)


def test_an_unfinished_run_is_refused(tmp_path):
    with pytest.raises(schema.RunRefused, match="did not finish"):
        schema.load_run(write_run(tmp_path, env=make_env(state="running")), KIND)


def test_a_non_ok_row_must_say_why(tmp_path):
    def fail(row):
        if row["system"] == "vector":
            row["status"] = "failed"
    with pytest.raises(schema.RunRefused, match="must say why"):
        schema.load_run(write_run(tmp_path, fail), KIND)


def test_a_forbidden_profile_is_refused_by_the_plan():
    plan = json.loads(json.dumps(PLAN))
    plan["cells"].append({"cell": "crash-K1", "repetitions": 1, "minutes_per_run": 1, "variants": [{"size": 1}],
                          "profiles": {"vtop": ["verify-off"]}})
    with pytest.raises(plans.PlanRefused, match="verify-off is never run in a crash cell"):
        plans.check_plan(plan, KIND)


def test_projected_hours_and_the_budget_refusal():
    assert plans.projected_hours(PLAN, KIND) == pytest.approx((3 * 2 + 12 * 10) / 60)
    with pytest.raises(plans.PlanRefused, match="over --max-hours"):
        plans.check_budget(PLAN, KIND, 1.0)


def test_systems_are_rotated_across_repetitions_when_the_kind_asks():
    rotating = KIND._replace(rotate_systems=True)
    plan = {**PLAN, "cells": [{"cell": "drain", "repetitions": 3, "minutes_per_run": 1, "variants": [{"size": 1}]}]}
    order = [item.system for item in plans.schedule(plan, rotating)]
    assert order == ["connect-lenses", "vtop", "vector", "vtop", "vector", "connect-lenses",
                     "vector", "connect-lenses", "vtop"]


# ------------------------------------------------------------- 9, 12 runner


def _describe(item):
    return {"system_version": "1", "image_digest": "d", "config_sha256": schema.sha256_text(CONFIG_TEXT[item.system])}


def test_resume_skips_finished_cells_and_refuses_a_changed_fingerprint(tmp_path):
    env = make_env()
    calls = []

    def execute(item):
        calls.append(item.key(KIND.variant_columns))
        return {"records_per_sec": 1, "gaps": 0, "sustained": "true"}

    clock = iter(range(0, 100000, 60)).__next__  # each call a minute later
    out = runner.run_plan(str(tmp_path), KIND, env, _describe, execute, budget_minutes=4, clock=clock,
                          log=lambda m: None)
    assert out.stopped_by_budget and out.done == len(calls) < 15
    first = list(calls)
    out = runner.run_plan(str(tmp_path), KIND, env, _describe, execute, log=lambda m: None)
    assert out.skipped == len(first) and out.done == 15 - len(first)
    assert len(set(calls)) == len(calls) == 15  # nothing ran twice
    runner.check_resume(env, dict(env["comparable"]))
    changed = dict(env["comparable"], host_cpu="another")
    with pytest.raises(schema.RunRefused, match="changed: host_cpu"):
        runner.check_resume(env, changed)


def test_tool_failure_is_recorded_not_retried_by_the_harness(tmp_path):
    env = make_env()
    calls = []

    def execute(item):
        calls.append(item)
        if item.system == "vector":
            raise runner.ToolFailed("vector: sink error: 503")
        if item.system == "connect-lenses" and item.cell == "smoke":
            raise runner.CellRefused("connector did not load")
        return {"records_per_sec": 1, "gaps": 0}

    runner.run_plan(str(tmp_path), KIND, env, _describe, execute, log=lambda m: None)
    rows = schema.read_rows(os.path.join(str(tmp_path), "testkind.csv"), KIND)
    assert len(calls) == len(rows) == 15
    vector = [r for r in rows if r["system"] == "vector"]
    assert {r["status"] for r in vector} == {"failed"} and vector[0]["note"] == "vector: sink error: 503"
    assert [r["status"] for r in rows if r["system"] == "connect-lenses" and r["cell"] == "smoke"] == ["refused"]


def test_a_harness_fault_writes_no_row(tmp_path):
    env = make_env()

    def execute(item):
        raise RuntimeError("docker went away")

    with pytest.raises(RuntimeError):
        runner.run_plan(str(tmp_path), KIND, env, _describe, execute, log=lambda m: None)
    assert schema.read_rows(os.path.join(str(tmp_path), "testkind.csv"), KIND) == []


# ---------------------------------------------------------------- 10 scoreboard


def test_scoreboard_has_a_vtop_row_for_every_metric_even_when_vtop_is_last(tmp_path):
    rows, env = schema.load_run(write_run(tmp_path), KIND)
    metrics = [creport.Metric("rps", "records/s", "records_per_sec", "higher", ("drain",)),
               creport.Metric("gaps", "gaps", "gaps", "lower", ("smoke",), reduce="sum", digits=0)]
    systems = ["connect-lenses", "vtop", "vector"]
    profiles = {s: creport.primary_profile(PLAN, s) for s in systems}
    scores = creport.scoreboard(rows, KIND, metrics, systems, profiles, lambda c, v: f"{c} {v['size']}")
    assert {(s.metric.id, s.group) for s in scores} == {("rps", "drain 100"), ("rps", "drain 1024"),
                                                         ("gaps", "smoke 100")}
    rps = [s for s in scores if s.metric.id == "rps"]
    assert all(s.best == "vector" and s.vtop_value == 50 and s.ratio == 0.25 and s.standing == "behind" for s in rps)
    gaps = [s for s in scores if s.metric.id == "gaps"][0]
    assert gaps.standing == "level" and gaps.vtop == "0"
    text = creport.standing_sentences(scores, {"vector": "Vector"})
    assert text[0].startswith("VTOP is behind on: records/s, drain 100 (50.0 against Vector's 200.0)")


def test_a_failed_system_is_a_stub_not_a_missing_bar(tmp_path):
    def fail(row):
        if row["system"] == "vtop" and row["cell"] == "drain":
            row["status"], row["note"] = "failed", "exited 1"
    rows, _env = schema.load_run(write_run(tmp_path, fail), KIND)
    metric = creport.Metric("rps", "records/s", "records_per_sec", "higher", ("drain",))
    v = creport.value(rows, KIND, metric, ("drain", ("100",)), "vtop", "strong")
    assert v.summary is None and v.stub == "failed"
    scores = creport.scoreboard(rows, KIND, [metric], ["connect-lenses", "vtop", "vector"],
                                {"connect-lenses": "exactly-once", "vtop": "strong", "vector": "acks"},
                                lambda c, v: c)
    assert all(s.vtop == "failed" and s.standing == "no comparison" for s in scores)


# ---------------------------------------------------------------- 11 colours


@pytest.mark.parametrize("family", registry.FAMILIES)
@pytest.mark.parametrize("mode", ["light", "dark"])
def test_every_shipped_colour_order_passes_the_validator(family, mode):
    order = registry.family_order(registry.load(), family)
    # The full four-system order, and the three-system first wave (no slot 4).
    for systems in (order, order[:3]):
        assert list(colours.series(order, systems, mode)) == systems


@pytest.mark.parametrize("mode", ["light", "dark"])
def test_orange_beside_yellow_is_refused(mode):
    order = registry.family_order(registry.load(), "link")
    with pytest.raises(colours.ColourRefused, match="normal-vision"):
        colours.series(order, ["aws-cli", "vtop", "s5cmd"], mode)


def test_a_fifth_system_and_four_without_a_table_are_refused():
    order = registry.family_order(registry.load(), "spool")
    with pytest.raises(colours.ColourRefused, match="without a table"):
        colours.series(order, order, with_table=False)
    with pytest.raises(colours.ColourRefused, match="not in the family order"):
        colours.series(order, order + ["kafka"])


# ---------------------------------------------------------------- 13 kills


def test_kill_schedule_is_identical_for_every_tool_given_the_seed():
    import inspect

    assert "tool" not in inspect.signature(kills.schedule).parameters
    assert "system" not in inspect.signature(kills.schedule).parameters
    for kind in kills.KINDS:
        assert kills.schedule(7, kind, 1) == kills.schedule(7, kind, 1)
        assert kills.schedule(7, kind, 1) != kills.schedule(8, kind, 1)
    k1 = kills.schedule(7, "K1", 2, 3)
    assert [k.index for k in k1] == [0, 1, 2] and all(2 <= k.n <= 12 and k.at_seconds is None for k in k1)
    k4 = kills.schedule(7, "K4", 1)
    assert all(k.at_seconds is not None and 0 <= k.window_ms <= 500 for k in k4)


# ----------------------------------------------------------------- the tap


async def _fake_store(reader, writer, delay=0.0, body=b"hello world"):
    while True:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, ConnectionError):
            break
        _line, headers = tap.parse_head(head)
        length = int(headers.get("content-length", "0"))
        if length:
            await reader.readexactly(length)
        await asyncio.sleep(delay)
        answer = b"" if _line.startswith("HEAD") else body
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + answer)
        await writer.drain()
    writer.close()


async def _client(port, requests):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    answers = []
    for method, path, body in requests:
        writer.write(f"{method} {path} HTTP/1.1\r\nHost: minio:9000\r\nContent-Length: {len(body)}\r\n\r\n".encode()
                     + body)
        await writer.drain()
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except asyncio.IncompleteReadError:
            answers.append(None)
            break
        _l, headers = tap.parse_head(head)
        answers.append(b"" if method == "HEAD" else await reader.readexactly(int(headers["content-length"])))
    writer.close()
    return answers


async def _with_tap(test, delay=0.0, **tap_kw):
    store_server = await asyncio.start_server(lambda r, w: _fake_store(r, w, delay), "127.0.0.1", 0)
    store_port = store_server.sockets[0].getsockname()[1]
    events = []

    async def kill(container):
        events.append(("kill", container))

    t = tap.Tap(("127.0.0.1", store_port), kill=kill, **tap_kw)
    tap_server = await asyncio.start_server(t.handle, "127.0.0.1", 0)
    port = tap_server.sockets[0].getsockname()[1]
    try:
        return await test(t, port, events)
    finally:
        tap_server.close()
        store_server.close()


def test_k1_kills_at_the_start_of_the_nth_put_and_never_forwards_it():
    async def test(t, port, events):
        t.arm("K1", 2, "tool-1")
        answers = await _client(port, [("PUT", "/b/k1", b"abc"), ("PUT", "/b/k2", b"def")])
        assert answers == [b"hello world", None]
        assert events == [("kill", "tool-1")] and t.fired[0]["kind"] == "K1" and t.fired[0]["n"] == 2
        assert t.armed is None

    asyncio.run(_with_tap(test))


def test_k2_trigger_fires_after_the_response_not_before():
    async def test(t, port, events):
        t.arm("K2", 1, "tool-1")
        received = []

        async def kill(container):
            # The tool must already hold the whole answer when the kill runs.
            events.append(("kill", list(received)))

        t.kill = kill
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"PUT /b/k HTTP/1.1\r\nHost: minio:9000\r\nContent-Length: 3\r\n\r\nabc")
        await writer.drain()
        head = await reader.readuntil(b"\r\n\r\n")
        body = await reader.readexactly(11)
        received.append(body)
        await asyncio.sleep(0.05)
        assert head.startswith(b"HTTP/1.1 200") and body == b"hello world"
        fired = t.fired[0]
        assert fired["kind"] == "K2" and fired["killed_at"] >= fired["trigger_at"]
        writer.close()

    # The store answers after 100 ms: a kill before the answer would show in the order of events.
    asyncio.run(_with_tap(test, delay=0.1))


def test_k2_holds_every_other_request_until_the_kill_returns():
    async def test(t, port, events):
        t.arm("K2", 1, "tool-1")
        release = asyncio.Event()

        async def kill(container):
            events.append("kill-start")
            await release.wait()
            events.append("kill-end")

        t.kill = kill
        first = asyncio.ensure_future(_client(port, [("PUT", "/b/a", b"x")]))
        await asyncio.sleep(0.1)
        second = asyncio.ensure_future(_client(port, [("GET", "/b/a", b"")]))
        await asyncio.sleep(0.1)
        assert not t.gets  # the GET waits at the gate
        release.set()
        await first
        await second
        assert events == ["kill-start", "kill-end"] and t.gets == ["/b/a"]

    asyncio.run(_with_tap(test))


def test_the_fault_flips_one_bit_in_selected_get_bodies_only():
    keys = [f"/b/obj-{i}" for i in range(200)]
    chosen = [k for k in keys if tap.fault_selected(7, k, 10)]
    assert 5 < len(chosen) < 40  # about one in ten

    async def test(t, port, events):
        answers = await _client(port, [("GET", k, b"") for k in keys[:40]])
        for key, answer in zip(keys[:40], answers):
            if key in chosen:
                diff = sum(bin(a ^ b).count("1") for a, b in zip(answer, b"hello world"))
                assert diff == 1, key
            else:
                assert answer == b"hello world"
        assert t.corrupted == [k for k in keys[:40] if k in chosen]

    asyncio.run(_with_tap(test, fault_seed=7, fault_one_in=10))


def test_the_tap_is_refused_in_a_drain_or_live_cell():
    for cell in ("drain", "live", "defaults", "smoke"):
        with pytest.raises(tap.TapRefused, match="only in crash and fault cells"):
            tap.refuse_in_cell(cell)
    tap.refuse_in_cell("crash-K2")
    tap.refuse_in_cell("verify-fault")


def test_the_tap_traces_every_request(tmp_path):
    trace = str(tmp_path / "tap.csv")

    async def test(t, port, events):
        await _client(port, [("PUT", "/b/k?partNumber=1&uploadId=u", b"abc"), ("HEAD", "/b/k", b"")])
        t.close()

    asyncio.run(_with_tap(test, trace_path=trace))
    with open(trace) as handle:
        lines = handle.read().splitlines()
    assert lines[0] == ",".join(tap.TRACE_COLUMNS)
    assert lines[1].split(",")[3:6] == ["PUT", "/b/k", "partNumber=1&uploadId=u"]
    assert len(lines) == 3


# -------------------------------------------------------------- cgroup, store


def test_cgroup_window_is_cpu_difference_and_sampled_peak_rss():
    v2 = {"cpu_stats": {"cpu_usage": {"total_usage": 2_000_000_000}}, "memory_stats": {"stats": {"anon": 300}}}
    v1 = {"cpu_stats": {"cpu_usage": {"total_usage": 5_000_000_000}}, "memory_stats": {"stats": {"rss": 200}}}
    samples = [cgroup.parse_stats(v2, 1.0), cgroup.parse_stats(v1, 2.0)]
    assert samples[0].rss_bytes == 300 and samples[1].rss_bytes == 200
    window = cgroup.window(samples, 0, 3)
    assert window.cpu_seconds == 3.0 and window.peak_rss_sampled_bytes == 300
    with pytest.raises(ValueError, match="went backwards"):
        cgroup.window(list(reversed([cgroup.Sample(1, 5, 1), cgroup.Sample(2, 3, 1)])), 0, 3)
    with pytest.raises(ValueError, match="at least 2"):
        cgroup.window(samples, 1.5, 3)


def test_store_requests_by_method_from_minio_metrics():
    before = store.request_counts('minio_s3_requests_total{api="putobject",server="a"} 10\n'
                                  'minio_s3_requests_total{api="getobject",server="a"} 4\n')
    after = store.request_counts('minio_s3_requests_total{api="putobject",server="a"} 25\n'
                                 'minio_s3_requests_total{api="getobject",server="a"} 9\n'
                                 'minio_s3_requests_total{api="listobjectsv2",server="a"} 2\n'
                                 'minio_s3_requests_total{api="weird",server="a"} 1\n')
    counts = store.requests_by_method(before, after)
    assert counts["PUT"] == 15 and counts["GET"] == 5 and counts["LIST"] == 2 and counts["OTHER"] == 1
    assert store.per_gib(counts, 2**30)["PUT"] == 15
    with pytest.raises(ValueError, match="backwards"):
        store.requests_by_method(after, before)
