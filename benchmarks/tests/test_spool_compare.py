"""The spool comparison (#748): the generator, the rotation hook, the checker across rotations, the
kill and rotation drivers, the configuration rules, and the shared tests of #744 section 8 for the
`spool` kind. No Docker: the drivers take their actions as callables."""
from __future__ import annotations

import gzip
import json
import os
import re

import pytest

from compare_common import checker, kills, lab, registry, runner, schema
from compare_common import plan as plans
from spool_compare import fixture, generator, run
from spool_compare.kind import KIND

HERE = os.path.dirname(os.path.abspath(__file__))
SPOOL = os.path.join(os.path.dirname(HERE), "spool_compare")
TAG = "abcd1234"


def _spool(tmp_path, rotation, rotate_bytes=10_000, name="s"):
    return generator.Spool(str(tmp_path / name), rotation, seed=748, tag=TAG, line_bytes=200,
                           rotate_bytes=rotate_bytes, clock=lambda: 1000.0)


def _read_all(directory):
    """Every line of every file in the spool, in file order: the source of truth the checker is judged against."""
    out = []
    for name in sorted(os.listdir(directory)):
        with open(os.path.join(directory, name), "rb") as handle:
            out.extend(line for line in handle.read().split(b"\n") if line)
    return out


# --------------------------------------------------------------- 2 generator


def test_generator_is_deterministic_for_a_seed_and_embeds_a_unique_seq_per_line(tmp_path):
    lines = [generator.line(748, TAG, seq, 200) for seq in range(2000)]
    assert lines == [generator.line(748, TAG, seq, 200) for seq in range(2000)]
    assert lines != [generator.line(749, TAG, seq, 200) for seq in range(2000)]
    assert all(len(line.encode()) == 200 for line in lines)
    assert all(re.match(r"^<\d{1,3}>1 \d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z \S+ \S+ \d+ ID\d+ - ", line)
               for line in lines)  # RFC 5424: PRI VERSION TIMESTAMP HOST APP PROCID MSGID SD
    ids = [checker.ID_TOKEN.findall(line.encode()) for line in lines]
    assert all(len(found) == 1 for found in ids)
    assert [int(found[0][1]) for found in ids] == list(range(2000))
    # The same bytes on disk, written twice from the same seed.
    one, two = _spool(tmp_path, "rename", name="a"), _spool(tmp_path, "rename", name="b")
    one.write(500)
    two.write(500)
    assert _read_all(one.directory) == _read_all(two.directory)
    assert checker.make_id(TAG, 7) == generator.make_id(TAG, 7)


# ---------------------------------------------------------- 3 rotation hook


def test_rotation_hook_is_the_same_for_every_tool_and_both_styles(tmp_path):
    import inspect

    # One hook: nothing about the tool reaches it, only the spool's style.
    assert list(inspect.signature(generator.Spool.rotate).parameters) == ["self"]
    # The harness asks the generator for a rotation the same way for every tool: no argument names one.
    source = inspect.getsource(run)
    assert source.count('"/rotate"') == source.count('gen("/rotate", {})') >= 2
    rename = _spool(tmp_path, "rename", rotate_bytes=10**9, name="r")
    rename.write(10)
    inode = os.stat(rename.path).st_ino
    event = rename.rotate()
    assert event["file"] == "app.log.1" and os.path.getsize(rename.path) == 0
    assert os.stat(os.path.join(rename.directory, "app.log.1")).st_ino == inode  # the same file, renamed
    copy = _spool(tmp_path, "copytruncate", rotate_bytes=10**9, name="c")
    copy.write(10)
    inode = os.stat(copy.path).st_ino
    event = copy.rotate()
    assert event["file"] == "app.log.1" and os.path.getsize(copy.path) == 0
    assert os.stat(copy.path).st_ino == inode  # truncated in place
    assert os.path.getsize(os.path.join(copy.directory, "app.log.1")) == 10 * 201
    # Both styles keep every line: the rotated files plus the active one hold the whole sequence.
    for spool in (rename, copy):
        spool.write(5)
        assert sorted(int(checker.ID_TOKEN.search(line).group(2)) for line in _read_all(spool.directory)) == \
            list(range(15))
        assert spool.files() == ["app.log", "app.log.1"]


def test_size_rotation_rotates_whenever_the_active_file_reaches_its_size_and_never_reuses_a_name(tmp_path):
    spool = _spool(tmp_path, "rename", rotate_bytes=201 * 10)
    spool.write(35)
    assert spool.files() == ["app.log", "app.log.1", "app.log.2", "app.log.3"]
    assert [r["next_seq"] for r in spool.rotations] == [10, 20, 30]
    with pytest.raises(ValueError):
        generator.Spool(str(tmp_path / "x"), "delete", seed=1, tag=TAG)


# ------------------------------------------------------------ 4, 5 checker


def _objects_from(lines, per_object):
    objects = {}
    for start in range(0, len(lines), per_object):
        body = b"".join(line + b"\n" for line in lines[start:start + per_object])
        objects[f"vtop/o-{start:06d}.gz"] = gzip.compress(body, mtime=0)
    return objects


def test_checker_counts_a_line_missing_across_a_rotation_boundary_as_a_gap(tmp_path):
    spool = _spool(tmp_path, "rename", rotate_bytes=201 * 10)
    spool.write(30)
    boundary = spool.rotations[0]["next_seq"] - 1  # the last line of app.log.1
    lines = [line for line in _read_all(spool.directory)
             if int(checker.ID_TOKEN.search(line).group(2)) != boundary]
    result = checker.check(checker.Snapshot(_objects_from(lines, 7), {}, []), spool.expected(), TAG)
    assert (result.gaps, result.duplicates, result.torn_objects) == (1, 0, 0)
    assert result.gap_sample == [boundary]


def test_checker_counts_a_line_present_in_two_objects_as_one_duplicate(tmp_path):
    spool = _spool(tmp_path, "copytruncate", rotate_bytes=201 * 10)
    spool.write(30)
    lines = _read_all(spool.directory)
    objects = _objects_from(lines, 7)
    objects["vector/extra.gz"] = gzip.compress(json.dumps({"message": lines[12].decode()}).encode() + b"\n")
    result = checker.check(checker.Snapshot(objects, {}, []), spool.expected(), TAG)
    assert (result.gaps, result.duplicates, result.torn_objects) == (0, 1, 0)


# -------------------------------------------------------------- 6 K4 driver


class FakeClock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


@pytest.mark.parametrize("run_number", [1, 2, 3])
def test_k4_kill_fires_only_inside_the_seeded_rotation_window(run_number):
    clock = FakeClock()
    events = []
    for point in kills.schedule(748, "K4", run_number):
        started = clock()
        rotated_at, killed_at = run.run_k4(point, started, lambda: events.append(("rotate", clock())),
                                           lambda: events.append(("kill", clock())) or clock(), clock=clock,
                                           sleep=clock.sleep)
        assert rotated_at == pytest.approx(started + point.at_seconds)
        assert killed_at - rotated_at == pytest.approx(point.window_ms / 1000.0)
        assert 0 <= point.window_ms <= 500
    kinds = [kind for kind, _at in events]
    assert kinds == ["rotate", "kill"] * 3  # never a kill before its rotation


# ---------------------------------------------------- 7 down-while-rotating


def test_down_while_rotating_cell_refuses_a_run_where_the_tool_was_running_during_a_rotation():
    log = []
    state = {"running": True}

    def stop():
        log.append("stop")
        state["running"] = False

    def start():
        log.append("start")
        state["running"] = True

    done = run.down_while_rotating(lambda: state["running"], stop, lambda: log.append("rotate"),
                                   lambda: log.append("write"), start)
    assert done == 2 and log == ["stop", "rotate", "write", "rotate", "write", "start"]
    # A stop that did not take (the tool is still running at the first rotation).
    with pytest.raises(runner.CellRefused, match="running at rotation 1"):
        run.down_while_rotating(lambda: True, lambda: None, lambda: None, lambda: None, lambda: None)
    # A tool that came back on its own between the rotations.
    calls = iter([False, True])
    with pytest.raises(runner.CellRefused, match="running at rotation 2"):
        run.down_while_rotating(lambda: next(calls), lambda: None, lambda: None, lambda: None, lambda: None)


# --------------------------------------------------- 8, 9 configuration rules


def test_fluent_bit_config_records_storage_type_and_db_sync_in_the_environment_block():
    configs = __import__("compare_common.environment", fromlist=["x"]).record_configs(run.CONFIGS)
    durability = run.record_durability(configs)
    assert durability["fluent-bit"]["durable"]["storage.type"] == "filesystem"
    assert durability["fluent-bit"]["durable"]["db.sync"] == "full"
    assert durability["vector"]["acks"]["buffer.type"] == "memory"
    assert durability["vector"]["disk-buffer"]["buffer.type"] == "disk"
    assert durability["vector"]["acks"]["acknowledgements.enabled"] == "true"
    weaker = {"fluent-bit": {"durable": dict(configs["fluent-bit"]["durable"])}}
    text = weaker["fluent-bit"]["durable"]["text"]
    weaker["fluent-bit"]["durable"]["text"] = re.sub(r"(?m)^\s*db\.sync.*$", "", text)
    with pytest.raises(SystemExit, match="storage.type and db.sync"):
        run.record_durability(weaker)


def test_a_tool_whose_documented_best_config_cannot_be_written_is_a_refused_row_not_a_silently_weaker_config(
        tmp_path, monkeypatch):
    monkeypatch.setitem(run.CONFIGS, "vector", {"acks": "benchmarks/spool_compare/tools/vector/acks.yaml"})
    with pytest.raises(runner.CellRefused, match="cannot be written"):
        run.require_config("vector", "disk-buffer")
    # Through the runner: a refused row with the reason, and the other profile still runs.
    env = fixture.environment("r")
    env["plan"] = {"kind": "spool", "name": "t", "host_class": "laptop", "systems": ["vector"],
                   "profiles": {"vector": ["acks", "disk-buffer"]},
                   "cells": [{"cell": "smoke", "repetitions": 1, "minutes_per_run": 1,
                              "variants": [{"source": "file", "rotation": "rename"}]}]}

    def execute(item):
        run.require_config(item.system, item.profile)
        return {"records_expected": 1, "records_present": 1, "gaps": 0, "duplicates": 0, "torn_objects": 0,
                "orphan_objects": 0, "unexpected": 0}

    describe = lambda item: {"system_version": "x", "image_digest": "x",  # noqa: E731
                             "config_sha256": env["configs"]["vector"][item.profile]["sha256"]}
    runner.run_plan(str(tmp_path), KIND, env, describe, execute, log=lambda m: None)
    rows = schema.read_rows(os.path.join(str(tmp_path), "spool.csv"), KIND)
    assert [(r["profile"], r["status"]) for r in rows] == [("acks", "ok"), ("disk-buffer", "refused")]
    assert "no weaker configuration is substituted" in rows[1]["note"]


# ----------------------------------------------- shared #744 tests, spool kind


@pytest.mark.parametrize("name", ["smoke", "hosted-crash", "hosted-rotation", "full"])
def test_every_shipped_plan_is_one_the_kind_can_run(name):
    plan = plans.select(plans.load_plans(os.path.join(SPOOL, "plans.json")), name)
    plans.check_plan(plan, KIND)
    for system in plan["systems"]:
        for profile in KIND.profiles[system]:
            if profile in plans.cell_profiles(plan, plan["cells"][0], system):
                assert os.path.isfile(run.require_config(system, profile))


def test_hosted_plans_hold_correctness_cells_only():
    for name in ("hosted-crash", "hosted-rotation"):
        plan = plans.select(plans.load_plans(os.path.join(SPOOL, "plans.json")), name)
        assert plan["host_class"] == "hosted"
        assert {KIND.cells[c["cell"]] for c in plan["cells"]} == {"correctness"}


def test_compose_limits_are_identical_within_a_limit_class():
    with open(os.path.join(SPOOL, "compose.yml"), encoding="utf-8") as handle:
        limits = lab.compose_limits(handle.read())
    tools = {service: value for service, value in limits.items() if service in KIND.profiles}
    assert set(tools) == set(KIND.profiles), f"every tool declares limits: {sorted(tools)}"
    assert len({tuple(sorted(v.items())) for v in tools.values()}) == 1, tools
    one = next(iter(tools.values()))
    assert set(one) == {"cpus", "mem_limit", "memswap_limit"} and one["mem_limit"] == one["memswap_limit"]


def test_every_tool_config_disables_telemetry():
    for system, profiles in run.CONFIGS.items():
        for rel in profiles.values():
            with open(os.path.join(lab.REPO, rel), encoding="utf-8") as handle:
                text = handle.read()
            assert re.search(r"(?m)^#\s*telemetry:", text), f"{rel} does not state its telemetry setting"
            assert system in run.TELEMETRY
    with open(os.path.join(lab.REPO, run.CONFIGS["otel-collector"]["file-storage"]), encoding="utf-8") as handle:
        assert re.search(r"telemetry:\s*\n\s*metrics:\s*\n\s*level:\s*none", handle.read())
    with open(os.path.join(lab.REPO, run.CONFIGS["fluent-bit"]["durable"]), encoding="utf-8") as handle:
        assert re.search(r"(?m)^\s*http_server\s+off", handle.read())
    with open(os.path.join(SPOOL, "compose.yml"), encoding="utf-8") as handle:
        assert 'OTEL_SDK_DISABLED: "true"' in handle.read()


def test_pins_match_products_toml():
    reg = registry.load()
    with open(os.path.join(SPOOL, "compose.yml"), encoding="utf-8") as handle:
        text = handle.read()
    for system in ("vector", "fluent-bit", "otel-collector"):
        image = registry.product(reg, system)["image"]
        assert f"image: {image}\n" in text, f"{system}: compose does not use {image}"
    assert lab.MINIO_IMAGE in text and lab.HARNESS_IMAGE in text
    for image in re.findall(r"(?m)^\s+image:\s*(\S+)", text):
        assert "@sha256:" in image or image.startswith(("${VTOP_IMAGE", "vtop-minio:", "${HARNESS_IMAGE")), image


def test_every_spool_system_is_registered_for_use_case_b():
    reg = registry.load()
    for system in KIND.profiles:
        registry.require_runnable(reg, system, private=False, use_case="b")
    assert registry.family_order(reg, "spool") == list(KIND.profiles)


def test_a_partial_or_truncated_spool_run_is_refused(tmp_path):
    run_dir = fixture.write_run(str(tmp_path))
    rows, _env = schema.load_run(run_dir, KIND)
    assert len(rows) == len(plans.expected_keys(_env["plan"], KIND))
    path = os.path.join(run_dir, "spool.csv")
    with open(path) as handle:
        lines = handle.read().splitlines(keepends=True)
    with open(path, "w") as handle:
        handle.writelines(lines[:-1])
    with pytest.raises(schema.RunRefused, match="missing"):
        schema.load_run(run_dir, KIND)
    assert run.check(run_dir) == 2


def test_a_smoke_row_with_a_gap_is_not_ok():
    row = {"source": "file", "rotation": "rename", "status": "ok", "cell": "smoke", "records_expected": "10",
           "records_present": "9", "gaps": "1", "duplicates": "0", "torn_objects": "0", "orphan_objects": "0",
           "unexpected": "0"}
    assert any("not ok" in problem for problem in KIND.check_row(row))


# -------------------------------------------------------- run.py helpers


def test_quiescence_needs_an_object_and_a_quiet_period_and_fails_a_tool_that_exited():
    clock = FakeClock()
    listings = iter([[], [{"size": 1}], [{"size": 1}, {"size": 2}]] + [[{"size": 1}, {"size": 2}]] * 100)
    entries = run.quiesce(lambda: next(listings), lambda: True, quiet=20, timeout=600, clock=clock,
                          sleep=clock.sleep, poll=5)
    assert len(entries) == 2 and clock.now == pytest.approx(100 + 10 + 20)
    with pytest.raises(runner.ToolFailed, match="exited"):
        run.quiesce(lambda: [], lambda: False, quiet=20, timeout=600, clock=clock, sleep=clock.sleep)
    with pytest.raises(runner.ToolFailed, match="no quiescence"):
        run.quiesce(lambda: [], lambda: True, quiet=20, timeout=30, clock=clock, sleep=clock.sleep)


def test_live_rate_is_half_the_slowest_tools_median_drain_rate(tmp_path):
    run_dir = fixture.write_run(str(tmp_path))
    item = plans.Planned("vector", "acks", "live", 1, {"source": "file", "rotation": "rename"}, {}, "performance")
    rows = schema.read_rows(os.path.join(run_dir, "spool.csv"), KIND)
    vtop = sorted(float(r["lines_per_sec"]) for r in rows if r["system"] == "vtop" and r["cell"] == "drain"
                  and r["source"] == "file")
    assert run.live_rate(run_dir, item, 0.5) == pytest.approx(round(vtop[1] * 0.5, 1))
    assert run.live_rate(str(tmp_path), item, 0.5, fixed=123) == 123.0
    with pytest.raises(runner.CellRefused, match="no drain result"):
        run.live_rate(str(tmp_path), item, 0.5)


def test_live_rate_counts_only_the_profiles_that_run_the_live_cell(tmp_path):
    # Review of #908: Vector drains under `acks` (100 lines/s) and `disk-buffer` (1000), but only `acks` is
    # offered live load; pooling them would put the median at 550 and overload the 100 lines/s profile.
    run_dir = str(tmp_path)
    variant = {"source": "file", "rotation": "rename"}
    base = {"cell": "drain", "status": "ok", **variant}
    rows = [dict(base, system="vector", profile="acks", lines_per_sec="100"),
            dict(base, system="vector", profile="disk-buffer", lines_per_sec="1000"),
            dict(base, system="vtop", profile="strong", lines_per_sec="1000")]
    for row in rows:
        schema.append_row(os.path.join(run_dir, "spool.csv"), KIND, row)
    item = plans.Planned("vector", "acks", "live", 1, variant, {}, "performance")
    plan = {"systems": ["vector", "vtop"], "profiles": {"vector": ["acks", "disk-buffer"], "vtop": ["strong"]},
            "cells": [{"cell": "drain"}, {"cell": "live", "profiles": {"vector": ["acks"]}}]}
    assert run.live_participants(plan) == {"vector": {"acks"}, "vtop": {"strong"}}
    assert run.live_rate(run_dir, item, 0.5, None, run.live_participants(plan)) == 50.0


def test_first_object_after_each_restart():
    listing = [{"lastModified": "2026-11-22T00:00:10Z"}, {"lastModified": "2026-11-22T00:00:30Z"}]
    base = checker.parse_time("2026-11-22T00:00:00Z")
    assert run.first_object_after(listing, [base + 5, base + 20]) == "7.5"
    assert run.first_object_after(listing, [base + 40]) == ""


@pytest.mark.parametrize("name", ["smoke", "hosted-crash", "full"])
def test_every_put_trigger_is_reachable_with_whole_32_mib_objects(name):
    # Fluent Bit and Vector upload one object per 32 MiB: the backlog must hold at least
    # kills x the largest Nth PUT of them, or a trigger can never fire (review of #908).
    plan = plans.select(plans.load_plans(os.path.join(SPOOL, "plans.json")), name)
    for cell in plan["cells"]:
        if cell["cell"] in ("crash-K1", "crash-K2"):
            params = cell["params"]
            need = params["kills_per_run"] * params.get("n_range", (2, 12))[1] * 32 * 2**20
            assert params["records"] * 201 >= need, (cell["cell"], params)


def test_nothing_is_timed_or_crashed_for_a_system_whose_smoke_did_not_pass(tmp_path):
    env = fixture.environment("r")
    driver = run.Runner(None, env, str(tmp_path), "")
    item = plans.Planned("vector", "acks", "drain", 1, {"source": "file", "rotation": "rename"}, {}, "performance")
    assert driver.smoke_failed(item) is None  # no smoke yet in this run
    row = runner.base_row(env, item._replace(cell="smoke", metric_class="correctness"), system_version="x",
                          image_digest="x", config_sha256="x")
    row.update({"status": "failed", "note": "smoke: 3 gaps", "started_at": "t", "wall_seconds": "1"})
    schema.append_row(os.path.join(str(tmp_path), "spool.csv"), KIND, row)
    assert "did not pass its smoke" in driver.smoke_failed(item)
    assert driver.smoke_failed(item._replace(system="fluent-bit", profile="durable")) is None
    assert driver.smoke_failed(item._replace(cell="smoke")) is None
    with pytest.raises(runner.CellRefused, match="did not pass its smoke"):
        driver.execute(item)


def test_the_harness_polling_is_taken_out_of_the_tools_request_counts():
    # Review of #908: quiescence polls list the same store whose counters are the tool's request cost.
    counts = {"PUT": 40, "GET": 0, "HEAD": 3, "LIST": 61, "POST": 0, "DELETE": 0, "OTHER": 0}
    unit = {"PUT": 0, "GET": 0, "HEAD": 1, "LIST": 1, "POST": 0, "DELETE": 0, "OTHER": 0}
    polls = 60
    harness = {method: n * polls for method, n in unit.items()}
    assert run.without_harness(counts, harness) == {"PUT": 40, "GET": 0, "HEAD": 0, "LIST": 1, "POST": 0,
                                                    "DELETE": 0, "OTHER": 0}


def test_the_drain_resource_window_ends_with_the_throughput_window_not_the_quiescence_wait():
    # Review of #908: the run ends 75 s of idle polling after the last object; CPU and RSS are accounted to the
    # last object, the window throughput is reported for.
    assert run.usage_end(1000.0, 120.0, 1195.0, 0.5) == 1120.0
    # A drain too short to hold two samples still gets two; and the window never outlasts the run.
    assert run.usage_end(1000.0, 0.001, 1195.0, 0.5) == 1001.0
    assert run.usage_end(1000.0, 500.0, 1195.0, 0.5) == 1195.0


def _live(offered=100.0, warmup=20.0, measure=100.0, visible_rate=None, batch_lag=5.0, drain_after=None):
    """A producer that wrote `offered` lines a second from t=0 for warmup+measure seconds, and a tool whose objects
    became store-visible `batch_lag` seconds behind the writer at `visible_rate` lines a second (the offered rate
    when None). With `drain_after`, the rest of the backlog is visible that many seconds after the window ends."""
    total = int((warmup + measure) * offered)
    written = [(seq, seq / offered) for seq in range(total)]
    rate = offered if visible_rate is None else visible_rate
    visible = {}
    for seq, at in written:
        if rate >= offered:
            visible[seq] = at + batch_lag
        elif seq / rate + batch_lag <= warmup + measure:
            visible[seq] = seq / rate + batch_lag
        elif drain_after is not None:
            visible[seq] = warmup + measure + drain_after
    return written, visible


def _progress(**kw):
    warmup, measure = kw.get("warmup", 20.0), kw.get("measure", 100.0)
    written, visible = _live(**kw)
    return run.live_progress(written, visible, 0.0, warmup, measure), measure


def test_the_live_window_opens_after_the_warmup_from_the_tools_start():
    assert run.window_bounds(1000.0, 120.0, 600.0) == (1120.0, 1720.0)


def test_a_tool_that_keeps_up_with_a_batching_delay_sustained_the_rate():
    # a 60 s batch age is a constant lag, not a falling behind (the warmup outlasts it)
    progress, measure = _progress(batch_lag=60.0, warmup=120.0)
    assert progress.visible_rate == pytest.approx(100.0, rel=0.02)
    assert abs(progress.lag_end - progress.lag_start) <= 100
    assert run.live_sustained(100.0, progress, measure, 0)


def test_a_tool_at_half_the_offered_rate_did_not_sustain_it_even_if_it_drains_later():
    # The producer logged every line at the offered rate; the tool's objects appeared at half of it and the
    # rest of the backlog only after the window ended. The old rule (lines the PRODUCER wrote / window) said
    # sustained; the sink's progress during the window says not.
    progress, measure = _progress(visible_rate=50.0, drain_after=30.0)
    written, _visible = _live()
    producer_rate = len([1 for _seq, at in written if at >= 20.0]) / measure
    assert producer_rate == pytest.approx(100.0, rel=0.02)
    assert progress.visible_rate == pytest.approx(50.0, rel=0.1)
    assert progress.lag_end - progress.lag_start > 100 * measure * 0.3
    assert not run.live_sustained(100.0, progress, measure, 0)


def test_a_tool_whose_lag_grows_while_its_rate_is_barely_under_the_offer_is_not_sustained():
    progress = run.LiveProgress(visible_rate=97.0, lag_start=500, lag_end=500 + 800)
    assert not run.live_sustained(100.0, progress, 100.0, 0)  # 800 > 5 % of the 10,000 offered records
    assert run.live_sustained(100.0, run.LiveProgress(97.0, 500, 500 + 400), 100.0, 0)


def test_a_tool_that_made_nothing_visible_in_the_window_has_a_zero_rate():
    written, _ = _live()
    progress = run.live_progress(written, {seq: 500.0 for seq, _ in written}, 0.0, 20.0, 100.0)
    assert progress.visible_rate == 0.0 and not run.live_sustained(100.0, progress, 100.0, 0)


def test_a_live_run_that_lost_lines_did_not_sustain_its_rate():
    good = run.LiveProgress(100.0, 10, 10)
    assert run.live_sustained(100.0, good, 100.0, 0)
    assert not run.live_sustained(100.0, good, 100.0, 9)  # every line accepted, nine never delivered
    assert not run.live_sustained(100.0, run.LiveProgress(10.0, 10, 10), 100.0, 0)


def test_lines_never_made_visible_count_as_lag_not_as_progress():
    written, visible = _live()
    for seq in list(visible)[::2]:
        del visible[seq]  # half the lines are never visible (gaps)
    progress = run.live_progress(written, visible, 0.0, 20.0, 100.0)
    assert progress.visible_rate == pytest.approx(50.0, rel=0.05)
    assert not run.live_sustained(100.0, progress, 100.0, 0)


# -------------------------------------------------- resume and incomplete kill schedules (review of #908)


def _stub_the_lab(monkeypatch, tmp_path):
    """Everything main() would ask Docker or git for, answered; the host does not change between calls."""
    facts = {"host_cpu": "fixture CPU", "host_logical_cpus": "8", "host_os": "Linux 6.1",
             "host_disk": "fixture NVMe", "docker_platform": "Docker Engine", "docker_vm_cpus": 8,
             "docker_vm_kernel": "6.1"}
    monkeypatch.setattr(run.environment, "host_facts", lambda probe_disk=True: dict(facts))
    monkeypatch.setattr(run.environment, "git", lambda *argv: "0" * 40)
    monkeypatch.setattr(run.environment, "config_drift", lambda paths: [])
    monkeypatch.setattr(run.lab, "engine_tree", lambda: "tree")
    monkeypatch.setattr(run.lab, "image_id", lambda image: "sha256:" + "1" * 64)
    monkeypatch.setattr(run.lab, "ensure_vtop_image", lambda rebuild: "vtop-compare:tree")
    monkeypatch.setattr(run.lab, "ensure_minio_image", lambda: None)
    monkeypatch.setattr(run.lab, "ensure_image", lambda image: None)
    monkeypatch.setattr(run, "check", lambda run_dir: 0)
    seen = []

    def run_plan(run_dir, kind, env, describe, execute, **kw):
        seen.append(env)
        return runner.Outcome(0, 0, 0, False)

    monkeypatch.setattr(run.runner, "run_plan", run_plan)
    return seen


@pytest.mark.parametrize("first", [
    ["--plan", "full", "--host-class", "dedicated"],
    ["--plan", "full", "--host-class", "dedicated", "--tools", "vtop,vector"],
])
def test_a_resume_needs_no_repeated_arguments(monkeypatch, tmp_path, first):
    seen = _stub_the_lab(monkeypatch, tmp_path)
    common = ["--results", str(tmp_path), "--max-hours", "1000"]
    assert run.main(first + common + ["--run-id", "spool-x"]) == 0
    recorded = seen[-1]
    assert recorded["comparable"]["host_class"] == "dedicated"
    # The default plan is `smoke` on a `laptop`: the resume must not build its environment from that.
    assert run.main(["--resume", "spool-x"] + common) == 0
    assert len(seen) == 2
    assert seen[-1]["plan"] == recorded["plan"]
    assert seen[-1]["comparable"]["host_class"] == "dedicated"
    assert seen[-1]["plan"]["systems"] == recorded["plan"]["systems"]


def test_a_resume_that_repeats_a_different_plan_is_refused(monkeypatch, tmp_path, capsys):
    seen = _stub_the_lab(monkeypatch, tmp_path)
    common = ["--results", str(tmp_path), "--max-hours", "1000"]
    assert run.main(["--plan", "full", "--host-class", "dedicated", "--tools", "vtop,vector",
                     "--run-id", "spool-x"] + common) == 0
    for argv in (["--tools", "vtop"], ["--plan", "smoke"], ["--host-class", "laptop"]):
        assert run.main(["--resume", "spool-x"] + argv + common) == 2
    assert "cannot resume with" in capsys.readouterr().err
    assert len(seen) == 1


def _crash_rows(tmp_path):
    run_dir = fixture.write_run(str(tmp_path))
    rows, env = schema.load_run(run_dir, KIND)
    return rows, env


def test_a_crash_run_with_fewer_kills_than_planned_is_left_out_of_the_totals(tmp_path):
    from compare_common import report as creport

    rows, env = _crash_rows(tmp_path)
    metric = creport.Metric("gaps", "lines lost", "gaps", "lower", ("crash-K1",), reduce="sum", digits=0)
    victim = next(r for r in rows if r["cell"] == "crash-K1" and r["system"] == "vtop")
    group = ("crash-K1", (victim["source"], victim["rotation"]))
    whole = creport.value(rows, KIND, metric, group, "vtop", victim["profile"])
    assert whole.summary is not None and whole.stub is None
    victim["kills"] = "2"  # the tool went idle before its third trigger: gaps and duplicates are the observed ones
    victim["gaps"] = "7"
    assert KIND.check_row(victim) == []  # the observed counts are kept, not refused
    assert "2 of 3 kills" in KIND.unusable(victim)
    partial = creport.value(rows, KIND, metric, group, "vtop", victim["profile"])
    assert partial.summary is None
    assert "incomplete kill schedule" in partial.stub
    victim["kills"] = "4"
    assert any("4 kills of 3 planned" in problem for problem in KIND.check_row(victim))


def test_a_setup_request_cannot_name_a_path_outside_the_spool_root():
    assert generator.source_directory("file") == "file"
    assert generator.source_directory("syslog-spool") == "syslog-spool"
    for bad in ("../etc", "/etc", "file/../../x", "", None):
        with pytest.raises(ValueError, match="is not one of"):
            generator.source_directory(bad)
