"""The link comparison (#747): its tests 2 to 10, and the kind-specific halves of #744 section 8."""
from __future__ import annotations

import csv
import hashlib
import inspect
import json
import os
import re

import pytest

from compare_common import lab, registry, runner, schema
from compare_common import plan as plans
from link_compare import fixture, gen
from link_compare import kind as link
from link_compare import report as link_report
from link_compare import run as link_run
from link_compare.kind import KIND

HERE = os.path.dirname(os.path.abspath(__file__))
BENCH = os.path.dirname(HERE)
REPO = os.path.dirname(BENCH)
COMPOSE = os.path.join(BENCH, "link_compare", "compose.yml")
MIB = 1024 * 1024


def _row(**change):
    row = {column: "" for column in KIND.columns}
    row.update({"system": "aws-cli", "profile": "exit-code", "cell": "defaults", "repetition": "1",
                "link_profile": "wan100", "connections": "default", "grid_point": "", "status": "ok",
                "host_class": "dedicated", "metric_class": "performance"})
    row.update(fixture.metrics(plans.Planned(row["system"], row["profile"], row["cell"], 1,
                                             {"link_profile": "wan100", "connections": "default", "grid_point": ""},
                                             {"files": 48, "file_mib": 20}, "performance"),
                               plans.select(plans.load_plans(link_run.PLANS), "full")))
    row.update({k: str(v) for k, v in change.items()})
    return {k: str(v) for k, v in row.items()}


# ------------------------------------------------------------- test 2


def test_a_run_whose_two_calibrations_differ_by_more_than_ten_percent_is_refused_and_names_both():
    with pytest.raises(link.LinkRefused) as refused:
        link.check_calibrations("wan100", 9.5, 8.1)
    assert "9.5 Mbit/s" in str(refused.value) and "8.1 Mbit/s" in str(refused.value)
    assert link.check_calibrations("wan100", 9.5, 9.0) == pytest.approx(9.25)
    # The policed link's reference is its token rate, and the lan profile is never gated.
    assert link.check_calibrations("wan100-policed", 4.0, 7.0) == link.POLICER_MBPS
    assert link.check_calibrations("lan", 900.0, 400.0) == 650.0
    with pytest.raises(link.LinkRefused, match="measured nothing"):
        link.check_calibrations("wan100", 0.0, 9.0)
    # And an ok row that carries disagreeing calibrations is refused by the loader's row check.
    problems = link.check_row(_row(calib_before_mbps="9.5", calib_after_mbps="8.1"))
    assert any("9.5 Mbit/s" in p and "8.1 Mbit/s" in p for p in problems)


# ------------------------------------------------------------- test 3


def test_objects_committed_comes_from_the_harness_readback_not_from_the_tools_exit_code(tmp_path):
    manifest = gen.generate(str(tmp_path), 3, 0.01, 7)
    manifest = {name: (size, sha) for name, (size, sha) in manifest.items()}
    with open(tmp_path / "files" / "f001.bin", "rb") as handle:
        truncated = handle.read()[:-100]
    stored = [("f000.bin", manifest["f000.bin"][0], manifest["f000.bin"][1]),
              ("f001.bin", len(truncated), hashlib.sha256(truncated).hexdigest()),  # a fake tool's torn write
              ("f002.bin", manifest["f002.bin"][0], manifest["f002.bin"][1]),
              ("x/batch.manifest.json", 10, "0" * 64)]
    judged = link.judge_readback(manifest, stored)
    assert judged.objects_committed == 2 and judged.objects_mismatched == 1 and judged.missing == ["f001.bin"]
    assert judged.objects_extra == 0  # VTOP's manifest is neither committed nor extra
    assert "exit" not in " ".join(inspect.signature(link.judge_readback).parameters)
    # VTOP names objects by batch, not by file: a stored copy under any key counts.
    renamed = [("tenant=default/b1.bin", size, sha) for size, sha in manifest.values()]
    assert link.judge_readback(manifest, renamed).objects_committed == 3


def test_the_seeded_files_are_deterministic_and_incompressible(tmp_path):
    import zlib

    one = gen.generate(str(tmp_path / "a"), 2, 0.05, 747)
    two = gen.generate(str(tmp_path / "b"), 2, 0.05, 747)
    assert one == two and gen.generate(str(tmp_path / "c"), 2, 0.05, 748) != one
    with open(tmp_path / "a" / "files" / "f000.bin", "rb") as handle:
        data = handle.read()
    assert len(zlib.compress(data)) > 0.99 * len(data)


# ------------------------------------------------------------- test 4


@pytest.mark.parametrize("tool", link.SYSTEMS)
@pytest.mark.parametrize("target", link.CONNECTION_TARGETS)
def test_equal_connections_cell_computes_workers_times_parts_per_tool_and_refuses_a_setting_that_misses_the_target(
        tool, target):
    settings = link.equal_connections(tool, target)
    assert link.check_connections(tool, settings, target, 20 * MIB, 48) == target


def test_a_setting_that_misses_the_connection_target_is_refused():
    # rclone at 4 transfers x 4 parts: a 20 MiB file is under the 200 MiB cutoff, one PUT each: 4, not 16.
    with pytest.raises(link.LinkRefused, match="opens 4 connections"):
        link.check_connections("rclone", {"transfers": 4, "upload_concurrency": 4}, 16, 20 * MIB, 48)
    # VTOP: 8 objects x 4 parts in flight, but a 20 MiB object has only 3 parts of 8 MiB: 24, not 32.
    assert link.connections_of("vtop", {"max_concurrency": 8, "parts_in_flight": 4}, 20 * MIB, 48) == 24
    # s5cmd: 8 workers x 2 parts of 5 MiB: 16.
    assert link.connections_of("s5cmd", {"numworkers": 8, "concurrency": 2, "part_size": 5}, 20 * MIB, 48) == 16
    # aws-cli: one pool of requests shared by files and parts.
    assert link.connections_of("aws-cli", {"max_concurrent_requests": 16}, 20 * MIB, 48) == 16
    # And never more than the workload offers.
    assert link.connections_of("aws-cli", {"max_concurrent_requests": 16}, 1 * MIB, 4) == 4


def test_an_equal_conns_row_whose_connections_miss_the_cell_is_refused():
    row = _row(cell="equal-conns", connections="8", connections_actual="4", settings="{}")
    assert any("connections_actual 4" in p for p in link.check_row(row))


# ------------------------------------------------------------- test 5


def test_tool_order_is_rotated_across_repetitions_and_recorded(tmp_path):
    plan = plans.select(plans.load_plans(link_run.PLANS), "full")
    plan["cells"] = [cell for cell in plan["cells"] if cell["cell"] == "defaults"]
    plan["cells"][0]["variants"] = plan["cells"][0]["variants"][:1]
    order = [item.system for item in plans.schedule(plan, KIND)]
    assert order == ["aws-cli", "vtop", "rclone", "s5cmd", "vtop", "rclone", "s5cmd", "aws-cli",
                     "rclone", "s5cmd", "aws-cli", "vtop"]
    env = fixture.environment("r", "full")
    env["plan"] = plan
    runner.run_plan(str(tmp_path), KIND, env, lambda item: {"system_version": "1", "image_digest": "d",
                                                             "config_sha256": "x"},
                    lambda item: {"status": "refused", "note": "no lab in a unit test"}, log=lambda m: None)
    with open(tmp_path / "link.csv", newline="") as handle:
        recorded = [row["system"] for row in csv.DictReader(handle)]
    assert recorded == order  # the CSV is written in run order, so the rotation is in the record


# ------------------------------------------------------------- test 6


def test_tuned_grid_has_the_same_size_for_every_tool_and_the_chart_subtitle_names_it(tmp_path):
    plan = plans.select(plans.load_plans(link_run.PLANS), "full")
    size = link.check_grids(plan["grids"], plan["systems"])
    assert size == link_report.GRID_SIZE <= link.MAX_GRID
    tuned = [chart for chart in link_report.REPORT.charts if chart.name == "link-tuned"][0]
    assert f"grid of {size} points" in tuned.subtitle
    uneven = dict(plan["grids"], s5cmd=plan["grids"]["s5cmd"][:5])
    with pytest.raises(link.LinkRefused, match="differ in size"):
        link.check_grids(uneven, plan["systems"])
    with pytest.raises(link.LinkRefused, match="between 1 and 6"):
        link.check_grids({s: [{}] * 7 for s in plan["systems"]}, plan["systems"])
    run = fixture.write_run(str(tmp_path))
    with open(os.path.join(run, "environment.json")) as handle:
        env = json.load(handle)
    env["plan"]["grids"] = {s: g[:5] for s, g in env["plan"]["grids"].items()}
    with open(os.path.join(run, "environment.json"), "w") as handle:
        json.dump(env, handle)
    with pytest.raises(schema.RunRefused, match="names 6"):
        link_report.check_grid(run)


# ------------------------------------------------------------- test 7


def test_vtop_goodput_is_reported_with_and_without_verification_and_never_mixed():
    vtop = _row(system="vtop", profile="strong", vtop_upload_goodput_fraction="")
    assert any("upload-only goodput" in p for p in link.check_row(vtop))
    assert link.check_row(_row(system="vtop", profile="strong", vtop_upload_goodput_fraction="0.8")) == []
    other = _row(vtop_upload_goodput_fraction="0.8")
    assert any("VTOP rows only" in p for p in link.check_row(other))
    # The ranked goodput is the end-to-end column; the upload-only one is never drawn.
    drawn = {m.column for chart in link_report.REPORT.charts for m in chart.metrics}
    assert "goodput_fraction" in drawn and "vtop_upload_goodput_fraction" not in drawn


# ------------------------------------------------------------- test 8


def test_a_hosted_run_is_accepted_only_for_a_cell_whose_recorded_bottleneck_is_the_link_and_whose_cpu_share_is_below_the_stated_limit(tmp_path):  # noqa: E501
    base = dict(host_class="hosted", cell="committed", metric_class="correctness", cpu_seconds="",
                cpu_seconds_per_gib="")
    assert link.check_row(_row(**base, bottleneck="link", cpu_share="0.10")) == []
    assert link.check_row(_row(**base, bottleneck="cpu", cpu_share="0.10"))
    assert link.check_row(_row(**base, bottleneck="link", cpu_share=str(link.HOSTED_CPU_SHARE_LIMIT + 0.01)))
    assert link.check_row(_row(**dict(base, cpu_seconds_per_gib="12"), bottleneck="link", cpu_share="0.1"))
    assert link.bottleneck("wan100-policed", 0.1) == "link" and link.bottleneck("wan100", 0.9) == "cpu"
    assert link.bottleneck("lan", 0.1) == "unknown"
    # And the shared loader refuses any performance row from a hosted runner.
    run = fixture.write_run(str(tmp_path), host_class="hosted")
    with pytest.raises(schema.RunRefused, match="performance row from a hosted runner"):
        schema.load_run(run, KIND)
    hosted = plans.select(plans.load_plans(link_run.PLANS), "hosted")
    assert {KIND.cells[cell["cell"]] for cell in hosted["cells"]} == {"correctness"}


# ------------------------------------------------------------- test 9


def test_bytes_on_the_wire_counts_the_middlebox_interface_not_the_tools_own_report():
    before = {"rx_bytes": 1_000, "tool_reported_bytes": 0}
    after = {"rx_bytes": 1_000 + 22_000_000, "tool_reported_bytes": 20_971_520}
    assert link.wire_bytes(before, after) == 22_000_000
    with pytest.raises(link.LinkRefused, match="backwards"):
        link.wire_bytes(after, before)
    source = inspect.getsource(link_run.Lab.rx_bytes)
    assert "statistics/rx_bytes" in source and 'container("netem")' in source


# ------------------------------------------------------------- test 10


def test_jain_index_is_never_written_without_both_per_flow_goodputs():
    assert link.jain(5.0, 5.0) == 1.0
    for missing in ((None, 5.0), (5.0, None), ("", 5.0)):
        with pytest.raises(link.LinkRefused, match="both per-flow goodputs"):
            link.jain(*missing)
    row = _row(cell="neighbour", jain_index="0.9", neighbour_with_mbps="", tool_with_mbps="6")
    assert any("Jain index without both" in p for p in link.check_row(row))


# ---------------------------------------------- the kind's halves of #744 section 8


def test_compose_limits_are_identical_within_a_limit_class():
    with open(COMPOSE) as handle:
        limits = lab.compose_limits(handle.read())
    tools = {service: limits.get(service) for service in link.SYSTEMS}
    assert set(map(json.dumps, tools.values())) == {json.dumps({"cpus": "2", "mem_limit": "2g",
                                                                "memswap_limit": "2g"})}, tools


def test_every_tool_config_disables_telemetry():
    lines = link_run.telemetry_lines()
    assert set(lines) == set(link.SYSTEMS)
    for system, line in lines.items():
        assert re.match(r"telemetry: none to turn off\. \S", line), (system, line)


def test_pins_match_products_toml():
    reg = registry.load()
    with open(COMPOSE) as handle:
        text = handle.read()
    for system in ("aws-cli", "rclone", "s5cmd"):
        image = registry.product(reg, system)["image"]
        block = re.search(rf"^  {re.escape(system)}:\n(.*?)(?=^  \S|\Z)", text, re.M | re.S).group(1)
        assert f"image: {image}" in block, system
    for image in re.findall(r"image: (\S+)", text):
        assert "@sha256:" in image or image.startswith(("vtop-minio:", "${VTOP_IMAGE")), image


def test_every_committed_plan_is_valid_and_the_smoke_plan_is_lan_only():
    data = plans.load_plans(link_run.PLANS)
    for name in data["plans"]:
        plan = plans.select(data, name)
        plans.check_plan(plan, KIND)
        link.check_grids(plan["grids"], plan["systems"])
    smoke = plans.select(data, "smoke")
    assert {v["link_profile"] for cell in smoke["cells"] for v in cell["variants"]} == {"lan"}
    for name in data["plans"]:
        for cell in plans.select(data, name)["cells"]:
            for variant in cell["variants"]:
                assert variant["link_profile"] in link.LINK_PROFILES


def test_the_config_hash_recorded_is_the_committed_files():
    from compare_common import environment

    configs = environment.record_configs({s: {p: link_run.CONFIGS[s] for p in KIND.profiles[s]}
                                          for s in link.SYSTEMS})
    for system, rel in link_run.CONFIGS.items():
        with open(os.path.join(REPO, rel)) as handle:
            assert configs[system][KIND.profiles[system][0]]["sha256"] == schema.sha256_text(handle.read())


# ----------------------------------------------------- the commands the tools get


def test_vtop_config_is_rendered_with_no_placeholder_left(tmp_path):
    link_run.render_config("vtop", {}, "bkt", str(tmp_path / "d"), 20 * MIB)
    text = (tmp_path / "d" / "config.yaml").read_text()
    assert not re.search(r"@[A-Z_]+@", text) and 'bucket: "bkt"' in text
    assert not re.search(r"^\s+transports:", text, re.M)
    link_run.render_config("vtop", {"max_concurrency": 4, "parts_in_flight": 1}, "bkt", str(tmp_path / "e"), 20 * MIB)
    tuned = (tmp_path / "e" / "config.yaml").read_text()
    assert "max_concurrent_batches: 4" in tuned and "      parts_in_flight: 1" in tuned
    assert (tmp_path / "e" / "run.sh").exists()


def test_tool_commands_carry_the_settings():
    assert link_run.tool_command("rclone", {"transfers": 8, "upload_concurrency": 1}, "b", 48)[-2:] == \
        ["--transfers=8", "--s3-upload-concurrency=1"]
    s5 = link_run.tool_command("s5cmd", {"numworkers": 8, "concurrency": 2, "part_size": 5}, "b", 48)
    assert s5[:3] == ["--endpoint-url=http://netem:9200", "--numworkers=8", "cp"]
    assert "--concurrency=2" in s5 and "--part-size=5" in s5 and s5[-1] == "s3://b/"
    aws = link_run.tool_command("aws-cli", {}, "b", 48)
    assert aws[:3] == ["s3", "cp", "--recursive"] and "http://netem:9200" in aws
    assert link_run.tool_command("vtop", {}, "b", 48) == ["48"]


def test_aws_cli_settings_go_into_its_config(tmp_path):
    link_run.render_config("aws-cli", {"max_concurrent_requests": 16}, "b", str(tmp_path), 20 * MIB)
    text = (tmp_path / "config").read_text()
    assert text.rstrip().endswith("max_concurrent_requests = 16") and "addressing_style = path" in text


def test_the_dry_run_prints_the_plan_and_touches_no_docker(capsys, monkeypatch):
    monkeypatch.setattr(lab, "sh", lambda *a, **k: pytest.fail("docker was called"))
    assert link_run.main(["--plan", "full", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "220 rows expected" in out and "tuned-grid" in out


def test_a_plan_projected_over_max_hours_is_refused(capsys):
    assert link_run.main(["--plan", "full", "--dry-run", "--max-hours", "5"]) == 2
    assert "over --max-hours" in capsys.readouterr().err


# ------------------------------------------------- review of #907: failures and the neighbour window


def test_a_failed_tool_is_a_failed_row_that_still_says_how_many_files_it_committed():
    status, note = link.run_status("defaults", 1, {}, 12, 48, "boom: policer\nreset")
    assert status == "failed" and "12 of 48 files committed" in note and "boom: policer reset" in note
    assert link.run_status("defaults", "timeout", {}, 0, 48)[0] == "failed"
    # Exit 0 with nothing committed: the payload-derived columns cannot be measured, so not an ok row.
    status, note = link.run_status("defaults", 0, {"objects_committed": 0}, 0, 48)
    assert status == "failed" and note.startswith("could not measure") and "0 of 48" in note
    assert link.run_status("smoke", 0, {}, 3, 4) == ("failed", "3 of 4 files committed")
    full = {column: "1" for column in link.REQUIRED["defaults"]}
    assert link.run_status("defaults", 0, full, 48, 48) == ("ok", "")


def test_both_flows_of_the_jain_index_come_from_the_same_counters_over_the_same_window():
    text = ("Chain INPUT (policy ACCEPT 0 packets, 0 bytes)\n"
            "    pkts      bytes target     prot opt in     out     source               destination\n"
            "     120   180000            tcp  --  *      *       0.0.0.0/0            0.0.0.0/0  tcp dpt:5201 "
            "/* vtb-neighbour */\n"
            "     300   450000            tcp  --  *      *       0.0.0.0/0            10.77.47.10 tcp dpt:9000 "
            "/* vtb-store */\n")
    assert link.parse_flow_counters(text) == {"vtb-neighbour": 180000, "vtb-store": 450000}
    samples = [(t, {"vtb-neighbour": 125_000 * t, "vtb-store": 250_000 * t}) for t in range(0, 21)]
    # The window excludes the samples before 5 s (the omitted ramp) and after 15 s (collection).
    assert link.window_mbps(samples, "vtb-neighbour", 5, 15) == pytest.approx(1.0)
    assert link.window_mbps(samples, "vtb-store", 5, 15) == pytest.approx(2.0)
    with pytest.raises(link.LinkRefused, match="fewer than two"):
        link.window_mbps(samples, "vtb-store", 5.2, 5.8)
    source = inspect.getsource(link_run.neighbour)
    assert "with_tool.bytes" not in source and source.count("window_mbps") >= 2
