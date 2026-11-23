"""The end-to-end CDF, the resource chart and the profile latency chart, drawn from a schema-3 run, and the crash
page's pairing with that run (#596, #600). Needs the pinned plotting stack; the arithmetic they rest on is in
test_kafka_compare_extra_charts.py."""
import json
import os
import shutil

import pytest

pytest.importorskip("matplotlib")
Image = pytest.importorskip("PIL.Image")

from test_kafka_compare_crash_report import synthetic_run  # noqa: E402
from test_kafka_compare_extra_charts import report_matrix, report_run  # noqa: E402

from kafka_compare import colours, crash_report, footer, schema  # noqa: E402
from kafka_compare import report as kreport  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PHASE_1 = os.path.join(REPO, "benchmarks", "kafka_compare", "results", "20261109T232328Z")
NEW_IMAGES = ("kafka-compare-e2e-cdf", "kafka-compare-resources", "kafka-compare-profile-latency")


def render(run_dir, tmp_path, name="out"):
    img, doc = tmp_path / name / "img", tmp_path / name / "kafka-comparison.md"
    written = kreport.build(run_dir, str(img), str(doc))
    return written, img, doc


# ---------------------------------------------------------------------------------- nothing old moves


def test_the_committed_phase_1_run_regenerates_byte_for_byte(tmp_path):
    """No new chart, section or sentence appears for a run that has no end-to-end or resource data."""
    results = tmp_path / "benchmarks" / "kafka_compare" / "results" / "20261109T232328Z"
    results.mkdir(parents=True)
    for name in ("results.csv", "environment.json"):
        shutil.copy(os.path.join(PHASE_1, name), results / name)
    doc = tmp_path / "docs" / "benchmarks" / "kafka-comparison.md"
    img = tmp_path / "docs" / "benchmarks" / "img"
    written = kreport.build(str(results), str(img), str(doc))
    committed = os.path.join(REPO, "docs", "benchmarks")
    assert doc.read_bytes() == open(os.path.join(committed, "kafka-comparison.md"), "rb").read()
    svgs = [p for p in written if p.endswith(".svg")]
    assert len(svgs) == 3
    for path in svgs:
        assert open(path, "rb").read() == open(os.path.join(committed, "img", os.path.basename(path)), "rb").read()
    assert not [p for p in written if os.path.basename(p).rsplit(".", 1)[0] in NEW_IMAGES]


# ---------------------------------------------------------------------------------- a schema-3 run


def test_a_schema_3_run_draws_the_three_new_charts_and_their_sections(tmp_path):
    written, img, doc = render(report_run(tmp_path / "run"), tmp_path)
    names = sorted(os.path.basename(p) for p in written)
    for image in NEW_IMAGES:
        assert f"{image}.png" in names and f"{image}.svg" in names
    page = doc.read_text()
    for heading in ("## End-to-end latency", "## CPU and bytes written", "## What each durability profile costs"):
        assert heading in page, heading
    for image in NEW_IMAGES:
        assert f"{image}.png" in page
    # What was not measured is no longer claimed unmeasured; the old sentences are for runs without the data.
    assert "are not measured yet" not in page
    assert "Per-node CPU and bytes written are not in this run's CSV" not in page


def test_every_new_image_carries_every_required_footer_field_and_the_registry_colours(tmp_path):
    _written, img, _doc = render(report_run(tmp_path / "run"), tmp_path)
    for name in NEW_IMAGES:
        svg = (img / f"{name}.svg").read_text()
        assert colours.colour("kafka", "light") in svg and colours.colour("vtop", "light") in svg
        with Image.open(img / f"{name}.png") as picture:
            fields = footer.parse(picture.info["Description"])
        for label in footer.REQUIRED:
            assert fields.get(label), (name, label)


def test_the_tables_say_what_the_run_measured_and_mark_a_rate_that_was_not_held(tmp_path):
    def slow(_env, rows):
        for r in rows:
            if r["mode"] == "e2e" and r["system"] == "vtop" and str(r["record_size"]) == "1024":
                r["sustained"], r["note"] = "false", "offered rate not sustained"

    page = render(report_run(tmp_path / "run", slow), tmp_path)[2].read_text()
    e2e_table = page.split("## End-to-end latency", 1)[1].split("\n## ", 1)[0]
    row = next(line for line in e2e_table.splitlines() if line.startswith("| 1 KiB") and "VTOP" in line)
    assert "**no**" in row
    row = next(line for line in e2e_table.splitlines() if line.startswith("| 100 B") and "VTOP" in line)
    assert "**no**" not in row
    # A zero is a value in the table, not an absence.
    resources = page.split("## CPU and bytes written", 1)[1].split("\n## ", 1)[0]
    assert "| 0 (0-0) |" in resources


def test_the_profile_latency_table_lists_the_three_configurations(tmp_path):
    page = render(report_run(tmp_path / "run"), tmp_path)[2].read_text()
    section = page.split("## What each durability profile costs", 1)[1].split("\n## ", 1)[0]
    for label in ("Kafka, defaults", "Kafka, fsync on every message", "VTOP, as shipped"):
        assert label in section


def test_a_run_without_the_extra_profile_has_no_profile_chart(tmp_path):
    matrix = report_matrix()
    matrix["e2e"] = {"load_basis": "common", "load_pct": 50}
    written, _img, doc = render(report_run(tmp_path / "run", matrix=matrix), tmp_path)
    assert not [p for p in written if "profile-latency" in os.path.basename(p)]
    assert "## What each durability profile costs" not in doc.read_text()
    assert [p for p in written if "e2e-cdf" in os.path.basename(p)]


def test_a_run_without_resources_has_no_resource_chart(tmp_path):
    matrix = report_matrix()
    matrix.pop("resources")
    written, _img, doc = render(report_run(tmp_path / "run", matrix=matrix), tmp_path)
    assert not [p for p in written if "compare-resources" in os.path.basename(p)]
    assert "## CPU and bytes written" not in doc.read_text()


def test_the_new_output_is_deterministic(tmp_path):
    run = report_run(tmp_path / "run")
    _a, img_a, doc_a = render(run, tmp_path, "a")
    _b, img_b, doc_b = render(run, tmp_path, "b")
    assert doc_a.read_bytes() == doc_b.read_bytes()
    for name in NEW_IMAGES:
        assert (img_a / f"{name}.svg").read_bytes() == (img_b / f"{name}.svg").read_bytes()


def test_a_run_with_an_undrawn_dimension_is_refused_through_the_cli_and_writes_nothing(tmp_path, capsys):
    run = report_run(tmp_path / "run", matrix={**report_matrix(), "partitions": [1, 6]})
    out = tmp_path / "out"
    assert kreport.main([run, "--img-dir", str(out / "img"), "--doc", str(out / "p.md")]) == 2
    assert "partitions" in capsys.readouterr().err and not out.exists()


# -------------------------------------------------------------------------------- the crash pairing


MATCHING = ("host_cpu", "host_logical_cpus", "host_memory_bytes", "host_os", "docker_vm_kernel", "kafka_image",
            "node_limits", "vtop_engine_tree")


def paired_latency_run(tmp_path, crash_dir, change=None, drop_config=None):
    with open(os.path.join(crash_dir, "environment.json")) as handle:
        crash = json.load(handle)["comparable"]

    def mutate(env, rows):
        env["comparable"].update({k: crash[k] for k in MATCHING})
        env["comparable"].update(change or {})
        for row in rows:
            row["kafka_image"] = env["comparable"]["kafka_image"]
        if drop_config:
            rows[:] = [r for r in rows if (r["system"], r["profile"]) != drop_config]

    return report_run(tmp_path, mutate)


def paired(tmp_path, **kw):
    crash = synthetic_run(tmp_path / "crash")
    return crash, paired_latency_run(tmp_path / "lat", crash, **kw)


def test_a_paired_crash_page_has_the_combined_figure_and_the_table_beside_the_loss(tmp_path):
    crash, latency = paired(tmp_path)
    img, doc = tmp_path / "out" / "img", tmp_path / "out" / "kafka-crash.md"
    written = crash_report.build(crash, str(img), str(doc), kreport._setup_matplotlib, latency_run=latency)
    assert any(p.endswith("kafka-crash-vs-latency.svg") for p in written)
    page = doc.read_text()
    assert "## What each profile costs in latency, beside what it loses" in page
    assert "kafka-crash-vs-latency.png" in page
    with Image.open(img / "kafka-crash-vs-latency.png") as picture:
        description = picture.info["Description"]
    # The footer names both runs.
    assert "run: crash-synthetic" in description and "latency run: v2-run" in description
    assert "disk:" in description and "VTOP:" in description and "Kafka:" in description


def test_an_unpaired_crash_page_is_unchanged(tmp_path):
    crash = synthetic_run(tmp_path / "crash")
    img_a, doc_a = tmp_path / "a" / "img", tmp_path / "a" / "kafka-crash.md"
    img_b, doc_b = tmp_path / "b" / "img", tmp_path / "b" / "kafka-crash.md"
    crash_report.build(crash, str(img_a), str(doc_a), kreport._setup_matplotlib)
    crash_report.build(crash, str(img_b), str(doc_b), kreport._setup_matplotlib, latency_run=None)
    assert doc_a.read_bytes() == doc_b.read_bytes()
    assert sorted(os.listdir(img_a)) == sorted(os.listdir(img_b))
    assert "kafka-crash-vs-latency" not in doc_a.read_text()


@pytest.mark.parametrize("change", [{"host_cpu": "Other CPU"}, {"docker_vm_kernel": "9.9"},
                                    {"node_limits": {"cpus": "8", "mem_limit": "2g", "memswap_limit": "2g"}},
                                    {"vtop_engine_tree": "c" * 40}])
def test_a_latency_run_from_another_environment_is_refused_and_nothing_is_written(tmp_path, change):
    crash, latency = paired(tmp_path, change=change)
    out = tmp_path / "out"
    with pytest.raises(schema.ReportRefused, match="not the same environment"):
        crash_report.build(crash, str(out / "img"), str(out / "kafka-crash.md"), kreport._setup_matplotlib,
                           latency_run=latency)
    assert not out.exists()


@pytest.mark.parametrize("missing", [("kafka", "fsync-parity"), ("vtop", "defaults")])
def test_a_latency_run_missing_a_configuration_the_crash_run_names_is_refused(tmp_path, missing):
    crash, latency = paired(tmp_path, drop_config=missing)
    # Dropping every row of a configuration also makes the run partial, which is refused first by name.
    out = tmp_path / "out"
    with pytest.raises(schema.ReportRefused):
        crash_report.build(crash, str(out / "img"), str(out / "kafka-crash.md"), kreport._setup_matplotlib,
                           latency_run=latency)
    assert not out.exists()


def test_a_latency_run_without_the_extra_profile_cannot_pair_with_a_run_that_names_it(tmp_path):
    crash = synthetic_run(tmp_path / "crash")
    matrix = report_matrix()
    matrix["e2e"] = {"load_basis": "common", "load_pct": 50}

    def mutate(env, rows):
        with open(os.path.join(crash, "environment.json")) as handle:
            comparable = json.load(handle)["comparable"]
        env["comparable"].update({k: comparable[k] for k in MATCHING})
        for row in rows:
            row["kafka_image"] = env["comparable"]["kafka_image"]

    latency = report_run(tmp_path / "lat", mutate, matrix)
    out = tmp_path / "out"
    with pytest.raises(schema.ReportRefused, match="kafka-fsync-parity"):
        crash_report.build(crash, str(out / "img"), str(out / "kafka-crash.md"), kreport._setup_matplotlib,
                           latency_run=latency)


def test_the_cli_pairs_the_run_report_json_names(tmp_path, capsys):
    results = tmp_path / "results"
    results.mkdir()
    crash = synthetic_run(results / "crash-x")
    latency = paired_latency_run(tmp_path / "scratch", crash)
    shutil.copytree(latency, results / "latency-x")
    with open(results / "crash-x" / "report.json", "w") as handle:
        json.dump({"doc": "docs/benchmarks/kafka-crash.md", "img_dir": "docs/benchmarks/img/aws",
                   "latency_run": "latency-x"}, handle)
    out = tmp_path / "out"
    assert kreport.main([str(results / "crash-x"), "--img-dir", str(out / "img"),
                         "--doc", str(out / "kafka-crash.md")]) == 0
    assert "kafka-crash-vs-latency.svg" in capsys.readouterr().out


def test_a_throughput_run_may_not_name_a_latency_run(tmp_path, capsys):
    run = report_run(tmp_path / "run")
    with open(os.path.join(run, "report.json"), "w") as handle:
        json.dump({"doc": "docs/benchmarks/kafka-comparison-x.md", "img_dir": "docs/benchmarks/img/x",
                   "latency_run": "other"}, handle)
    out = tmp_path / "out"
    assert kreport.main([run, "--img-dir", str(out / "img"), "--doc", str(out / "p.md")]) == 2
    assert "latency_run" in capsys.readouterr().err
