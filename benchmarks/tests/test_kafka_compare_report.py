"""The results CSV and the report's refusals (#595 rule 9, #600).

A missing, partial or mixed-environment run must refuse - exit non-zero with
the reason - and write nothing, rather than draw a chart with holes. A cell
that could not run is a ROW saying so, and that is not a hole.
"""
import copy
import csv
import json
import os

import pytest

from kafka_compare import report, schema

MATRIX = {
    "record_sizes": [100, 1024], "partitions": [1], "producers": [1], "acks": ["all"], "profile": "defaults",
    "repetitions": 3, "load_pcts": [25, 50, 75], "consume": True, "sat_records": {"100": 1000, "1024": 1000},
    "fixed_seconds": 30, "warmup_fraction": 0.2, "smoke_records": 10,
}

COMPARABLE = {
    "host_cpu": "Test CPU", "host_model": "Test1,1", "host_logical_cpus": "8", "host_memory_bytes": str(16 * 2**30),
    "host_os": "macOS 1.0", "docker_platform": "Docker Desktop 1.0", "docker_server_version": "1.0",
    "docker_vm_os": "Docker Desktop", "docker_vm_kernel": "6.0", "docker_vm_arch": "aarch64", "docker_vm_cpus": 8,
    "docker_vm_memory_bytes": 8 * 2**30, "docker_storage_driver": "overlayfs", "volume_filesystem": "/dev/vda1 ext4",
    "node_limits": {"cpus": "2", "mem_limit": "2g", "memswap_limit": "2g"},
    "client_limits": {"cpus": "4", "memory": "4g", "heap": "-Xms1g -Xmx1g"},
    "kafka_image": "apache/kafka:4.3.2@sha256:00", "kafka_image_id": "sha256:00",
    "vtop_commit": "a" * 40, "vtop_engine_tree": "b" * 40, "vtop_image": "vtop-bench:aaaa",
    "vtop_image_id": "sha256:11", "client": "perf-test 4.3.2 (JVM)", "client_java": "openjdk 21",
    "profile": "defaults", "producer_props": ["acks=all"], "consumer_props": ["enable.auto.commit=false"],
}


def environment():
    return {"run_id": "test-run", "run_state": "complete", "started_at": "2026-11-09T00:00:00+00:00",
            "finished_at": "2026-11-09T01:00:00+00:00", "comparable": copy.deepcopy(COMPARABLE),
            "matrix": copy.deepcopy(MATRIX), "observed": {}, "events": [], "other_containers_at_start_count": 0}


def complete_rows(env):
    """One plausible row for every key a complete run of MATRIX writes."""
    fp = schema.fingerprint(env)
    rows = []
    for (system, mode, size, partitions, producers, rep, pct) in sorted(schema.expected_keys(env["matrix"])):
        base = 300000.0 if system == "kafka" else 30000.0
        rate = base / (1 if size == 100 else 10) * (1 + rep / 100)
        row = {
            "run_id": env["run_id"], "env_fingerprint": fp, "vtop_commit": env["comparable"]["vtop_commit"],
            "kafka_image": env["comparable"]["kafka_image"], "client": env["comparable"]["client"],
            "profile": "defaults", "system": system, "mode": mode, "record_size": size, "partitions": partitions,
            "producers": producers, "acks": "all", "repetition": rep, "load_pct": pct, "status": "ok",
            "records": 1000, "records_per_sec": rate, "mib_per_sec": rate * size / 2**20,
            "started_at": "2026-11-09T00:00:00+00:00", "wall_seconds": 1.0, "note": "",
        }
        if mode == "fixed_rate":
            offered = int(300000 / (1 if size == 100 else 10) * int(pct) / 100)
            achieved = offered if system == "kafka" else min(offered, rate)
            row.update(offered_rate=offered, records_per_sec=achieved,
                       sustained=schema.format_value(achieved >= offered * 0.95),
                       lat_avg_ms=2.0, lat_max_ms=9.0, lat_p50_ms=1.0 if system == "kafka" else 0.0,
                       lat_p95_ms=3.0, lat_p99_ms=5.0 + rep, lat_p999_ms=8.0)
        rows.append(row)
    return rows


def write_run(directory, env, rows):
    os.makedirs(directory, exist_ok=True)
    with open(os.path.join(directory, "environment.json"), "w") as handle:
        json.dump(env, handle)
    path = os.path.join(directory, "results.csv")
    schema.write_header(path)
    for row in rows:
        schema.append_row(path, row)
    return directory


@pytest.fixture
def run_dir(tmp_path):
    env = environment()
    return write_run(str(tmp_path / "run"), env, complete_rows(env))


def rewrite(run_dir, mutate):
    """Load the run's rows as text, let `mutate` change them, write them back."""
    path = os.path.join(run_dir, "results.csv")
    with open(path, newline="") as handle:
        rows = list(csv.DictReader(handle))
    rows = mutate(rows)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=schema.COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


# ------------------------------------------------------------------ acceptance


def test_a_complete_run_loads(run_dir):
    run = schema.load_run(run_dir)
    assert len(run.rows) == len(schema.expected_keys(MATRIX)) == 2 * 2 * 3 * (1 + 3 + 1)


def test_a_cell_that_could_not_run_is_a_row_not_a_hole(run_dir):
    def refuse_vtop_1k(rows):
        for row in rows:
            if row["system"] == "vtop" and row["record_size"] == "1024":
                for column in ("records", "records_per_sec", "mib_per_sec", "lat_p50_ms", "lat_p95_ms",
                               "lat_p99_ms", "lat_p999_ms", "sustained", "offered_rate"):
                    row[column] = ""
                row.update(status="refused", note="cluster refused before timing: smoke read back 0 records")
        return rows

    rewrite(run_dir, refuse_vtop_1k)
    run = schema.load_run(run_dir)
    assert any(row["status"] == "refused" for row in run.rows)


# ---------------------------------------------------------------------- refusals


def refused(run_dir, match):
    with pytest.raises(schema.ReportRefused, match=match):
        schema.load_run(run_dir)


def test_refuses_a_missing_csv(run_dir):
    os.remove(os.path.join(run_dir, "results.csv"))
    refused(run_dir, "no results CSV")


def test_refuses_a_missing_environment(run_dir):
    os.remove(os.path.join(run_dir, "environment.json"))
    refused(run_dir, "no environment.json")


def test_refuses_a_run_that_did_not_finish(run_dir, tmp_path):
    env = environment()
    env["run_state"] = "interrupted"
    write_run(run_dir, env, complete_rows(environment()))
    refused(run_dir, "did not finish")


def test_refuses_a_header_only_csv(run_dir):
    rewrite(run_dir, lambda rows: [])
    refused(run_dir, "no rows")


def test_refuses_unexpected_columns(run_dir):
    path = os.path.join(run_dir, "results.csv")
    with open(path) as handle:
        text = handle.read()
    with open(path, "w") as handle:
        handle.write(text.replace("lat_p999_ms", "lat_p9999_ms", 1))
    refused(run_dir, "columns")


def test_refuses_a_partial_run(run_dir):
    rewrite(run_dir, lambda rows: [row for row in rows if not (
        row["system"] == "vtop" and row["mode"] == "fixed_rate" and row["load_pct"] == "75"
        and row["repetition"] == "3" and row["record_size"] == "1024")])
    refused(run_dir, "partial: 1 of 60 expected rows are missing")


def test_refuses_a_truncated_last_line(run_dir):
    path = os.path.join(run_dir, "results.csv")
    with open(path) as handle:
        text = handle.read()
    with open(path, "w") as handle:
        handle.write(text[: text.rstrip("\n").rfind(",")] + "\n")
    refused(run_dir, "wrong number of fields")


def test_refuses_rows_from_two_commits(run_dir):
    def two_commits(rows):
        rows[-1]["vtop_commit"] = "c" * 40
        return rows

    rewrite(run_dir, two_commits)
    refused(run_dir, "mixes 2 values of vtop_commit")


def test_refuses_rows_from_two_environments(run_dir):
    def two_envs(rows):
        rows[0]["env_fingerprint"] = "0000000000000000"
        return rows

    rewrite(run_dir, two_envs)
    refused(run_dir, "mixes 2 values of env_fingerprint")


def test_refuses_a_csv_written_under_another_environment(run_dir):
    env = environment()
    env["comparable"]["docker_vm_cpus"] = 4  # the VM changed after the rows were written
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    refused(run_dir, "fingerprints to")


def test_refuses_a_failed_row_that_does_not_say_why(run_dir):
    def silent_failure(rows):
        rows[0].update(status="failed", note="")
        return rows

    rewrite(run_dir, silent_failure)
    refused(run_dir, "must say why")


def test_refuses_an_ok_row_without_its_number(run_dir):
    def hole(rows):
        rows[0]["records_per_sec"] = ""
        return rows

    rewrite(run_dir, hole)
    refused(run_dir, "records_per_sec is ''")


def test_refuses_a_nan_measurement(run_dir):
    def nan(rows):
        for row in rows:
            if row["mode"] == "fixed_rate":
                row["lat_p99_ms"] = "nan"
                break
        return rows

    rewrite(run_dir, nan)
    refused(run_dir, "lat_p99_ms")


def test_refuses_duplicate_rows(run_dir):
    rewrite(run_dir, lambda rows: rows + [dict(rows[0])])
    refused(run_dir, "duplicate")


def test_refuses_rows_outside_the_recorded_matrix(run_dir):
    def extra(rows):
        row = dict(rows[0])
        row["record_size"] = "10240"
        return rows + [row]

    rewrite(run_dir, extra)
    refused(run_dir, "outside the recorded matrix")


def test_refuses_an_unknown_status(run_dir):
    def odd(rows):
        rows[0]["status"] = "maybe"
        return rows

    rewrite(run_dir, odd)
    refused(run_dir, "unknown status")


def test_fingerprint_ignores_timestamps_but_not_the_environment():
    a, b = environment(), environment()
    b["started_at"] = "2030-01-01T00:00:00+00:00"
    assert schema.fingerprint(a) == schema.fingerprint(b)
    b["comparable"]["kafka_image"] = "apache/kafka:9.9.9@sha256:ff"
    assert schema.fingerprint(a) != schema.fingerprint(b)


# ------------------------------------------------------------- the report's exit


def test_report_exits_nonzero_and_writes_nothing_on_a_partial_run(run_dir, tmp_path, capsys):
    rewrite(run_dir, lambda rows: rows[:-1])
    img_dir = tmp_path / "img"
    doc = tmp_path / "doc" / "kafka-comparison.md"
    assert report.main([run_dir, "--img-dir", str(img_dir), "--doc", str(doc)]) == 2
    assert "report refused: results.csv is partial" in capsys.readouterr().err
    assert not img_dir.exists() and not doc.exists()


def test_report_exits_nonzero_on_a_missing_run(tmp_path, capsys):
    assert report.main([str(tmp_path / "nope"), "--img-dir", str(tmp_path / "img"),
                        "--doc", str(tmp_path / "d.md")]) == 2
    assert "report refused" in capsys.readouterr().err


# ------------------------------------------------------------------- rendering


def test_report_renders_deterministically(run_dir, tmp_path):
    pytest.importorskip("matplotlib")
    outputs = []
    for attempt in ("a", "b"):
        img_dir = tmp_path / attempt / "img"
        doc = tmp_path / attempt / "kafka-comparison.md"
        assert report.main([run_dir, "--img-dir", str(img_dir), "--doc", str(doc)]) == 0
        files = sorted(os.listdir(img_dir))
        assert files == sorted(f"kafka-compare-{name}.{ext}" for name in
                               ("produce-throughput", "produce-latency", "consume-throughput")
                               for ext in ("png", "svg"))
        outputs.append({name: (img_dir / name).read_bytes() for name in files})
        outputs[-1]["doc"] = doc.read_bytes()
    assert outputs[0] == outputs[1]


def test_report_tables_come_from_the_csv(run_dir, tmp_path):
    pytest.importorskip("matplotlib")
    doc = tmp_path / "kafka-comparison.md"
    assert report.main([run_dir, "--img-dir", str(tmp_path / "img"), "--doc", str(doc)]) == 0
    text = doc.read_text()
    # Kafka 100 B saturation: reps at 303000, 306000, 309000 -> median 306,000 (303,000-309,000).
    assert "| 100 B | Apache Kafka | 306,000 (303,000-309,000) |" in text
    assert "indicative, not a production benchmark" in text
    assert "**Kafka** saturates at 10.0x" in text


# ------------------------------------------------------------ review follow-ups


def test_refuses_an_environment_that_is_not_an_object(run_dir):
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(["complete"], handle)
    refused(run_dir, "not an object")


@pytest.mark.parametrize("key, value", [
    ("record_sizes", "100"), ("record_sizes", [100, 100]), ("partitions", []), ("producers", [0]),
    ("repetitions", "3"), ("repetitions", 0), ("load_pcts", [25, 100]), ("acks", "all"), ("consume", "yes"),
    ("smoke_records", None),
])
def test_refuses_a_malformed_matrix_instead_of_a_traceback(run_dir, key, value):
    env = environment()
    env["matrix"][key] = value
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    refused(run_dir, f"matrix.{key}")


def test_report_refuses_a_malformed_matrix_with_exit_2(run_dir, tmp_path, capsys):
    env = environment()
    del env["matrix"]["repetitions"]
    with open(os.path.join(run_dir, "environment.json"), "w") as handle:
        json.dump(env, handle)
    assert report.main([run_dir, "--img-dir", str(tmp_path / "img"), "--doc", str(tmp_path / "d.md")]) == 2
    assert "matrix.repetitions" in capsys.readouterr().err


@pytest.mark.parametrize("column, spelling", [
    ("record_size", "100.0"), ("record_size", "1e2"), ("repetition", "01"), ("partitions", "1.5"),
])
def test_refuses_a_key_column_that_is_not_a_canonical_integer(run_dir, column, spelling):
    def respell(rows):
        rows[0][column] = spelling  # the same number, written another way
        return rows

    rewrite(run_dir, respell)
    refused(run_dir, f"{column} is '{spelling}', not a positive whole number")


def test_refuses_a_non_canonical_load_pct(run_dir):
    def respell(rows):
        next(row for row in rows if row["load_pct"] == "25")["load_pct"] = "25.0"
        return rows

    rewrite(run_dir, respell)
    refused(run_dir, "load_pct is '25.0'")


def test_refuses_a_malformed_offered_rate_on_a_failed_row(run_dir):
    def bad_offer(rows):
        row = next(row for row in rows if row["mode"] == "fixed_rate")
        row.update(status="failed", note="timed out after 1800 s", offered_rate="fast")
        return rows

    rewrite(run_dir, bad_offer)
    refused(run_dir, "offered_rate is 'fast'")


def test_refuses_rows_under_another_acks_mode(run_dir):
    def acks_one(rows):
        for row in rows:
            row["acks"] = "1"
        return rows

    rewrite(run_dir, acks_one)
    refused(run_dir, "acks='1'")


@pytest.mark.parametrize("column, value", [
    ("vtop_commit", "c" * 40), ("kafka_image", "apache/kafka:9.9.9@sha256:ff"), ("client", "other client"),
    ("profile", "fsync-parity"), ("run_id", "another-run"),
])
def test_refuses_csv_identity_columns_that_do_not_match_the_environment(run_dir, column, value):
    def relabel(rows):
        for row in rows:
            row[column] = value
        return rows

    rewrite(run_dir, relabel)
    refused(run_dir, f"{column}=")


def _fail_one_repetition(rows):
    row = next(row for row in rows if row["system"] == "kafka" and row["mode"] == "fixed_rate"
               and row["record_size"] == "100" and row["load_pct"] == "25" and row["repetition"] == "2")
    for column in ("records", "records_per_sec", "mib_per_sec", "lat_avg_ms", "lat_max_ms", "lat_p50_ms",
                   "lat_p95_ms", "lat_p99_ms", "lat_p999_ms", "sustained"):
        row[column] = ""
    row.update(status="failed", note="timed out | after\nretry `x`")
    return rows


def test_a_failed_repetition_means_the_rate_was_not_held_in_every_repetition(run_dir):
    run = schema.load_run(run_dir)
    assert report.cell_summary(run.rows, "kafka", "fixed_rate", 100, "lat_p50_ms", 25)["sustained"]
    rewrite(run_dir, _fail_one_repetition)
    run = schema.load_run(run_dir)
    cell = report.cell_summary(run.rows, "kafka", "fixed_rate", 100, "lat_p50_ms", 25)
    assert cell["ok"] == 2 and cell["n"] == 3
    assert not cell["sustained"]


def test_failure_notes_cannot_break_a_markdown_table():
    assert report.md_cell("a | b\nc `d` <e>") == "a \\| b c \\`d\\` \\<e\\>"
    cell = {"summary": None, "not_ok": [{"note": "exit 1 | broken\npipe"}], "ok": 0, "n": 1}
    text = report._summary_cell(cell)
    assert "\n" not in text and "|" not in text.replace("\\|", "") and "\\| broken" in text


def test_no_ratio_when_either_saturation_is_zero():
    zero = {"summary": report.stats.Summary(0.0, 0.0, 0.0, 3)}
    some = {"summary": report.stats.Summary(5.0, 4.0, 6.0, 3)}
    assert report._ratio(zero, some) is None
    assert report._ratio(some, zero) is None
    assert report._ratio(some, some) == 1.0


@pytest.mark.parametrize("value", ["", None, "n/a", "0"])
def test_footer_renders_unknown_host_memory_instead_of_crashing(value):
    env = environment()
    env["comparable"]["host_memory_bytes"] = value
    assert "logical CPUs, unknown)" in report.footer_text(env, "test-run")


def test_report_writes_a_doc_given_as_a_bare_file_name(run_dir, tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    monkeypatch.chdir(tmp_path)
    assert report.main([run_dir, "--img-dir", str(tmp_path / "img"), "--doc", "kafka-comparison.md"]) == 0
    assert (tmp_path / "kafka-comparison.md").is_file()


def test_report_marks_a_failed_repetition_in_the_tables(run_dir, tmp_path):
    pytest.importorskip("matplotlib")
    rewrite(run_dir, _fail_one_repetition)
    doc = tmp_path / "kafka-comparison.md"
    assert report.main([run_dir, "--img-dir", str(tmp_path / "img"), "--doc", str(doc)]) == 0
    text = doc.read_text()
    assert "[2/3 reps ok]" in text
    assert "- 100 B records, Apache Kafka: sustained 2 of 3 offered rates in every repetition (50%, 75%)." in text
