"""A schema-4 run written by the runner the v0.11.1 AWS round ran still loads and reports unchanged.

The AWS round's data was written by the harness at 22e18046 as schema 4, and this train changes the runner
(sized legs, resume, producers and partitions) without bumping the schema. `fixtures/kafka-schema4-runner` is a
run that runner wrote: 22e18046's `run.main` with `--group-commit off,on`, one repetition, 100 B, the
end-to-end stage with Kafka also under fsync-parity, and the resource windows, its Docker calls replaced by
`kafka_compare_stub` (so the numbers are the stub's, the shape is the runner's). `expected-report.md` and
`expected-report-sha256.json` are what 22e18046's report drew from it.
"""
import hashlib
import json
import os
import shutil

import pytest

from kafka_compare import plan, run, schema

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(HERE, "fixtures", "kafka-schema4-runner")


def test_the_old_runner_s_run_is_schema_4_and_complete():
    loaded = schema.load_run(FIXTURE)
    assert loaded.environment["schema_version"] == 4 == schema.SCHEMA_VERSION
    assert {schema.row_key(row) for row in loaded.rows} == plan.expected_keys(loaded.environment["matrix"])
    assert len(loaded.rows) == len(plan.expected_keys(loaded.environment["matrix"])) == 19


def test_the_runner_s_check_passes_it(capsys):
    assert run.check(FIXTURE) == 0
    assert "19 of 19 expected rows present; run_state complete" in capsys.readouterr().out


def test_the_report_draws_it_as_the_old_report_did(tmp_path):
    pytest.importorskip("matplotlib")
    from kafka_compare import report

    run_dir = tmp_path / "run"
    shutil.copytree(FIXTURE, run_dir, ignore=shutil.ignore_patterns("expected-report*"))
    doc, img = tmp_path / "docs" / "kafka-comparison.md", tmp_path / "docs" / "img"
    written = report.build(str(run_dir), str(img), str(doc))
    with open(os.path.join(FIXTURE, "expected-report.md"), encoding="utf-8") as handle:
        assert doc.read_text(encoding="utf-8") == handle.read()
    with open(os.path.join(FIXTURE, "expected-report-sha256.json"), encoding="utf-8") as handle:
        expected = json.load(handle)
    drawn = {os.path.basename(path): hashlib.sha256(open(path, "rb").read()).hexdigest()
             for path in written if path.endswith((".svg", ".md"))}
    assert drawn == expected
