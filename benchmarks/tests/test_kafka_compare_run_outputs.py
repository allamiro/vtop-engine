"""A committed run's own output paths (`report.json`, run_outputs.py): what it accepts and what it refuses.

The file lets a second run of a kind (the AWS round's next to the laptop's, the two halves of a split plan) write
its page and images beside the first instead of onto the same files, which `regenerate.py` refuses. It must not
let a run write outside what the drift check watches.
"""
import json
import os

import pytest

from kafka_compare import run_outputs as ro

KAFKA = "docs/benchmarks/img"
ARCHIVE = "docs/benchmarks/archive/img"


def run_with(tmp_path, content, name="run"):
    run = tmp_path / name
    run.mkdir()
    if content is not None:
        (run / ro.FILE).write_text(content if isinstance(content, str) else json.dumps(content))
    return str(run)


def test_a_run_without_the_file_writes_the_kinds_default_paths(tmp_path):
    assert ro.resolve(run_with(tmp_path, None), str(tmp_path), KAFKA) is None


def test_the_file_names_the_page_and_the_image_directory(tmp_path):
    run = run_with(tmp_path, {"doc": "docs/benchmarks/kafka-comparison-aws-v0.11.0.md",
                              "img_dir": "docs/benchmarks/img/aws-v0.11.0"})
    got = ro.resolve(run, "/repo", KAFKA)
    assert got.doc == "/repo/docs/benchmarks/kafka-comparison-aws-v0.11.0.md"
    assert got.img_dir == "/repo/docs/benchmarks/img/aws-v0.11.0"


def test_a_comparison_kind_keeps_its_images_under_its_own_directory(tmp_path):
    ok = {"doc": "docs/benchmarks/archive-comparison-aws.md", "img_dir": "docs/benchmarks/archive/img/aws"}
    assert ro.resolve(run_with(tmp_path, ok), "/repo", ARCHIVE).img_dir == "/repo/docs/benchmarks/archive/img/aws"
    with pytest.raises(ro.OutputsRefused, match="must be a directory under docs/benchmarks/archive/img/"):
        ro.resolve(run_with(tmp_path, dict(ok, img_dir="docs/benchmarks/img/aws"), "other"), "/repo", ARCHIVE)


BAD = [
    ("not json", "is not readable JSON"),
    ([], "exactly the keys"),
    ({"doc": "docs/benchmarks/a.md"}, "exactly the keys"),
    ({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/img/a", "extra": 1}, "exactly the keys"),
    ({"doc": 3, "img_dir": "docs/benchmarks/img/a"}, "non-empty string"),
    ({"doc": "/etc/a.md", "img_dir": "docs/benchmarks/img/a"}, "plain relative path"),
    ({"doc": "docs/benchmarks/../a.md", "img_dir": "docs/benchmarks/img/a"}, "plain relative path"),
    ({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/img/../../../x"}, "plain relative path"),
    ({"doc": "docs\\benchmarks\\a.md", "img_dir": "docs/benchmarks/img/a"}, "plain relative path"),
    ({"doc": "docs/a.md", "img_dir": "docs/benchmarks/img/a"}, "directly under docs/benchmarks/"),
    ({"doc": "docs/benchmarks/sub/a.md", "img_dir": "docs/benchmarks/img/a"}, "directly under docs/benchmarks/"),
    ({"doc": "docs/benchmarks/a.txt", "img_dir": "docs/benchmarks/img/a"}, "Markdown page"),
    ({"doc": "docs/benchmarks/index.md", "img_dir": "docs/benchmarks/img/a"}, "not index.md"),
    ({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/img"}, "must be a directory under"),
    ({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/other/a"}, "must be a directory under"),
    ({"doc": "docs/benchmarks/a.md", "img_dir": "docs/benchmarks/img//a"}, "plain relative path"),
]


@pytest.mark.parametrize(("content", "reason"), BAD)
def test_a_report_json_that_could_write_elsewhere_is_refused(tmp_path, content, reason):
    with pytest.raises(ro.OutputsRefused, match=reason):
        ro.resolve(run_with(tmp_path, content), str(tmp_path), KAFKA)


def test_the_report_refuses_with_exit_2_and_writes_nothing(tmp_path, capsys):
    from kafka_compare import report

    run = run_with(tmp_path, {"doc": "docs/benchmarks/index.md", "img_dir": "docs/benchmarks/img/a"})
    assert report.main([run]) == 2
    assert "report refused" in capsys.readouterr().err
    assert not os.path.exists(os.path.join(report.REPO, "docs", "benchmarks", "img", "a"))
