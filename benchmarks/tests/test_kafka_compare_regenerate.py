"""regenerate.py, the driver behind CI's drift check on docs/benchmarks/ (#600).

The check is only worth something if it can fail: each refusal below is one
way the committed charts could stop being what the committed CSVs produce
while a drift check over them still passed.
"""
import io
import os
import shlex
import subprocess

import pytest

from kafka_compare import regenerate


def git(repo, *args):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@localhost", *args], cwd=repo, check=True,
                   capture_output=True)


@pytest.fixture
def repo(tmp_path):
    git(tmp_path, "init", "-q")
    return tmp_path


def put(repo, name, data=b"x"):
    path = repo / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return str(path)


def commit(repo):
    git(repo, "add", "-A")
    git(repo, "commit", "-qm", "c")


# ------------------------------------------------------------------ the stack


def test_pins_must_be_exact(tmp_path):
    pins = tmp_path / "pins.txt"
    pins.write_text("# comment\nmatplotlib==3.10.3\nnumpy>=2\n")
    with pytest.raises(regenerate.Refused, match="numpy>=2.*not an exact"):
        regenerate.read_pins(str(pins))


def test_an_empty_pin_file_is_refused(tmp_path):
    pins = tmp_path / "pins.txt"
    pins.write_text("# nothing\n\n")
    with pytest.raises(regenerate.Refused, match="no pins"):
        regenerate.read_pins(str(pins))


PLOTTING_STACK = {"matplotlib", "contourpy", "cycler", "fonttools", "kiwisolver", "numpy", "packaging", "pillow",
                  "pyparsing", "python-dateutil", "tzdata"}


def test_the_committed_pins_are_exactly_the_plotting_stack():
    assert set(regenerate.read_pins()) == PLOTTING_STACK


def _requirement_name(spec):
    name = spec.split(";")[0]
    for stop in " <>=!~[(":
        name = name.split(stop)[0]
    return name.strip().lower().replace("_", "-")


def test_every_package_matplotlib_runs_on_is_pinned():
    """The pins cover matplotlib's whole installed dependency closure, so no unpinned package can change
    the output under them."""
    pytest.importorskip("matplotlib")
    from importlib import metadata

    seen, todo = set(), ["matplotlib"]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        seen.add(name)
        for spec in metadata.requires(name) or []:
            if "extra ==" in spec:
                continue
            dep = _requirement_name(spec)
            try:
                metadata.version(dep)
            except metadata.PackageNotFoundError:
                continue  # an environment marker excluded it here
            todo.append(dep)
    assert seen - set(regenerate.read_pins()) == set()


def test_every_pin_is_checked_against_what_is_installed():
    pins = regenerate.read_pins()
    asked = []

    def installed(name):
        asked.append(name)
        return pins[name]

    regenerate.check_stack(pins, installed)
    assert sorted(asked) == sorted(pins)
    for name in pins:  # and any one of them off by a version is refused, by name
        def off(other, name=name):
            return "0.0.0" if other == name else pins[other]

        with pytest.raises(regenerate.Refused, match=f"{name} 0.0.0"):
            regenerate.check_stack(pins, off)


def test_refuses_a_stack_that_is_not_the_pinned_one():
    installed = {"matplotlib": "3.10.4", "numpy": "2.4.6"}.get
    with pytest.raises(regenerate.Refused) as caught:
        regenerate.check_stack({"matplotlib": "3.10.3", "numpy": "2.4.6", "pillow": "13.0.0"}, installed)
    message = str(caught.value)
    assert "matplotlib 3.10.4 (pinned 3.10.3)" in message
    assert "pillow not installed (pinned 13.0.0)" in message
    assert "numpy" not in message


def test_accepts_exactly_the_pinned_stack():
    regenerate.check_stack({"matplotlib": "3.10.3"}, {"matplotlib": "3.10.3"}.get)


# ------------------------------------------------------------------- the runs


def test_only_committed_runs_are_regenerated(repo):
    put(repo, "benchmarks/kafka_compare/results/b-run/results.csv")
    put(repo, "benchmarks/kafka_compare/results/a-run/logs/x.log")
    commit(repo)
    put(repo, "benchmarks/kafka_compare/results/local-run/results.csv")  # a developer's uncommitted run
    assert regenerate.committed_runs(str(repo)) == [
        os.path.join(str(repo), "benchmarks/kafka_compare/results", run) for run in ("a-run", "b-run")]


def test_a_staged_but_uncommitted_run_is_not_a_committed_run(repo):
    put(repo, "benchmarks/kafka_compare/results/a-run/results.csv")
    commit(repo)
    put(repo, "benchmarks/kafka_compare/results/staged-run/results.csv")
    git(repo, "add", "-A")
    assert regenerate.committed_runs(str(repo)) == [
        os.path.join(str(repo), "benchmarks/kafka_compare/results/a-run")]


RUN = "benchmarks/kafka_compare/results/a-run"


def _committed_run(repo):
    put(repo, f"{RUN}/results.csv", b"committed\n")
    put(repo, f"{RUN}/environment.json", b"{}")
    commit(repo)
    return os.path.join(str(repo), RUN)


def test_a_committed_run_as_head_has_it_is_accepted(repo):
    regenerate.check_runs_unchanged([_committed_run(repo)], str(repo))


def test_refuses_an_unstaged_edit_to_a_committed_run(repo):
    run = _committed_run(repo)
    put(repo, f"{RUN}/results.csv", b"edited\n")
    with pytest.raises(regenerate.Refused, match=f"{RUN}/results.csv"):
        regenerate.check_runs_unchanged([run], str(repo))


def test_refuses_a_staged_edit_to_a_committed_run(repo):
    run = _committed_run(repo)
    put(repo, f"{RUN}/results.csv", b"edited\n")
    git(repo, "add", "-A")
    with pytest.raises(regenerate.Refused, match=f"{RUN}/results.csv"):
        regenerate.check_runs_unchanged([run], str(repo))


def test_refuses_a_deleted_or_added_file_in_a_committed_run(repo):
    run = _committed_run(repo)
    os.remove(os.path.join(run, "environment.json"))
    put(repo, f"{RUN}/failover.csv")
    with pytest.raises(regenerate.Refused) as caught:
        regenerate.check_runs_unchanged([run], str(repo))
    message = str(caught.value)
    assert f"{RUN}/environment.json" in message
    assert f"{RUN}/failover.csv" in message
    # The hints name the run directory, not a placeholder ...
    restore = f"git restore --staged --worktree -- {RUN}"
    clean = f"git clean -f -- {RUN}"
    assert f"`{restore}`" in message
    assert f"`git clean -n -- {RUN}`" in message
    assert f"`{clean}`" in message
    # ... and, run as printed, they put the run back the way HEAD has it.
    for command in (restore, clean):
        subprocess.run(shlex.split(command), cwd=repo, check=True, capture_output=True)
    regenerate.check_runs_unchanged([run], str(repo))


def test_refuses_when_no_run_is_committed(repo):
    put(repo, "README.md")
    commit(repo)
    with pytest.raises(regenerate.Refused, match="no committed run"):
        regenerate.committed_runs(str(repo))


def test_refuses_two_runs_writing_the_same_file():
    outputs = {"run-a": ["/d/one.png", "/d/page.md"], "run-b": ["/d/two.png", "/d/page.md"]}
    with pytest.raises(regenerate.Refused, match=r"/d/page.md \(by run-a and run-b\)"):
        regenerate.regenerate(["run-a", "run-b"], outputs.__getitem__)


def test_records_which_run_wrote_each_file():
    outputs = {"run-a": ["/d/a.png"], "run-b": ["/d/b.png"]}
    assert regenerate.regenerate(["run-a", "run-b"], outputs.__getitem__) == {"/d/a.png": "run-a",
                                                                              "/d/b.png": "run-b"}


def test_a_run_report_py_refuses_fails_the_regeneration_with_its_code(tmp_path):
    # No results.csv: report.py refuses (exit 2) before it imports matplotlib.
    (tmp_path / "environment.json").write_text("{}")
    with pytest.raises(regenerate.ReportFailed, match="exited 2: report refused") as caught:
        regenerate.run_report(str(tmp_path))
    assert caught.value.code == 2


# ----------------------------------------------------------------- stale output


def test_reports_committed_output_no_run_writes(repo):
    kept = put(repo, "docs/benchmarks/img/kafka-compare-produce-throughput.png")
    put(repo, "docs/benchmarks/img/kafka-compare-renamed-away.svg")
    page = put(repo, "docs/benchmarks/kafka-comparison.md", b"<!-- Generated by benchmarks/kafka_compare/report.py -->")
    put(repo, "docs/benchmarks/kafka-old.md", b"# old\n<!-- Generated by benchmarks/kafka_compare/report.py -->")
    put(repo, "docs/benchmarks/notes.md", b"# written by hand\n")
    commit(repo)
    assert regenerate.orphans([kept, page], str(repo)) == [
        "docs/benchmarks/img/kafka-compare-renamed-away.svg", "docs/benchmarks/kafka-old.md"]


# ------------------------------------------------------------------------- PNGs


def _png(pixels, level):
    image_mod = pytest.importorskip("PIL.Image")
    image = image_mod.new("RGBA", (64, 32), (252, 252, 251, 255))
    for xy, colour in pixels.items():
        image.putpixel(xy, colour)
    out = io.BytesIO()
    image.save(out, format="PNG", compress_level=level)
    return out.getvalue()


def test_a_png_with_the_same_pixels_keeps_its_committed_bytes(repo):
    committed = _png({(3, 4): (42, 120, 214, 255)}, level=9)
    path = put(repo, "docs/benchmarks/img/chart.png", committed)
    commit(repo)
    other_zlib = _png({(3, 4): (42, 120, 214, 255)}, level=1)
    assert other_zlib != committed  # the premise: same pixels, other bytes
    put(repo, "docs/benchmarks/img/chart.png", other_zlib)
    assert regenerate.keep_committed_pngs([path], str(repo)) == [path]
    assert open(path, "rb").read() == committed


def test_a_png_with_one_changed_pixel_is_left_for_git_diff(repo):
    committed = _png({(3, 4): (42, 120, 214, 255)}, level=9)
    path = put(repo, "docs/benchmarks/img/chart.png", committed)
    commit(repo)
    changed = _png({(3, 4): (235, 104, 52, 255)}, level=9)
    put(repo, "docs/benchmarks/img/chart.png", changed)
    assert regenerate.keep_committed_pngs([path], str(repo)) == []
    assert open(path, "rb").read() == changed


def test_a_new_png_is_left_for_git_status(repo):
    put(repo, "README.md")
    commit(repo)
    path = put(repo, "docs/benchmarks/img/new.png", _png({}, level=9))
    assert regenerate.keep_committed_pngs([path], str(repo)) == []
