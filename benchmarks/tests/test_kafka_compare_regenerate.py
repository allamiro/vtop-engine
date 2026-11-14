"""regenerate.py, the driver behind CI's drift check on docs/benchmarks/ (#600).

The check is only worth something if it can fail: each refusal below is one
way the committed charts could stop being what the committed CSVs produce
while a drift check over them still passed.
"""
import glob
import io
import os
import shlex
import subprocess
import sys

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
                  "pyparsing", "python-dateutil", "six", "tzdata"}


def test_pins_are_read_from_a_hash_locked_file(tmp_path):
    # The lock's own layout: a continuation backslash, --hash lines, `# via` comments.
    pins = tmp_path / "pins.txt"
    pins.write_text("# header\n\nmatplotlib==3.10.3 \\\n    --hash=sha256:" + "a" * 64 + " \\\n"
                    "    --hash=sha256:" + "b" * 64 + "\n    # via -r requirements-report.in\n"
                    "Six==1.17.0 \\\n    --hash=sha256:" + "c" * 64 + "\n")
    assert regenerate.read_pins(str(pins)) == {"matplotlib": "3.10.3", "six": "1.17.0"}


def test_a_pin_with_a_marker_or_an_inline_hash_reads_as_its_version(tmp_path):
    pins = tmp_path / "pins.txt"
    pins.write_text('numpy==2.4.6 ; python_version >= "3.11"\npillow==13.0.0 --hash=sha256:' + "d" * 64 + "\n")
    assert regenerate.read_pins(str(pins)) == {"numpy": "2.4.6", "pillow": "13.0.0"}


def test_the_refusal_names_the_hash_checked_install():
    with pytest.raises(regenerate.Refused, match="--require-hashes -r"):
        regenerate.check_stack({"matplotlib": "3.10.3"}, {}.get)


def test_a_hash_line_without_a_pin_is_refused(tmp_path):
    pins = tmp_path / "pins.txt"
    pins.write_text("    --hash=sha256:" + "a" * 64 + "\n")
    with pytest.raises(regenerate.Refused, match="no pins"):
        regenerate.read_pins(str(pins))


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


# ---------------------------------------------------------------- determinism


COMMITTED_SVGS = sorted(
    glob.glob(os.path.join(regenerate.REPO, "docs", "benchmarks", "img", "*.svg"))
    + glob.glob(os.path.join(regenerate.REPO, "docs", "assets", "readme", "*.svg")))


def test_there_are_committed_svgs_to_check():
    assert len(COMMITTED_SVGS) >= 8


@pytest.mark.parametrize("path", COMMITTED_SVGS, ids=os.path.basename)
def test_no_committed_svg_carries_a_date_or_a_local_path(path):
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    assert "<dc:date>" not in text, f"{path} carries a date, so every regeneration would drift"
    assert "<dc:creator>" not in text, f"{path} names the software that drew it, which moves with a bump"
    for local in ("/Users/", "/home/", "/private/", "/tmp/", regenerate.REPO):
        assert local not in text, f"{path} carries the local path {local!r}"


@pytest.mark.parametrize("path", COMMITTED_SVGS, ids=os.path.basename)
def test_committed_svgs_draw_text_as_paths(path):
    # A <text> element is laid out by the viewer's font, not the pinned one, and an SVG with
    # glyphs as text would move with whichever DejaVu Sans the drawing host had.
    with open(path, encoding="utf-8") as handle:
        assert "<text" not in handle.read(), f"{path} has a <text> element"


# Variations a developer's shell or a CI runner could bring, none of which may reach the output.
# The first is a plain C locale with Python's UTF-8 mode and locale coercion
# off, so the default text encoding is ASCII wherever the C locale is ASCII
# (Linux): a generator that writes non-ASCII text without naming an encoding
# fails there, and only there.
ENVIRONMENTS = (
    {"PYTHONHASHSEED": "1", "TZ": "UTC", "LC_ALL": "C", "LANG": "C", "PYTHONUTF8": "0",
     "PYTHONCOERCECLOCALE": "0"},
    {"PYTHONHASHSEED": "4242", "TZ": "Pacific/Kiritimati", "LC_ALL": "C.UTF-8", "LANG": "C.UTF-8",
     "PYTHONUTF8": "1", "SOURCE_DATE_EPOCH": "1"},
)
VARIED = ("PYTHONHASHSEED", "TZ", "LC_ALL", "LANG", "PYTHONUTF8", "PYTHONCOERCECLOCALE", "SOURCE_DATE_EPOCH")


def _env(extra):
    env = {key: value for key, value in os.environ.items() if key not in VARIED}
    env.update(extra)
    return env


def _same_output(first, second):
    """The names each run wrote, then the names whose bytes differ: a file only one run writes
    is a difference too, not something to skip."""
    assert first, "nothing was written"
    assert set(first) == set(second), f"the two runs wrote different files: {sorted(set(first) ^ set(second))}"
    return sorted(name for name in first if first[name] != second[name])


def _read_all(directory):
    out = {}
    for root, _dirs, files in os.walk(directory):
        for name in files:
            if name.endswith((".svg", ".md")):
                path = os.path.join(root, name)
                with open(path, "rb") as handle:
                    out[os.path.relpath(path, directory)] = handle.read()
    return out


def _render_report_twice(tmp_path, run):
    outputs = []
    for index, extra in enumerate(ENVIRONMENTS):
        # One output directory per run, at the same depth (the page's relative links depend on
        # it): a shared one would let the second run pass on files only the first wrote.
        out = tmp_path / f"out{index}"
        cwd = tmp_path / f"cwd{index}"
        cwd.mkdir()
        run_dir = os.path.join(regenerate.REPO, regenerate.RESULTS, run)
        proc = subprocess.run([sys.executable, regenerate.REPORT, run_dir, "--img-dir", str(out / "img"),
                               "--doc", str(out / "page.md")],
                              cwd=cwd, env=_env(extra), capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        outputs.append(_read_all(out))
    return outputs


def _committed_run_ids():
    return sorted(os.path.basename(path) for path in regenerate.committed_runs())


@pytest.mark.parametrize("run", _committed_run_ids())
def test_report_output_does_not_depend_on_the_environment(tmp_path, run):
    pytest.importorskip("matplotlib")
    changed = _same_output(*_render_report_twice(tmp_path, run))
    assert not changed, f"the same run drew different bytes under a different hash seed, zone, locale or cwd: {changed}"


def test_readme_chart_output_does_not_depend_on_the_environment(tmp_path):
    pytest.importorskip("matplotlib")
    outputs = []
    for index, extra in enumerate(ENVIRONMENTS):
        repo = tmp_path / f"repo{index}"
        repo.mkdir()
        (repo / "benchmarks").symlink_to(os.path.join(regenerate.REPO, "benchmarks"))
        cwd = tmp_path / f"cwd{index}"
        cwd.mkdir()
        code = "import sys; sys.path.insert(0, sys.argv[1]); import readme_charts; readme_charts.render(sys.argv[2])"
        proc = subprocess.run([sys.executable, "-c", code, os.path.join(regenerate.REPO, "benchmarks"), str(repo)],
                              cwd=cwd, env=_env(extra), capture_output=True, text=True)
        assert proc.returncode == 0, proc.stderr
        outputs.append(_read_all(repo / "docs"))
    changed = _same_output(*outputs)
    assert not changed, f"the README charts differ under a different hash seed, zone, locale or cwd: {changed}"
