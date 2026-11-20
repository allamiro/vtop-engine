"""readme_charts.py: the README's charts and the figures its prose quotes.

The charts are only as trustworthy as the two links checked here: the README
quotes what the data says, and the images it embeds are the ones the script
writes. Rendering itself needs matplotlib and is exercised where CI installs
the pinned stack (the benchmark-charts job); the rest runs everywhere.
"""
import os
import re
import subprocess

import pytest

import readme_charts

README = os.path.join(readme_charts.REPO, "README.md")


def readme_text():
    with open(README, encoding="utf-8") as handle:
        return handle.read()


def test_the_readme_quotes_every_figure_the_data_gives():
    # A re-run that changes a number changes facts(); the README must follow.
    text = readme_text()
    missing = {key: value for key, value in readme_charts.facts().items() if value not in text}
    assert not missing, f"README.md no longer quotes these figures from the committed data: {missing}"


def test_the_shipped_default_figures_are_the_research_documents():
    # The same two numbers docs/EGRESS_TRANSPORT_RESEARCH.md's linted table carries.
    facts = readme_charts.facts()
    assert (facts["default_clean"], facts["default_lossy"]) == ("0.960", "0.967")


def test_the_policed_runs_split_before_and_after_by_tag():
    runs = readme_charts.policed_runs()
    assert runs[("policed", "before")]["committed"] == 0
    after = runs[("policed", "after")]
    assert after["committed"] == after["seeded"] > 0
    for link in ("clean", "loss1"):
        for when in ("before", "after"):
            cell = runs[(link, when)]
            assert cell["committed"] == cell["seeded"], (link, when, cell)


def test_a_policed_run_with_an_unknown_tag_is_refused(tmp_path):
    rel = readme_charts.POLICED
    path = tmp_path / rel
    path.parent.mkdir(parents=True)
    path.write_text("tag,run_id,committed,seeded\nexperiment-r1,policed-uplink-x,16,16\n")
    with pytest.raises(readme_charts.Refused, match="experiment-r1"):
        readme_charts.policed_runs(str(tmp_path))


def test_every_chart_is_embedded_in_both_themes():
    text = readme_text()
    for name, _draw, _load in readme_charts.CHARTS:
        dark = f'srcset="docs/assets/readme/{name}-dark.svg"'
        light = f'src="docs/assets/readme/{name}-light.svg"'
        assert dark in text and light in text, f"README.md does not embed {name} as a light/dark <picture>"


def test_the_readme_embeds_no_chart_the_script_does_not_write():
    names = {name for name, _draw, _load in readme_charts.CHARTS}
    embedded = set(re.findall(r"docs/assets/readme/([a-z0-9-]+)-(?:light|dark)\.svg", readme_text()))
    assert embedded <= names, f"README.md embeds charts no generator writes: {sorted(embedded - names)}"


def test_orphans_names_committed_output_no_chart_wrote(tmp_path):
    def git(*args):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@localhost", *args], cwd=tmp_path,
                       check=True, capture_output=True)

    git("init", "-q")
    out = tmp_path / readme_charts.OUT
    out.mkdir(parents=True)
    (out / "kept-light.svg").write_text("<svg/>")
    (out / "renamed-light.svg").write_text("<svg/>")
    git("add", "-A")
    git("commit", "-qm", "c")
    stale = readme_charts.orphans([str(out / "kept-light.svg")], str(tmp_path))
    assert stale == [f"{readme_charts.OUT}/renamed-light.svg"]


def test_render_writes_a_distinct_light_and_dark_svg_per_chart(tmp_path):
    pytest.importorskip("matplotlib")
    (tmp_path / "benchmarks").symlink_to(os.path.join(readme_charts.REPO, "benchmarks"))
    written = readme_charts.render(str(tmp_path))
    assert len(written) == 2 * len(readme_charts.CHARTS)
    for name, _draw, _load in readme_charts.CHARTS:
        light = (tmp_path / readme_charts.OUT / f"{name}-light.svg").read_bytes()
        dark = (tmp_path / readme_charts.OUT / f"{name}-dark.svg").read_bytes()
        assert light != dark, name
        assert b"<dc:date>" not in light, "the SVG carries a date, so every run would drift"
