"""Scenario tests that require a real PyYAML install.

Kept separate so the rest of the scenario suite still runs when PyYAML is absent
- which is the configuration the fallback parser exists for, and therefore the
one most worth testing.
"""

import glob
import os
import textwrap

import pytest

from lib.scenario import _fallback_parse, load_scenario

yaml = pytest.importorskip("yaml", reason="tests PyYAML-specific behaviour")


def write(tmp_path, text, name="s.yaml"):
    p = tmp_path / name
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return str(p)


def test_invalid_yaml_raises_rather_than_silently_falling_back(tmp_path):
    # Unclosed bracket: unambiguously invalid YAML. This used to be swallowed by
    # a blanket except, so the run silently continued with default settings and
    # reported confident, meaningless numbers.
    path = write(tmp_path, "name: bad\nformats: [jsonl, cef\n")
    with pytest.raises(yaml.YAMLError):
        load_scenario(path)


def test_valid_yaml_parses_real_structures(tmp_path):
    path = write(
        tmp_path,
        """
        name: my-run
        formats: [jsonl, cef]
        batch_max_records: 500
        """,
    )
    s = load_scenario(path)
    assert s.values["name"] == "my-run"
    # A real YAML list - the flat fallback parser cannot produce this.
    assert s.values["formats"] == ["jsonl", "cef"]
    assert s.values["batch_max_records"] == 500


def test_every_bundled_scenario_parses_the_same_under_both_parsers():
    # The no-PyYAML mode is first-class — CI uninstalls PyYAML and reruns the
    # suite — so which parser happens to load a scenario must never change
    # what the run measures. This is the test that would have caught the
    # fallback parser reading a `>-` description as the literal ">-": the
    # subset the fallback covers is defined by the scenarios this repository
    # actually ships, so every one of them is compared.
    scenarios_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scenarios")
    paths = sorted(glob.glob(os.path.join(scenarios_dir, "*.yaml")))
    assert paths, "the bundled scenarios must be found, or this pins nothing"
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        assert _fallback_parse(text) == yaml.safe_load(text), (
            f"{os.path.basename(path)} reads differently without PyYAML — "
            "either extend the fallback parser or simplify the scenario"
        )
