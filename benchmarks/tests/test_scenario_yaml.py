"""Scenario tests that require a real PyYAML install.

Kept separate so the rest of the scenario suite still runs when PyYAML is absent
- which is the configuration the fallback parser exists for, and therefore the
one most worth testing.
"""

import textwrap

import pytest

from lib.scenario import load_scenario

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
