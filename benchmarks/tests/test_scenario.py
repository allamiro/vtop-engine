"""Tests for benchmarks/lib/scenario.py that run with or without PyYAML.

The harness is deliberately dependency-light: load_scenario() falls back to a
minimal flat parser when PyYAML is absent. These tests therefore avoid depending
on PyYAML so they still execute in that configuration - see
test_scenario_yaml.py for the PyYAML-specific behaviour.
"""

import builtins
import textwrap

import pytest

from lib.scenario import _coerce, _fallback_parse, load_scenario, reseed_count


def write(tmp_path, text, name="s.yaml"):
    p = tmp_path / name
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return str(p)


# --------------------------------------------------------------------------
# Naming / defaults - identical under either parser
# --------------------------------------------------------------------------


def test_name_defaults_to_the_filename(tmp_path):
    # Regression: DEFAULTS presets name="scenario", so the filename-derivation
    # branch was dead and every unnamed scenario was called "scenario",
    # colliding in run ids and making matrix results indistinguishable.
    path = write(tmp_path, "batch_max_records: 10\n", name="derived-name.yaml")
    s = load_scenario(path)
    assert s.values["name"] == "derived-name"


def test_explicit_name_wins_over_the_filename(tmp_path):
    path = write(tmp_path, "name: explicit\n", name="ignored.yaml")
    s = load_scenario(path)
    assert s.values["name"] == "explicit"


def test_defaults_are_applied_for_absent_keys(tmp_path):
    path = write(tmp_path, "name: sparse\n")
    s = load_scenario(path)
    assert s.values.get("batch_max_records") is not None


def test_null_values_do_not_clobber_defaults(tmp_path):
    """An explicit `key:` (null) must not wipe out the default."""
    path = write(tmp_path, "name: nulls\nbatch_max_records:\n")
    s = load_scenario(path)
    assert s.values["batch_max_records"] is not None


def test_fallback_is_used_when_pyyaml_is_missing(tmp_path, monkeypatch):
    """With PyYAML absent the flat parser takes over and must still work."""
    real_import = builtins.__import__

    def no_yaml(name, *args, **kwargs):
        if name == "yaml":
            raise ImportError("simulated: PyYAML not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_yaml)
    path = write(tmp_path, "name: flat-run\nbatch_max_records: 42\n")
    s = load_scenario(path)
    assert s.values["name"] == "flat-run"
    assert s.values["batch_max_records"] == 42


# --------------------------------------------------------------------------
# Fallback parser + coercion
# --------------------------------------------------------------------------


def test_fallback_parse_reads_flat_keys():
    parsed = _fallback_parse("name: x\ncount: 3\n")
    assert parsed["name"] == "x"
    assert parsed["count"] == 3


def test_fallback_parse_ignores_comments_and_blanks():
    parsed = _fallback_parse("# a comment\n\nname: y\n")
    assert parsed == {"name": "y"}


def test_fallback_parse_folds_a_block_scalar_and_keeps_the_keys_after_it():
    parsed = _fallback_parse(
        "name: soak\n"
        "description: >-\n"
        "  the seeder outruns the engine (#98): a real\n"
        "  backlog accumulates\n"
        "volume: 400\n"
    )
    assert parsed["description"] == (
        "the seeder outruns the engine (#98): a real backlog accumulates"
    ), (
        "the '#98' is a citation inside prose, not a comment, and folded "
        "lines join with spaces — the old parser stored the literal '>-' and "
        "dropped the prose of every bundled soak scenario"
    )
    assert parsed["volume"] == 400, (
        "flat parsing must resume at the first line back at the key's indent, "
        "or every key after a description is silently lost"
    )


def test_fallback_parse_keeps_literal_block_lines_on_their_own_lines():
    parsed = _fallback_parse("notes: |-\n  line one\n  line two\nafter: 1\n")
    assert parsed["notes"] == "line one\nline two", (
        "literal blocks promise line structure; folding them would change "
        "what the scenario said"
    )
    assert parsed["after"] == 1


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("3", 3),
        ("3.5", 3.5),
        ("true", True),
        ("false", False),
        ("hello", "hello"),
    ],
)
def test_coerce_types(raw, expected):
    assert _coerce(raw) == expected


# --------------------------------------------------------------------------
# Sustained-load backlog (#98)
# --------------------------------------------------------------------------


def test_backlog_multiplier_defaults_to_a_quarter_of_the_volume(tmp_path):
    # The historical behaviour, kept as the default so existing scenarios run
    # exactly as before: a quarter of the volume per cycle, which the engine
    # drains within the cycle.
    path = write(tmp_path, "name: s\nvolume: 400\n")
    sc = load_scenario(path)
    assert sc.values["backlog_multiplier"] == 0.25
    assert reseed_count(sc.values["volume"], sc.values["backlog_multiplier"]) == 100


def test_a_multiplier_above_one_seeds_more_than_a_cycle_drains(tmp_path):
    # The condition every sustained-backpressure hypothesis in #98 needs. At or
    # below the drain rate the engine is caught up by construction and the
    # deficit never appears, however long or large the run is — which is why
    # the 1M rec/s target was never the thing that mattered.
    path = write(tmp_path, "name: s\nvolume: 400\nbacklog_multiplier: 1.5\n")
    sc = load_scenario(path)
    assert reseed_count(sc.values["volume"], sc.values["backlog_multiplier"]) == 600


def test_a_multiplier_that_rounds_to_zero_still_seeds_one_file():
    # Otherwise a sustained-load scenario silently becomes a single-cycle
    # drain, and reports a duration it never actually spent working.
    assert reseed_count(10, 0.01) == 1
    assert reseed_count(1, 0.0) == 1
