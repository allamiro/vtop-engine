"""Tests for benchmarks/lib/scenario.py that run with or without PyYAML.

The harness is deliberately dependency-light: load_scenario() falls back to a
minimal flat parser when PyYAML is absent. These tests therefore avoid depending
on PyYAML so they still execute in that configuration - see
test_scenario_yaml.py for the PyYAML-specific behaviour.
"""

import builtins
import textwrap

import pytest

from lib.scenario import _coerce, _fallback_parse, _strip_inline_comment, load_scenario, reseed_count


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


def test_fallback_parse_reads_a_block_sequence():
    # Without PyYAML a YAML-list value must still reach the config as a list, or
    # a command_env_allowlist would vanish and an env-authenticated command
    # backend would lose its credentials (#499).
    parsed = _fallback_parse(
        "command_env_allowlist:\n"
        "  - AWS_ACCESS_KEY_ID\n"
        "  - AWS_SECRET_ACCESS_KEY\n"
        "backend: awscli\n"
    )
    assert parsed["command_env_allowlist"] == [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    assert parsed["backend"] == "awscli", (
        "the key after a sequence must still parse"
    )


def test_fallback_parse_sequence_strips_quotes_and_skips_interior_blanks():
    parsed = _fallback_parse(
        'k:\n  - "A"\n\n  - \'B\'\nnext: 5\n')
    assert parsed["k"] == ["A", "B"]
    assert parsed["next"] == 5


def test_fallback_parse_empty_value_without_a_sequence_stays_empty():
    # A bare "key:" with no list underneath is still the empty string, not a
    # spurious list — the sequence branch must not fire on ordinary empties.
    parsed = _fallback_parse("endpoint_url:\nname: x\n")
    assert parsed["endpoint_url"] == ""
    assert parsed["name"] == "x"


def test_fallback_parse_sequence_skips_a_comment_between_items():
    # A YAML comment between items must not terminate the sequence and drop the
    # remaining entries (#499).
    parsed = _fallback_parse(
        "command_env_allowlist:\n"
        "  - AWS_ACCESS_KEY_ID\n"
        "  # the secret follows\n"
        "  - AWS_SECRET_ACCESS_KEY\n"
        "backend: awscli\n"
    )
    assert parsed["command_env_allowlist"] == [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    assert parsed["backend"] == "awscli"


def test_fallback_parse_sequence_key_may_carry_an_inline_comment():
    # "key: # note" is still an empty scalar introducing a sequence; the inline
    # comment must not leave the value non-empty and skip the sequence branch.
    parsed = _fallback_parse(
        "command_env_allowlist: # credentials\n"
        "  - AWS_ACCESS_KEY_ID\n"
        "  - AWS_SECRET_ACCESS_KEY\n"
        "next: 3\n"
    )
    assert parsed["command_env_allowlist"] == [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    assert parsed["next"] == 3


def test_fallback_parse_keeps_a_hash_inside_a_token():
    # A "#" that is not a comment (an issue reference, a URL fragment) stays in
    # the value: comment-stripping keys on a preceding space, not any "#".
    assert _strip_inline_comment("issue#98") == "issue#98"
    assert _strip_inline_comment("value # note") == "value "


def test_fallback_parse_keeps_a_hash_inside_a_quoted_value():
    # A "#" inside a quoted region is content, not a comment: quote state is
    # tracked before a comment can start, so a quoted sequence item survives.
    assert _strip_inline_comment('"foo # bar"') == '"foo # bar"'
    assert _strip_inline_comment("'foo # bar'") == "'foo # bar'"
    parsed = _fallback_parse('k:\n  - "foo # bar"\n  - B\n')
    assert parsed["k"] == ["foo # bar", "B"]


def test_a_quote_mid_scalar_is_literal_not_a_delimiter():
    # An apostrophe inside a PLAIN scalar (John's) must not open a quoted
    # region and swallow a following comment: a quote opens a scalar only as
    # its first non-whitespace character.
    assert _strip_inline_comment("John's # note") == "John's "
    assert _strip_inline_comment('  "quoted"') == '  "quoted"'
    parsed = _fallback_parse("k:\n  - John's\n  - B\n")
    assert parsed["k"] == ["John's", "B"]


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


def test_coerce_reads_a_flow_sequence():
    # Inline "[a, b]" must become a list, matching PyYAML, so a flow-style
    # command_env_allowlist does not arrive as a bracketed string that splits
    # into unusable "[AWS_..." / "...]" names (#499).
    assert _coerce("[AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]") == [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    assert _coerce("[]") == []
    # a comma inside a quoted item does not split it
    assert _coerce('["a,b", c]') == ["a,b", "c"]
    # an apostrophe inside a PLAIN item is literal, not a quote delimiter, so it
    # does not swallow the comma and the items after it
    assert _coerce("[John's, B]") == ["John's", "B"]
    assert _coerce("[a, don't, c]") == ["a", "don't", "c"]
    # a doubled '' inside a single-quoted item is an escaped literal quote and
    # stays in the region, so an interior comma does not split it, and _coerce
    # unescapes it to a single quote (matching YAML)
    assert _coerce("['a''b,c', B]") == ["a'b,c", "B"]
    assert _coerce("'it''s'") == "it's"


def test_a_hash_inside_a_doubled_quote_region_is_content():
    # A doubled '' escape must keep a following '#' inside the quoted region
    # rather than closing it and treating the '#' as a comment.
    assert _strip_inline_comment("'a''b # c'") == "'a''b # c'"


def test_fallback_parse_reads_a_flow_style_allowlist():
    parsed = _fallback_parse(
        "command_env_allowlist: [AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]\n"
        "backend: awscli\n"
    )
    assert parsed["command_env_allowlist"] == [
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    assert parsed["backend"] == "awscli"


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


def test_every_bundled_scenario_survives_the_fallback_parser():
    # The PyYAML-side parity test cannot run in the no-PyYAML lane, so this
    # is that lane's own tripwire: no bundled scenario may parse into a
    # block-scalar indicator literal — the exact corruption the folded
    # descriptions used to produce.
    import glob
    import os
    scen_dir = os.path.join(os.path.dirname(__file__), "..", "scenarios")
    files = sorted(glob.glob(os.path.join(scen_dir, "*.yaml")))
    assert files, "the bundled scenarios must be visible to this test"
    for path in files:
        with open(path, encoding="utf-8") as fh:
            parsed = _fallback_parse(fh.read())
        for key, value in parsed.items():
            assert value not in (">", ">-", ">+", "|", "|-", "|+"), (
                f"{os.path.basename(path)}: {key} parsed as a bare block "
                "indicator — the fallback parser dropped its content"
            )


def test_fallback_parse_reads_a_nested_transport_tuning_map():
    # The fallback parser must handle nested maps so a transports block survives
    # when PyYAML is absent (#480), instead of being dropped to "".
    parsed = _fallback_parse(
        "transport: tcp_tls\n"
        "transports:\n"
        "  tcp_tls:\n"
        "    max_concurrency: 4\n"
        "    part_size_bytes: 8388608\n"
        "after: 1\n"
    )
    assert parsed["transports"] == {
        "tcp_tls": {"max_concurrency": 4, "part_size_bytes": 8388608}
    }
    # Keys after the nested block still parse.
    assert parsed["after"] == 1
    assert parsed["transport"] == "tcp_tls"
