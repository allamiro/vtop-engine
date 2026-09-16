"""A matrix row says what a run DID, never what its scenario asked for.

`matrix_row` flattens one `summary.json` into one comparison row, and it fills
the gaps from the scenario the run was launched with — that is how columns the
runner does not compute (format, volume, compression) reach the table at all.
The hazard is the columns that appear in BOTH: the scenario states a request,
the summary states the outcome, and a blanket copy of the scenario silently
files the request as the result.

It has already happened three times, in three different shapes:

  - #477: every scenario carries the loader's default `shaping_driver:
    toxiproxy`, so unshaped rows claimed a driver they never used and the
    mixed-driver refusal fired on the one comparison it exists to permit.
  - #479/#480: `VTOP_S3_TRANSPORT` outranks the scenario's `transport`, so an
    overridden run would be filed under the wire it asked for.
  - #484: the summary records `h3-proxy` or blank, the scenario records `True`
    or `False`, and the scenario was winning — so the column added to tell the
    proxied baseline from the direct one carried the declaration instead of the
    topology, spelled in a way none of the other rows use.

Each is the same bug, so the rule is one list (`RESOLVED_COLUMNS`) rather than
one special case per issue, and this module holds all three shapes to it.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.shaping import SHAPING_COLUMNS  # noqa: E402
from run_matrix import COMPARE_COLS, RESOLVED_COLUMNS, matrix_row  # noqa: E402


def summary(scenario=None, **resolved):
    """A summary shaped like the runner's: resolved values at the top level,
    the scenario it was launched with nested underneath."""
    values = {"run_id": "r-1", "scenario_name": "s", "duration_seconds": 1.0}
    values.update(resolved)
    values["scenario"] = dict(scenario or {})
    return values


def test_the_proxy_hop_column_records_the_topology_not_the_declaration():
    row = matrix_row(summary(
        {"name": "proxy-hop-tcp-baseline", "h3_proxy": True, "backend": "s3_native"},
        h3_proxy="h3-proxy"))
    assert row["h3_proxy"] == "h3-proxy", (
        "the matrix is where scenario 17 sits beside scenario 12, and the column that "
        "distinguishes them must carry the service the bytes crossed. It read "
        f"{row['h3_proxy']!r} — the scenario's own declaration, which is a boolean and "
        "cannot be compared against the 'h3-proxy'/'' the runner writes for every other row"
    )


def test_a_direct_run_states_the_absence_of_the_hop_the_same_way():
    row = matrix_row(summary(
        {"name": "backpressure-soak-minio", "h3_proxy": False, "backend": "s3_native"},
        h3_proxy=""))
    assert row["h3_proxy"] == "", (
        "the direct half of the pair must render as the runner wrote it — blank, not "
        f"'False'. It read {row['h3_proxy']!r}, and a column whose two rows are spelled in "
        "different vocabularies is a column nobody can sort or diff"
    )


def test_an_overridden_transport_is_not_relabelled_as_the_wire_it_asked_for():
    # VTOP_S3_TRANSPORT outranks the scenario value in the engine's own
    # precedence, and the runner records the resolved one for that reason. A
    # row that took the scenario's back is a comparison of wires mislabelled
    # by exactly the override that made it interesting.
    row = matrix_row(summary(
        {"name": "proxy-hop-tcp-baseline", "transport": "tcp_tls"},
        transport="h3", transport_tuning_flat="max_concurrency=8"))
    assert row["transport"] == "h3", (
        f"the row says the bytes went over {row['transport']!r}, but the run resolved 'h3'. "
        "The scenario's value is the request; the override is what happened"
    )
    assert row["transport_tuning_flat"] == "max_concurrency=8", (
        "and the tuning beside it belongs to the transport that actually ran"
    )


def test_an_unshaped_run_does_not_inherit_the_loaders_default_driver():
    # The original of this family (#477), kept here because the mechanism is
    # now shared: if a future column pushes shaping out of the resolved list,
    # the refusal that protects netem comparisons goes with it.
    row = matrix_row(summary(
        {"name": "baseline", "shaping_driver": "toxiproxy"},
        **{key: "" for key in ("shaping_driver", "shaping_proxy")}))
    assert row["shaping_driver"] == "", (
        f"an unshaped run must state blank, not the loader's default; it read "
        f"{row['shaping_driver']!r}, which makes refuse_mixed_drivers reject a netem run "
        "beside its own unshaped baseline — the one comparison it exists to allow"
    )


def test_the_scenario_still_supplies_what_the_run_did_not_resolve():
    # The other half of the contract: most columns exist ONLY in the scenario,
    # and a fix that stopped copying it would empty the table.
    row = matrix_row(summary(
        {"format": "jsonl", "compression": "gzip", "volume": 400},
        h3_proxy=""))
    assert row["format"] == "jsonl" and row["compression"] == "gzip" and row["volume"] == 400, (
        f"the scenario's own knobs must still reach the row; it has {row!r}"
    )


def test_every_resolved_column_is_one_the_matrix_actually_prints():
    # A column preserved but not printed is a rule with no effect, and the
    # next reader cannot tell which of the two is the mistake.
    missing = [column for column in RESOLVED_COLUMNS if column not in COMPARE_COLS]
    assert not missing, (
        f"{missing} are protected from the scenario's values but never written to "
        "matrix.csv; either they belong in COMPARE_COLS or the protection is dead code"
    )


def test_the_runner_writes_a_top_level_value_for_every_protected_column():
    # The rule only bites when the summary carries the column. This reads the
    # runner's own summary literal rather than running a benchmark: if a
    # column is added to the matrix's protected list and never written by the
    # runner, the scenario's request wins again and this module would still
    # pass on hand-built summaries.
    #
    # The shaping columns are excluded because the runner writes them as a
    # dict splat — `**shape.flat_columns()`, or a blank one per
    # lib.shaping.SHAPING_COLUMNS — which is the same guarantee expressed in
    # code rather than in a literal, and both sides already splice that one
    # tuple.
    source = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "run_benchmark.py")
    with open(source, encoding="utf-8") as fh:
        runner = fh.read()
    for column in (column for column in RESOLVED_COLUMNS
                   if column not in SHAPING_COLUMNS):
        assert f'"{column}":' in runner, (
            f"run_benchmark.py never writes {column!r} into its summary, so matrix_row has "
            "nothing to protect and the scenario's requested value is what the matrix "
            "reports — the exact failure RESOLVED_COLUMNS exists to prevent"
        )
