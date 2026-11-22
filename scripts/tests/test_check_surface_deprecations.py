"""Unit tests for scripts/check-surface-deprecations.py (#719).

Each test builds a base and a head surface, and a deprecations table, and
asserts what the checker asks for. Run with:

    python3 -m unittest discover -s scripts/tests -p 'test_check_surface_deprecations.py'
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / "check-surface-deprecations.py"
REPO = HERE.parent.parent

_spec = importlib.util.spec_from_file_location("check_surface_deprecations", SCRIPT)
checker = importlib.util.module_from_spec(_spec)
# Registered before it runs: its dataclasses look their module up by name.
sys.modules[_spec.name] = checker
_spec.loader.exec_module(checker)

METRICS = "docs/surface/metrics-engine.txt"
CLI = "docs/surface/vtopctl.txt"

BASE_METRICS = """\
# header
vtop_batches_total counter format,source_type,state,tenant
vtop_inflight_batches gauge
vtop_stage_duration_seconds histogram stage
"""

BASE_CLI = """\
vtopctl
vtopctl --json flag global
vtopctl process-once
vtopctl process-once --source value=<SOURCE> values=kafka,file,syslog required
vtopctl process-once --config value=<CONFIG> required
vtopctl scrub
vtopctl scrub --limit value=<LIMIT>
vtopctl scrub --concurrency short=-c value=<CONCURRENCY> default=8
vtopctl scrub --check-retention flag
vtopctl meta
vtopctl meta status
vtopctl meta status --config value=<CONFIG> required
"""


def table(*rows: str) -> str:
    head = (
        "# Deprecations\n\n"
        "| Kind | Name | Deprecated in | Removed in | Replacement | Notes |\n"
        "|---|---|---|---|---|---|\n"
    )
    return head + "".join(row + "\n" for row in rows) + "\nAfter the table.\n"


def run(head_metrics=BASE_METRICS, head_cli=BASE_CLI, entries=()):
    base = {METRICS: BASE_METRICS, CLI: BASE_CLI}
    head = {METRICS: head_metrics, CLI: head_cli}
    ok, missing = checker.check(base, head, table(*entries))
    return ok, [(n.kind, n.name) for n in missing]


class Metrics(unittest.TestCase):
    def test_an_unchanged_surface_passes(self):
        self.assertEqual(run(), ([], []))

    def test_a_new_family_or_label_needs_no_entry(self):
        head = BASE_METRICS.replace("stage\n", "reason,stage\n") + "vtop_new_total counter\n"
        self.assertEqual(run(head_metrics=head), ([], []))

    def test_a_removed_family_needs_an_entry(self):
        head = BASE_METRICS.replace("vtop_inflight_batches gauge\n", "")
        _, missing = run(head_metrics=head)
        self.assertEqual(missing, [("metric", "vtop_inflight_batches")])

    def test_a_renamed_family_needs_an_entry_for_the_old_name(self):
        head = BASE_METRICS.replace("vtop_inflight_batches", "vtop_batches_in_flight")
        _, missing = run(head_metrics=head)
        self.assertEqual(missing, [("metric", "vtop_inflight_batches")])

    def test_the_entry_satisfies_it(self):
        head = BASE_METRICS.replace("vtop_inflight_batches gauge\n", "")
        ok, missing = run(
            head_metrics=head,
            entries=["| metric | `vtop_inflight_batches` | v0.11.0 | v0.12.0 | `vtop_batches_in_flight` | |"],
        )
        self.assertEqual(missing, [])
        self.assertEqual([(n.kind, n.name) for n in ok], [("metric", "vtop_inflight_batches")])

    def test_a_changed_type_needs_an_entry(self):
        head = BASE_METRICS.replace("vtop_inflight_batches gauge", "vtop_inflight_batches counter")
        _, missing = run(head_metrics=head)
        self.assertEqual(missing, [("metric", "vtop_inflight_batches")])

    def test_a_removed_or_renamed_label_needs_a_label_or_metric_entry(self):
        head = BASE_METRICS.replace("format,source_type,state,tenant", "format,source,state,tenant")
        _, missing = run(head_metrics=head)
        self.assertEqual(missing, [("label", "vtop_batches_total{source_type}")])
        for row in (
            "| label | `vtop_batches_total{source_type}` | v0.11.0 | v0.12.0 | `source` | |",
            "| metric | `vtop_batches_total` | v0.11.0 | v0.12.0 | | |",
        ):
            self.assertEqual(run(head_metrics=head, entries=[row])[1], [])

    def test_line_order_does_not_matter(self):
        head = "\n".join(reversed(BASE_METRICS.splitlines())) + "\n"
        cli = "\n".join(reversed(BASE_CLI.splitlines())) + "\n"
        self.assertEqual(run(head_metrics=head, head_cli=cli), ([], []))

    def test_an_entry_for_an_item_that_still_exists_is_allowed(self):
        ok, missing = run(
            entries=["| metric | `vtop_inflight_batches` | v0.11.0 | not scheduled | | notice only |"]
        )
        self.assertEqual((ok, missing), ([], []))


class Commands(unittest.TestCase):
    def test_a_new_command_flag_or_value_needs_no_entry(self):
        head = (
            BASE_CLI.replace("values=kafka,file,syslog", "values=kafka,file,syslog,spool")
            + "vtopctl scrub --sample value=<SAMPLE>\nvtopctl audit\n"
        )
        self.assertEqual(run(head_cli=head), ([], []))

    def test_a_changed_default_or_value_name_needs_no_entry(self):
        head = BASE_CLI.replace("value=<CONCURRENCY> default=8", "value=<N> default=16")
        self.assertEqual(run(head_cli=head), ([], []))

    def test_a_renamed_flag_needs_an_entry(self):
        head = BASE_CLI.replace("--check-retention", "--retention")
        _, missing = run(head_cli=head)
        self.assertEqual(missing, [("flag", "vtopctl scrub --check-retention")])
        row = "| flag | `vtopctl scrub --check-retention` | v0.11.0 | v0.12.0 | `--retention` | |"
        self.assertEqual(run(head_cli=head, entries=[row])[1], [])

    def test_a_dropped_short_form_needs_an_entry(self):
        head = BASE_CLI.replace("short=-c ", "")
        _, missing = run(head_cli=head)
        self.assertEqual(missing, [("flag", "vtopctl scrub --concurrency")])

    def test_a_flag_that_now_takes_a_value_or_became_required_needs_an_entry(self):
        head = BASE_CLI.replace("--check-retention flag", "--check-retention value=<MODE>")
        head = head.replace("--limit value=<LIMIT>", "--limit value=<LIMIT> required")
        _, missing = run(head_cli=head)
        self.assertEqual(
            missing,
            [("flag", "vtopctl scrub --check-retention"), ("flag", "vtopctl scrub --limit")],
        )

    def test_a_value_no_longer_accepted_needs_a_value_or_flag_entry(self):
        head = BASE_CLI.replace("values=kafka,file,syslog", "values=kafka,file")
        _, missing = run(head_cli=head)
        self.assertEqual(missing, [("value", "vtopctl process-once --source syslog")])
        for row in (
            "| value | `vtopctl process-once --source syslog` | v0.11.0 | v0.12.0 | | |",
            "| flag | `vtopctl process-once --source` | v0.11.0 | v0.12.0 | | |",
        ):
            self.assertEqual(run(head_cli=head, entries=[row])[1], [])

    def test_a_new_required_argument_on_an_existing_command_needs_an_entry(self):
        head = BASE_CLI + "vtopctl scrub --tenant value=<TENANT> required\n"
        _, missing = run(head_cli=head)
        self.assertEqual(missing, [("flag", "vtopctl scrub --tenant")])
        row = "| flag | `vtopctl scrub --tenant` | v0.11.0 | not scheduled | | every scrub now names a tenant |"
        self.assertEqual(run(head_cli=head, entries=[row])[1], [])

    def test_a_required_argument_of_a_new_command_or_an_optional_one_needs_no_entry(self):
        head = (
            BASE_CLI
            + "vtopctl audit\nvtopctl audit --since value=<SINCE> required\n"
            + "vtopctl scrub --tenant value=<TENANT>\n"
        )
        self.assertEqual(run(head_cli=head), ([], []))

    def test_an_option_that_took_any_value_and_now_takes_a_list_needs_an_entry(self):
        head = BASE_CLI.replace("--limit value=<LIMIT>", "--limit value=<LIMIT> values=10,100")
        _, missing = run(head_cli=head)
        self.assertEqual(missing, [("flag", "vtopctl scrub --limit")])

    def test_an_option_that_drops_its_list_of_values_needs_no_entry(self):
        head = BASE_CLI.replace(" values=kafka,file,syslog", "")
        self.assertEqual(run(head_cli=head), ([], []))

    def test_a_removed_command_is_covered_by_one_command_entry(self):
        head = "\n".join(l for l in BASE_CLI.splitlines() if not l.startswith("vtopctl meta")) + "\n"
        _, missing = run(head_cli=head)
        self.assertIn(("command", "vtopctl meta"), missing)
        self.assertIn(("command", "vtopctl meta status"), missing)
        self.assertIn(("flag", "vtopctl meta status --config"), missing)
        row = "| command | `vtopctl meta` | v0.11.0 | v0.12.0 | `vtopctl cluster` | |"
        self.assertEqual(run(head_cli=head, entries=[row])[1], [])

    def test_a_deleted_snapshot_file_removes_every_line(self):
        ok, missing = checker.check({CLI: BASE_CLI}, {}, table())
        self.assertIn(("command", "vtopctl"), [(n.kind, n.name) for n in missing])


class Table(unittest.TestCase):
    def assert_rejected(self, row: str, says: str):
        with self.assertRaises(checker.TableError) as raised:
            checker.parse_table(table(row))
        self.assertIn("line 5", str(raised.exception))
        self.assertIn(says, str(raised.exception))

    def test_a_malformed_row_is_named_by_its_line(self):
        self.assert_rejected("| metric | `vtop_x` | v0.11.0 |", "3 cells")
        self.assert_rejected("| widget | `vtop_x` | v0.11.0 | v0.12.0 | | |", "kind 'widget'")
        self.assert_rejected("| metric | `x_total` | v0.11.0 | v0.12.0 | | |", "starts with vtop_")
        self.assert_rejected("| label | `vtop_x_total` | v0.11.0 | v0.12.0 | | |", "vtop_family{label}")
        self.assert_rejected("| flag | `scrub --x` | v0.11.0 | v0.12.0 | | |", "vtopctl or vtop-node")
        self.assert_rejected("| metric | `vtop_x` | soon | v0.12.0 | | |", "Deprecated in")
        self.assert_rejected("| metric | `vtop_x` | v0.11.0 | later | | |", "Removed in")

    def test_a_missing_table_is_an_error(self):
        with self.assertRaises(checker.TableError):
            checker.parse_table("# Deprecations\n\nNo table here.\n")

    def test_the_committed_table_parses(self):
        checker.parse_table((REPO / "docs/deprecations.md").read_text())


class CommandLine(unittest.TestCase):
    """The script end to end, against a git base, as CI runs it."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir)
        (self.dir / "docs/surface").mkdir(parents=True)
        self.git("init", "-q")
        self.write(BASE_METRICS, table())
        self.git("add", "-A")
        self.git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-qm", "base")

    def git(self, *args):
        subprocess.run(["git", "-C", str(self.dir), *args], check=True)

    def write(self, metrics: str, deprecations: str):
        (self.dir / METRICS).write_text(metrics)
        (self.dir / "docs/deprecations.md").write_text(deprecations)

    def main(self) -> tuple[int, str]:
        err = io.StringIO()
        with contextlib.redirect_stderr(err), contextlib.redirect_stdout(io.StringIO()):
            code = checker.main(["--base", "HEAD", "--repo", str(self.dir)])
        return code, err.getvalue()

    def test_a_removal_fails_and_names_the_item_until_its_entry_is_added(self):
        removed = BASE_METRICS.replace("vtop_inflight_batches gauge\n", "")
        self.write(removed, table())
        code, err = self.main()
        self.assertEqual(code, 1)
        self.assertIn("metric `vtop_inflight_batches`", err)
        self.assertIn("docs/deprecations.md", err)
        self.write(removed, table("| metric | `vtop_inflight_batches` | v0.11.0 | v0.12.0 | | |"))
        self.assertEqual(self.main(), (0, ""))

    def test_a_base_without_snapshots_passes(self):
        empty = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, empty)
        subprocess.run(["git", "-C", str(empty), "init", "-q"], check=True)
        (empty / "x").write_text("x")
        subprocess.run(["git", "-C", str(empty), "add", "x"], check=True)
        subprocess.run(
            ["git", "-C", str(empty), "-c", "user.name=t", "-c", "user.email=t@example.com",
             "commit", "-qm", "x"],
            check=True,
        )
        self.assertEqual(checker.git_snapshots(empty, "HEAD"), {})


if __name__ == "__main__":
    unittest.main()
