"""Tests for scripts/mutants-gate.py (#729).

Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_*.py'

Each case writes a crafted `outcomes.json` in the shape cargo-mutants
writes it (a `Baseline` scenario, then one `{"Mutant": {...}}` scenario per
mutant with its `summary`) and runs the gate on it as CI does.
"""

import importlib.util
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parents[1] / "mutants-gate.py"

_spec = importlib.util.spec_from_file_location("mutants_gate", SCRIPT)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)

STATE = "crates/vtop-core/src/state_machine.rs"
REPLAY = "crates/vtop-core/src/replay.rs"


def mutant(file, line, summary, name=None, replacement="true"):
    body = {
        "package": "vtop-core",
        "file": file,
        "function": {"function_name": "transition", "return_type": "-> bool"},
        "span": {"start": {"line": line, "column": 5}, "end": {"line": line, "column": 9}},
        "replacement": replacement,
        "genre": "FnValue",
    }
    if name is not None:
        body["name"] = name
    return {"scenario": {"Mutant": body}, "summary": summary, "log_path": "log", "phase_results": []}


def baseline(summary="Success"):
    return {"scenario": "Baseline", "summary": summary, "log_path": "log", "phase_results": []}


def outcomes(*entries, finished=True):
    # cargo-mutants rewrites outcomes.json as the run goes and sets
    # `end_time` only when the whole campaign has finished.
    return {
        "outcomes": list(entries),
        "cargo_mutants_version": "27.1.0",
        "start_time": "2026-11-14T10:00:00Z",
        "end_time": "2026-11-14T10:09:00Z" if finished else None,
    }


class GateRun:
    """Run the script as CI does and keep its exit code and output."""

    def __init__(self, document, *args):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "outcomes.json"
            if isinstance(document, str):
                path.write_text(document)
            elif document is not None:
                path.write_text(json.dumps(document))
            summary = pathlib.Path(tmp) / "summary.md"
            proc = subprocess.run(
                [sys.executable, str(SCRIPT), "--outcomes", str(path), "--summary", str(summary), *args],
                capture_output=True,
                text=True,
                check=False,
            )
            self.code = proc.returncode
            self.out = proc.stdout + proc.stderr
            self.summary = summary.read_text() if summary.exists() else ""


class ScoreTest(unittest.TestCase):
    def test_all_caught_passes(self):
        run = GateRun(
            outcomes(baseline(), mutant(STATE, 10, "CaughtMutant"), mutant(REPLAY, 20, "CaughtMutant")),
            "--min-score", "0.90",
        )
        self.assertEqual(run.code, 0, run.out)
        self.assertIn("1.000", run.out)

    def test_a_missed_mutant_below_the_threshold_fails_and_is_named(self):
        run = GateRun(
            outcomes(
                baseline(),
                mutant(STATE, 10, "CaughtMutant"),
                mutant(REPLAY, 42, "MissedMutant", name="crates/vtop-core/src/replay.rs:42:5: replace rewind -> bool with true"),
            ),
            "--min-score", "0.90",
        )
        self.assertEqual(run.code, 1, run.out)
        self.assertIn("replace rewind -> bool with true", run.out)
        self.assertIn("0.500", run.out)
        self.assertIn("replace rewind -> bool with true", run.summary)

    def test_a_missed_mutant_without_a_name_is_still_located(self):
        run = GateRun(
            outcomes(baseline(), mutant(REPLAY, 42, "MissedMutant", replacement="false")),
            "--min-score", "0.90",
        )
        self.assertEqual(run.code, 1, run.out)
        self.assertIn(f"{REPLAY}:42", run.out)
        self.assertIn("false", run.out)

    def test_exactly_at_the_threshold_passes(self):
        caught = [mutant(STATE, n, "CaughtMutant") for n in range(9)]
        run = GateRun(outcomes(baseline(), *caught, mutant(STATE, 99, "MissedMutant")), "--min-score", "0.90")
        self.assertEqual(run.code, 0, run.out)

    def test_unviable_mutants_are_excluded_from_the_score(self):
        unviable = [mutant(STATE, n, "Unviable") for n in range(50)]
        run = GateRun(outcomes(baseline(), mutant(STATE, 1, "CaughtMutant"), *unviable), "--min-score", "1.0")
        self.assertEqual(run.code, 0, run.out)
        self.assertIn("50 unviable", run.out)

    def test_timeouts_count_against_the_score(self):
        run = GateRun(
            outcomes(baseline(), mutant(STATE, 1, "CaughtMutant"), mutant(STATE, 2, "Timeout")),
            "--min-score", "0.90",
        )
        self.assertEqual(run.code, 1, run.out)
        self.assertIn("1 timeout", run.out)

    def test_per_file_scores_are_reported(self):
        run = GateRun(
            outcomes(
                baseline(),
                mutant(STATE, 1, "CaughtMutant"),
                mutant(REPLAY, 2, "CaughtMutant"),
                mutant(REPLAY, 3, "MissedMutant"),
            ),
            "--min-score", "0.5",
        )
        self.assertEqual(run.code, 0, run.out)
        self.assertRegex(run.out, r"state_machine\.rs\s+1 caught\s+0 missed\s+0 timeout\s+0 unviable\s+score 1\.000")
        self.assertRegex(run.out, r"replay\.rs\s+1 caught\s+1 missed\s+0 timeout\s+0 unviable\s+score 0\.500")


class RefusalTest(unittest.TestCase):
    def test_no_viable_mutants_is_refused_not_passed(self):
        run = GateRun(outcomes(baseline(), mutant(STATE, 1, "Unviable")), "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)
        self.assertIn("no viable mutants", run.out)

    def test_empty_outcomes_are_refused(self):
        run = GateRun(outcomes(), "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)

    def test_a_missing_file_is_refused(self):
        run = GateRun(None, "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)
        self.assertIn("cannot read", run.out)

    def test_invalid_json_is_refused(self):
        run = GateRun("{not json", "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)

    def test_a_failed_baseline_is_refused(self):
        run = GateRun(outcomes(baseline("Failure"), mutant(STATE, 1, "CaughtMutant")), "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)
        self.assertIn("baseline", run.out)

    def test_an_unclassified_mutant_outcome_is_refused(self):
        # `Success` for a mutant is what a --check-only run records: nothing
        # was tested, so it is not evidence either way.
        run = GateRun(outcomes(baseline(), mutant(STATE, 1, "Success")), "--min-score", "0.9")
        self.assertEqual(run.code, 2, run.out)
        self.assertIn("Success", run.out)

    def test_an_expected_file_with_no_viable_mutant_is_refused(self):
        # A rename that drops replay.rs out of `--file` must not pass the gate
        # on state_machine.rs alone.
        run = GateRun(
            outcomes(baseline(), mutant(STATE, 1, "CaughtMutant")),
            "--min-score", "0.9", "--expect-file", STATE, "--expect-file", REPLAY,
        )
        self.assertEqual(run.code, 2, run.out)
        self.assertIn(REPLAY, run.out)

    def test_an_unfinished_run_is_refused(self):
        # Review of #766: a run that died after one caught mutant per file
        # leaves a partial outcomes.json with no end_time; scoring it would
        # pass on mutants that were never tested.
        partial = outcomes(
            baseline(), mutant(STATE, 1, "CaughtMutant"), mutant(REPLAY, 2, "CaughtMutant"), finished=False
        )
        run = GateRun(partial, "--min-score", "0.9", "--expect-file", STATE, "--expect-file", REPLAY)
        self.assertEqual(run.code, 2, run.out)
        self.assertIn("did not finish", run.out)
        del partial["end_time"]
        self.assertEqual(GateRun(partial, "--min-score", "0.9").code, 2)

    def test_a_threshold_outside_zero_to_one_is_refused(self):
        run = GateRun(outcomes(baseline(), mutant(STATE, 1, "CaughtMutant")), "--min-score", "90")
        self.assertEqual(run.code, 2, run.out)


class ReportOnlyTest(unittest.TestCase):
    def test_report_only_never_fails_on_the_score(self):
        run = GateRun(
            outcomes(baseline(), mutant(STATE, 1, "MissedMutant")),
            "--report-only",
        )
        self.assertEqual(run.code, 0, run.out)
        self.assertIn("report only", run.out)
        self.assertIn("score 0.000", run.summary)


class LibraryTest(unittest.TestCase):
    def test_score_helper(self):
        self.assertEqual(gate.score(9, 1, 0), 0.9)
        self.assertIsNone(gate.score(0, 0, 0))


if __name__ == "__main__":
    unittest.main()
