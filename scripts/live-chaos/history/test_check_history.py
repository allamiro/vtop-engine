"""Unit tests for check_history.py (#728).

Each failing case is the shape a known data-safety bug leaves in a history,
so the checker is shown to catch it without re-introducing the bug into a
build: #611 (a replica truncated an acknowledged suffix at the fence),
#641 (a deposed leader's old-epoch records under the new high watermark),
#261 (a diverged replica acknowledged writes it did not hold).
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_history as ch  # noqa: E402


class History:
    """Builds a history as the recorder writes it."""

    def __init__(self):
        self.lines = []
        self.clock = 0

    def _event(self, **fields):
        self.clock += 1
        fields.setdefault("time_us", self.clock)
        self.lines.append(json.dumps(fields))

    def produce(self, process, batch, first, count, outcome, next_offset=None, first_offset=None):
        self._event(op="produce", type="invoke", process=process, batch=batch,
                    first_sequence=first, count=count, fencing_epoch=1)
        if outcome is None:
            return self
        event = dict(op="produce", type=outcome, process=process, batch=batch,
                     first_sequence=first, count=count)
        if outcome == "ok":
            event["committed_next_offset"] = next_offset
            if first_offset is not None:
                event["first_offset"] = first_offset
        self._event(**event)
        return self

    def read(self, process, runs, high_watermark, unkeyed=(), start_offset=None):
        """`runs`: (offset, first_sequence, count[, values_match]).
        `start_offset`: the read started there (retention, #727)."""
        for run in runs:
            offset, first, count = run[:3]
            values_match = run[3] if len(run) > 3 else True
            self._event(op="read", type="records", process=process, offset=offset,
                        first_sequence=first, count=count, values_match=values_match)
        for offset in unkeyed:
            self._event(op="read", type="unkeyed", process=process, offset=offset)
        end = dict(op="read", type="end", process=process, high_watermark=high_watermark,
                   records=sum(r[2] for r in runs))
        if start_offset is not None:
            end["start_offset"] = start_offset
        self._event(**end)
        return self

    def check(self, allow_unkeyed=False):
        batches, reads = ch.load(self.lines)
        return ch.check(batches, reads, allow_unkeyed)


def kinds(result):
    return set(result["violations"])


class CleanHistories(unittest.TestCase):
    def test_acknowledged_batches_read_back_in_place(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "ok", 20)
        h.read("r", [(0, 0, 20)], 20)
        result = h.check()
        self.assertTrue(result["valid"], result)
        self.assertEqual(result["acknowledged_records"], 20)

    def test_an_indeterminate_batch_may_be_visible_once_or_not_at_all(self):
        for visible in (10, 20):
            h = History()
            h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "info")
            h.read("r", [(0, 0, visible)], visible)
            self.assertTrue(h.check()["valid"], visible)

    def test_a_process_killed_mid_batch_leaves_an_indeterminate_batch(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, None)
        h.read("r", [(0, 0, 20)], 20)
        self.assertTrue(h.check()["valid"])

    def test_a_resumed_producer_restarting_its_sequences_is_told_apart_by_offset(self):
        # After a failover the producer bumps its epoch and restarts at
        # sequence 0, so its records carry the same keys as the first ones.
        h = History()
        h.produce("p1", 1, 0, 10, "ok", 10).produce("p1", 2, 10, 10, "info")
        h.produce("p2", 1, 0, 10, "ok", 30)
        h.read("r", [(0, 0, 20), (20, 0, 10)], 30)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_later_read_may_see_more(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "ok", 20)
        h.read("r1", [(0, 0, 10)], 10).read("r2", [(0, 0, 20)], 20)
        self.assertTrue(h.check()["valid"])


    def test_a_retry_acknowledged_where_its_unanswered_attempt_landed_is_one_copy(self):
        # The leader died before answering; the producer, restarted as a new
        # process under the same id and epoch, resent the batch, and the new
        # leader answered it from the copy that had landed.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "info")
        h.produce("produce:P:1:200", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10)], 10)
        self.assertTrue(h.check()["valid"])

    def test_the_search_backtracks_to_the_attempt_that_explains_the_whole_run(self):
        # An earlier unanswered 0..4 fits first and dead-ends at 5; the later
        # unanswered 0..9 explains the visible run whole (review of #878).
        h = History()
        h.produce("a", 1, 0, 5, "info").produce("b", 1, 0, 10, "info")
        h.read("r", [(0, 0, 10)], 10)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_producer_under_a_new_epoch_writes_new_records(self):
        # Scenario 09's resumed producer: epoch 2 restarts at sequence 0, and
        # its records are not copies of epoch 1's.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "info")
        h.produce("produce:P:2:200", 1, 0, 10, "ok", 20)
        h.read("r", [(0, 0, 10), (10, 0, 10)], 20)
        self.assertTrue(h.check()["valid"])

    def test_a_restarted_producer_acknowledged_again_at_the_same_offsets_is_one_copy(self):
        # Review of #878: the broker answers a resent, already-acknowledged
        # batch with its original offsets, so two acknowledgements of one
        # offset for the same logical records are one copy, not two.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "ok", 10)
        h.produce("produce:P:1:200", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10)], 10)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_resent_batch_answered_under_a_later_watermark_sits_where_it_was(self):
        # Review of #878: the broker answers a resent, already-acknowledged
        # batch with its original offsets and the range's current watermark.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "ok", 10).produce("produce:P:1:100", 2, 10, 10, "ok", 20)
        h.produce("produce:P:1:200", 1, 0, 10, "ok", 20, first_offset=0)
        h.read("r", [(0, 0, 20)], 20)
        self.assertTrue(h.check()["valid"], h.check())

    def test_unanswered_batches_are_assigned_across_groups(self):
        # Review of #878: B (0..4) and C (5..9) tile the first visible run, but
        # then nothing explains the second; A (0..9) explains the first and B
        # the second.
        h = History()
        h.produce("b", 1, 0, 5, "info").produce("c", 1, 5, 5, "info").produce("a", 1, 0, 10, "info")
        h.read("r", [(0, 0, 10), (10, 0, 5)], 15)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_retry_of_part_of_an_unanswered_batch_is_one_copy(self):
        # Review of #878: the unanswered 0..9 landed whole; a retry of 0..4
        # under the same producer id and epoch is answered at 0..4.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "info")
        h.produce("produce:P:1:200", 1, 0, 5, "ok", 10, first_offset=0)
        h.read("r", [(0, 0, 10)], 10)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_thousand_unanswered_batches_in_one_run_get_a_verdict(self):
        # Review of #878: the search keeps its own stack, so a run explained by
        # a thousand one-record batches is not a RecursionError.
        h = History()
        for i in range(1000):
            h.produce(f"produce:P:{i + 1}:{i}", 1, 0, 1, "info")
        h.read("r", [(i, 0, 1) for i in range(1000)], 1000)
        self.assertTrue(h.check()["valid"])
        # And one run of a thousand sequences, a batch each.
        h = History()
        for i in range(1000):
            h.produce("a", i + 1, i, 1, "info")
        h.read("r", [(0, 0, 1000)], 1000)
        self.assertTrue(h.check()["valid"])

    def test_a_search_out_of_steps_leaves_nothing_applied(self):
        saved = ch.SEARCH_BUDGET
        ch.SEARCH_BUDGET = 3
        try:
            h = History()
            for i in range(10):
                h.produce("a", i + 1, i, 1, "info")
            h.read("r", [(0, 0, 10)], 10)
            result = h.check()
        finally:
            ch.SEARCH_BUDGET = saved
        self.assertEqual(kinds(result), {"phantom"})

class Violations(unittest.TestCase):
    def test_611_an_acknowledged_suffix_truncated_at_failover_is_lost(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "ok", 20)
        h.read("r", [(0, 0, 10)], 10)
        result = h.check()
        self.assertEqual(kinds(result), {"lost"})
        self.assertEqual(result["violations"]["lost"]["count"], 10)
        self.assertIn("acknowledged record 10", result["violations"]["lost"]["first"][0])

    def test_261_a_write_acknowledged_but_not_held_is_lost(self):
        # The diverged replica answered the new leader's write as a duplicate
        # of what it held: the acknowledgement names offsets that hold other
        # records.
        h = History()
        h.produce("p1", 1, 0, 10, "ok", 10)
        h.produce("p2", 1, 100, 10, "ok", 20)
        h.read("r", [(0, 0, 10), (10, 50, 10)], 20)
        result = h.check()
        self.assertIn("lost", kinds(result))
        self.assertIn("holds sequence 50", result["violations"]["lost"]["first"][0])

    def test_641_a_deposed_leaders_refused_records_are_never_visible(self):
        h = History()
        h.produce("new", 1, 0, 10, "ok", 10)
        h.produce("old", 1, 500, 5, "fail")
        h.read("r", [(0, 0, 10), (10, 500, 5)], 15)
        result = h.check()
        self.assertEqual(kinds(result), {"phantom"})
        self.assertIn("refused produce", result["violations"]["phantom"]["first"][0])

    def test_a_record_nobody_wrote_is_a_phantom(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10), (10, 77, 3)], 13)
        self.assertEqual(kinds(h.check()), {"phantom"})

    def test_part_of_an_indeterminate_batch_is_not_a_batch(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "info")
        h.read("r", [(0, 0, 15)], 15)
        self.assertEqual(kinds(h.check()), {"phantom"})

    def test_an_indeterminate_batch_visible_twice_is_a_duplicate(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "info")
        h.read("r", [(0, 0, 20), (20, 10, 10)], 30)
        self.assertEqual(kinds(h.check()), {"duplicate"})

    def test_an_unanswered_attempt_cannot_explain_a_copy_of_its_acknowledged_retry(self):
        # Review of #878: the retry was acknowledged at 10..19, so the same
        # producer's records at 0..9 are a second copy, not the attempt.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "info")
        h.produce("produce:P:1:200", 1, 0, 10, "ok", 20)
        h.read("r", [(0, 0, 10), (10, 0, 10)], 20)
        self.assertEqual(kinds(h.check()), {"duplicate"})

    def test_an_acknowledged_record_visible_a_second_time_is_a_duplicate(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10), (10, 0, 10)], 20)
        self.assertEqual(kinds(h.check()), {"duplicate"})

    def test_two_acknowledgements_of_one_offset_are_a_duplicate(self):
        h = History()
        h.produce("p1", 1, 0, 10, "ok", 10)
        h.produce("p2", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10)], 10)
        self.assertIn("duplicate", kinds(h.check()))

    def test_one_logical_record_acknowledged_at_two_offsets_is_a_duplicate(self):
        # Review of #878: a retry under the same producer id and epoch that
        # the broker appended again instead of deduplicating. Both copies are
        # visible where they were acknowledged, so nothing is lost; the second
        # placement is the violation.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "ok", 10)
        h.produce("produce:P:1:200", 1, 0, 10, "ok", 20)
        h.read("r", [(0, 0, 10), (10, 0, 10)], 20)
        result = h.check()
        self.assertEqual(kinds(result), {"duplicate"}, result)
        self.assertIn("acknowledged at offset 0", result["violations"]["duplicate"]["first"][0])

    def test_an_acknowledgement_behind_the_previous_one_is_out_of_order(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 20).produce("p", 2, 10, 10, "ok", 10)
        h.read("r", [(0, 10, 10), (10, 0, 10)], 20)
        self.assertIn("order", kinds(h.check()))

    def test_a_record_with_the_wrong_value_is_lost(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 9), (9, 9, 1, False)], 10)
        self.assertEqual(kinds(h.check()), {"lost"})

    def test_an_earlier_read_that_missed_a_committed_record_is_caught(self):
        # Review of #878: a read with a hole below its own high watermark,
        # where a later read shows the record.
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r1", [(0, 0, 9)], 10).read("r2", [(0, 0, 10)], 10)
        result = h.check()
        self.assertEqual(kinds(result), {"missed"}, result)
        self.assertIn("offset 9", result["violations"]["missed"]["first"][0])

    def test_an_acknowledgement_placed_by_its_offsets_is_checked_there(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 20, first_offset=5)
        h.read("r", [(0, 0, 10)], 20)
        self.assertIn("lost", kinds(h.check()))

    def test_a_retry_of_part_of_an_unanswered_batch_does_not_excuse_a_second_copy(self):
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "info")
        h.produce("produce:P:1:200", 1, 0, 5, "ok", 5, first_offset=0)
        h.read("r", [(0, 0, 5), (5, 0, 10)], 15)
        # The unanswered batch cannot account for 0..4 a second time, so
        # offsets 5..14 have no explanation.
        self.assertFalse(h.check()["valid"])

    def test_two_reads_under_one_name_are_two_reads(self):
        # Review of #881: a pid reused days later names a second read the
        # same; its `end` closes the first, so a rewrite between them shows.
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("read:7", [(0, 0, 5), (5, 40, 5)], 10).read("read:7", [(0, 0, 10)], 10)
        result = h.check()
        self.assertEqual(result["reads"], 2)
        self.assertIn("rewritten", kinds(result))

    def test_a_producer_name_that_comes_round_again_keeps_both_invocations(self):
        # Review of #878: a reused pid under the same producer id and epoch
        # numbers its batches from 1 again; the first invocation's
        # acknowledgements still count.
        h = History()
        h.produce("produce:P:1:7", 1, 0, 10, "ok", 10)
        h.produce("produce:P:1:7", 1, 10, 10, "ok", 20)
        h.read("r", [(10, 10, 10)], 20)
        result = h.check()
        self.assertIn("lost", kinds(result))
        self.assertEqual(result["acknowledged_records"], 20)

    def test_records_reordered_inside_an_acknowledged_batch_are_out_of_order(self):
        # Review of #878: sequences 0 and 1 answered at offsets 1 and 0.
        h = History()
        h.produce("p", 1, 0, 2, "ok", 2)
        h.lines[-1] = h.lines[-1][:-1] + ', "offsets": [1, 0]}'
        h.read("r", [(0, 1, 1), (1, 0, 1)], 2)
        result = h.check()
        self.assertEqual(kinds(result), {"order"}, result)

    def test_a_restarted_producer_acknowledged_below_its_earlier_sequences_is_out_of_order(self):
        # Review of #878: the same producer id and epoch, two processes.
        h = History()
        h.produce("produce:P:1:100", 1, 0, 10, "ok", 20, first_offset=10)
        h.produce("produce:P:1:200", 1, 10, 10, "ok", 20, first_offset=0)
        h.read("r", [(0, 10, 10), (10, 0, 10)], 20)
        self.assertEqual(kinds(h.check()), {"order"})

    def test_the_high_watermark_never_goes_back(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r1", [(0, 0, 10)], 12).read("r2", [(0, 0, 10)], 10)
        self.assertIn("hwm", kinds(h.check()))

    def test_a_record_an_earlier_read_saw_is_not_rewritten(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "info")
        h.produce("q", 1, 10, 10, "info")
        h.read("r1", [(0, 0, 20)], 20)
        h.read("r2", [(0, 0, 10), (10, 10, 10, False)], 20)
        self.assertIn("rewritten", kinds(h.check()))

    def test_unkeyed_records_are_violations_unless_allowed(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r", [(0, 0, 10)], 11, unkeyed=[10])
        self.assertEqual(kinds(h.check()), {"unkeyed"})
        allowed = h.check(allow_unkeyed=True)
        self.assertTrue(allowed["valid"])
        self.assertTrue(allowed["notes"])

    def test_no_read_is_a_violation(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        self.assertEqual(kinds(h.check()), {"input"})


class Retention(unittest.TestCase):
    """Reads of a range whose retention reclaimed a prefix (#727): each read
    starts at the log start, and an earlier read stands for the offsets
    below the last read's start."""

    def history(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10).produce("p", 2, 10, 10, "ok", 20)
        return h

    def test_a_record_a_read_saw_before_it_was_reclaimed_is_not_lost(self):
        h = self.history()
        h.read("r1", [(0, 0, 20)], 20).read("r2", [(10, 10, 10)], 20, start_offset=10)
        self.assertTrue(h.check()["valid"], h.check())

    def test_a_record_reclaimed_before_any_read_saw_it_is_lost(self):
        h = self.history()
        h.read("r", [(10, 10, 10)], 20, start_offset=10)
        result = h.check()
        self.assertEqual(kinds(result), {"lost"}, result)
        self.assertEqual(result["violations"]["lost"]["count"], 10)
        self.assertIn("no read saw it", result["violations"]["lost"]["first"][0])

    def test_a_record_rewritten_inside_a_later_reads_window_is_caught(self):
        h = self.history()
        h.read("r1", [(0, 0, 20)], 20)
        h.read("r2", [(10, 10, 5), (15, 40, 5)], 20, start_offset=10)
        self.assertIn("rewritten", kinds(h.check()))

    def test_a_record_missing_inside_a_later_reads_window_is_caught(self):
        h = self.history()
        h.read("r1", [(0, 0, 20)], 20)
        h.read("r2", [(10, 10, 5)], 20, start_offset=10)
        self.assertIn("rewritten", kinds(h.check()))

    def test_a_phantom_an_earlier_read_saw_is_still_a_phantom_once_reclaimed(self):
        h = History()
        h.produce("p", 1, 0, 10, "ok", 10)
        h.read("r1", [(0, 0, 10), (10, 77, 3)], 13)
        h.read("r2", [], 13, start_offset=13)
        self.assertEqual(kinds(h.check()), {"phantom"})


class CommandLine(unittest.TestCase):
    def test_exit_codes(self):
        script = str(Path(ch.__file__))
        with tempfile.TemporaryDirectory() as tmp:
            good = History().produce("p", 1, 0, 5, "ok", 5).read("r", [(0, 0, 5)], 5)
            bad = History().produce("p", 1, 0, 5, "ok", 5).read("r", [(0, 0, 4)], 4)
            for name, history, code in (("good", good, 0), ("bad", bad, 1)):
                path = os.path.join(tmp, f"{name}.jsonl")
                Path(path).write_text("\n".join(history.lines) + "\n")
                out = subprocess.run([sys.executable, script, path], capture_output=True,
                                     text=True, check=False)
                self.assertEqual(out.returncode, code, out.stderr)
                self.assertEqual(json.loads(out.stdout)["valid"], code == 0)
            garbage = os.path.join(tmp, "garbage.jsonl")
            Path(garbage).write_text("not json\n")
            out = subprocess.run([sys.executable, script, garbage], capture_output=True,
                                 text=True, check=False)
            self.assertEqual(out.returncode, 2)


if __name__ == "__main__":
    unittest.main()
