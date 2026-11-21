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

    def read(self, process, runs, high_watermark, unkeyed=()):
        """`runs`: (offset, first_sequence, count[, values_match])."""
        for run in runs:
            offset, first, count = run[:3]
            values_match = run[3] if len(run) > 3 else True
            self._event(op="read", type="records", process=process, offset=offset,
                        first_sequence=first, count=count, values_match=values_match)
        for offset in unkeyed:
            self._event(op="read", type="unkeyed", process=process, offset=offset)
        self._event(op="read", type="end", process=process, high_watermark=high_watermark,
                    records=sum(r[2] for r in runs))
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
