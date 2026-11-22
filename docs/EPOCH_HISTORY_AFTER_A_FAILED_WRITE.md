# Epoch history after a failed journal write

This is a design note from #881, a recommendation only: nothing here is
implemented. It answers one question: once space returns, can a replica whose
epoch-journal write failed safely retry that write, or rebuild its history,
instead of waiting for `vtopctl node repair`?

## What happens today

Each replica keeps a fencing-epoch journal next to its log: the epoch
that wrote each stretch of the log, as `(epoch, start_offset)` entries, plus
the leader lineage it is filing records under (#640). Promotion and fencing
compare these histories, not offsets (#577). Two replicas both at offset 90
need not hold the same records, and only a history can tell.

When a write to the journal fails, the replica marks its history **broken**.
A full disk is the case the cluster soak hit. The failed write may be an
epoch adoption, a leader-lineage filing, a truncation, or the first entry at
open. From then on, the replica:

- answers every fence and promotion probe with an empty history ("unknown").
  So no leader opens a replication session to it (#649), and no promotion
  counts its offset (#577);
- since #881, logs one ERROR line and exports
  `vtop_broker_epoch_history_broken`, and the `VtopEpochHistoryBroken` alert
  pages.

The refusal is deliberate. After the failure the in-memory vector and the
file may disagree, and the file may hold a torn entry (the journal is
poisoned until reopened). Worse, the vector may have a **hole**: an epoch the
replica adopted, or a stretch it was told to file under a leader's lineage,
that is recorded nowhere.

## Why a hole is dangerous

The journal's job is to say which leadership wrote each record. Suppose the
write that recorded epoch E+1 at offset X failed, and the replica then took
records at and after X under E+1, for example over a session that was
already open, since appends are not gated on the mark. On disk, those
records are attributed to E, the last entry that was written. A history
with that misattribution then answers fences as if it were proven:

- **A promotion could count it** as agreeing with a candidate whose epoch-E
  records at X are different bytes. That is the #577 hole: an offset counted
  for records that are not the same records, and a boundary established over
  them. An acknowledged record held only by the replicas that are not
  counted can then be overwritten.
- **Reconciliation could cut the wrong suffix.** Against a leader's history,
  the divergence point is computed from the misattributed entry. #611 keeps
  a suffix the caller cannot prove unacknowledged, so the replica would not
  discard acknowledged records on its own. But it could keep records under
  an epoch that never wrote them and later show them under a mark (#261).

#315 is the same lesson from repair. A replica holding records whose lineage
it cannot prove is not a replica whose offset may be counted. The answer
there was to carry the history with the records, never to infer it.

## The mark is durable (v0.11.0)

The first version of this change kept the broken mark in memory, so a
restart cleared it: the node reopened the journal, dropped a torn tail, and
offered whatever entries survived, with nothing to say a write was lost
after them. That was an "auto-heal" already, and the unsafe one.

The mark is now written beside the journal, as `fencing-epochs.broken`. It
holds the operator message, written the way the journal's sidecars are
(staged, fsynced, renamed over, the directory fsynced). It is read back when
the journal is attached at every open, and it refuses the history exactly as
the in-memory mark does. Only `vtopctl node repair` clears it, because repair
installs the source's history. A directory with no mark behaves exactly as
before.

**The full disk that broke the history can refuse the mark too.** The mark
then stays pending. It is retried whenever the history is consulted (every
fence and promotion probe) and before every append, at most once a second,
and a warning says it is pending until it lands. An append needs the space
the mark needs, so in practice the mark is on disk before the replica can
take a record after its break.

**A crash before the mark lands leaves no mark, and that is safe.** The
danger a mark guards against is records taken *after* the failed write,
under a history that does not describe them. A process that died in the
window took none. What it reopens is the pre-failure history over a log that
ends where it ended at the failure: the state ordinary journal recovery after
a crash mid-write already handles (#648). The test
`a_crash_before_the_mark_lands_leaves_the_pre_failure_history_over_an_unchanged_log`
pins this.

**A residual case is not closed.** The mark's write fails, the process keeps
running and takes records, and it then dies before a retry lands the mark.
That needs an append to find space where the mark, written just before it,
did not. Closing it fully means refusing appends until the mark is on disk.
That is the append gate of option 1 below, a behaviour change left to its
own decision.

## Options once space returns

### 1. Retry the failed write (narrow, recommended)

Retrying is safe exactly when the retried entry restores the history the
replica would have had if the write had succeeded, and nothing happened in
between that the entry would mis-describe. Three things must be proven:

1. **The file is the pre-failure history.** Reopen the journal (this
   truncates a torn tail and revalidates every checksum) and require its
   entries to equal the in-memory entries from before the failed write. Any
   difference refuses the retry.
2. **The log has not moved past the failed entry's start.** The intended
   entry is `(E+1, X)`. Require the log's next offset to be X now, and to
   have been X when the write failed, so no record exists at or after X
   whose attribution is in question. That means a broken replica must refuse
   appends from the moment of the mark, which it does not do today: an
   already-open session can still append. **This gate is the prerequisite**,
   and it is a behaviour change, so it needs its own decision.
3. **The entry is the one that failed.** Store the failed operation and its
   arguments with the mark: the adoption's epoch and offset, or the
   lineage-filing epoch and tail. Retry exactly that operation, never a
   recomputed one, since the replica's view may have moved on (a newer grant,
   say).

Only adoption at the tail and the first entry at open fit this narrowly.
Lineage filing (#640) and truncation have more state to restore, so they
should stay on repair until each has its own proof.

### 2. Rebuild from the leader (not recommended yet)

Truncate the broken replica to the last offset its surviving history
provably describes, the start of the failed entry, and let the leader catch
it up from its log (#887). It then files the records under the leader's
lineage. This discards records past the cut, which is safe only if the
leader holds every one of them byte for byte. The #611 rule only lets a
replica discard what the caller *proves* unacknowledged. Proving it means
comparing content, chunk roots for example, not offsets, and a broken
replica cannot answer the fence that would normally carry that proof. Too
much new machinery for the benefit; repair already does this with a whole
directory.

### 3. Repair (today)

`vtopctl node repair` rebuilds the range directory, history included, from
a replica whose history is intact. It is always safe and needs an operator.

## Recommendation

1. **Done (v0.10.4):** make the break loud, with a log line, a gauge, an
   alert and a runbook, and leave the refusal as it is.
2. **Done (v0.11.0):** persist the broken mark so a restart cannot silently
   clear it.
3. **Then, if a full disk proves common in the field:** gate appends on the
   broken mark, then retry adoption and first-entry writes under the three
   proofs above, with a crash and fault sweep over every storage operation of
   the retry. Everything else stays on repair.
4. **Not recommended:** rebuilding by truncation and catch-up (option 2),
   until there is a content proof a broken replica can give.
