# Roadmap

What shipped, what is next, and — where it is useful — what a release taught us
that the plan did not anticipate.

Milestones are the source of truth for scope; this file is the narrative. It is
written for someone deciding whether to depend on a version, so it says plainly
what each release does **not** yet do.

---

## Next release — per-stream checksum selection

- **[#669](https://github.com/allamiro/vtop-engine/issues/669)** — choose
  `checksum.algorithm` per stream, inheriting the global value by default.
  This is the manual selection step motivated by the paper's cost-aware
  future work (Eq. 4), recorded in the
  [implementation plan](https://github.com/allamiro/vtop-engine/issues/669#issuecomment-6806355703).
  Automatic selection still needs a measured cost model and is future work.
- **Upgrade note:** older engines silently ignore the new stream key and
  use the global algorithm. Upgrade all engines before relying on it; keep
  the global algorithm strong during mixed-version operation. Recovery and
  scrub continue checking each old batch with its manifest's algorithm.
  No manifest format or ledger migration is introduced. Downgrade remains
  unsupported.

## v0.1.0 — a range that fails over

Metadata leases, a lease agent on the data node, verified promotion, and the
first live-chaos scenarios on real processes.

The release pipeline ships `vtop-node` alongside `vtopctl`, publishes a
multi-arch GHCR image, and attaches an SBOM, cosign signatures and
`SHA256SUMS`. Tagged as a pre-release: the 0.x series makes no compatibility
promise across minor versions.

**Not in it:** replicated-range failover without harness-style follower
restarts, and any form of replica replacement.

## v0.2.0 — failover hardening

Admin transport authorization (#238), the follower-side lease watcher that
removed the restart-per-epoch requirement (#239), and the safety half of the
recovery-protocol arc: per-replica epoch history, bounded divergence
truncation, fence-and-read in one round trip, reconcile-while-fenced, and
undroppable high-water marks.

One item was built and **withdrawn**: the new-epoch marker (#265) did not close
the hazard it was written for. The open question is stated precisely on #240
rather than papered over — a marker that looks like a fix and is not is worse
than an acknowledged gap.

**Not in it:** a lost replica had no road back. The transfer plane did not
exist.

## v0.3.0 — verified sealed-segment transfer and evidence-gated retirement

The theme is **a sealed segment can move between replicas verbatim, and the
copy is proven before the original is retired.**

It stops short of a replacement that resumes serving. See the limitations
below — that boundary is the most important thing on this page.

- **#270** — segment rolling at runtime, verbatim sealed-segment transfer, and
  follower adoption. A sealed segment moves byte-for-byte; the receiver
  rebuilds `.index`/`.chunks` so it validates the artifact rather than trusting
  it.
- **#301 / #303** — `vtopctl node repair`: pull a leader's sealed prefix into a
  stranded replica's directory and adopt it into a servable range, reporting
  the gap that remains rather than declaring success.
- **#304 / #305 / #308** — the operator surface for placement, replacement and
  retirement, and a linearizable read of the values every one of those commands
  compares against.
- **#242** — the whole flow driven on real processes, asserting the **ordering**
  rather than the end state: retirement is refused before the replacement proof
  commits and accepted after.
- **#289** — CI stopped rebuilding every dependency to rebuild one line of Rust.

### What this release actually taught us

Worth recording, because it changed how the milestone was finished.

Writing #242 — the first thing to drive the replacement flow end to end on real
processes — found **ten gaps**, each invisible until something tried to use the
feature rather than test its parts. The four that mattered most:

- `vtopctl node repair` **could not work at all**. No node installed
  `LeaderSegmentTransferHandler`; only the tests constructed it, so every layer
  had coverage and the composition had none (#312).
- `register-sealed-segment` and `mark-segment-verified` had no CLI, and a
  placement refuses an unverified segment — so the chain register → verify →
  place was broken at both of its first two links.
- `segment verify` reported neither the offsets nor the content root, so
  nothing emitted the evidence the proof commands require.
- `max_segment_bytes`, `max_group_bytes` and `max_record_bytes` were constants.
  A range could not roll, and **only sealed segments transfer** — so the
  shipped default gave repair nothing to move.

The lesson is narrow and repeatable: **exercise the composition, not only the
layers.** Every one of those had passing tests around it. The scenario that
drove the real binaries is what found them, and it is now in CI so they cannot
reopen quietly.

### Known limitations in v0.3.0

- **A repaired replica does not survive a leader transition (#315).** The
  transfer carries `.segment`, `.manifest.json` and `.producers` but not the
  epoch history, so a repaired replica holds records whose lineage it cannot
  prove. The first reconciliation with a promoted leader truncates its range to
  the base. Repair therefore populates a directory and hands an operator the
  bytes; it does not yet bring a replica back into service. This is the
  boundary of what "replacement" means in this release.
- Only sealed segments transfer, so a gap remains in the leader's active
  segment that the retransmission buffer may be unable to replay (#306).
- Replacement is **operator-driven**. A follower learns only that it was
  refused; the leader is what knows its retransmission buffer no longer covers
  the gap. Where that signal belongs is a separate design question, deliberately
  not smuggled into a CLI command.
- The leader's replica set is **static configuration**. Retiring a replica in
  metadata does not stop the leader replicating to it, and adding one does not
  start it — the leader must be restarted with the new set.
- Sealed-segment transfer is authorized against the leader's followers plus an
  explicit `transfer_peers` list. A replacement must be named there for the
  duration of its repair, and removed after.
- Roll thresholds apply to ranges **as they are created**. A directory that
  already exists keeps the thresholds written into its segment headers, so
  changing them in a node's config has no effect on it. Reconfiguring an
  existing range is #314.

## v0.3.1 — the papercuts this release exposed

- **#310** — a `kill -9` during a commit write leaves a quarantined `.tmp` and
  the node refuses to restart. The losing side of a rename is garbage by
  definition and should be swept, not judged.
- **#315** — the repair carries the epoch history with the records. Sealed
  segments moved but their lineage did not, so the first leader transition
  truncated a repaired replica back to zero. `vtopctl node repair` now
  installs the source's `fencing-epochs` (truncated to the sealed prefix)
  alongside the transfer — the same trust replication already extends, since
  a follower's journal is leader-derived too — and a replica holding records
  with no history now answers "unknown" at a fence instead of fabricating a
  lone entry that compared as divergence-at-zero. Scenario 12 asserts the
  repair outlives the failover instead of pinning the loss.
- **#290** — a range no longer grows until the disk does not. A size-bounded
  retention policy (`retention.max_total_bytes` in node config) reclaims whole
  sealed segments oldest-first under a durable intent marker — the #276
  pattern, so an interrupted reclamation finishes instead of quarantining the
  range — and never past the acknowledged floor. A consumer at a reclaimed
  offset gets a nameable `OffsetRetained` refusal instead of a silent skip
  forward, and a transfer chunk for a reclaimed segment fails closed as
  `WrongLineage` (#278's re-resolution, now asserted for retention). Age
  bounds need a durable per-segment seal time no manifest records yet — a
  format change deferred to its own slice.
- **#280** — every orderly stop is no longer a crash stop. vtop-node handles
  SIGTERM/SIGINT: the listeners drain, a departing leader RELEASES its range
  lease (the `ReleaseRangeLease` verb existed; nothing in production called
  it) so failover starts immediately instead of waiting out the lease
  deadline, and the final commit boundary is written so the next open has no
  torn tail to truncate. The helm chart's terminationGracePeriodSeconds now
  means what its name says. Scenario 13 proves the drain on real processes
  and fails if SIGTERM is ever ignored again. Durability still never depends
  on the clean path — the k8s smoke test keeps force-deleting pods to prove
  it.
- **#291** — an upgrade is no longer an untested assumption. CI derives the
  newest patch release of every minor series since the native data plane
  (v0.2+; the v0.1.x CLI predates the surface the script drives) and proves
  each one's data directory opens under the current build — today that is
  v0.2.1's single `range.active` (the exact compatibility claim `open_range`
  makes) and v0.3.0's rolled sealed segments; each opens, serves every
  record byte-exactly, and still serves after an orderly stop and reopen
  (`scripts/upgrade-compat.sh`). New tags join the matrix the moment they
  exist. The old nodes are stopped with SIGKILL —
  deliberately, because that is the stop every pre-#280 release performs.
  Downgrade is NOT supported: stated here as a decision, not left as an
  untested assumption. A directory written by release N+1 may hold artifacts
  release N cannot interpret (retention markers, epoch journals with
  adoption semantics N predates), and no test defends the reverse path.
- **#554** — the metadata Raft tier had no mixed-version story while the chart
  rolled it. A command kind a voter does not know fails to decode, and with it
  the whole AppendEntries batch: an upgraded leader that proposed one left the
  old voters unable to append anything more, and an old node restarted on the
  newer log called it corrupt. Now each command kind carries a **feature
  level** (the release that introduced it), and the cluster's level lives in
  replicated state (absent means v0.6.0's level). Every node refuses, before
  replication, any command above the level it has applied - a leader with a
  reason saying how to proceed, a follower by sending the caller to the
  leader - except the level-setting command itself, which is how the level
  rises;
  `vtopctl meta enable-features --level N` raises it - monotonically, never
  down - once every voter runs a release that knows N. A checksum-valid entry
  or record of an unknown kind is reported as written by a newer release, not
  as corruption, and a voter receiving one says so on stderr instead of
  dropping the connection silently. Scenario 22 rolls three voters from the
  previous release to this one, one at a time, requiring a commit through the
  mixed tier and every voter to converge at each step, then raises the level
  and restarts a voter on it; CI runs it against the newest release tag.
- **#553** - a range's quorum was sized from each data node's own
  configuration (`followers` + 1), and metadata held no member set, so
  growing a set by rolling configs could leave two nodes counting majorities
  of different sets with no node in common: from {A,B,C} to {A..E}, a node
  on the new config could promote on {A,D,E} over records acknowledged on
  {B,C}. Metadata now records a range's replica set (`vtopctl meta
  set-replica-set`, feature level 5), and a candidate whose configured set
  differs refuses to promote and stands aside. A change adds or removes ONE
  member - consecutive sets' majorities then always intersect - clears the
  lease so the range is re-won under the new set, and is refused until a
  promotion under the set it replaces shows, from its recorded quorum, the
  leader's whole log on a majority of that set: the election restriction
  makes the new leader hold everything acknowledged under the set before,
  but promotion truncates nothing and the part above the boundary may still
  sit on that older majority alone. A range with no record behaves as
  before. An older metadata node refuses the read outright; the CLIENT reads
  that refusal as "none recorded" only after the same node's status says it
  is the leader (an older leader cannot hold a set), and otherwise asks the
  leader it names - so data nodes may be upgraded ahead of the metadata
  tier without one stale endpoint switching the check off.
- **#577** - a promotion's quorum answers were bare offsets. The fence
  reconciles each replica against the candidate's epoch vector before it
  answers, but where lineage cannot be proven (a replica with records and no
  epoch history, a broken journal, a candidate with no history of its own)
  it reconciles nothing, and that replica's offset was still counted - toward
  the majority, the boundary, the §5.4.1 vote and the replica-set gate -
  whatever leadership wrote its records. The candidate now judges each
  answer from the history the fence returned: Agreed, Reconciled (it
  discarded n records first) or Unknown, and only the first two count,
  even when nothing else is available - falling back to offsets then would
  be a fence that fails open exactly when needed. A replica that cannot
  prove lineage is repaired from one that can (`vtopctl node repair`, #315),
  not promoted on a guess. A candidate with no history stands aside for a
  replica that has records: a candidate holding records it cannot vouch
  for always did, and one holding none now does too (#612). The costs
  (#612): a range that holds records but where NO replica has epoch
  history (records that predate the journal on every replica, or journals
  lost on every replica) cannot promote, and repair cannot fix it, since a
  history-less source has nothing to install; and a
  replica whose only dispute is below its retained base (#408) is not
  counted. The verdicts are recorded with the quorum and the replica-set gate
  and `vtopctl meta transitions` count only the proven answers. Recording
  them is a new outcome tag inside the promotion report, so it is gated at
  feature level 6: below it (or against a metadata tier that predates
  levels) a data node records the report without verdicts AND without the
  Unknown answers, which that form's readers would count by offset. Records
  written before verdicts carry none and are judged exactly as before.
- **#611** - the fence discarded acknowledged records before the vote was
  read. A candidate adopts the new epoch at its own tail before it sends its
  history, so a replica holding records the candidate never received compared
  the two as diverged at the candidate's tail and truncated down to it,
  provided its own high-water mark was at or below that. In a three-replica
  range a record the dead leader and that replica had acknowledged was lost,
  and the replica then voted for the candidate that lacked it. A fence now discards a suffix only
  when the candidate holds a record at the divergence point written under an
  epoch newer than every record discarded, which is proof that none of them
  was acknowledged. Otherwise the replica keeps the records, answers with its
  full offset, the candidate counts that answer, and the election restriction
  refuses the candidate and names the replica as the one to lead. A replica
  that kept such a suffix and was outvoted discards it on the established
  leader's first append or committed high-water mark past its start,
  instead of acking a record it does not hold (#261) or exposing the kept
  records as committed. Until it is settled, that dispute caps the replica's
  acks and reconnect status at its start. A replica absent from the fence,
  such as a deposed leader that missed the election, is no longer left
  unreconciled (#641). Once a leader is attached to its replica set, every
  session it opens with a follower starts with the probe's own fence frame,
  at the leader's epoch and with its history, before the status read and
  before any record or mark. A session opened before the attachment is
  dropped before it carries a record or a mark. The fence reconciles the
  returning replica exactly as it reconciles a voter. Histories that agree
  leave the log as it is. A suffix the leader provably supersedes is
  truncated at once. A suffix the leader cannot yet prove superseded is kept
  in dispute, from the divergence (or, below the replica's high-water mark,
  from past both the mark and the leader's tail), and settled as above; the
  replica then catches up and never reports a disputed suffix as held, takes
  it under the new mark, or acks the leader's records over it. Nothing at or
  below a replica's own mark is touched. A replica that holds records but
  whose own history cannot be compared (no journal, or a broken one) gets no
  session at all (#649 review): the leader cannot tell which of its records
  match, so it sends it nothing and takes no ack from it, leaves its log as
  it is, and reports that it needs `vtopctl node repair`. Its answers never
  count toward a promotion either. An empty replica with no history is not
  in that position: it holds nothing to dispute, the candidate judges its
  answer Agreed, and it counts. A leader with no history of its own cannot
  reconcile anyone; its fences leave every log as it is. Plaintext planes
  skip the session fence, since they refuse fences and do not fail over.
  **The protection is the fenced replica's, so it holds only on upgraded
  replicas.** There is no wire or feature-level change and the fence frames
  are identical, so nothing tells a candidate which replicas run this
  release. A replica still on an older release truncates exactly as before,
  during its own fence and before any candidate judges its answer. Until
  every data node of a range runs this release, a failover can still lose an
  acknowledged write that way. PRODUCTION_HA's upgrade procedure says how to
  keep the rolling upgrade itself from triggering it. An older candidate
  facing an upgraded replica judges the kept answer refused (its offset is
  higher) or Unknown, never a grant.
- **#635** - the #611 dispute lived in memory only. A replica restarted
  between the fence reply that kept the suffix and its settlement came back
  holding the old records with nothing marking them, reported its full log,
  and acked the new leader's record at the disputed offset as a duplicate of
  a different one (#261). The dispute (fence epoch, first disputed offset,
  and, when this replica's history can say, the epoch that owned that
  offset) is now written beside the fencing-epoch journal, as
  `fencing-epochs.kept-dispute`, and fsynced before the fence replies. Every
  retirement, whether by settlement, truncation, a newer fence or the node's
  own fence as a candidate, clears it durably before anything is acked past
  it. A restart restores it, so it caps acks and status and is settled
  exactly as before. A record the log has provably moved past, or one that
  starts below the replica's committed mark or below the caller's tail its
  fence bounded it by, is discarded at open, never settled over the new
  leader's records. A lease adoption of a newer epoch is not such evidence:
  adoption alone retires nothing. A dispute is retired only by its
  settlement, a truncation past its start, a newer fence, or the node's own
  fence as a candidate, and each of these clears the record.
  The record holds two checksummed frames, so a crash while it changes
  reads back the old state or the new one, never neither. **Older releases refuse the
  directory while a dispute may be pending:** the journal's header is
  version 2 from before the record is written until after it is cleared,
  and every earlier release opens only version 1, so a downgrade cannot
  silently serve the suffix. A crash between clearing the record and
  lowering the header leaves the header at 2 until this release next opens
  the directory, which lowers it. A crash sweep
  (`kept_dispute_crash_sweep.rs`) cuts the fence and the settlement at every
  storage operation and every byte of every write.
- **#640** - a follower filed the records a new leader caught it up on under
  the wrong epoch. It recorded an epoch it adopted at its own tail, so when
  it was behind, the records the new leader then sent it were filed under the
  new epoch, while the leader filed the same bytes under the epoch that wrote
  them. The two histories diverged below both tails, every later fence answer
  between them was Unknown (#577), and after a second failover no candidate
  could be established, though no acknowledged record was lost. The fence
  that installs a leader already carries that leader's history; a fenced
  follower whose own history agrees with it below its tail now keeps it (in a
  `fencing-epochs.leader` sidecar, so a restart keeps it too) and files every
  record that leader sends under the leader's entries. After a catch-up its
  vector equals the leader's for the caught-up range (after a partial
  catch-up, only that far). A crash between an append and that filing is
  repaired when the replica next opens, before it answers anything, provided
  the `fencing-epochs.leader` sidecar survived intact; a missing or torn
  sidecar reads as "no lineage" and repairs nothing, leaving those records
  filed as before #640 (an Unknown answer later, never a counted one). A
  follower that kept a dispute (#611) holds the fence's history in the same
  sidecar, as #635 holds the dispute, and takes it only once the dispute
  settles, at the tail the settlement leaves; a newer fence or the node's
  own candidate fence drops it, as they retire the dispute. A follower the
  fence did not reach adopts at its tail as before.
  **Compatibility:** the lineage changes nothing in the journal's own format
  (its header gate is #635's), so an older binary that may open it reads it
  as its own and simply resumes adopting at its tail. The sidecar is
  a name older binaries never open, and this release reads a missing, torn or
  newer-version sidecar as "no lineage", the old behaviour. **Journals
  mislabelled by an earlier release are not rewritten**: nothing on a replica
  tells a record it was caught up on under an epoch from one that epoch's
  leader wrote, so such a replica's answer stays Unknown against a correctly
  labelled history (and is never counted) until it is repaired with `vtopctl
  node repair` from a replica whose history is right (#315). No wire or
  feature-level change: an older follower keeps mislabelling until upgraded,
  which costs elections availability, never an acknowledged record.
- **#634** - a data node's release was invisible to the cluster, so a
  feature level was raised on the metadata tier's word alone, and the
  replica plane had no version on the wire. Data nodes now report
  `(release, max replica protocol, supported feature level)` to metadata (a
  new command kind, feature level 7) from startup until it is recorded, and
  `vtopctl meta node-versions` lists the reports. A raise above level 7 is
  refused while any counted node has not reported that level or higher: a
  lease holder, a member of a recorded replica set, or any node that has
  reported, unless marked dead. The refusal names the nodes. The CLI and the state machine
  both judge the raise, so the check and the raise cannot race. The replica session now
  opens with a version exchange carried in the hello frames every older
  peer already decodes. A peer before the handshake answers it with a typed
  refusal and is spoken to in the old protocol, and both sides use the lower
  version. A peer below the finalized level is refused on both ends, and
  one before the handshake is refused once the level is above 7. Limits: no
  release supports a level above 7 yet, so the gate and the replica-plane
  refusal have nothing to refuse until one does. They are exercised in tests
  as that release will run them. A node is reported once per process, not on
  every renewal, because the renewals share one Raft queue and an unchanged
  report carries nothing. A data node with no metadata configured
  holds no peer to a level. A data-node follower that metadata cannot name, because it
  never reported and no recorded replica set lists it, does not hold a raise
  back; after the raise the handshake refuses it if it is behind.

One observation rode along without becoming a fix: scenario 09 once left a
promotion unacquired within its deadline (#318). The flake has not recurred —
twenty clean runs on the fix branch, plus every CI run since — so the release
ships the diagnosis kit instead of a guess: `await_lease_holder` now reports
what it observed (reads attempted, epochs answered, holders seen) whenever it
gives up, so the next occurrence names its cause. The issue stays open in
v0.4.0 as a watch item.

## v0.4.0 — the failures a healthy lab never shows

The theme is **failover you do not have to babysit**. Every headline item in
this release was found by running the thing — a pod that came back on a new
address, a quorum that blinked at the wrong moment, a winner that could not
serve what it won — rather than by reading the design.

- **#367 / #374** — a candidate that won the lease but could not build its
  leader re-won it forever, starving healthy survivors. Root cause: a peer's
  address was resolved once at boot and believed forever, so a recreated pod
  at a new address was unreachable one-way. Addresses are now re-resolved, a
  name not yet published is a retry rather than a fatal startup error, and
  two pieces of hardening ride alongside: a candidate that cannot build its
  leader stands down (#368), and a fail-stop hands the range back before
  exit (#371).
- **#375 / #380** — a quorum miss raised the fencing epoch, which is what
  stopped the followers catching up: each retry was refused at an epoch one
  higher than the one it had nearly reached. A refusal for **eligibility**
  still lapses the lease on purpose; a refusal because the **quorum did not
  answer** now renews it and holds the epoch still — bounded to one lease
  lifetime per epoch, latched once spent so a backwards clock step cannot
  reopen the window.
- **#240, first half (#342)** — the election restriction. Promotion now
  requires a majority of the answering fenced replicas to sit at or below
  the candidate's own committed offset — Raft dissertation §5.4.1 in its
  per-voter form, with the majority size derived from the replication
  factor, never from how many happened to answer. Three tests pin it,
  including the case the floor check alone would wave through. The second
  half — the signed leadership-transition record, proving who led rather
  than restricting who may — remains open by design: #265 was built and
  withdrawn for not closing its hazard, and that lesson is why this half is
  not being hurried.
- **#284 (#343 / #344)** — candidate mode. The data node's role follows the
  metadata lease inside the binary — every pod a candidate, an election
  deciding who leads, failover in place with no restart and no re-render.
  Scenario 14 proves the composition on real processes, and the chart's
  `replicated` topology renders three identical leased candidates
  (`data.leaderOrdinal` is retired; setting it now explains the migration
  instead of steering roles).
- **#306 (#341)** — a leader seals its tail on demand (`vtopctl node repair
  --seal-tail`), so repair reaches the leader's whole position instead of
  stopping at the last segment roll. This was v0.3.0's sharpest known
  limitation; with #315 (v0.3.1) it closes the road back for a lost
  replica end to end.
- **#314 (#339)** — roll thresholds change on a range that already exists,
  by rolling once (`vtopctl node reconfigure-range`). The thread catalogued
  six interacting cases proving the feature wanted a design, not a patch.
- **#318** — closed on a 60-run soak: the promotion-never-acquired flake
  was fixed incidentally by #315/#280, zero recurrences of the signature in
  59 clean runs (the one failure under load was a different failure, filed
  as #340). #324's `await_lease_holder` diagnostics stand by to
  self-diagnose any recurrence.
- **#326 (#327)** — the suite's deadline-poll doctrine applied to the one
  read that violated it: scenario 12's one-shot `get-placement` now polls
  to a deadline and absorbs a momentary ReadIndex quorum lapse.
- **#81 (#338)** — the lab compose runs under a hardening baseline:
  loopback-bound published ports, dropped capabilities, read-only roots,
  segmented networks per plane.
- **#87 (#362)** — the batch pipeline's read/flush overlap, which had
  quietly shipped inside earlier work, is now pinned by tests and
  documented — so it cannot un-ship silently.
- **#289 (#309) / #295 (#297)** — CI builds the engine image once instead
  of three times, and the image carries `curl`, because nothing else in it
  could read `/readyz`.
- **#376 (#377)** — two globs matching one file archived it twice; the file
  source deduplicates across overlapping patterns. Whether two directory
  entries for one **inode** are one source or two is deliberately not
  answered here — #378 records the question.
- **#361, detection half (#382)** — a Kafka cursor overtaken by the
  partition's low watermark resumed at the new earliest offset and the gap
  was never mentioned. The source now names the records retention took —
  count, partition, resume point — measured against the broker's low
  watermark and nothing else, because offset jumps are not evidence.
  Whether the adapter should *refuse* rather than resume is a deployment
  decision the issue keeps open.
- **#92 (#383)** — `docs/THROUGHPUT_RESEARCH.md`: what 1M records/sec would
  take, what the compression measurements actually show, and what nobody
  has shown — no end-to-end rate demonstration exists, and the document
  says so instead of implying otherwise. The sweep grid (#130) and
  sustained-backpressure soak (#98) that would produce those numbers moved
  to v0.5.0: they inform tuning, not shipped behavior.

A documentation truth pass (#379) rode with the release: three status
markers the code disproved are fixed, and the docs now state which
conformance gap the engine has never claimed to close.

### Known limitations in v0.4.0

- **Scenario 09's post-failover produce can miss quorum under heavy I/O
  load (#340).** Open as a watch item — it needs the 30-run adversarial
  soak that produced it, and the late-vs-never fork is answered *never*
  from code: uneven follower offsets at the kill make in-place repair
  impossible. (It was briefly closed by GitHub's keyword parser matching
  "fix #340" inside a sentence that said the opposite; reopened.)
- Retention loss on the Kafka plane is **detected, not refused** (#361) —
  and the count is a log line, not yet a field in the read report.
- The Kafka wire-compatibility gateway is **not in this release**: its
  protocol foundation (PR #350) lands at the start of v0.5.0 (#225).
- The chart still deploys co-located nodes only (#287), and TLS remains a
  hard dependency of some planes rather than a configured capability
  (#294).

## v0.5.0 — compatibility, and the numbers behind the claims

The theme is **speaking a protocol the ecosystem already speaks, without
giving up what the native plane proves**. Every item shipped as bounded
slices behind bot review; the ones that were measurements rather than
behavior were run, posted with their caveats, and closed on what the numbers
said.

- **The Kafka wire-compatibility gateway, phase 1 (#225).** `vtop-kafka`
  speaks ApiVersions, Metadata, Produce, Fetch and ListOffsets (LATEST and
  EARLIEST) over a `Bridge` seam whose native backend appends onto
  `LocalBroker` under a minted producer epoch and one sequence space (#350
  laid the wire codec; #447 the listener, the bridge and the native
  backend). A leader or standalone serves it beside its native plane from a
  `kafka:` block (#452): the producer identity is derived from the
  principal and never equal to it, the gateway drains and is joined before
  the lease is released, a listener failure ends the node, and a stock
  client's default 10,000-record batch is appended in as many native
  appends as the replica plane can frame. The acceptance is live-chaos
  scenario 17 (#455): librdkafka produces 20,000 keyed records with
  `acks=all` through the gateway on a leader with two followers as one
  stream, a follower is SIGKILLed while that stream is provably mid-flight,
  the same process finishes clean, the same client reads everything back
  byte-exact, and the sealed segments verify. It runs in CI on every push.
- **TLS as a configured capability (#294).** Every plane's transport is a
  knob — `tls`, `plaintext`, `plaintext_on_any_interface` — with TLS the
  default and plaintext a choice the config makes you name (#436, #432); a
  plaintext plane off loopback, a plaintext admin plane promoted into a
  leased cluster, and an admin policy the transport cannot enforce are
  refused by name; a peer speaking the other transport is refused as
  cross-mode rather than as a bare handshake failure; the observability
  endpoint serves TLS, mutual when a client CA is given (#443); scenario 15
  boots a whole cluster on loopback without a certificate (#445); and the
  chart carries the knobs with `acknowledgePlaintext` as the named choice
  (#450).
- **A chart with two tiers (#287).** `deployment.mode: separated` renders a
  metadata tier and a data tier that scale apart (#390, #431), and the kind
  smoke runs its replicated pass twice, co-located and then separated
  (#434).
- **The failover recovery protocol, completed (#240).** The acknowledged
  floor survives restarts (#402); verify names records by position (#415);
  the boundary marker exists, publishes on proof and hides from consumers
  (#417, #428); every grant leaves a signed transition record the holder
  fills in (#430, #437 for the chart's key); and `vtopctl meta transitions`
  audits a range's leadership chain against the identity asked for, each
  replica's epoch vector held to it (#438 and its follow-up). The one item
  that is a design decision rather than a slice is #449.
- **The adaptive upload side (#102).** A throttle is told apart from every
  other upload failure (#433) and the upload width follows the store's
  throttles by AIMD (#441, `batching.adaptive_width`, off by default). The
  latency term stayed out on purpose: the throttle signal is unambiguous
  and the latency one is not.
- **Two directory entries for one inode are two sources (#378).** Ruled, not
  knobbed: no cursor identity survives one alias being removed safely, so
  there is no merge; a grown alias group warns again, naming what changed
  (#398).
- **Bandwidth-shaped runs (#403).** A thin pipe is a fault the harness can
  inject — the broker's appends released by a token bucket (#440) — and the
  benchmark lab shapes the upload plane through toxiproxy (#446): the proxy
  is claimed atomically, the toxics are named per run, and the shape rides
  every result view so a p95 is never read without the pipe it was
  measured through.

### What the measurements said

Both measurement issues ran on a busy laptop under Docker Desktop, not the
quiet machine they asked for; the **shapes** below are stable across cells
and the **absolute numbers** are not quotable.

- **The sweep grid (#130)**: 112 cells, zero failures. zstd:1 and zstd:3
  dominate on throughput per CPU at the same ratio band as gzip:6, which
  costs 2–4× the CPU; gzip:9 and zstd:9 buy almost no ratio; and throughput
  is flat across codecs, which points the next lever at the pipeline's
  serialisation rather than the codec. The topic-count dimension runs 1, 8,
  28 and 100 topics: the read phase is the poll window at every count (253
  to 255 ms), so topic count costs about one percent at 100 — the
  pre-#96 "N topics cost N poll waits" hypothesis is falsified. Getting
  that row took three harness fixes (#456).
- **The sustained-backpressure soak (#98)**: five minutes at 200k rec/s
  against an engine reading ≈6k. Buffers stayed bounded (the gauge counts
  open per-partition buffers, not a queue), RSS plateaued near 125 MB, the
  ledger's committed history is pruned (114 KB after 147 batches; the WAL
  is checkpointed), `commits_total ≤ verified_total` held at every sample,
  and retention loss is **counted and warned**, not silent:
  `vtop_retention_lost_records_total` recorded 27,582,996 records when the
  broker's retention outran the engine. What is missing is the alert on
  that counter (#454). 1M rec/s remains undemonstrated on this hardware for
  a lab-host reason: Docker Desktop's embedded DNS fails under that load.

### Known limitations in v0.5.0

- The gateway is **phase 1**: no consumer groups, no idempotent producers
  (InitProducerId is refused by name, so a retry after a timeout can
  duplicate), no transactions; one partition per range; a set larger than
  one native append is appended in order and a failure between appends
  leaves the prefix durable. Phase 2 is #457, phase 3 is #458.
- **The replica plane refuses an oversized frame silently** (#453): a
  follower drops an append above `DEFAULT_MAX_RECORDS` at decode and the
  leader reports a quorum that never arrives. The gateway splits around it;
  the plane should refuse by name.
- **A published high-water mark may still regress across a leader change**
  (#449): two majority-sized answer sets need share only one member, and
  nothing yet holds a promotion's published watermark to the previous one.
- **Retention loss is counted but not on a dashboard** (#454).
- **The kind smoke flakes on cluster DNS under a loaded runner** (#416):
  one-shot deadlines meeting a slow runner, passing on rerun.
- **The benchmark numbers carry the busy-laptop caveat** stated above; a
  quiet-machine run would tighten them, not change their shape.

## v0.6.0 — the gateway's second phase, and the evidence gaps the first one exposed

The theme is **a stock Kafka client can join a group, produce idempotently,
and migrate a topic with a receipt that survives restart**, without giving
up what the native plane already proved. The evidence-gap items that
landed on `main` after v0.5.0 ride this tag too.

- **Consumer groups and idempotent producers (#457).** InitProducerId mints
  an id; a retried batch appends once. A topology-elected coordinator
  (Java `String.hashCode` then Kafka's abs, by partition index, not peer
  order) covers every partition and stores commits on ranges it does not
  lead, fenced by the lease it does hold. Each gateway serves exactly the
  partition it leads and advertises the rest, so a produce or fetch for
  another is refused by name. Live-chaos scenario 21: two kcat members,
  one partition each, byte-exact reads.
- **Per-topic virtualization with verifiable receipts (#458).** `kafka.topics`
  binds each Kafka name to native storage, an external cluster, or a
  dual-write / shadow-read pair. A dual-write lands on the primary first;
  a shadow failure is `KAFKA_STORAGE_ERROR`, never an ack for a one-sided
  write. Shadow-reads compare key, timestamp, headers and value.
  Cutover translates committed offsets through a JSONL receipt that
  survives restart; `vtopctl receipt` prints it. Kafka-only catalog names
  refuse OffsetCommit rather than a process-local cursor.
- **An append the replica plane cannot frame is refused at the leader
  (#453)**, before the append, instead of a quorum that never arrives.
- **A published high-water mark never regresses across a leader change
  (#449):** a promotion's boundary is held to a watermark the range
  already published.
- **Retention loss is on the dashboard and alerts (#454).**
- **The kind suite waits on conditions rather than one-shot deadlines
  (#416)**, and a lease-read error is named rather than swallowed (#469).
- **A node that voted for the loser still records the elected leader's
  committed vote (#465)**, so hard-state cannot wedge the metadata plane.

### Known limitations in v0.6.0

- **One gateway still serves one range.** Duplicate `node_id` is refused
  even when endpoints agree. Multi-partition-per-broker is range-split —
  #473 / v0.7.0.
- **Transactions are out of scope.** InitProducerId with a transactional
  id is refused by name.
- **RemoteBridge virtualizes one log per Kafka name** and always speaks
  remote partition 0. Another partition is refused at this gateway.
- **A sequenced remote produce the client never saw acked can still
  duplicate across a gateway restart**: remote producer identity is
  process-local. In-process retries are rebased and reminted.
- **The benchmark numbers still carry the busy-laptop caveat** from
  v0.5.0; a quiet-machine run would tighten them, not change their shape.

## v0.7.0 — an engine that keeps its word across restarts, deadlines and stops

The roadmap above promised range splits for this tag; the first slice of
that landed, but most of what `main` gathered after v0.6.0 is the archive
engine's own failure modes, found by asking what happens at each edge and
fixing the answer. The theme is **the engine never claims more than it
did**: a stop, a rotation, a lost claim, a throttle or a hung call each end
in a state replay can explain.

- **A range splits at a barrier its leaseholder names (#473, slice 1).**
  `SplitRange` on the metadata plane, fenced like a sealed-segment
  registration, into children the formats already expected. One broker
  serving several Kafka partitions is not in this tag.
- **The archive engine's object-store and state-store calls have deadlines
  (#556)**, including the upload body, so a stalled endpoint cannot hold a
  batch forever. `vtopctl migrate` is not covered: its Postgres connection
  and schema statements are still unbounded.
- **A stop signal that arrives mid-cycle is honoured (#526)**, and SIGTERM
  flushes like Ctrl-C.
- **A failed batch hands its range back to the source (#552)**; recovery
  resumes from the lowest incomplete range.
- **A rotated or truncated file is read from byte zero (#558)**, and a
  restart knows which file its offset belongs to.
- **A claim generation fences an engine that lost its claim (#557).**
- **The time partition is the seal time, or event time on request (#559).**
- **An archive coverage audit, and a prune that keeps its anchor (#560).**
- **On the native S3 backend, a large object uploads as resumable
  multipart (#482)**, and the verification gap that path walked into is
  closed. The command backends (AWS CLI, s3cmd, `mc`) and LocalFS still
  hand the object over in one call, with no resume.
- **A store throttle during a command backend's read-back or verify is a
  throttle (#529)**, not a failure, on every backend.
- **Upload transports are a trait and a registry (#479, #480)**, one tuning
  shape for both paths, with the scheme policy proved at every door (#488).
- **Metadata plane:** a lease request from a skewed proposer is refused
  rather than trusted (#555); a feature level gates new command kinds so a
  rolling upgrade cannot stall the tier (#554); each range records its
  replica set and counts quorums only over it (#553).
- **Observability:** blocked retention is visible and replication health
  alerts (#561); per-group cursors sit beside the log start, with alerts
  before a group is evicted (#571).
- **Helm refuses an even replicated data tier (#562)** — chart 0.5.0.
- **Benchmarks:** a long-lived engine with its own metrics scraped, so a
  rate reads per second (#510); what an upload costs the flow beside it,
  measured (#478); a netem link that bites, with queues somebody sized
  (#477, #516).
- **Supply chain:** the November 2026 advisory wave fixed by lockfile
  bumps where a fix existed, documented ignores where none did.

### Known limitations in v0.7.0

- **One gateway still serves one range.** Range split's plane slice is in;
  serving the children as separate Kafka partitions is not.
- **The operator egress ceiling (#481) is not in this tag**; it waits on
  its competitor-goodput measurement.
- **The Kafka gateway is far slower than Apache Kafka on the same machine.**
  The first same-client comparison (#596) measured produce throughput
  about 12–14x below a 3-broker Kafka at `acks=all`, and the gateway lacks
  CreateTopics, DescribeConfigs, OffsetCommit without a metadata plane and
  zstd. Failover and crash-durability comparisons (#597, #599) are not run.
- **Transactions remain out of scope**, and the v0.6.0 RemoteBridge
  limitations still hold.

## v0.8.0 — the engine proves what it archived, and the cluster survives its own failovers

Most of what `main` gathered after v0.7.0 is the same work at two layers:
find the edge where a claim could outrun its evidence, and close it with a
test that failed first.

- **Archive engine.**
  - A range whose source has already committed past it is retired as
    SUPERSEDED, but only on positive proof of the file's identity (#567).
  - A VERIFIED row left uncommitted is settled by a later pass, not only at
    the next restart (#624).
  - A failed range from a rotated-away file is re-read from that file (#621).
  - A range whose replay decision can't be recorded holds back the VERIFIED
    ranges above it (#639).
  - `delete_after_commit` waits until a file stops being written (#619).
  - The Kafka source resumes from the ledger (#614), and a stalled partition
    is re-assigned instead of going silent (#623).
  - Committed objects can be re-read and checked with `vtopctl scrub` (#628).
  - Size-only verification is announced at startup, in `validate` and in
    recovery (#629).
- **Upload.**
  - A request body's deadline grows with its size, so a policed uplink
    commits instead of timing out (#663).
  - An operator egress ceiling (#481).
  - Each stream may choose its checksum algorithm, with
    `require_strong_verification` as a floor (#669).
  - `vtopctl validate --print-effective` shows what each stream runs with
    (#670).
  - The #483 gate kept TCP as the only egress path (#475 is closed).
- **Manifests (#671, release A).** Readers check the version first and
  verify `0.4` over the stored document. Writers still write `0.3`.
- **Cluster plane.**
  - A follower files caught-up records under the epoch that wrote them (#640).
  - A returning deposed leader is reconciled at session start (#641).
  - A fence holds the meta lock through its outcome (#660).
  - A snapshot never holds a record it can't read back (#657).
  - Data nodes report their release, and a feature level is finalized only
    when every node can run it (#634).
  - The promotion crash sweep runs with no known issue open.
- **Measured.**
  - Failover under load against Apache Kafka (#597, partly).
  - The Kafka comparison's charts are regenerated on CI with no drift (#600).

**Upgrading from v0.7.0.** Downgrade is not supported.

- **Upgrade every engine and data node first, then raise the feature level**
  with `vtopctl node-versions` and `vtopctl enable-features`. Level 7
  turns node version reports on and is judged by the metadata tier alone;
  every level above it is finalized only when every node reports it can run
  it (#634).
- **One-way changes:**
  - Once a SUPERSEDED row is written, the state store carries schema marker
    3 and v0.7.0 refuses it (#567).
  - An older binary refuses to start while a kept dispute is pending (#635).
  - Fencing journal and sidecar rewrites stage through `<file>.rewrite` and
    a rename (#640).
- **Older engines silently ignore new keys:**
  - `upload.request_min_bytes_per_second` (#663);
  - `upload.max_egress_bytes_per_second` (#481);
  - a stream's `checksum` block (#669).

  Upgrade every engine before relying on them, and keep the global checksum
  algorithm strong during a rolling upgrade.
- **Known open:**
  - #675: a Kafka stall-recovery test failed once in CI.
  - #626: replay object keys.
  - #664: the verification class of recovered commits.
  - #668: resuming a failed multipart upload.
  - #671 release B.

## v0.9.0 — bug fixes: deletes that wait for proof, Kafka partitions that are never left unread, portable builds

A bug-fix release: only issues labelled `bug`, and the PRs that fix them. Features wait for v0.10.0.

- **File source: `delete_after_commit` deletes only on proof (#619, #689).**
  - On Linux the engine takes a write lease before moving a file aside, so a file that any process still holds open is kept. The lease is trusted only on local filesystems.
  - Identity goes through `continuity::proves`.
  - A change of mtime or ctime keeps the file.
  - Symlinks are never deleted, and a failed rename keeps the file.
  - A file a crash left under its hidden name is archived, then deleted.
  - Every decision is logged and counted in `vtop_file_delete_decisions_total{outcome}`.
- **Kafka source: every partition-metadata condition handled (#675, #687).**
  - A topic listed with no partitions is asked again next pass, not cached.
  - A failed lookup serves the last known list.
  - A topic that grows is noticed every cycle.
  - A deleted topic no longer stalls the others.
  - A recreated or truncated topic is reported.
  - All of these are counted in `vtop_kafka_partition_events_total{event}`.
- **Recovery counts by the stored verification class (#664, #691).** A VERIFIED row records whether it was verified strongly or by size only. Recovery and the settle pass count from that record, in `vtop_recovery_rechecks_total`. Engines and `vtopctl migrate` refuse a state-store schema newer than they support.
- **Replays reuse the object already archived (#626, partly, #688).** Object keys are content-addressed within a source unit's range. A replay of the same bytes reuses the stored object, and the engine never overwrites one that does not verify. Decisions are counted in `vtop_object_key_decisions_total{decision}`.
- **Portable builds and binaries (#677, #679).**
  - OpenSSL is compiled in.
  - Cyrus SASL is only needed for the opt-in `kafka-gssapi` feature.
  - macOS binaries target macOS 11 and load only system libraries; the release checks this.
- **Docs.**
  - A documentation site at https://allamiro.github.io/vtop-engine/ (#692).
  - A README with charts drawn from committed measurements (#690).
  - Native prerequisites for Debian/Ubuntu, RHEL/AlmaLinux/Rocky 8–10, Fedora and macOS (#678).
  - CITATION.cff (#686).
  - Issue forms and a PR template (#682).

**Upgrading from v0.8.0.** Downgrade is not supported.

- **`delete_after_commit` keeps files it can't prove are closed.** That covers macOS, NFS or FUSE, another user's file, and any container without `CAP_LEASE`. **The shipped Helm chart runs as uid 10001 with all capabilities dropped, so with `delete_after_commit` it now keeps files**: watch disk use, and the `vtop_file_delete_decisions_total{outcome}` counter. `delete_without_open_check: true` restores deletion where no lease is possible. It brings back the two windows a writer can lose lines in, and the engine warns about it at startup and in `vtopctl validate`.
- **PostgreSQL: run `vtopctl migrate` before starting v0.9.0 engines.** It adds the nullable `verification_class` column. The schema marker stays 3, so v0.8.0 engines keep working against the migrated database. **Restart any v0.8.0 engine that was running during the migration**: until its connections are replaced, its whole-row reads (startup recovery and the settle pass) fail. SQLite migrates on connect and now records `PRAGMA user_version = 1`.
- **Kafka with `auto.offset.reset: latest`:** a partition or topic that appears while the engine runs is now read from the group's commit, or else its beginning. Before, it could start at the end and skip what was written before the engine noticed it.
- **Kerberos (SASL GSSAPI) for the Kafka source is no longer in the default build.** The release binaries and image don't carry it. If you use `sasl_mechanism: GSSAPI`, build with `--features kafka-gssapi` (it needs Cyrus SASL). PLAIN, SCRAM and OAUTHBEARER are unchanged.
- **Release binaries need no extra libraries.** v0.8.0's macOS binaries needed Homebrew `openssl@3` and `cyrus-sasl`; these don't.
- **New key older engines silently ignore:** `sources.file.delete_without_open_check`. An older engine ignores it and deletes as v0.8.0 did, so upgrade every engine before relying on the new delete rules.
- **Object keys:** new objects get content-addressed keys, and objects archived under the older random keys still recover, scrub and audit as before. The command backends (`awscli`, `s3cmd`, `minio`) and `checksum.algorithm: none` keep per-attempt keys.

**Known open, moved to v0.10.0:**
- #626: a replay over a source that grew still writes a second object.
- #619: narrow residuals, listed on the issue.
- #671 release B: the manifest 0.4 writer and receipts.
- #683: OpenTelemetry export.
- #636: the unproven-range override.
- #684: the Object Lock profile.
- #685: MAC key rotation.
- #694: the audit trail.
- #668: multipart resume.

## v0.10.0 and beyond

The theme is **the gateway at the speed its durability allows**: wire the
broker's group commit into the gateway path and close the compatibility
gaps the Kafka comparison found (#596), measure failover and crash
durability against Kafka (#597, #599), serve split children as Kafka
partitions (#473), and ship the egress ceiling once measured (#481).

**Manifest format evolution (#671).** v0.8.0's readers check a
manifest's `version` first, read `0.1`-`0.4`, verify `0.4` over the stored
document so fields they do not know stay authenticated, and report anything
newer as newer rather than as tampered: recovery leaves such a batch VERIFIED
and pending, and `vtopctl scrub` reports `unsupported_version`. Both act on
that only for the row's pinned manifest version carrying the ledger's
self-hash; anything else is unauthenticated, as before. It still writes
`0.3`. A later release adds the `0.4` fields (the verification plan, then
optional receipts, after #664), opt-in, and only once every engine and
auditor runs a reader release. Known limitations until then:

- **Readers before v0.8.0 cannot read `0.4`.** v0.7.0 and earlier
  re-encode a manifest without the fields they do not know, so a `0.4`
  manifest fails their self-hash: recovery replays the batch, the scrub
  reports it, `verify-manifest` calls it corrupt. Upgrade every reader first.
- **A pending newer-release range is still re-read.** An engine older than a
  batch's writer leaves its VERIFIED row alone, but its next read of that
  source starts at the committed point, below that range, and archives those
  bytes again: a duplicate, not a gap.
- **Without object versions, a genuine newer manifest is replayed.** A
  row with no pinned manifest version cannot prove its manifest is the
  writer's, so an older engine treats it as unauthenticated and replays it
  (duplicates), as before #671.
- **`scripts/upgrade-compat.sh` does not cover manifests.** The old-release
  coverage is the committed v0.7.0 fixtures in
  `crates/vtop-core/tests/fixtures/manifests/`.

---

## Versioning

0.x. Minor versions may break compatibility, and releases are published as
GitHub pre-releases to say so. **Upgrades are tested** (#291): CI proves a
data directory written by the newest patch of each released minor series
(v0.2+) opens under the current build, serves byte-exactly, and still serves
after a restart; newly published tags join that matrix automatically.
**Downgrades are not supported** — a newer release's directory may hold
artifacts an older release cannot interpret, and nothing tests that path;
going back means a fresh directory or a verified backup taken before the
upgrade.
