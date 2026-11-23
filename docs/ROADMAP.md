# Roadmap

What shipped, what is next, and — where it is useful — what a release taught us
that the plan did not anticipate.

Milestones are the source of truth for scope; this file is the narrative. It is
written for someone deciding whether to depend on a version, so it says plainly
what each release does **not** yet do.

---

## Next release — a broken epoch history survives a restart

- **A broken epoch history survives a restart (#881 follow-up).** When a write
  to a data node replica's fencing-epoch journal fails (a full disk, above
  all), the replica's history is marked broken: it answers every fence with
  no history and gets no replication session (v0.10.4 made that loud, with an
  ERROR line, `vtop_broker_epoch_history_broken` and `VtopEpochHistoryBroken`).
  The mark is now also written to disk, as `fencing-epochs.broken` beside the
  journal, so a restart keeps refusing; before, a restart cleared it and
  offered the surviving history as if it were whole. `vtopctl node repair`
  clears it. A mark a full disk refuses is retried on every fence and append
  until it lands. [Design note](EPOCH_HISTORY_AFTER_A_FAILED_WRITE.md).
  **Upgrade note: a behaviour change.** A replica whose history broke stays
  out of its range across restarts until it is repaired, where a restart
  used to bring it back. A directory that never broke is unaffected. Before
  restarting each data node onto this release, check
  `vtop_broker_epoch_history_broken`: a replica that broke under v0.10.4 has
  no mark on disk, so a restart onto any release forgets its break, as
  v0.10.4's own restart would. Repair such a replica rather than restarting
  it. An older binary
  ignores the file, so downgrading drops the protection, not the data.

## Next minor release, v0.11.0 — the actor of a cluster command is committed in the replicated log

- **[#706](https://github.com/allamiro/vtop-engine/issues/706)** — the
  actor of an audited cluster command is committed in the replicated log,
  from metadata feature level 9. The serving leader wraps the command in a new
  command kind (40, `Attributed`) carrying the certificate identity it
  verified, the address the connection came from, its own node id and its own
  clock, so the actor and the command are one log entry on every replica. The
  last 1024 such entries are kept in the cluster state (key category 23, value
  tag 26) and travel in snapshots. Applying the entry does exactly what
  applying the bare command did, and a retry never rewrites who acted. A
  client cannot send an actor of its own: the admin endpoint refuses kind 40
  before authorization, and audits the refusal. The wrapper is applied only to
  commands the audit list already names, never to a node's own traffic (a data
  node handing a range back); a certificate name over 128 bytes is refused
  before anything is proposed. Reading the tail back, request ids for
  bootstrap and membership, and the applied records' `requested_by` field are
  the rest of #706.
- **Upgrade note (#706, one-way): feature level 9.** Below level 9 nothing
  changes: the leader proposes the bare command, byte for byte as before, and
  logs once per term that the actor is recorded at the admin endpoint only.
  Upgrade every metadata voter and data node, then
  `vtopctl meta enable-features --level 9`; the raise is refused until every
  metadata member and every counted data node supports 9, and it is permanent.
  The raise itself is a bare entry: no kind-40 entry precedes it. A voter
  older than this release cannot decode kind 40 and reads it as a newer
  format, not as corruption; raising the level while one remains stalls that
  voter (and the tier, if it is a majority) exactly as every earlier level
  does. Level 9 adds a command kind, a key category and a record value tag,
  none of which is written below it.

## Next minor release, v0.11.0 — group commit is wired into the data node

- **[#752](https://github.com/allamiro/vtop-engine/issues/752)** — group
  commit is wired into the data node, **off by default**. A leader,
  candidate or standalone node's `group_commit:` section
  (`enabled`, `max_delay_ms`, `max_records`, `max_bytes`,
  `max_pending_requests`) makes concurrent produce requests on a range share
  one fsync and one replication round; each is still acknowledged only after
  the group's local fsync, the quorum ack and the commit advance. The window
  defaults to 0 (natural batching), so a lone request pays nothing. A node
  logs its settings at INFO when it is on, refuses invalid settings at
  startup (a window of a second or more, a zero bound, a group the replica
  plane could not frame in one append, and the section enabled on a follower;
  a follower accepts a disabled section and logs that it ignores it), and
  exports `vtop_broker_group_commit_overloaded_total` and
  `vtop_broker_group_commit_split_groups_total` beside the existing
  `vtop_broker_group_commit_*` series. Live-chaos scenario 25 kills the leader
  under four idempotent `acks=all` producers. Nothing changes for a config
  without the section. The default turns on only after the single- and
  two-producer A/B shows no regression; until then the gain for the Kafka
  comparison is unmeasured.

## v0.1.0 — a range that fails over

Metadata leases, a lease agent on the data node, verified promotion, and the
first live-chaos scenarios on real processes.

The release pipeline ships `vtop-node` alongside `vtopctl`, publishes a
multi-arch GHCR image, and attaches an SBOM, cosign signatures and
`SHA256SUMS`. The 0.x series makes no compatibility promise across minor
versions.

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
  counted. In both cases the promotion is refused when the answers whose
  lineage was proven fall short of a majority of the replica set, however
  many Unknown answers there are. #636 gives that refusal its own reason,
  `lineage_unproven` (a data-node log field and metric at once; in the
  transition record `Refused{LineageUnproven}` from feature level 8, and
  `Refused{QuorumUnavailable}` as before below it), and adds the one way out
  after restore from backup, at feature level 8: `vtopctl node
  promote-unproven` records an operator's attestation naming the replica
  whose log becomes canonical. Only that replica, at its next promotion,
  counts Unknown answers; metadata records the promotion only while the
  attestation is live and names its holder, and only from a report for the
  range's current epoch (the attestation carries no epoch: it stays usable
  by that replica until it is used, replaced or retired), and uses it up in
  the same apply. The promoted replica's whole log
  then becomes its epoch history, so repair can install it on the others.
  It is never automatic, and every record the named replica does not hold
  may be lost (PRODUCTION_HA.md). The verdicts are recorded with the quorum and the replica-set gate
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
    `require_strong_verification` as a floor (#669). Automatic selection
    still needs a measured cost model
    ([plan](https://github.com/allamiro/vtop-engine/issues/669#issuecomment-6806355703)).
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

## v0.10.0 — an archive that can be audited: receipts, Object Lock, key rotation, an audit trail, and replays that land on the object already archived

v0.9.0 was bug-only; this release carries the features it held back and the remainders of its two largest bugs, #626 and #619. VTOP is still a prototype: the bar for calling it more is #714.

- **Evidence in the archive.**
  - A manifest `0.4` writer, opt-in through `manifest.write_version`, carrying a `verification_plan` (#671, #698). Readers of v0.8.0 and later verify it.
  - Verification receipts: a signed `<batch_id>.receipt.json` beside each manifest, recording how the store checked the object, checked by `vtopctl scrub` (#671, #711). `scrub --check-retention` reads each receipt's Object Lock retention too (#710, #749).
  - MAC key rotation through a key id and a keyring, `manifest.mac` (#685, #703).
  - An S3 Object Lock profile: every object and manifest written under retention, and the retention read back before a batch may be VERIFIED (#684, #708).
- **Upload.**
  - A failed multipart part no longer discards the parts that completed; the replay resumes the session and sends only the missing parts (#668, #702).
  - BLAKE3 and no-checksum writes send a whole-object SHA-256 header, not the SDK's CRC32 trailer, which Garage and Zenko CloudServer refuse (#778, #788).
  - A store that does not implement Object Lock or object versions is refused at startup when the configuration needs one (#722, #770).
- **Archive engine.**
  - A replay resolves to the object already archived: in a later hour (#626, #755), over a file or spool that grew (#793), and over a Kafka partition that grew (#796).
  - A range that recovery holds is no longer passed by the running pipeline, so it is archived before anything above it commits (#782, #785).
  - File source, `delete_after_commit`: the ledger records the read's ctime, so a rewrite that sets the mtime back is seen (#619, #713); an unchecked delete is counted as its own outcome, a changed link count has its own, and late opens are tested (#619, #753); a macOS CI job runs the delete tests (#619, #712).
- **Operating it.**
  - An attributable audit record for every command that changes engine or cluster state (#694, #705).
  - OpenTelemetry traces, OTLP metrics and log correlation, built in and idle until configured (#683, #693).
  - The Kafka gateway answers an OffsetCommit it cannot keep with `INVALID_REQUEST` and counts what it refuses (#596, #754).
  - Time spent in each phase of the produce path, and whether group commit is on, are exported; no behaviour change (#752, #779).
- **Cluster plane.**
  - A range with no provable lineage says why it refuses, and `vtopctl node promote-unproven` is an explicit, audited, single-use last resort, at feature level 8 (#636, #656).
  - A candidate serves once a write quorum of replica streams is up, not every stream, so a failover with one peer down no longer waits ten seconds (#750, #756).
  - A replica that catches up shows its leader's last mark over the records that arrived after it (#787, #790).
- **Documentation and measurement.**
  - SECURITY.md, with a disclosure policy and response times (#716, #760).
  - A data-safety register and a check that keeps it complete (#715, #759).
  - A compatibility and upgrade policy for 1.0, and the inventory of every format (#743, #768).
  - Benchmark results record their disk, and every Kafka chart shares one footer and colour registry (#784); the chart job runs every chart test (#745).
  - Chaos scenario: two replicated Kafka partitions led from different hosts, one idempotent producer across both (#765).
  - CI: a MinIO job runs the live S3 tests (#707); k8s-smoke waits for each port-forward (#786, #789).
  - Intermittent test failures fixed at their cause (#791, #794; #792, #797); the third, #787, was a replication bug and is listed under the cluster plane (#790).

**Upgrading from v0.9.0.** Downgrade is not supported.

- **Order.**
  1. **PostgreSQL: run `vtopctl migrate` from this release, with the migration identity, before starting v0.10.0 engines.** It adds the nullable `sealed_at` column (#626) and the append-only `audit_events` table (#694). Engines of this release refuse a schema without them, naming the missing piece. The runtime and maintenance roles need `SELECT, INSERT` on `audit_events` and `USAGE` on `audit_events_seq_seq` (POSTGRES_DEPLOYMENT.md). The schema marker stays 3, and v0.9.0 engines keep running on the migrated schema with no restart: they never name the new column or table. SQLite adds both on open, and `PRAGMA user_version` stays 1.
  2. **Upgrade every engine, metadata voter and data node.**
  3. **Then raise the cluster feature level to 8**, only if you want `vtopctl node promote-unproven` (#636): `vtopctl meta node-versions --level 8`, then `vtopctl meta enable-features --level 8`. The raise is refused, naming each node, until every node reports it can run level 8 (#634).
  4. **Then turn on the new settings below**, once every engine, `vtopctl` and auditor that reads the archive runs a release that understands them.
- **One-way changes.** Installing the binary takes none of them. Only an operator action does:
  - **Raising the feature level to 8 (#656)** is permanent; there is no lowering. A v0.9.0 metadata voter supports levels up to 7 and refuses a raise above it ("newer than this release supports"). Level 8 adds a metadata command kind (39, the promotion attestation), a key category (22), a record value tag (25), a promotion outcome tag (4) and a refusal tag (5), none of which is written below level 8. A v0.9.0 voter that met them would stop rather than guess: it reads the first four as a newer format and the refusal tag as corrupt.
  - **`manifest.write_version: "0.4"` (#698).** A reader before v0.8.0 re-encodes a `0.4` manifest without the fields it does not know, so the self-hash and MAC fail: it reports the manifest as tampered, and recovery replays the batch (duplicates, not loss). Set `"0.3"` again to go back; `0.4` manifests already written stay readable by v0.8.0 and later.
  - **`manifest.mac` (#703).** v0.8.0 and v0.9.0 hold one key and ignore the id: they pass a manifest their key signed and report the rest as unauthenticated, so an engine on them replays those batches. Every reader must run this release before the active key changes.
  - **`upload.object_lock` (#708)** with `mode: compliance`: nobody, the account root included, can remove an object before its date. Objects from failed attempts are locked too.
  - **No schema marker bump and no refusal to start in an older binary** comes from this release's ledger changes: PostgreSQL stays at marker 3 and SQLite at `user_version` 1. A v0.9.0 engine writes NULL `sealed_at` and reads prune digests that carry it. Only N-1 may share a ledger with this release (#743).
- **Older engines silently ignore these config keys.** Upgrade every engine before relying on them:
  - `manifest.write_version`: harmless. An older engine keeps writing `0.3`, the safe direction (#698).
  - `manifest.verification_receipts`: harmless. An older engine writes no receipts, and the scrub reports the rest `missing`, a finding only under `--require-receipts` (#711).
  - `manifest.mac`: **not harmless in a mixed fleet.** An older engine keeps signing with `manifest_mac_key_env` and no id, and cannot verify manifests signed with a new key. Rotation is complete only when none remains. It also needs `write_version: "0.4"`, or this release refuses the file (#703).
  - `upload.object_lock`: **not harmless.** An older engine writes unlocked objects and nothing at the object says so. Keep a bucket default retention as the floor, drain older engines first, and run `vtopctl scrub --check-retention` afterwards to find any unlocked object (#708).
  - `upload.multipart_part_attempts`: harmless. An older engine keeps the SDK's retries only (#702). An older engine that finds a session this release kept refuses it, fails that attempt and aborts the session; its replay uploads afresh: one wasted attempt, no data at risk.
  - The audit trail and OpenTelemetry are driven by `VTOP_AUDIT_LOG` and `OTEL_*` environment variables, not by `config.yaml` keys. An older engine ignores them: harmless, but it writes no audit records and exports no spans, so a mixed fleet's trail is only as complete as its least-upgraded node.
  - Unknown keys **inside** `manifest.mac` and `upload.object_lock` are refused by this release, unlike the rest of the engine config.
- **Behaviour changes to check before upgrading.**
  - `vtopctl prune-ledger`, `replay` and `migrate` now fail when their audit record cannot be written (a full disk, a missing grant, an unwritable `VTOP_AUDIT_LOG`). With a manifest MAC key configured they also need it in their environment (#705).
  - `upload.require_object_versioning: true` against a literal bucket without versioning, or on a backend that cannot address object versions, now refuses to start instead of failing every batch (#770).
  - A delete under `delete_without_open_check` with no lease is counted `deleted_unchecked`, no longer `deleted`; a dashboard that sums `deleted` must add it. A hard-link keep that was `metadata_changed` is now `links_changed` (#753).
  - Rows written before this release carry no ctime, so a file archived before the upgrade is re-armed on its mtime alone until its quiet period ends (#713). Rows without a seal instant keep the old behaviour: a replay of one in a later hour may still write a second object, once (#755).
  - Every command's stderr gains one INFO line saying OpenTelemetry is on or off, and `vtopctl validate --print-effective` gains an `opentelemetry:` block (#693).
  - On a node with no metadata plane, a Kafka client's OffsetCommit now gets code 42 where it got 43. Both are non-retriable; a consumer with auto-commit logs the failure and keeps reading (#754).
  - `vtopctl scrub --check-retention` on an archive with receipts can now exit non-zero for a receipt written without a lock (#749).
  - BLAKE3 and no-checksum uploads read the local file once more to compute the SHA-256 header. A store that still refuses it can be told `AWS_REQUEST_CHECKSUM_CALCULATION=when_required` (#788).
- **Planned.** `0.4` becomes the default manifest writer at v0.12.0, announced here and in the v0.11.0 notes; `write_version: "0.3"` stays accepted through 1.x with a deprecation warning ([compatibility policy](reference/compatibility.md), #743).
- **Known open:**
  - #798, #799: two intermittent test failures (an OpenTelemetry message test, a produce-phase timing test on a slow runner). Both moved to v0.11.0.
  - #709: a reused object whose Object Lock ended, or that was stored unlocked, fails every replay of the same bytes until the operator extends its retention or deletes that version.
  - #773, #774: #626 follow-ups (adopting an earlier attempt's object on backends that cannot report absence; reporting failed attempts' objects no committed row names).
  - #706: the actor of a cluster command is in the admin endpoint's record only, not in the replicated log.
  - #752, #751: group commit is measured, not yet wired into the data node, and a Kafka client cannot yet follow a failover through a candidate node.
  - #743: the compatibility policy is written; the format registry and the tested downgrade matrix are not complete.
  - #733: key and state-store files readable by other local users are not yet refused.
  - #714: the bar for calling VTOP production-ready. VTOP remains a prototype.

### v0.10.0 in detail

The long form of what the list above names: how each piece works, its limits, and what an operator does about them.

- **[#668](https://github.com/allamiro/vtop-engine/issues/668)** — a failed
  multipart part no longer discards the parts that completed. The failed part
  alone is retried, up to the new `upload.multipart_part_attempts` (default 3),
  with backoff; a batch that still fails keeps its multipart session, and the
  replay of the same bytes resumes it and sends only the missing parts. A
  running engine sweeps abandoned sessions periodically, not only at startup.
  New metrics: `vtop_multipart_parts_total{result}`,
  `vtop_multipart_sessions_total{outcome}`, `vtop_multipart_bytes_reused_total`.
  `vtopctl tier copy` shares the code, so it gets the per-part retries too.
- **Upgrade note (#668):** an older engine ignores
  `upload.multipart_part_attempts` and keeps its behaviour (the SDK's retries
  only). Session files keep schema 1. An older engine that finds a session
  the new one kept refuses it (it still compares the local file's name), fails
  that attempt and aborts the session, and its replay uploads the object
  afresh: one wasted attempt, no data at risk. Incomplete uploads
  kept for a replay are billed storage until resumed or swept: add the
  `AbortIncompleteMultipartUpload` lifecycle rule in PRODUCTION_HA §7.6.
  Downgrade remains unsupported.
- **[#782](https://github.com/allamiro/vtop-engine/issues/782)** — a range
  recovery holds because its replay decision could not be recorded, or
  because a newer release wrote it, is no longer passed by the running
  pipeline: recovery applies every rewind above it in its source unit at the
  held start, so the held range is read again with the ranges above it and
  archived before anything above it commits. The ranges above it are
  archived twice. New metrics: `vtop_recovery_held_total{reason}`,
  `vtop_recovery_rewinds_lowered_total`. No format or ledger change.

**The audit trail (#694).** Every operator command that changes engine or cluster
state writes one versioned, attributable record (`audit_version: 1`): who
(the local uid and host, or the admin endpoint's certificate identity), what,
on what, with which parameters (an allowlist, redacted), and how it ended,
failures and refusals included. Records go to the `vtop::audit` log target,
which `RUST_LOG` cannot silence, to the JSONL file `VTOP_AUDIT_LOG` names,
and, for `replay`, `prune-ledger` and `migrate`, to a new append-only,
hash-chained `audit_events` table, keyed with the manifest MAC key when one
is configured. Those three commands write an `attempt` record first and are
refused when it cannot be written. The admin endpoint records every refusal
and each operator command with the caller's identity, and every metadata
replica records the entry it applied with its log index. `vtopctl audit
list` reads the table or a file; `vtopctl audit verify` checks the chain.
New metrics: `vtop_audit_events_total{action,outcome}` and
`vtop_audit_write_failures_total{sink}`. See
[operate/auditing.md](operate/auditing.md). Upgrade notes and limitations:

- **PostgreSQL: run `vtopctl migrate` and grant the new table before
  starting engines of this release.** They refuse a schema without
  `audit_events`, naming it. The runtime and maintenance roles need `SELECT,
  INSERT` on it and `USAGE` on `audit_events_seq_seq`
  ([POSTGRES_DEPLOYMENT.md](POSTGRES_DEPLOYMENT.md)). An older release on a
  migrated database ignores the table. SQLite creates it on open.
- **With a manifest MAC key configured, `prune-ledger`, `replay` and
  `migrate` need it** (they key their audit rows with it), so a migration
  job or a prune cron needs the engine's key environment; they stop naming
  the variable when it is missing. `vtopctl audit list` does not.
- **A destructive ledger command now fails when its audit record cannot be
  written** (a full disk, a missing grant, an unwritable `VTOP_AUDIT_LOG`).
  That is the intended cost of "fail rather than run unrecorded"; there is
  no break-glass switch.
- **The actor is in the edge record only.** Each replica's applied record
  carries the request id and log index; the requester's identity is in the
  admin endpoint's record of the same request id. Making the actor durable
  in the replicated log is a separate format change.
- **A mixed fleet's trail is only as complete as its least-upgraded node**:
  older engines and nodes write no audit records.
- **`vtopctl meta`, `node` and `tier` now log** to stderr at WARN (audit
  records always), where they logged nothing before.

**A replay resolves to the object already archived (#626, remainder).**
v0.9.0 made a replay of the same bytes reuse the stored object. A replay in a
later hour still named another key, because the time partition came from the
instant the replay was sealed. Each ledger row now records the instant its
window was sealed (`sealed_at`), and a window that covers exactly a range an
earlier attempt sealed and handed back is sealed at that instant again, from
memory while the engine runs and from the row after a restart. It is filed
under the same hour and names the same key, so the object, or a multipart
session the failed upload kept (#668), is reused. What each such replay came
to is counted in `vtop_replay_rebuilds_total{result}` (`exact`,
`bytes_differ`, `fallback`, `refused`, `too_large`, `dropped_at_stop`) and
logged (`replay_rebuilt`, `replay_bytes_differ`, `replay_key_fallback`,
`replay_bound_refused`, `replay_bound_too_large`, `replay_window_dropped`). Upgrade
notes:

- **PostgreSQL: run `vtopctl migrate` before starting engines of this
  release.** It adds the nullable `sealed_at` column; engines of this
  release refuse a schema without it, naming it. The schema marker stays 3.
- **SQLite adds the column when an engine of this release opens the file;**
  the schema version stays 1, so this is no one-way step.
- **A v0.9.0 engine ignores `sealed_at` and writes NULL**, on either backend,
  and reads prune digests whose rows carry it. Only N-1 may share a ledger
  with this release.
- **Rows written before this release** carry no seal instant. A replay of one
  of them is filed under the hour it is replayed in, as before, so it may
  still write a second object once.
- **A replay over a file, spool or Kafka partition that grew is rebuilt as
  first cut.** Its
  re-read stops at the failed window's recorded end, the window is sealed
  there (not by a threshold, its age or a forced flush), and the bytes after
  it are the next window. A range over 4x the current batching limits is not
  bounded (`too_large`); one whose re-read cannot stop at its end is read as
  before (`refused`); a stop inside a rebuild drops the partial window, which
  the next start reads again (`dropped_at_stop`). The new trait method
  `SourceAdapter::bound_replays` defaults to bounding nothing.
- **Not changed:** a range whose row a power loss rolled back has nothing to
  rebuild from, and a Kafka range whose records retention or compaction
  removed is read on from where the log starts (`refused`).

**Kafka gateway: an OffsetCommit it cannot keep says so (#596).** A node
without a metadata plane (a static leader or a standalone node) has no
durable offset store and still refuses OffsetCommit, but now answers
`INVALID_REQUEST` (42) instead of `UNSUPPORTED_FOR_MESSAGE_FORMAT` (43).
OffsetCommit carries no error message, so a client shows only the code's
own text: 43's sent the first Kafka comparison looking at record versions,
and 42's ends "See the broker logs for more details", where the gateway's
WARN now says what happened, that produce and fetch are unaffected, and to
run a leased leader on a metadata plane or set `enable.auto.commit=false`.
A request for an api the gateway does not serve (CreateTopics,
DescribeConfigs and four other admin keys) is logged by name with the
operator path (`vtopctl meta create-topic`), at most at each power of two
per key. Both are counted in a new node metric,
`vtop_kafka_requests_refused_total{api,reason}`, on every node that runs
a gateway. A consumer with auto-commit on logs the failed commit and keeps
reading; an explicit commit fails, as it did with 43. Live-chaos scenario
19 asserts the refusal for a librdkafka group consumer that reads every
record and exits cleanly, and scenario 21 runs two such consumers under it.
Other refusals (a metadata plane that predates head cursors, a version
outside a served range) are not counted yet.

**Kafka gateway: gzip and zstd produce batches are served (#596).** A
producer with `compression.type=gzip` or `zstd` was refused with
`UNSUPPORTED_COMPRESSION_TYPE`; the gateway now decompresses the batch and
stores its records as ordinary native records, so Fetch serves them as an
uncompressed batch (codec bits 0, CRC recomputed, the producer's create
times kept). The CRC is checked over the compressed bytes before the
decoder sees any of them, and the record count stays binding over the
decompressed records. The compressed batches of one produce request, across all
its partition entries, may expand to 16 MiB in total, the most an
uncompressed field can carry: past that the partition is refused whole with
`MESSAGE_TOO_LARGE`, a stream that does not
decompress is `CORRUPT_MESSAGE`, and snappy, lz4 and codec ids Kafka does not
define stay `UNSUPPORTED_COMPRESSION_TYPE` with the codecs to use instead. Every
compressed field is decompressed off the async workers, and a zstd frame
that asks for a window wider than 16 MiB is refused before it is allocated
(`CORRUPT_MESSAGE`; producers at zstd levels 20 to 22 can ask for one). The cost is
stated, not hidden: the native log stores and Fetch serves the uncompressed
bytes where Kafka keeps the producer's compressed ones. No stored format
and no advertised API version changes.
librdkafka clients (kcat, and the libraries built on it) compress zstd
against the gateway but send gzip, snappy and lz4 uncompressed: librdkafka
2.8.0 logs "Broker does not support compression type gzip: not compressing
batch", because its feature check wants protocol versions below the
gateway's Produce v3 floor. Those batches are stored as sent (live-chaos
scenario 26). The gateway's unit tests decode gzip and zstd batches the
pinned Java producer wrote (`scripts/gen-kafka-fixtures.sh`).

**Manifest format evolution (#671).** v0.8.0's readers check a
manifest's `version` first, read `0.1`-`0.4`, verify `0.4` over the stored
document so fields they do not know stay authenticated, and report anything
newer as newer rather than as tampered: recovery leaves such a batch VERIFIED
and pending, and `vtopctl scrub` reports `unsupported_version`. Both act on
that only for the row's pinned manifest version carrying the ledger's
self-hash; anything else is unauthenticated, as before.

Release B (v0.10.0) adds the `0.4` writer behind an opt-in setting:
`manifest.write_version: "0.4"` in a new top-level `manifest:` section
(default `"0.3"`). A `0.4` manifest carries `verification_plan` (the
configured verification method and `require_strong`), is hashed and MAC'd
over the canonical form v0.8.0 verifies, and still says
`verification_status: not_verified`, which v0.8.0 requires. With the setting
off, manifests are byte for byte what v0.8.0 writes. Startup logs the version
written and why, `vtopctl validate --print-effective` shows
`manifest.write_version`, and `vtop_manifests_written_total{tenant,version}`
counts manifests by version. Verification receipts (below) complete release
B. Known limitations:

- **Every reader must run v0.8.0 or later before `0.4` is enabled.** That
  means every engine sharing the ledger, every `vtopctl scrub` and
  `verify-manifest`, and every downstream consumer or auditor of the archive.
  The engine cannot check this: archive engines do not report their release
  the way data nodes do (#634), so the setting is the operator's
  acknowledgement.
- **v0.7.0 and earlier report a `0.4` manifest as tampered.** They re-encode
  it without the fields they do not know, so the self-hash (and MAC) fail:
  `verify-manifest` reports a failed self-hash and authentication, the scrub
  reports it, and recovery replays the batch (duplicates, not loss). This was
  checked with the released v0.7.0 and v0.8.0 `vtopctl verify-manifest`
  against a `0.4` archive written by release B: v0.8.0 passes it, v0.7.0
  fails it.
- **Older engines ignore the `manifest:` section.** An engine before release
  B keeps writing `0.3` whatever the key says, which is the safe direction:
  setting it on a fleet that is not yet upgraded changes nothing there. A typo
  inside the section is ignored too, and leaves `0.3`.
- **Going back.** Set `"0.3"` again. `0.4` manifests already written stay
  readable by v0.8.0 and later. Downgrading the binary stays unsupported.
- **A pending newer-release range is still re-read.** An engine older than a
  batch's writer leaves its VERIFIED row alone, but its next read of that
  source starts at the committed point, below that range, and archives those
  bytes again: a duplicate, not a gap.
- **Without object versions, a genuine newer manifest is replayed.** A
  row with no pinned manifest version cannot prove its manifest is the
  writer's, so an older engine treats it as unauthenticated and replays it
  (duplicates), as before #671.
- **`scripts/upgrade-compat.sh` does not cover manifests; `manifest-compat`
  does.** Old writers are covered by the committed fixtures in
  `crates/vtop-core/tests/fixtures/manifests/` (v0.7.0 and v0.8.0 output, and
  the first `0.4` manifests and receipts release B wrote). Old readers are
  covered by the `manifest` cells of the compatibility matrix
  (`.github/workflows/compat.yml`, running `scripts/manifest-compat.sh`, #743):
  HEAD writes a signed `0.4` archive with receipts, the published v0.8.0 and
  later `vtopctl` (checked against each release's `SHA256SUMS` and Sigstore
  bundle) must pass `verify-manifest` on it and accept the config in
  `validate`, and v0.7.0 must report the self-hash failure. Pull requests run
  the newest release and the two sides of the cutoff (v0.7.0 and v0.8.0);
  the nightly run covers every release. The `ledger-sqlite` cell runs
  `scripts/ledger-compat.sh` on the newest release.

**Verification receipts (#671 release B).** With
`manifest.verification_receipts: true` the engine writes
`<batch_id>.receipt.json` beside each manifest once the batch is VERIFIED:
the class the ledger recorded (#664) and the method the store actually ran
(`store_checksum`, or `read_back` for a multipart object on native S3, or
`size_only`), bound to the manifest's self-hash and signed like a `0.4`
manifest, naming its key (#685). It is evidence, not part of the commit: a
receipt that cannot be written is a rate-limited WARN and
`vtop_verification_receipts_total{result="failed"}`, and the batch commits.
Recovery writes the receipt of a VERIFIED batch that has none (`origin:
recovery`), never over an existing one. `vtopctl scrub` checks receipts when
the config writes them or with `--require-receipts`, and reports `missing`,
`invalid`, `unauthenticated` and `mismatched` (another batch, digest,
version, object or class than the ledger's) in
`vtop_scrub_receipts_total{result}`. The procedure is
[Verification receipts](operate/verification-receipts.md). Upgrade notes and
known limitations:

- **Off by default; no reader upgrade needed.** A reader that does not know
  about receipts never fetches one. A consumer that **lists** the archive's
  prefix sees one extra `.receipt.json` object per batch: check such
  consumers before turning it on.
- **Older engines ignore the key** and write no receipts, so in a mixed fleet
  only upgraded engines write them; the scrub reports the rest `missing`,
  which is a finding only with `--require-receipts`. Turn that flag on once
  every engine writes receipts and the scrubbed archive was written with them.
- **No backfill.** A batch committed before receipts were enabled, or whose
  receipt could not be written, keeps none: a later re-read would be a
  different statement. Recovery writes one only for a batch still VERIFIED.
- **Object Lock.** A receipt is written and its retention read back like a
  manifest's, and recovery never adds a second version beside one.
  When receipts are checked, `vtopctl scrub --check-retention` reads each
  existing receipt's retention again, at its current key (#710), counted
  apart from the batch's in `vtop_scrub_receipt_retention_total{result}`: no
  retention and another mode are findings, an ended retention is not, and a
  missing receipt is not a retention finding.
- **A scheduled `scrub --check-retention` can now fail on receipts.** On an
  archive that has receipts, a receipt written before `upload.object_lock`
  was set, one the engine's read-back found unlocked (it logged
  `verification_receipt_failed`), or one whose lock was removed carries no
  lock and is a finding: the run exits non-zero and
  `vtop_scrub_last_finding_timestamp_seconds` moves, as for an unlocked
  object. Each read is one more request per receipt.
- **Going back.** Set `verification_receipts: false`. Receipts already written
  stay readable; drop `--require-receipts` from scheduled scrubs.

**Manifest MAC key rotation (#685).** A keyring, `manifest.mac`, holds
several keys by id: `active` signs, every key in `keys` verifies the manifests
that name it, the one marked `legacy` verifies manifests with no id, and
`retired` tombstones a withdrawn key. A `0.4` manifest records its signing key
in `manifest.mac_key_id`, covered by its hash and MAC, and a reader verifies
it with that key, never by trying the others. A manifest naming a key the
reader does not hold, or a retired one, is unauthenticated (not corrupt) and
the message names the key; recovery replays it and the scrub reports it.
`manifest_mac_key_env` stays as one key that writes no id. Startup logs the
keyring by id, `validate --print-effective` shows ids and source kinds, and
`vtop_manifest_authentications_total{key_id,result}` and the scrub's
`vtop_scrub_manifest_keys_total{key_id}` count by configured id. The
procedure is [docs/operate/manifest-key-rotation.md](operate/manifest-key-rotation.md).
Known limitations:

- **Every reader must run the keyring release before the active key
  changes.** v0.8.0 and earlier hold one key and ignore the id: they pass a
  manifest their key signed and report the rest as unauthenticated (checked
  with the released v0.8.0 `vtopctl verify-manifest`: it passed a manifest
  signed by its key under id `q3` and failed one signed by `q4`). An engine
  on such a release replays those batches. The engine cannot check this.
- **A keyring needs `manifest.write_version: "0.4"`,** refused at config load
  otherwise, so the `0.4` reader rule (every reader on v0.8.0 or later)
  applies first.
- **Older engines ignore `manifest.mac`.** One that still has
  `manifest_mac_key_env` keeps signing with that key and no id (verified on
  keyring engines through the `legacy` key) and cannot verify new-key
  manifests. Rotation is complete only when none remains.
- **Unknown keys inside `manifest.mac` are refused,** unlike the rest of the
  engine config, so a misspelt `retired` cannot leave a withdrawn key
  working. A later release's new key there will be refused by this one.
- **Manifests are never re-signed.** A retired (tombstoned) key's manifests
  fail on purpose; re-archiving that data is a separate decision.
- **Key files are permission-checked** since v0.10.2 (#733), as are
  `engine.state_store` files: see
  [Secret files and their permissions](operate/secrets.md).

**The S3 Object Lock profile (#684).** `upload.object_lock` (`mode`,
`retain_days`, `legal_hold`) makes the `s3_native` backend write every object,
manifest and multipart upload under an S3 Object Lock retention. The engine
refuses a bucket without Object Lock (at startup for a literal bucket, at the
first batch for each bucket a template names), and before a batch may be
VERIFIED it reads the retention of the exact versions it wrote and compares it
with what it asked for: a missing, shorter, other-mode or hold-less lock fails
the batch, which is replayed. `vtop_object_lock_checks_total{tenant,result}`
counts the checks, `vtop_object_lock_mode{tenant,mode}` shows the mode, and
`vtopctl scrub --check-retention` reports locked, elapsed, missing and
mode_mismatch. The deployment profile is
[OBJECT_LOCK_DEPLOYMENT.md](OBJECT_LOCK_DEPLOYMENT.md). Known limitations:

- **Older engines ignore `upload.object_lock` and write unlocked objects.**
  Nothing at the object says so. Keep a bucket default retention as the
  floor, drain engines before v0.10.0 before relying on the setting, and run
  `scrub --check-retention` afterwards to find any unlocked object.
- **Unknown keys inside `upload.object_lock` are refused,** like
  `manifest.mac`, so a later release's new key there is refused by this one.
- **Upgrade note (#709, v0.10.2): a replay no longer reuses an object that is
  not locked as configured.** Under the profile, a replay that resolves to an
  object already archived under its derived key (#626) whose lock has ended,
  that was stored unlocked (for example before the profile was enabled), that
  is in the other mode, or that lacks the configured legal hold, writes the
  batch as a fresh locked object under its per-attempt key and commits it,
  where #684 failed that range on every replay until an operator extended the
  old object's retention or deleted it. The old object is left untouched, so
  each such range costs one extra object; the four cases are counted as
  `fallback_lock_elapsed`, `fallback_unlocked`, `fallback_lock_mode` and
  `fallback_hold_missing` in `vtop_object_key_decisions_total`, and logged
  as a WARN. A mode mismatch falls back in both directions, a stronger
  compliance lock included, because the committed object must be under the
  configured mode. A retention the store cannot report still fails the
  batch, so an outage does not multiply objects. The engine remembers the
  fresh key per range for the process and reuses it on the next replay, and
  when the store does not keep the lock it writes nothing for five minutes
  (`lock_withheld`), so the extra objects are one per range per window, not
  one per replay; a restart forgets it. An engine without #709 in a
  mixed fleet still stalls on such a range until it is upgraded.
- **Objects from failed attempts are locked too,** and under `compliance`
  nobody can remove them before the date: storage cost, said at startup.
- **`vtopctl tier copy` locks cold-tier copies** under the block and reads
  both retentions back before it proposes the evidence; it used to refuse
  the block.
- **Proven against MinIO only** (RELEASE.2025-09-07, the lab's build). AWS S3
  behaviour is taken from its documentation (the integrity header a locked
  write needs, the permissions a HEAD needs to return the lock headers), not
  measured.

**File source: the read's ctime is recorded in the ledger (#619).** A file
marker now carries `ctime`, the file's ctime from the same fstat as its
`mtime`, after the read. `delete_after_commit` deletes a file only while it
still has that ctime: at the covering commit, and when a restart re-arms the
delete (`CommittedCursor::observed_ctime`). Before, a same-length rewrite
between a file's first page and the page below the archived end that also
set the mtime back agreed with everything the ledger recorded, so after a
restart, or between a batch's read and its commit, it was deleted unread. Now
it is kept, with a `metadata_changed` warning. The field is ledger only: no
DDL on either backend, so `vtopctl migrate` has nothing to apply, and the
manifest never carries it, so manifests stay byte for byte what released
readers verify (ARCHITECTURE.md, "Ledger format note: ctime").

- **Upgrading: rows written before v0.10.0 have no ctime, and arm the delete
  as before.** After the upgrade, a file archived before it is re-armed on the
  mtime alone, with the file's ctime at the restart, so for those rows the
  limit stands - a rewrite while the engine was down that set the mtime back
  is not seen - until their quiet period ends and they are deleted or kept.
  Refusing them instead would keep every file archived within the last
  `delete_quiet_seconds` before the upgrade until someone removed it by hand.
- **Older engines ignore the field** (serde skips unknown keys) and judge as
  they always did. Downgrade is still not supported.
- **Known, and on the side of keeping:** a restart after one of the engine's
  own delete attempts moved a file aside and put it back (a process opened
  it during the delete, or the unlink failed) finds the file's ctime moved
  by the put-back, so the file is kept with a warning that lists that
  attempt among the causes, and is removed by hand. A stored ctime that is
  not a timestamp reads as absent, and arms as a pre-v0.10.0 row does.

## v0.10.1 — the Linux archives run on glibc 2.28

v0.10.0's Linux archives did not start on Debian 12, Ubuntu 22.04 or RHEL 8 and 9. This release fixes that and carries what else merged since: test fixes, the release pipeline's `latest` rule, the alert rules, and CI. VTOP is still a prototype: the bar for calling it more is #714.

- **The Linux archives.**
  - The archives of v0.8.0 to v0.10.0 were built on Ubuntu 24.04 (glibc 2.39). v0.10.0's `vtopctl` needs `GLIBC_2.39` and its `vtop-node` `GLIBC_2.34`, so on a host with an older glibc `vtopctl` stopped with ``version `GLIBC_2.38' not found`` (#808).
  - Both Linux archives (x86_64 and aarch64) are now built inside AlmaLinux 8, so the floor is glibc 2.28: Debian 10 and later, Ubuntu 20.04 and later, RHEL, AlmaLinux and Rocky Linux 8 and later (#809).
  - Before anything is uploaded, the release workflow fails if either binary asks for a newer symbol version than that floor or links a library a base install lacks, then runs both binaries from each archive on Debian 12, Ubuntu 22.04 and AlmaLinux 8, with nothing installed, through a file-to-local-directory round trip and a scrub (#809). The other distributions in the list are expected to work by the same symbol versions and are not run. musl distributions such as Alpine are not supported; use the container image.
  - The container image is unchanged: it is built from `docker/Dockerfile` in its own Debian 12 base (`rust:1-bookworm`, run on `debian:bookworm-slim`), never on the runner's glibc.
- **Release pipeline.**
  - `latest` (the image tag and GitHub's "Latest release") follows the newest final release, so a fix for an older line no longer moves it back; the image-manifest job checks the registry after the push (#804).
  - The container page pulls the current release and shows `latest` beside the pinned tag (#803); a check fails the docs workflow when the version copied into the container page, the README's citation, `CITATION.cff` or the chart's `appVersion` is not the workspace version (#804).
- **Operating it.** The engine's Prometheus alert rules ship as `observability/prometheus/vtop-rules.yaml`, tested with `promtool`, with a runbook section per alert and the SLOs they protect; the Helm chart can render them as a `PrometheusRule` (#720, #772).
- **Tests and CI.**
  - A produce-phase timing test no longer depends on the runner's speed (#799, #805); the OpenTelemetry export test says which stage lost a batch span, diagnostics only (#798, #806); `ledger compat` expects `sealed_at` from a previous release that writes it (#807).
  - A PostgreSQL 14 to 18 matrix runs `vtopctl migrate`, a ledger restore and recovery on each major: 14 and 18 on pull requests, all five nightly (#723, #761).
  - Decoders are fuzzed nightly from a persisted corpus, and a mutation gate covers the commit path (`state_machine.rs`, `replay.rs`) (#729, #766).
  - Two small parsing helpers (`parse_digest`, `parse_cursor`) were split out of their readers so the fuzz targets reach the same code; behaviour and error text are unchanged (#766).

**Upgrading from v0.10.0.** Downgrade is not supported.

- **A drop-in replacement.** Replace the binaries; start order does not matter, and v0.10.0 and v0.10.1 engines, voters and data nodes can run side by side. This release changes no schema (PostgreSQL stays at marker 3, SQLite at `user_version` 1), no on-disk or wire format, no `config.yaml` key and no cluster feature level (it stays 8). No one-way step is added; the one-way steps of v0.10.0 (feature level 8, `manifest.write_version: "0.4"`, `manifest.mac`, `upload.object_lock`) are unchanged. No config key is newly ignored by an older engine, and there is no new behaviour to check first.
- **Hosts that could not run v0.10.0's `vtopctl`.** Install the v0.10.1 archive; `vtopctl` and `vtop-node` need nothing beyond the base system libraries. Verify the download as before ([release verification](RELEASE_VERIFICATION.md)).
- **Helm chart 0.6.0** (from 0.5.3; `appVersion` 0.10.1). The new values `metrics.prometheusRule.enabled` (default `false`), `labels` and `runbookBaseUrl` are optional, and an existing values file renders as before. Turning it on needs the Prometheus Operator CRDs: the chart refuses to render without them, naming the alternative (load `observability/prometheus/vtop-rules.yaml` with `rule_files`). The chart runs the cluster plane, not the engine: the rules fire for engines the same Prometheus scrapes.
- **Known open.**
  - #798: an OpenTelemetry message test (`otel_messages`) fails intermittently in CI. #806 only makes the next failure say which stage lost the span; the cause is not found. Moved to v0.11.0.
  - #729: the nightly fuzzing and the commit-path mutation gate are in; the gate on the verify, recovery and file-continuity code is not.
  - #724: build, test and smoke the release archives on every supported platform. This release checks the floor and runs three distributions; the full matrix is not done.
  - Everything under v0.10.0's "Known open" still stands, except #799, which is fixed: #709, #773, #774, #706, #752, #751, #743, #733, and #714, the bar for calling VTOP production-ready. VTOP remains a prototype.

## v0.10.2 — batches stay within max_bytes, and a Linux archive for every Linux

A patch release: no schema, on-disk or wire format, feature level or one-way step changes. It fixes how large a batch `vtopctl run` seals, makes the engine refuse key, ledger and audit files that other local users can reach (a behaviour change at startup, described below), lets a replay recover from an Object Lock that ended, and widens the Linux archives to every glibc since 2.17 plus a static musl build. VTOP is still a prototype: the bar for calling it more is #714.

- **Batches in `run` hold at most `max_bytes` (bug).** `vtopctl run` sealed batches at about twice `batching.max_bytes`: with 16 MiB, a 73 MB log was cut into 33,554,257-byte batches, at v0.7.0 and v0.10.1 alike. A read that would take the buffered window past `max_records` or `max_bytes` now seals the window first. `process-once` never did this. A window rebuilt for a replay (#626) is never split. Expect more, smaller batches at the same throughput: roughly twice as many objects for a source that filled every window (#824).
- **Linux archives for every Linux.** The `-linux-gnu` archives are built inside manylinux2014 (CentOS 7), so the floor is glibc 2.17: RHEL and CentOS 7 and later, Amazon Linux 2 and 2023, Debian 8 and later, Ubuntu 14.04 and later. v0.10.1's glibc 2.28 floor did not cover Amazon Linux 2 or RHEL 7. New `-linux-musl` archives (x86_64 and aarch64) are fully static and run on any Linux, Alpine included. Each release now has six archives. The release workflow checks the symbol floor and the static link, and runs every Linux archive on Amazon Linux 2 and 2023, Debian 12 and 13, Ubuntu 22.04 and 24.04 and AlmaLinux 8, 9 and 10 (musl ones on Alpine too), and the macOS archives on both macOS runners. The musl archives use musl's own allocator, whose performance under load has not been measured (#724, #842).
- **Build provenance.** The archives and the image carry a GitHub build provenance attestation, verified with `gh attestation verify` before the release publishes; [release verification](RELEASE_VERIFICATION.md) has the commands. Earlier releases have none. This release is the first to run those steps, so a failure there stops the publish before anything is released (#731, #825).
- **Files other local users can reach are refused.** The manifest MAC key files, the `engine.state_store` secret file, the SQLite ledger (with its `-wal` and `-shm` files) and the `VTOP_AUDIT_LOG` file were read without looking at their mode. The engine now refuses them when they are writable by group or other, or readable by other users (or by group, outside a read-only Kubernetes secret mount, which is how `defaultMode: 0400` appears once the pod sets `fsGroup`), unless the directory holding them is closed (owned by the engine's user, mode 0700), so an existing 0644 ledger in a directory others can enter is refused until it is `chmod 0600`. The audit file may be group-readable. New SQLite and audit files are created 0600. TLS private keys and the files `vtop-node` reads are not checked yet. `vtopctl validate` reports every refused file at once. See "Upgrading from v0.10.1" for what to do (#733, #775).
- **A replay over an unlocked or ended Object Lock recovers.** Under `upload.object_lock`, a replay that resolved to an object already archived for its range, but not locked as configured (the lock had ended, it was stored unlocked, it is in the other mode, or it lacks the legal hold), failed that range on every replay. It now writes a fresh locked object under its per-attempt key and commits it, leaves the old object untouched, logs a WARN and counts a `fallback_*` decision in `vtop_object_key_decisions_total`. A new alert, `VtopObjectLockFallback`, ships in the rules (a ticket, not a page). A retention the store cannot report still fails the batch (#709, #757).
- **`libz-sys` 1.1.33** replaces 1.1.32 for RUSTSEC-2026-0544 (a heap buffer overflow). chrono's unmaintained notice (RUSTSEC-2026-0543, no fixed release) is ignored with its justification and tracked (#822). Dependency bumps: openraft 0.9.26, and five minor and patch updates (#815, #814).
- **A raise to feature level 8 or above waits for every metadata member.** Before proposing it, the leader asks every voter and learner which release it runs and the highest level it implements (new peer-plane message kinds 7 and 8). A member that does not answer, cannot be reached or answers as another node holds the raise back, and the refusal names it. Kinds 7 and 8 are new in this release, so a v0.10.2 leader treats a voter on v0.10.1 or earlier as unable to answer: a raise to level 8 on a cluster still at level 7 is refused until every voter runs v0.10.2. A level already in force asks nobody, so a cluster that has taken v0.10.0's raise to 8 is not affected; the check matters for the first raise to a level above 8 in a later release (#706, #758).
- **Seven enums an older voter would call corrupt are reported as a newer format.** No byte written or read changes; an unknown tag is reported as "newer format" instead of "corrupt" (#743, #763).
- **The group-commit coordinator answers its leader first, and one member's refusal fails only that member.** No node calls it yet, so no behaviour changes in a deployment (#752, #780).
- **Advisory reviews.** Every ignored advisory carries a review date, and a check fails after 90 days (#819).
- **Tests, CI and docs.**
  - The live-broker Kafka suite runs on every Apache Kafka release from 3.7 to 4.4, through TLS and SASL listeners, and on Redpanda; the refusal tests check that wrong passwords, unknown users, a missing CA and a plaintext client against a TLS port are each refused within a bounded time (#721, #820, #834).
  - The archive engine is killed with `kill -9` at every state transition and at the commit boundary, against real Kafka, MinIO and SQLite, through a `failpoints` feature that is off in release builds (#725, #818).
  - A format registry and a check that the sources match it; a check that the versions copied into the docs match the workspace; a versioning, deprecation and support policy (#743, #783, #776, #777).
  - The managed PostgreSQL smoke no longer queues on unrelated pull requests, and the manifest compat job no longer fails on a release bump commit (#821, #817).

**Upgrading from v0.10.1.** Downgrade is not supported.

- **Run `vtopctl validate` with the v0.10.2 binary first.** The engine now refuses to start when a key, state-store, ledger or audit file can be reached by other local users (#775). A ledger written 0644 by an earlier release is refused unless the directory holding it is closed to others (owned by the engine's user, mode 0700); a file that is group- or world-writable is always refused. `validate` lists every refused file at once and changes nothing. Fix each by `chmod 0600` (`chmod 0700` on the directory), or accept a readable file by setting `security.allow_loose_secret_permissions: true`: each file accepted that way is announced with a WARN, counted on `vtop_secret_file_permission_overrides_total` and recorded as a `security.override` audit action. The override never accepts a writable file. No file's mode is changed by the engine. In a pod, mount a Secret as a whole volume with `defaultMode: 0400` and the chart's `fsGroup`; see [Secret files and their permissions](operate/secrets.md). This is not a one-way step: a v0.10.1 engine starts on the same files. The Helm chart runs the cluster plane, which does not apply this check. An earlier engine ignores the `security` section, so the override can be set ahead of the upgrade.
- **Otherwise a drop-in replacement.** Replace the binaries; start order does not matter, and v0.10.1 and v0.10.2 engines, voters and data nodes can run side by side. No schema (PostgreSQL stays at marker 3, SQLite at `user_version` 1), on-disk or wire format, and no cluster feature level (it stays 8) changes. No one-way step is added; v0.10.0's (feature level 8, `manifest.write_version: "0.4"`, `manifest.mac`, `upload.object_lock`) are unchanged. The two new peer-plane message kinds are used only to judge a raise to level 8 or above.
- **One new key.** `security.allow_loose_secret_permissions` (a new `security` section, default `false`, unknown keys inside it refused). The top level does not refuse unknown sections, so a v0.10.1 engine ignores it without a word. That is harmless: it never refuses the files this release refuses, so the override changes nothing there.
- **Batch size.** A `run` engine now seals at `max_bytes` where it sealed at about twice that. The number of objects per byte archived rises to what the setting says, up to about twice the count for a source that filled every window.
- **Object Lock.** Under `upload.object_lock`, a replay over an object that is not locked as configured now writes one extra locked object per range instead of failing it. An engine on v0.10.1 in a mixed fleet still stalls on such a range until it is upgraded. Watch for the `VtopObjectLockFallback` alert; Helm chart 0.6.1 renders it when `metrics.prometheusRule.enabled` is on.
- **Linux hosts.** Take the `-linux-gnu` archive on any glibc 2.17 or later host, or the `-linux-musl` archive on Alpine, an older glibc, or where no shared library may load; [binaries](install/binaries.md#which-linux-archive-to-pick) says how to tell. Verify the download as before, and with `gh attestation verify` ([release verification](RELEASE_VERIFICATION.md)).
- **Helm chart 0.6.1** (from 0.6.0; `appVersion` 0.10.2). No template or values key changes. The shipped alert rules gain `VtopObjectLockFallback`, `values.yaml` gains comments on `extraVolumes` and `extraVolumeMounts` for secret files from an external manager, and `ci/external-secret-values.yaml` is a rendered example.
- **Known open.**
  - Fixed in open pull requests, for v0.10.3, not in this release: run-mode stage timings in the `batch_metrics` log line (#843), the local-directory upload backend fsyncing the file and its directory before a write is reported done (#827), the stall when a source sits under a long directory name, whose derived names the filesystem refuses (#851), and the OpenTelemetry final flush waiting for spans another thread still holds, the cause of the intermittent `otel_messages` test failure (#830, #798).
  - Stage timings in v0.10.2: `vtopctl run` does not log them, but the Prometheus histogram `vtop_stage_duration_seconds` carries them when `VTOP_METRICS_ADDR` is set.
  - #725: the kill-and-restart suite covers the transitions; the second pull request for the issue is not in.
  - #724: the archives are built and smoked on the platforms above; the full matrix of supported platforms is not done. #731: the attestations first run on a real tag, so the issue stays open until a tag verifies them. #729: nightly fuzzing and the commit-path mutation gate are in; the gate on the verify, recovery and file-continuity code is not.
  - #752, #751: group commit is measured and its coordinator fixed, but not wired into the data node, and a Kafka client cannot yet follow a failover through a candidate node.
  - #706: the actor of a cluster command is in the admin endpoint's record only, not in the replicated log; only the voter check from #758 is in.
  - #743: the format registry and the compatibility policy are in; the tested downgrade matrix is not complete.
  - #773, #774: #626 follow-ups (adopting an earlier attempt's object on backends that cannot report absence; reporting failed attempts' objects no committed row names).
  - #726: the 72 hour soak has not run. #714: the bar for calling VTOP production-ready. VTOP remains a prototype.

## v0.10.3 — a Kafka resume that survives a recreated topic, durable local writes, and client certificates bound to their principal

A patch release: no schema, wire format, feature level or one-way step changes. It fixes where a Kafka partition resumes after a failed source commit or a recreated topic, makes the local-directory backend fsync before it reports a write done, unsticks a source under a long directory, and binds a native client certificate to the principal it acts as (a behaviour change for clients whose certificate names something else, described below). It also adds encryption at rest through S3 server-side encryption, two #626 follow-ups (adopting an earlier attempt's object on the command backends, and reporting the objects failed attempts left) and an end-to-end tamper suite. VTOP is still a prototype: the bar for calling it more is #714.

- **A Kafka partition resumes from the highest committed end of its topic's current id (bug).** The ledger resumed a partition from the newest-created committed row. A replay after a failed source commit can commit a lower range after a higher one, so a restart re-read a range already archived (a duplicate, never a loss), and pruning could delete the higher row. Taking the highest end alone is unsafe, because a topic deleted and created again numbers its offsets from zero. The Kafka progress marker in the ledger now records the topic's broker id (`topic_id`, from DescribeTopics, Kafka 2.8 and later), and a partition resumes from the highest committed end among the rows of the topic's current id. A recreated topic is a new id: the partition follows the group's committed offset, else `auto.offset.reset`, and never the old incarnation's offsets. Rows with no id (written before this release, or against a broker that gives none) keep the old rule, the newest row's end, and are never ranked against rows of a known id. If two committed rows of one id overlap, which a partition truncated by an unclean leader election can cause, that id falls back to its newest row's end: a re-read, never a skip. Pruning keeps the rows each answer depends on. The id is ledger only; manifests are byte for byte what released readers verify (#855).
- **A Kafka partition is resumed before it is assigned (bug).** When the adapter renewed its assignment it resumed partitions after `assign()`, and librdkafka could drop that resume as outdated, leaving a partition paused: it read as an empty topic until the stall check re-assigned it about 10 s later. Nothing was skipped, since the position was kept. The adapter now resumes before it assigns, in the full re-assign and in the stall check's incremental one (#877).
- **The local-directory backend fsyncs before a write is reported done (bug).** `localfs` copied each object, manifest and receipt to a staging file and renamed it onto the key without an fsync, and its verification read the bytes back from the page cache, so a power loss after `SOURCE_COMMITTED` could lose an object the ledger counted as archived, or a new directory with everything under it. The staging file is now fsynced before the rename and, after it, the key's directory, every directory the write created and the parent of each; the `.vtopck` sidecar is fsynced too. A failed sync fails the write. Durability still depends on the filesystem and device honouring `fsync`. S3 is not affected (#827).
- **A source under a long directory no longer stalls (bug).** Object names, batch ids, and the manifest, receipt and work-file names built from them embedded the whole source path, so a source under a directory of about 140 characters could produce a name over the 255-byte file-name limit: the write failed with `File name too long`, the range was handed back, and every later pass built the same name. Names are now bounded to a 200-byte stem. A name that fits is byte for byte what it was; a longer one keeps the last 48 bytes of the source token plus 16 hex digits of the SHA-256 of the full path, so a replay of the same range still computes the same key (#626). A `source=` partition component over 255 bytes is shortened the same way. Nothing reads the source path back out of a name (#851).
- **A native client certificate is bound to the principal it acts as (bug, security).** The data node's native produce/fetch plane admitted any client certificate the CA had signed, as long as the hello named the node's `principal_id`, which is not a secret. A TLS session is now admitted only when the chain-verified leaf certificate's Common Name is that `principal_id`, the field the metadata, replica and admin planes already read as an identity and the Helm chart already documented (`data.principalId`, the client certificate's CN). A refusal returns `Unauthorized` and is recorded as a `native.session` audit record ([Auditing](operate/auditing.md)), counted as `vtop_audit_events_total{action="native.session",outcome="refused"}`. A plaintext native plane has no certificate and admits as before; its refusals are now audited too. There is no key to turn this off (#854).
- **`vtopctl run` logs the per-stage timings of each committed batch (bug).** The `batch_metrics` line carried only `total_ms` and `state_write_ms`, though the docs said it carried every stage. It now also carries `compress_ms`, `checksum_ms`, `object_upload_ms`, `manifest_upload_ms`, `verify_ms`, `commit_ms` and `object_reused`, the values `process-once` prints, in whole milliseconds. No new timer runs on the batch path (#843).
- **The OpenTelemetry final flush waits for spans another thread still holds (bug).** The SQLite connection worker can hold a clone of the last batch's span for a moment after the engine finishes, so that span closed after the shutdown flush and was never exported: the cause of the intermittent `otel_messages` test failure. The final flush now waits up to 1 s for exported spans still open, then logs a WARN with the count. With nothing open it does not wait, and a second stop signal still cuts it short (#830, #798).
- **A command backend adopts an earlier attempt's object when its bytes verify.** On `awscli`, `minio` and `s3cmd`, which cannot tell a missing key from a refused request, a replay that rebuilds exactly the window of a failed attempt wrote another object under a fresh attempt key. It now reads back the object the failed attempt recorded and, when the stored bytes pass a strong verification against the rebuilt batch's digest, commits the replay under that key with no second put. Anything else (damaged, missing, size-only, outside the configured bucket and prefix) falls back to a fresh attempt key, and the recorded key is never written to. Each adoption costs one read-back. Counted as `adopted` or `adopt_rejected` in `vtop_object_key_decisions_total`, with `object_key_adopted` (INFO) and `object_key_adopt_rejected` (WARN) events. It needs a checksum and never applies under the Object Lock profile, which these backends cannot run (#773, #848).
- **`vtopctl audit-coverage` reports the objects failed attempts left.** An object a FAILED, REPLAY_REQUIRED or SUPERSEDED row recorded as uploaded, and that no row in a live or committed state names, is reported per source: count, recorded bytes, count by kind, and the keys, paged with `--orphans-limit` (default 200) and `--orphans-after`. It reads the ledger and the prune digests only: no LIST permission, no store request, nothing deleted. It is informational and does not change the exit status. Two gauges are written to the `--metrics-file`: `vtop_coverage_audit_unreferenced_objects` and `vtop_coverage_audit_unreferenced_object_bytes`, by `source_type` (#774, #845).
- **Encryption at rest through S3 server-side encryption.** VTOP asked the store for no encryption, so an object was encrypted at rest only if the bucket's default said so, and nothing in the engine could tell. `upload.server_side_encryption` (`mode: none` by default, `sse-s3` or `sse-kms`, with optional `kms_key_id` and `bucket_key` for `sse-kms`) asks the store to encrypt every object, manifest and verification receipt write, single-part and multipart. Verification then reads the encryption the store reports and fails the batch, as a checksum mismatch does, when it reports none, another algorithm or (with `kms_key_id` set) another key; the batch is not committed and is replayed. An object reused from an earlier attempt goes through the same check. Recovery's re-check, `vtopctl scrub` (the manifest and each receipt), `vtopctl tier copy` and `vtopctl verify` judge it too. `s3_native` only: any other backend refuses a mode other than `none` at load, and unknown keys inside the block are refused. With `sse-kms` and no `kms_key_id` the store's default key is used and only the algorithm is checked; a KMS alias is checked by algorithm only, and the engine says so at startup. Startup logs the mode and sets `vtop_server_side_encryption_mode{tenant,mode}`. SSE-C is out of scope ([configuration reference](configure/reference.md#uploadserver_side_encryption)) (#856).
- **Tests, CI and docs.**
  - An end-to-end tamper suite changes a stored archive the way someone with bucket write access (and, where stated, ledger write access) could, against a real S3 store and through the audit trail, and asserts what each defence reports, including the cases it does not catch. [Tamper evidence](reference/tamper-evidence.md) lists every case, its defence, its result and its test, and what is not protected; a test fails when the page and the tests drift apart (#859).
  - A threat map in the [security model](SECURITY_MODEL.md#17-threat-map): 67 threats across the archive engine, the audit trail, the cluster planes, the Kafka gateway, transport and the host, each with its control, its test and its residual risk, and the gaps listed. A CI check fails when a referenced file or test disappears (#732, #828).
  - The rolling-upgrade chaos scenario reads what the previous release supports, so `upgrade compat` is green again after v0.10.2 (#861).

**Upgrading from v0.10.2.** Downgrade is not supported.

- **Native client certificates: reissue any whose CN is not the node's `principal_id`, before upgrading a data node.** A v0.10.3 data node refuses a native TLS session whose client certificate's Common Name is not the node's `principal_id` (#854). Check each native client's certificate (`openssl x509 -noout -subject -in <cert>`) against the node's `principal_id` and reissue the ones that differ with `CN=<principal_id>`. A v0.10.2 node does not read the CN, so a reissued certificate works on both: reissue first, then upgrade. A refused client gets `Unauthorized`; the node logs the CN it saw and writes a `native.session` audit record. A plaintext native plane is unaffected. This is a behaviour change at session time, not a one-way step: nothing is written that a v0.10.2 node cannot read.
- **Kafka sources: nothing to do, and the first restart behaves as before.** Rows written by v0.10.2 carry no `topic_id`, so a partition resumes by the old rule (the newest row's end) until the engine has committed ranges under the topic's id (#855). Nothing changes when the broker gives no topic id (before Kafka 2.8) or the engine cannot ask it. The `topic_id` field is an optional key in the ledger's progress marker, not a one-way step: a v0.10.2 engine pointed at the same ledger ignores it, and resumes and prunes by its own rule, which allows the duplicate this release fixes ([compatibility policy](reference/compatibility.md)). Downgrade remains unsupported.
- **PostgreSQL: run `vtopctl migrate` from v0.10.3 before its engines start.** It adds two partial indexes on Kafka rows of `batches` (`idx_batches_kafka_max_end`, `idx_batches_kafka_id_newest`) that pruning uses. The schema marker stays 3, and the migration is idempotent. Until it runs, pruning is correct and slower. `CREATE INDEX` blocks writes to `batches` while it builds, so on a large ledger run it in a quiet window. SQLite creates the indexes itself on connect.
- **Sources under long paths.** Names of 200 bytes or less, which covers every source under a path of about 100 characters, are unchanged. A name that was 201 to about 241 bytes long is now shortened, so a replay of a range archived under the old name writes a new object instead of reusing the old one: a duplicate, never a loss, and the old object and its manifest stay verifiable (#851). An engine stuck on `File name too long` drains after the upgrade.
- **`localfs` writes cost an fsync of the file and its directories (#827).** Expect slower writes on that backend; the cost has not been measured. No setting changes.
- **New log fields, metric values and gauges.** The `batch_metrics` line gains seven fields, and the old ones are unchanged (#843). `vtop_server_side_encryption_mode` is new (#856). `vtop_object_key_decisions_total{decision}` gains `adopted` and `adopt_rejected` (#848); add them where a dashboard or alert lists the values. `audit-coverage` prints a new section of unreferenced objects and writes two new gauges to its `--metrics-file` (#845). With OpenTelemetry export on, a stop can take up to 1 s longer while spans close (#830).
- **Server-side encryption: upgrade every engine before setting `upload.server_side_encryption`, and keep a bucket default encryption.** `upload` has no `deny_unknown_fields` in v0.10.2, so a v0.10.2 engine ignores the key without a word and keeps writing objects unencrypted, unless the bucket's default encryption covers them. Keep a bucket default encryption as the floor while any engine is older, and after. Objects written before the mode was set stay as they were, and `vtopctl scrub` reports each as a mismatch (the store reports no encryption). A multipart upload begun before the mode was set completes unencrypted, fails verification and is replayed. With `sse-kms`, the engine's identity needs `kms:GenerateDataKey` to write and `kms:Decrypt` to read back (BLAKE3, or a multipart SHA-256); the [security model](SECURITY_MODEL.md#7-data-at-rest-and-object-immutability) lists the permissions. Leaving the key unset changes nothing (#856).
- **Otherwise a drop-in replacement.** Replace the binaries in any order; v0.10.2 and v0.10.3 engines, voters and data nodes can run side by side, apart from the native client certificates above. No schema marker (PostgreSQL stays at 3, SQLite at `user_version` 1), on-disk format other than the optional ledger field, wire format or cluster feature level (it stays 8) changes, and no one-way step is added; v0.10.0's (feature level 8, `manifest.write_version: "0.4"`, `manifest.mac`, `upload.object_lock`) are unchanged.
- **Helm chart 0.6.2** (from 0.6.1; `appVersion` 0.10.3). No template or values change.
- **Known open.**
  - #725: the kill-and-restart suite covers the transitions; the second pull request for the issue is not in.
  - #724: the full matrix of supported platforms is not done. #731: the attestations stay open until a tag verifies them. #729: the mutation gate on the verify, recovery and file-continuity code is not in.
  - #752, #751: group commit is not wired into the data node, and a Kafka client cannot yet follow a failover through a candidate node.
  - #706: the actor of a cluster command is in the admin endpoint's record only, not in the replicated log.
  - #743: the tested downgrade matrix is not complete.
  - The threat map's open gaps, listed in [its gaps table](SECURITY_MODEL.md#179-gaps): among them, the Kafka gateway has no TLS, SASL or ACLs, and the metadata, replica and admin servers have no connection cap.
  - #726: the 72 hour soak has not run. #714: the bar for calling VTOP production-ready. VTOP remains a prototype.

## v0.10.4 — two failed windows of one source no longer lose the lower one, and a bucket that stops versioning pauses writes

A patch release: no schema, wire format, feature level, config key or one-way step changes. It fixes a lost range in the archive engine that every release from v0.7.0 through v0.10.3 carries, pauses writes to a bucket whose versioning is suspended under a running engine, and fixes four cluster-plane faults the cluster soak (#881) found. The cycle pipelining work (#892, #895, #896) is **not** in v0.10.4. VTOP is still a prototype: the bar for calling it more is #714.

- **Two failed windows of one unit no longer lose the lower range (bug, data safety).** In v0.7.0 through v0.10.3 (introduced with #563), when two or more windows of one unit (a file at one inode, a syslog spool file, a Kafka partition) failed in the same cycle, the engine handed every failed start back to the source and the source applied them in turn, so the highest start won. The next pass read on from above the lower window, a later window committed past it, and a restart's rewind was clamped to that commit: the lower range was never archived. The engine now rewinds each unit once per content generation (a same-inode replacement keeps its own rewind), to its lowest failed start, and each source's replay resumes a unit at the lowest start it is given whatever their order, so neither layer alone can bring the loss back. Recovery after a restart already rewound to the lowest start (#552) and is unchanged. Reproduced through the engine for the file and spool sources and at the adapter for all three; every test fails without the fix. The [data-safety register](reference/data-safety-register.md#found-and-fixed-in-a-pull-request) gains a table for bugs found and fixed in a pull request, with this one as its first row (#891).
- **A bucket that stops keeping versions under a running engine is not written to until it does (bug).** With `upload.require_object_versioning` set, a bucket whose versioning was suspended under a running engine was written to on every retry: one manifest per retry, each failing "no object version to pin". A manifest PUT that comes back with no version, an object PUT with none when GetBucketVersioning then says versioning is not enabled, or a first-upload check that finds it not enabled, now pauses writes to that bucket: each batch for it fails before its first PUT and is replayed, the bucket is checked again after 5 s and then on a backoff doubling to 60 s, and writes resume once versioning is Enabled. Nothing commits meanwhile. A rate-limited ERROR `bucket_versioning_paused` names the bucket and says to re-enable versioning; the `vtop_bucket_versioning_paused` gauge and the `vtop_bucket_versioning_paused_batches_total` counter count it, and the new `VtopBucketVersioningPaused` alert pages after 5 minutes ([runbook](operate/runbooks.md#vtopbucketversioningpaused)). Without the key nothing changes (#883).
- **Cluster plane.**
  - **A full disk during a segment roll no longer poisons a node (bug).** A data node whose disk filled while a range's tail rolled panicked on every later request, `/metrics` included, until it was restarted. A failed roll now leaves the tail as it was, every reader keeps answering, and a write returns a storage error; once space is freed the next write rebuilds the tail from disk and takes the refused record at the refused offset (#886).
  - **A new leader catches a lagging follower up from its own log (bug).** A new leader could only resend what its own replication session had buffered, so a follower missing records that leader had taken while it was itself a follower was refused for good, and the range ran a replica short. The leader now reads its own log from the follower's offset and sends it as replica appends at its epoch (v2 ranges, fenced sessions). A follower below the leader's log start, which no append can reach and which needs a rebuild, is flagged by the `vtop_broker_follower_below_catch_up_horizon{follower}` gauge (leader only), a rate-limited warning and the Grafana alert "Follower below the catch-up horizon". A replica append batch refused part-way no longer leaves its earlier members uncommitted (#887).
  - **A candidate that cannot promote sits out three polls (bug).** A replica that could not reach a quorum could win the lease race again 30 to 50 ms after its lapse, every time, while caught-up replicas waited: the soak saw one win 21 epochs running. Once its quorum-miss budget is spent it now sits out three poll rounds, so a rival campaigns first; a range with a single candidate waits three poll intervals before it campaigns again, about 6 s at the default `poll_interval_ms` of 2000. Refused lease acquisitions and replicas a promotion probe could not fence are now logged, rate-limited (#888).
  - **A broken epoch history is logged, exported and alerted (bug).** A replica whose epoch-journal write failed (a full disk at an epoch adoption) was shut out silently. The refusal is unchanged, but the first break logs one ERROR naming the replica, the range and the failed write, and saying what to do (free space, then `vtopctl node repair`, not a restart); `vtop_broker_epoch_history_broken{topic,range}` exports it and the new `VtopEpochHistoryBroken` alert pages ([runbook](operate/runbooks.md#vtopepochhistorybroken)). [Epoch history after a failed write](EPOCH_HISTORY_AFTER_A_FAILED_WRITE.md) is a design note on self-healing; none of it is implemented (#894).
- **A state-store write-latency SLO.** The new `VtopStateStoreWritesSlow` alert (ticket) fires when more than 5% of committed batches spend over 1 s in ledger writes over 15 minutes ([runbook](operate/runbooks.md#vtopstatestorewritesslow)) (#872).
- **Tests, CI and docs:** a drilled backup and restore of the ledger with measured RPO and RTO, [Backup and recovery](operate/backup-and-recovery.md) (#734, #829); metadata directories written by v0.8.0, v0.9.0 and v0.10.1 open under this build, and the compatibility matrix runs published, signature-checked binaries (#743, #846, #771); a 72-hour engine soak harness with a ci-smoke profile (#838); the engine crash sweep on PostgreSQL, a file source, a store outage, ENOSPC and a syslog spool (#725, #836, #871); live-chaos operation histories and their checker, with `vtop-node produce --history` and `--fenced-probe` and `verify --dump` (#728, #878); a Kafka live suite on a three-broker cluster (#721, #874); a power-loss model test of `localfs` (#869); a metadata decoder fuzz target (#857); the workspace's tests nightly on macOS arm64, macOS x86_64 and aarch64 Linux (#724, #876); a report-only release job that rebuilds each archive and compares the binaries (#731, #823); a generated list of the config keys an older engine ignores (#858); a component status page and a check that the docs claim no more (#740, #826); benchmark results schema 2 and the competitor pair on a hosted runner (#769, #610, #650); and the README's license section (#884).

**Upgrading from v0.10.3.** Downgrade is not supported.

- **Nothing to do for the #891 fix.** Replace the binaries. A ledger written by v0.7.0 through v0.10.3 can be checked for a range the bug lost: run `vtopctl audit-coverage --config <config>` (add `--check-storage` to also probe the objects) and look for a unit with a hole below a later commit, an unarchived lower range. A range the bug lost still has its FAILED or SUPERSEDED row (or a prune digest of one), so it is reported as a failed hole and the command fails, for Kafka partitions as for files; a Kafka gap that no failed row explains is reported without failing the command, so read the report as well as the exit status. What a hole means and what to do is in [Scrub and coverage audit](operate/scrub-and-audit.md#coverage-audit).
- **`upload.require_object_versioning`: writes to a bucket whose versioning is suspended now pause instead of retrying (#883).** Load the new rules (`observability/prometheus/vtop-rules.yaml`, or the chart's PrometheusRule) so `VtopBucketVersioningPaused` pages. Without the key nothing changes.
- **New metrics, alerts and log lines.** `vtop_bucket_versioning_paused`, `vtop_bucket_versioning_paused_batches_total` (#883), `vtop_broker_follower_below_catch_up_horizon` (#887) and `vtop_broker_epoch_history_broken` (#894) are new. The Prometheus rules gain `VtopBucketVersioningPaused`, `VtopStateStoreWritesSlow` and `VtopEpochHistoryBroken` (#883, #872, #894), and the Grafana alerts gain "Follower below the catch-up horizon" (#887). New ERRORs: `bucket_versioning_paused` and a broken epoch history; new rate-limited lines for refused lease acquisitions and unfenced replicas (#888).
- **Otherwise a drop-in replacement.** Replace the binaries in any order; v0.10.3 and v0.10.4 engines, voters and data nodes can run side by side. No config key or default changes (`scripts/config-keys-since.py v0.10.3` lists none), and no schema marker (PostgreSQL stays at 3, SQLite at `user_version` 1), on-disk format, wire format or cluster feature level (it stays 8) changes; no one-way step is added. A v0.10.4 leader's catch-up goes through the replica append path a v0.10.3 follower already runs, and the wire format is unchanged, so mixed versions work. The follower did change in one place (#887): a batch refused part-way now commits its earlier members instead of leaving them uncommitted.
- **Helm chart 0.6.3** (from 0.6.2; `appVersion` 0.10.4). No template or values change; the chart's copy of the alert rules gains the three alerts above.
- **Not in this release.** The cycle pipelining work (#892, #895, #896) is not in v0.10.4.
- **Known open.**
  - #724: the full matrix of supported platforms is not done. #731: the attestations stay open until a tag verifies them. #729: the mutation gate on the verify, recovery and file-continuity code is not in.
  - #752, #751: group commit is not wired into the data node, and a Kafka client cannot yet follow a failover through a candidate node.
  - #706: the actor of a cluster command is in the admin endpoint's record only, not in the replicated log.
  - #743: the tested downgrade matrix is not complete. #728: the history checker covers two live-chaos scenarios.
  - The threat map's open gaps, listed in [its gaps table](SECURITY_MODEL.md#179-gaps): among them, the Kafka gateway has no TLS, SASL or ACLs, and the metadata, replica and admin servers have no connection cap.
  - #726: the 72 hour soak has not run; its harness is in. #881: the cluster-plane soak is not in. #714: the bar for calling VTOP production-ready. VTOP remains a prototype.

## v0.11.0 and beyond

The theme is **the gateway at the speed its durability allows, and the road
to 1.0**: turn group commit on by default for the data node's quorum write
(#752; it is wired and off until the A/B gates hold) and let a
Kafka client follow a failover through a candidate node (#751: the gateway
is served from candidate nodes and live-chaos scenarios 27 and 28 show a
librdkafka and a JVM client resuming 5.5 s to 6.6 s after a leader kill on one
host, with debug binaries and a 6 s lease; what is left is the chart's default
lease, the co-located `vtop-node node` shape, and a consumer group's
coordinator across a failover, which #751 scopes out); finish the
format inventory and the tested upgrade and downgrade matrix (#743); and work
the criteria in #714 that must be met before VTOP is called anything but a
prototype (crash and soak testing, real S3 targets, security hardening, a
restore drill, a pilot). Failover and crash-durability measurements against
Apache Kafka stay open (#597, #599; the failover run has not been repeated
through a Kafka client), as does the head-to-head throughput run
(#596).

### Merged since v0.10.1

Notes for the next release, gathered here until its version-bump PR
folds them into its section.

- **Applied records are delivered at least once (#706).** Each metadata
  replica keeps an audit frontier (`meta.audited`, a new file older releases
  ignore) saved only after its sinks accepted the records, so a crash or a
  refusing audit file re-delivers them (marked `replayed`, from the log) at
  the next apply, every 10 seconds and at restart, instead of losing them.
  Applied records of commands carry the entry's term and are timed by the
  entry's `issued_at_ms`, so every replica writes the same record. Entries a
  replica never applied one by one (a snapshot install, a log purged past a
  frontier that fell behind) get one `audit.gap` record naming the range.
  New metadata-node metrics: `vtop_audit_replayed_total{reason}`,
  `vtop_audit_gaps_total{reason}` and `vtop_audit_delivery_lag_entries`. A
  directory from an older release starts its trail at its applied index.
- **The audit file is safe to share between processes (#706).** A node and
  `vtopctl` runs that append to one `VTOP_AUDIT_LOG` take an exclusive lock
  on the file for each append, so a failed append is cut back without
  deleting another process's record. A writer from v0.10.1 or earlier takes
  no lock.


**Upgrade note for v0.11.0: the Helm chart turns admin authorization on by
default (#732, M2).** `meta.adminAuthorization.enabled` now defaults to
`true`. An existing install that never set it now renders only when
`meta.adminAuthorization.operatorCommonNames` names the Common Names of your
operator certificates. Otherwise `helm upgrade` stops at render with a message
naming the three ways out, and nothing is changed in the cluster. Before
upgrading:

- Find the CN of each certificate your `vtopctl` uses against the admin plane
  (`openssl x509 -noout -subject -in operator.pem`), and add it:
  `--set 'meta.adminAuthorization.operatorCommonNames={vtop-admin}'`.
- Once the policy is on, other certificates the CA signed, data nodes' included,
  can no longer run `meta init`, change membership or grant leases. A data
  node keeps its own lease, version report and group cursors. From metadata
  feature level 9 a data node's lease *release* at shutdown names its
  holder, and the policy admits it from the node's own certificate (#850,
  #870). Raise to level 9 after upgrading (every metadata voter and data node
  first, then `vtopctl meta enable-features --level 9`), and releases are
  immediate. Below level 9 a release names no holder, so the policy refuses
  it as an operator's command and the lease lapses on its deadline (15 s by
  default): a planned handover waits that long.
- A cluster administered out of band can set `allowNoOperators: true` (nobody
  may run cluster-scoped commands). A lab can opt out with `enabled: false`;
  the install notes then print a WARNING. A plaintext admin plane
  (`transport.admin: plaintext`) needs the opt-out.
- Turning the policy on changes no on-disk or wire format and is not a
  one-way step: setting `enabled: false` again removes it on the next
  rollout.

---

## Versioning

0.x. Minor versions may break compatibility. Every release so far is published
as a full GitHub release; only a hyphenated version such as `1.0.0-rc.1` would
be published as a pre-release. What each format promises, the rules for
changing one, the deprecation notice and the path to 1.0 are in the
[compatibility policy](reference/compatibility.md) (#743). **Upgrades are
tested** (#291): CI proves a data directory written by the newest patch of
each released minor series (v0.2+) opens under the current build, serves
byte-exactly, and still serves after a restart; newly published tags join that
matrix automatically. **Downgrades are not supported**, except for what the
[compatibility matrix](reference/compatibility.md#downgrade) records for the
previous release (the state ledger, its prune digests and the audit trail): a
newer release's directory may hold artifacts an older release cannot
interpret, and nothing else tests that path; going back means a fresh
directory or a verified backup taken before the upgrade.

Which releases get fixes, what a 0.x minor may break, and what 1.x will
commit to are in [Versioning and support](SUPPORT.md).
