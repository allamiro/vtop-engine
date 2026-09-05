# Roadmap

What shipped, what is next, and — where it is useful — what a release taught us
that the plan did not anticipate.

Milestones are the source of truth for scope; this file is the narrative. It is
written for someone deciding whether to depend on a version, so it says plainly
what each release does **not** yet do.

---

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

## v0.6.0 and beyond

The theme is **the gateway's second phase and the evidence gaps the first
one exposed**: consumer groups and idempotent producers over lineage-bound
cursors (#457), then per-topic virtualization with verifiable offset
translation (#458); the replica plane refusing what it cannot frame (#453);
the published watermark that never regresses (#449); the retention-loss
alert (#454); a kind suite that waits on conditions rather than deadlines
(#416). Behind those: the FIPS story (#296) and the adaptive source side
(#100, #101) whose target side shipped here.

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
