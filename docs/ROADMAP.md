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
- **#240 remainder** — the election-restriction question (Raft dissertation §5.4.1), then the signed
  leadership-transition record.

## v0.4.0 and beyond

Retention and reclamation (#290), a Helm chart that can deploy a replicated
range and separate metadata from data (#284, #287), orderly shutdown (#280),
cross-version data-directory compatibility testing (#291), and TLS as a
configured capability rather than a hard dependency across every deployment
method (#294). FIPS (#296) and the Kafka wire-compatibility gateway (#225) sit
behind those.

---

## Versioning

0.x. Minor versions may break compatibility, and releases are published as
GitHub pre-releases to say so. Data-directory compatibility across versions is
**not yet tested** — that is #291, and until it lands, treat an upgrade as
requiring a fresh directory or a verified backup.
