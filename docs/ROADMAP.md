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

## v0.3.0 — the durability keystone

The theme is **a replica that is lost can be replaced, and the replacement is
proven before the original is retired.**

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
feature rather than test its parts:

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
- Lowering a roll threshold takes effect from the next roll. The current tail
  keeps the limits written into its header, because that file already exists.

## v0.3.1 — the papercuts this release exposed

- **#310** — a `kill -9` during a commit write leaves a quarantined `.tmp` and
  the node refuses to restart. The losing side of a rename is garbage by
  definition and should be swept, not judged.
- **#240 remainder** — the §5.4.1 election-restriction question, then the signed
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
