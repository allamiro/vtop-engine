# VTOP vs Kafka vs LinkedIn Northguard/Xinfra

An honest positioning document: where VTOP is genuinely ahead, where it is
behind, and which roadmap item closes each gap. Claims about VTOP are grounded
in this repository; claims about Kafka reflect Apache Kafka with KRaft; claims
about Northguard/Xinfra come from LinkedIn's public materials (their system is
closed source, so that comparison is necessarily against what they have
published, not against their code).

VTOP is a **prototype / reference implementation** (0.x pre-releases). This
document compares architectures and guarantees, not production mileage —
Kafka and Northguard both carry years of at-scale operation that no design
argument substitutes for.

## The one-paragraph version

VTOP's bet is **evidence over convention**: every durability, repair, tiering,
and (since #223) leadership-transition claim is backed by something a third
party can re-verify — content roots, sealed segments, byte-exact offline
verification, commit-ordered proofs — enforced by a deterministic metadata
state machine with strictly monotonic fencing epochs. Kafka's bet is ecosystem
and operational maturity; Northguard's bet is decentralized metadata and
range-based scale within LinkedIn. VTOP shares Northguard's range/segment data
model and Kafka's log semantics, and aims to beat both on *provability* while
using a wire-compatibility gateway (#225) to avoid fighting Kafka's ecosystem
head-on.

## VTOP vs Apache Kafka

### Where VTOP is ahead

| Area | VTOP | Kafka |
|---|---|---|
| Leader election safety | Leadership is a metadata **lease**; acquisition mints `fencing_epoch + 1` through a linearizable Raft proposal, so the old leader is fenced **by construction**. There is no unclean-election knob to misconfigure. | ISR-based election; `unclean.leader.election.enable` exists and, when enabled (or mis-set), silently trades durability for availability. |
| End-to-end verifiability | Content roots per segment; sealed artifacts verified **offline and byte-exactly** (`vtopctl segment verify`); repair, retirement, and tiering commit only after **evidence** (`CommitReplacementProof`, `CommitTierEvidence`) with verifier identity and epoch recorded. These are metadata-log commits — distinct from the archive protocol's *source-progress* commit ([VTOP_PROTOCOL_DRAFT.md §3, §13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule)). On the archive path, verification is graded strong / backend-limited / disabled (§17), with strong the default and non-strong commit requiring explicit opt-out. | Checksums protect against corruption in flight/at rest, but there is no end-to-end, third-party-re-verifiable proof chain for replicas, repairs, or tiered copies. |
| Source-commit safety (archive path) | The commit rule ([§13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule)): a Kafka offset / file cursor / spool position is never advanced until the object **and** its bound manifest are durably stored and verified; crash recovery is specified (§14) and replay never loses data. | `enable.auto.commit` and consumer-side offset management leave verify-before-commit discipline to each application. |
| Determinism and test rigor | Pure deterministic metadata state machine (no serde, hand-coded bounded codecs, golden byte vectors); crash sweeps drive the exact production byte paths at every write boundary; mutation testing in CI; deterministic fault harness plus a 15-scenario live-chaos suite (kill -9, disk-full, fsync-EIO, partitions, clock skew, live failover, co-location, admin authorization, replica replacement, restart-free candidate failover). | Strong integration test culture, but replay determinism and byte-exact snapshot equivalence are not design invariants, and crash-point sweeps of the storage engine are not part of CI. |
| Clock independence | Lease expiry is computed from data **in the replicated log** (`issued_at_ms` + duration), so every replica derives the same deadline; skew affects liveness, never safety (proven live in the clock-skew scenario). | Broker-side time is load-bearing in more places (log rolling, retention, transaction timeouts); safety does not depend on it, but the boundary is less explicit. |
| Failover that leaves evidence | Verified promotion establishes the committed boundary from a **quorum of replica disks** before a new leader serves, refuses when the candidate's own log is behind, and the arc ends (#240) with a signed leadership-transition statement. | High-water-mark handling on failover is correct post-KIP-101 but leaves no independently checkable record of what a transition decided. |
| Supply chain | Releases ship SBOM, provenance attestations, cosign-signed images and binaries, checksums; `--locked` builds. | Distribution-dependent; upstream Apache releases are signed but SBOM/provenance are not first-class. |

### Where VTOP is behind — and what closes it

| Gap | Honest state | Closing item |
|---|---|---|
| Epoch-qualified truncation (KIP-101/KIP-279) | Largely closed in v0.2.0: per-replica epoch history, bounded divergence truncation, fence-and-read reconciliation. The remaining piece is the §5.4.1-style election restriction and the signed leadership-transition record. | #240 remainder |
| Transactions / EOS | Kafka has cross-partition transactions and exactly-once streams. VTOP has producer idempotency (epoch + sequence dedup) — enough for exactly-once produce per range, no cross-range transactions. | Not scheduled; deliberate scope cut for now |
| Consumer-group sophistication | Kafka has incremental cooperative rebalancing, static membership, regex subscriptions. VTOP has groups, cursors-in-metadata, heartbeats, and assignment — the minimum honest core. | Grows with #225's needs |
| Ecosystem | Clients in every language, Connect, Streams, MirrorMaker, ksqlDB, decades of operational tooling. VTOP has `vtopctl` and dashboards-as-code. | #225 (Kafka wire-compatibility gateway) — inherit the client ecosystem instead of rebuilding it |
| Scale features | Quotas, throttled reassignment, multi-tenancy, tiered storage at scale, thousands-of-partition operations. | Post-v0.2.0; benchmarks tracked in #92/#98/#130 |

## VTOP vs LinkedIn Northguard/Xinfra

Northguard is LinkedIn's closed-source log storage successor to Kafka;
Xinfra is their virtualized pub/sub layer over both. Public materials
describe range-based sharding with dynamic splits, segment-oriented storage,
decentralized/sharded metadata, spread placement, and self-balancing.

### Where the designs agree

VTOP's data model is the same family: **key-range shards** (not fixed
partitions) with lineage across splits (`RangeLineage`, parent links),
segment-based storage with generations, and deterministic weighted-rendezvous
placement over failure domains. This is convergent evolution on the same
Kafka pain points: fixed partitioning, coupled metadata, opaque rebalancing.

### Where VTOP is ahead (of what they have published)

- **Open source.** Northguard is internal to LinkedIn; every claim here can be
  read, audited, and re-verified in this repository.
- **Evidence chain.** Nothing published about Northguard claims third-party
  re-verifiable proofs for repair, tiering, or leadership transitions;
  VTOP treats them as first-class records in the metadata log.
- **No unclean path by construction.** Northguard's published material
  describes strong consistency; VTOP additionally makes the fencing argument
  mechanical: epochs are minted by a linearizable state machine whose golden
  vectors and crash sweeps are in CI.
- **Auditable size.** A small, deterministic core that one person can read is
  a feature; it is also, honestly, a consequence of being early.

### Where VTOP is behind — and what closes it

| Gap | Honest state | Closing item |
|---|---|---|
| Metadata scale-out | Northguard shards metadata so no central group is a bottleneck. VTOP runs **one** metadata Raft group; saturation characteristics are researched (`METADATA_SATURATION_RESEARCH.md`) but sharding is not built. | Post-v0.2.0 arc; research doc is the seed |
| Live range splitting / self-balancing | VTOP models lineage and placement but does not yet split ranges under load or self-balance placements live. | Post-v0.2.0; placement plane (#180/#181) is the foundation |
| Virtualization / migration layer | Xinfra gives LinkedIn topic virtualization and transparent migration between backends. VTOP has nothing equivalent; #225's gateway is the nearest analog (protocol-level rather than address-level). | #225, then revisit |
| Production mileage | Northguard runs LinkedIn-scale traffic (trillions of records/day, per their materials). VTOP's scale story is a benchmark plan, not a track record. | #92 / #98 / #130 sweep grid and soaks |
| Multi-tenancy | Published Northguard material emphasizes tenant isolation. VTOP is single-tenant today (one principal per range in the authorizer). | After #238 lands the identity layer |

## What "better" means here, concretely

The strategy is not to out-Kafka Kafka or out-scale Northguard this year. It
is to be the system whose **claims are checkable**:

1. **Close the correctness gaps the incumbents already closed** — #239
   (epoch propagation) and admin authorization (#238) landed in v0.2.0; the
   #240 remainder (election restriction, signed transition record) is what's
   left — so parity is real, not asserted.
2. **Keep the evidence advantage** — every new mechanism (repair, tiering,
   failover) lands with proofs and offline verification, which neither Kafka
   nor Northguard's public material offers.
3. **Borrow the ecosystem instead of fighting it** — #225 speaks Kafka's wire
   protocol so existing clients work, the way Northguard hides behind Xinfra
   rather than migrating every producer.
4. **Stay honest in public** — gaps live in the issue tracker and in module
   docs (`promotion.rs` names its own limitations), not in a footnote. This
   document is part of that: when a row above stops being true, change it.

*Related reading: [`VTOP_PROTOCOL_DRAFT.md`](VTOP_PROTOCOL_DRAFT.md) (the
normative archive protocol: commit rule §13, verification semantics §17),
`ARCHITECTURE.md`, `NATIVE_BROKER_ARCHITECTURE.md`, `LIVE_CHAOS_VALIDATION.md`,
`PRODUCTION_HA.md` (§19 Known limitations), and the roadmap issue #243.*
