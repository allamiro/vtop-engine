# VTOP-native broker direction

## Decision

VTOP's target is a native Rust log-storage and Pub/Sub system. VTOP owns the
durable partitioned log and every mechanism that determines cluster
correctness. Kafka is an optional compatibility and ingestion adapter; it is
not the VTOP membership service, consensus layer, placement authority,
consumer-group coordinator, or progress store.

The existing verified archive engine remains supported. Its verify-before-
advance pipeline becomes a natural foundation for segment verification,
repair, retirement, and object-store tiering. PostgreSQL and object storage
remain optional integrations for that pipeline, not substitutes for the
native broker's control plane or replica protocol.

## Ownership boundary

VTOP owns:

- partitioned append-only records, active segments, immutable sealed segments,
  ranges, and topics;
- topic epochs and split/merge lineage;
- replica leadership, quorum writes, committed offsets, and fencing epochs;
- authoritative metadata, replica placement, membership, and failure handling;
- native consumer groups, checkpoints, assignment, and incremental rebalance;
- segment verification, repair, retention, and optional verified tiering.

Small Rust crates may supply algorithms or execution primitives behind narrow
VTOP-owned traits. An initial control-plane implementation may evaluate
`openraft`; a later membership layer may evaluate `foca`; deterministic tests
may evaluate `madsim`. None of those libraries defines VTOP's wire protocol,
metadata state machine, storage format, replication rules, placement policy,
or operator semantics.

## Structural model

The model follows the useful invariants in Northguard without copying its
dependency stack:

```text
records -> active/sealed segments -> buddy-aligned ranges -> topics
```

- A segment is the small, immutable unit of replication, verification,
  repair, retention, placement, and tiering.
- Ranges preserve explicit split/merge lineage and happens-before ordering.
- New segments are striped over eligible brokers. New capacity therefore
  receives new work without a mandatory central reshuffle of old segments.
- Metadata begins with one correct three-node Raft group while its keys retain
  a future shard boundary. Sharding follows measured hotspots, not a guessed
  vnode count.
- Produce, fetch, and replica traffic use sessionized framed streams,
  pipelining, byte windows, and acknowledgements that never exceed the
  committed point.
- Placement filters on administrator-defined failure-domain constraints, then
  uses a deterministic auditable score such as weighted rendezvous hashing.

VTOP does not initially require RocksDB, Direct I/O, `io_uring`, or a second WAL
copy. The baseline stores a record body once in a self-describing segment,
uses ordinary buffered I/O plus an explicit durability barrier, and derives a
sparse index and manifest from those bytes. Faster platform-specific storage
implementations must preserve the same contract and earn their complexity with
measurements.

## Storage kernel: first executable milestone

The `vtop-log` crate is the first implementation slice. It intentionally has no
Kafka, database, object-store, networking, or consensus dependency. Its v1
contract provides:

- a checksummed canonical segment header and checksummed record frames;
- producer ID and monotonically increasing sequence numbers, with byte-exact
  retry deduplication within a segment;
- append groups validated before writing and committed with one explicit
  durability barrier;
- hard per-record, append-group, segment-byte, and segment-record limits;
- strict byte- and record-bounded fetches;
- active-to-sealed transition, immutable sealed data, a BLAKE3 content root,
  and a deterministic JSON manifest;
- sparse indexes that are verified against and rebuilt from segment data;
- range lineage types before split and merge are enabled;
- recovery that truncates only an incomplete final frame and treats invalid
  magic, lengths, or checksums as corruption.

This slice is not yet a broker. It does not claim replication, network
streaming, Raft, placement, consumer groups, retention, or the full
proof-carrying-segment design. Those capabilities must compose around the
storage contract rather than weaken it.

## Implementation order

1. Feature-gate Kafka and define source/broker-neutral partition and cursor
   contracts. Preserve Kafka only as an adapter.
2. Finish and crash-test the single-node segment log and local produce/fetch
   API.
3. Add a persistent three-node Raft metadata prototype behind a VTOP-owned
   consensus interface.
4. Add partition leader/follower replication, quorum acknowledgement, fencing
   epochs, committed-point propagation, verification, and repair.
5. Add native consumer groups and durable lineage-aware checkpoints.
6. Add constraint-based deterministic placement and safe incremental
   rebalancing.
7. Scale membership, add deterministic simulation and chaos tests, implement
   retention, and add verified object-store tiering.
8. Measure end-to-end behavior before making throughput or fleet-size claims.

## Candidate differentiators to preserve

These are engineering directions, not novelty claims:

- **Proof-carrying segments:** canonical manifests, BLAKE3 roots, range
  lineage, and authenticated commit statements make stored content
  independently verifiable.
- **Verified retirement:** a replica or local extent cannot be removed until
  the replacement replica or tiered object has been content-verified.
- **Lineage-aware cursors:** progress binds the topic epoch, range, segment
  identity/root, and record position so split/merge traversal is unambiguous.
- **Risk-adaptive sealing:** bounded sealing policy may include replica health,
  repair time, tiering backlog, and recovery objectives as well as size/time.
- **Two-stage durability:** active data uses full quorum replication; verified
  sealed data may later use erasure coding or object tiering without losing its
  authenticated identity.
- **Deterministic simulation:** network, clock, disk, RNG, restart, truncation,
  and corruption faults are injectable rather than hidden behind concrete
  runtime types.

Any future design that makes Kafka, PostgreSQL, S3, or another platform a
correctness dependency for the native cluster conflicts with this decision.
