# Metadata group saturation research (#192)

Research measurements for the **single three-node metadata Raft group**.
This is **not** an architecture redesign and does **not** implement
multi-group / sharded metadata.

Epic [#93](https://github.com/allamiro/vtop-engine/issues/93) dependency
policy: ship one correct metadata group first (foundation
[#167](https://github.com/allamiro/vtop-engine/issues/167),
[#171](https://github.com/allamiro/vtop-engine/issues/171),
[#174](https://github.com/allamiro/vtop-engine/issues/174)). Shard only
after measured need — key space already carries a shard prefix
(`META_SHARD_ID = 0`) so a future control-plane split changes bytes, not
shape.

## Goals

1. Measure when the single three-node group approaches saturation under
   representative metadata workloads.
2. Emit machine-readable JSON covering commands/s, entity growth
   (topics / ranges / segments), placement updates, consumer heartbeats,
   cursor commits, snapshot size/duration, recovery duration, and
   leader-CPU / p99 latency **proxies**.
3. Publish **quantitative sharding-trigger criteria** with an explicit
   gate that stays closed on lab-limited CI runs unless thresholds trip.
4. Keep the **non-goal** explicit: no multi-group implementation in this
   issue.

## How to run

```bash
# CI / smoke (also covered by workspace tests)
cargo test -p vtop-meta --test metadata_saturation_harness --locked

# Emit machine-readable JSON
mkdir -p benchmarks/results/native-meta-saturation
VTOP_META_SATURATION_JSON=benchmarks/results/native-meta-saturation/summary.json \
  cargo test -p vtop-meta --test metadata_saturation_harness --locked -- --nocapture

# Optional extended entry point (larger entity counts; same schema)
cargo test -p vtop-meta --test metadata_saturation_harness --locked -- --ignored --nocapture
```

JSON lands wherever `VTOP_META_SATURATION_JSON` points. The directory is
created if missing. `benchmarks/results/` is git-ignored.

## Harness substrate

| Layer | What it measures |
|-------|------------------|
| In-process three-node openraft cluster (`SimStorage`, paused-clock tokio, explicit heartbeats) | Propose/commit rate and latency for metadata commands |
| Durable `MetaStorage` on `SimStorage` | Snapshot encode/write size + duration; reopen recovery duration |
| Process `getrusage` (Unix) | CPU user+sys during Raft scenarios (**process** proxy, not leader-only) |

This is a **lab harness**, not a production soak fleet. Wall-clock numbers
are noisy under CI shared runners; use relative comparisons on one host
and dedicated hardware for gate decisions.

## Workloads

| Scenario | Intent |
|----------|--------|
| `raft_mixed_ops` | Mix of heartbeats, cursor commits, segment registrations |
| `raft_heartbeat_storm` | Sustained `HeartbeatMember` proposes |
| `raft_cursor_commits` | Sustained `CommitGroupCursor` CAS advances |
| `raft_topic_range_growth` | `CreateTopic` (+ root range) growth |
| `raft_segment_registration` | Lease + sealed segment registration growth |
| `raft_placement_updates` | Placement attrs + `CommitSegmentPlacement` |
| `storage_snapshot_growth` | Encode/write snapshot after entity growth |
| `storage_recovery` | Reopen duration after snapshot + log tail |

CI default counts are small enough for `cargo test --workspace --locked`.
The `--ignored` extended path multiplies entity counts (~4×) for a
heavier local lab sample.

## Metrics

| Metric | Definition |
|--------|------------|
| Commands/s | Successful Raft `client_write` completions / wall seconds (includes apply wait) |
| Propose latency p50/p95/p99 | Wall ms for `client_write` only (excludes follower apply wait) |
| Commit latency p50/p95/p99 | Wall ms for propose + cluster apply wait |
| Entity inventory | Topics, ranges, sealed segments, consumer groups/members, placements |
| Snapshot bytes | Encoded state-machine snapshot payload size |
| Snapshot duration | Wall ms for `MetaStorage::write_snapshot` (and encode-only split) |
| Recovery duration | Wall ms for `MetaStorage::open_with` after reboot |
| CPU user/sys ms | `getrusage(RUSAGE_SELF)` delta over the Raft scenario |

### Honest proxies

- **Leader CPU**: process-wide user+sys during the scenario. The harness
  does not sample a dedicated leader OS process.
- **p99 metadata latency**: in-process openraft propose/commit latency,
  not admin/mTLS RPC RTT from a remote client.
- **Saturation**: inferred from throughput plateau / latency inflation /
  snapshot-recovery cost versus the quantitative thresholds below — not
  from production SLOs.

## Sharding trigger criteria (quantitative)

The JSON `recommendation` object always includes:

- `default_path`: `single_three_node_metadata_group`
- `pursue_multi_group_sharding`: `true` only when **at least two** of the
  following trip on a **dedicated three-node lab soak** (not CI smoke):
  1. mixed-ops commit **p99 > 50 ms** sustained across a ≥10-minute soak
  2. scaling the mixed entity mix **4×** yields **<1.25×** commands/s
     (throughput plateau)
  3. snapshot payload **> 64 MiB** **or** snapshot write **> 2000 ms**
  4. clean reopen recovery **> 5000 ms**
- `gate`: Epic #93 — open a multi-group design spike only after measured
  need; do not shard preventatively.

CI / macOS / shared-runner lab runs are expected to keep
`pursue_multi_group_sharding=false` and must say so in `lab_limits`.

### Default recommendation (this harness version)

**Keep the single three-node metadata group.** Do not implement
multi-group metadata sharding until a dedicated lab soak trips at least
two criteria above and a follow-up design spike owns the sharding plan.
Prefer profiling admin/mTLS path and data-plane load before splitting the
control plane.

## Non-goals

- Multi-group / sharded metadata Raft implementation
- Changing `META_SHARD_ID` or key layout beyond the existing prefix
- Multi-hour production soaks on dedicated hardware (deferred; harness
  remains the reproducible measurement entry point)
- Claiming production capacity numbers from CI timings

## Deferred

- Multi-hour cluster soaks on dedicated hardware
- Remote admin/mTLS latency under load
- Leader-only CPU via per-node processes / cgroups
- Actual multi-group sharding code (separate issue after gate trips)

## Related

- Epic dependency policy: [#93](https://github.com/allamiro/vtop-engine/issues/93)
- Meta foundation: [#167](https://github.com/allamiro/vtop-engine/issues/167),
  [#171](https://github.com/allamiro/vtop-engine/issues/171),
  [#174](https://github.com/allamiro/vtop-engine/issues/174)
- Issue: [#192](https://github.com/allamiro/vtop-engine/issues/192)
- Architecture: [NATIVE_BROKER_ARCHITECTURE.md §4.2](NATIVE_BROKER_ARCHITECTURE.md#42-initial-three-node-cluster)
  and [§15.2](NATIVE_BROKER_ARCHITECTURE.md#152-benchmarks)
- Harness: `crates/vtop-meta/tests/metadata_saturation_harness.rs`
- Archive / native research companions: [`benchmarks/README.md`](../benchmarks/README.md)
