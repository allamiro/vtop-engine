# Native segment write amplification and proof overhead (#189)

Validation measurements for the native `vtop-log` segment path. This is **not**
an architecture redesign and does **not** claim Kafka superiority. Archive /
matrix methodology for the legacy `vtopctl` path remains in issues
[#92](https://github.com/allamiro/vtop-engine/issues/92),
[#98](https://github.com/allamiro/vtop-engine/issues/98), and
[#130](https://github.com/allamiro/vtop-engine/issues/130).

## Goals

1. Quantify **write amplification**: physical application-layer bytes written
   per logical producer payload byte, with separate buckets for segment body,
   commit boundary, index, manifest, and chunk sidecar.
2. Confirm the **single-copy body path**: producer record bodies are written
   once into the active segment file; seal is rename-only; there is no second
   local data WAL of record bodies.
3. Quantify **proof-carrying overhead** of segment v2 versus a comparable v1
   baseline: append throughput, seal latency, estimated proof-state memory,
   `.chunks` sidecar size, sample inclusion-proof size, and repair
   localization benefit (chunk + proof path versus full segment scan).

## How to run

```bash
# CI / smoke (also covered by workspace tests)
cargo test -p vtop-log --test write_amp_proof_harness --locked

# Emit machine-readable JSON
mkdir -p benchmarks/results/native-write-amp
VTOP_WRITE_AMP_JSON=benchmarks/results/native-write-amp/summary.json \
  cargo test -p vtop-log --test write_amp_proof_harness --locked -- --nocapture

# Optional extended entry point (same schema; reserved for larger local runs)
cargo test -p vtop-log --test write_amp_proof_harness --locked -- --ignored --nocapture
```

JSON lands wherever `VTOP_WRITE_AMP_JSON` points. The directory is created if
missing. `benchmarks/results/` is git-ignored.

## Methodology

| Item | Definition |
|------|------------|
| Logical payload | `sum(key.len + value.len)` over appended records |
| Logical framed | Sealed manifest `content_bytes` (encoded frames only) |
| Physical writes | Sum of `SimStorage` `HandleWrite` lengths (exact payload sizes the storage trait observed) |
| Body bucket | Writes to `*.active` / `*.segment` |
| Commit / index / manifest / chunks | Atomic sidecar payloads (temp write attributed to the sidecar kind) |
| Write amp (payload) | `physical_total / logical_payload` |
| Write amp (framed) | `physical_total / logical_framed` |
| Body amp | `body_writes / logical_framed` (≈ 1.0 plus header bytes in the body bucket) |
| Single-copy pass | Body writes == header + framed; all body writes on `.active`; `Other == 0`; metadata sidecars smaller than framed content |
| Proof size | `8 + 33 * path_len` bytes for `(index, [(hash, side), ...])` |
| Localized repair read | Sample chunk bytes + proof bytes |
| Full scan baseline | Durable sealed segment file length |

Workload (CI default): 256 records × 1 KiB values, Fsync every 16 records,
v2 `chunk_size = 64 KiB`, then seal. Scenarios:

- `v1_fsync_seal` — baseline without chunk proofs
- `v2_fsync_seal` — proof-carrying v2 with keyed commit statement
- `v2_rollover` — seal → open next segment → append remainder

## Interpreting results

- **Commit-boundary amp** grows with Fsync frequency (atomic rewrite of the
  ~74-byte commit file via temp + rename). That is expected metadata amp, not
  a second body copy.
- **v2 chunks sidecar** is `O(chunk_count)` leaf digests, written once at seal.
- **Wall-clock** append/seal numbers are in-process and noisy under CI; use
  them for same-host v1 vs v2 deltas, not absolute SLAs.
- **Physical bytes here are not** filesystem journal, RAID, or drive-cache
  amplification. Those require real-hardware lab notes (deferred).

## Deferred

- Multi-hour soak
- Same-hardware Kafka comparison runs
- Exotic filesystem / drive-cache instrumentation beyond `SimStorage` traces

## Related

- Architecture storage model: [NATIVE_BROKER_ARCHITECTURE.md §5](NATIVE_BROKER_ARCHITECTURE.md#5-data-and-storage-model)
- Benchmarks checklist: [NATIVE_BROKER_ARCHITECTURE.md §15.2](NATIVE_BROKER_ARCHITECTURE.md#152-benchmarks)
- Harness: `crates/vtop-log/tests/write_amp_proof_harness.rs`
- Archive benchmark framework (different path): [`benchmarks/README.md`](../benchmarks/README.md)
