# Native fetch I/O research (#190)

Research measurements for native segment **fetch** I/O strategies. This is
**not** an architecture redesign, does **not** ship three production fetch
engines, and does **not** claim Kafka superiority. Archive / matrix methodology
for the legacy `vtopctl` path remains in issues
[#92](https://github.com/allamiro/vtop-engine/issues/92),
[#98](https://github.com/allamiro/vtop-engine/issues/98), and
[#130](https://github.com/allamiro/vtop-engine/issues/130).

Epic [#93](https://github.com/allamiro/vtop-engine/issues/93) dependency policy:
buffered file I/O is the baseline; Direct I/O and `io_uring` are optional
measured implementations pursued **only** after buffered I/O is shown to be the
bottleneck.

## Goals

1. Compare **page-cache buffered reads** (current `SegmentReader::fetch` path)
   against measurement-only probes for `sendfile` / `splice` and experimental
   `O_DIRECT`.
2. Cover workloads: hot, cold/lagging (best-effort), sequential catch-up,
   concurrent same vs disjoint consumers, and plain vs TLS-proxy transport cost.
3. Emit machine-readable JSON with CPU-per-GiB (or proxy), latency percentiles,
   disk read amplification where observable, and buffer-footprint estimates.
4. Keep **correctness unchanged**: fetch never exposes records at/above the
   committed high-water mark (local commit and cluster clamp).
5. Publish an explicit **recommendation gate** grounded in harness data.

## How to run

```bash
# CI / smoke (also covered by workspace tests)
cargo test -p vtop-log --test fetch_io_research_harness --locked

# Emit machine-readable JSON
mkdir -p benchmarks/results/native-fetch-io
VTOP_FETCH_IO_JSON=benchmarks/results/native-fetch-io/summary.json \
  cargo test -p vtop-log --test fetch_io_research_harness --locked -- --nocapture

# Optional extended entry point (same schema)
cargo test -p vtop-log --test fetch_io_research_harness --locked -- --ignored --nocapture
```

JSON lands wherever `VTOP_FETCH_IO_JSON` points. The directory is created if
missing. `benchmarks/results/` is git-ignored.

## Engines under test

| Engine | Production? | Notes |
|--------|-------------|-------|
| `buffered_page_cache` | **Yes (baseline)** | Real sealed-segment fetch via `SegmentReader` |
| `sendfile` | No (Linux probe) | Raw sealed-file bytes → `/dev/null` |
| `splice` | No (Linux probe) | File → pipe → `/dev/null` |
| `odirect` | No (experimental probe) | `O_DIRECT` aligned reads of the sealed file |
| `io_uring` | No | Deferred slot — no `io_uring` dependency in tree |

### TLS and zero-copy

`sendfile` / `splice` can avoid a userspace copy on **plaintext** fanout.
Under TLS, ciphertext construction needs userspace bytes, so kernel zero-copy
does **not** apply end-to-end. The harness records that limit on every
sendfile/splice row and measures a **TLS proxy** (BLAKE3 over fetched payloads)
as a CPU stand-in — not a full TLS stack.

## Workloads

| Scenario | Intent |
|----------|--------|
| `buffered_hot` | Consumer immediately behind a warm page cache |
| `buffered_cold` | Best-effort cold (`posix_fadvise` DONTNEED on Linux) |
| `buffered_sequential_catchup` | Windowed fetch from offset 0 to HWM |
| `buffered_concurrent_same` | N threads, full committed range |
| `buffered_concurrent_disjoint` | N threads, disjoint offset ranges |
| `buffered_plain_transport` | Fetch + userspace touch/copy proxy |
| `buffered_tls_proxy` | Fetch + BLAKE3 CPU proxy |
| `sendfile_cold_raw` / `splice_cold_raw` / `odirect_cold_raw` | Linux probes |
| `io_uring_probe` | Always `deferred` in this harness version |

CI default workload: 256 records × 1 KiB values, sealed, then windowed fetch
(`64 KiB` / `32` records per call).

## Metrics

| Metric | Definition |
|--------|------------|
| Fetched logical bytes | Sum of `FetchBatch.encoded_bytes` (or raw transfer for probes) |
| Throughput | Fetched MiB / wall seconds |
| Latency p50/p95/p99 | Per fetch/transfer call, milliseconds |
| CPU ms / GiB | `(user+sys)` from `getrusage` / GiB fetched |
| Disk read amp | Linux `/proc/self/io` `read_bytes` / fetched bytes |
| Buffer footprint | Peak batch/copy buffer estimate (not full RSS) |

## Recommendation gate

The JSON `recommendation` object always includes:

- `default_path`: `buffered_page_cache`
- `gate`: only pursue `O_DIRECT` / `io_uring` if buffered is the bottleneck
- `pursue_odirect_io_uring`: `true` only when **both** hold on a measured run:
  1. buffered cold `disk_read_amp > 1.5`, and
  2. `O_DIRECT` cold throughput beats buffered cold by **>35%**

Otherwise the gate stays closed. Lab-limited CI/macOS runs are expected to keep
`pursue_odirect_io_uring=false` and must say so in `lab_limits`.

### Default recommendation (this harness version)

**Keep buffered page-cache fetch as the default.** Do not introduce production
`O_DIRECT` / `io_uring` fetch engines until a Linux lab run trips the gate above
and a follow-up design spike owns the storage-trait implementation. Prefer
profiling TLS/CPU and application decode before changing the I/O engine.

## Correctness

The harness asserts:

1. Buffered (uncommitted) tails are invisible to fetch.
2. `fetch_through` never returns records at/above the requested HWM.
3. Cluster HWM is clamped to `min(requested, local committed)`.

Production fetch semantics are unchanged by this research PR.

## Deferred

- Production sendfile / splice / `O_DIRECT` / `io_uring` fetch engines
- Multi-hour soaks
- Exotic kernel / filesystem tuning
- Real TLS stack (handshake, record framing, kernel TLS)
- Same-hardware Kafka comparison

## Related

- Architecture storage model: [NATIVE_BROKER_ARCHITECTURE.md §3.1](NATIVE_BROKER_ARCHITECTURE.md#31-dependency-policy)
- Benchmarks checklist: [NATIVE_BROKER_ARCHITECTURE.md §15.2](NATIVE_BROKER_ARCHITECTURE.md#152-benchmarks)
- Harness: `crates/vtop-log/tests/fetch_io_research_harness.rs`
- Write-amp companion (#189): [WRITE_AMP_PROOF_OVERHEAD.md](WRITE_AMP_PROOF_OVERHEAD.md)
- Archive benchmark framework: [`benchmarks/README.md`](../benchmarks/README.md)
