# What it would take to reach 1M, 5M and 20M records/sec

Closes the research question in #92.

**The one sentence that matters, before anything else: no end-to-end
demonstration of 1M records/sec exists. Nothing here claims one. This document
records what has been measured, what has been derived from those measurements,
and which of the two each number is.**

---

## 1. A correction that came first

An earlier estimate put 1M rec/s at "~770× current" and called it
architecturally out of reach. That was wrong, and the error is worth keeping
because it shaped everything after it.

Two mistakes: extrapolating from a measured 1,300 rec/s without asking what
limited it — serial per-topic polling (#85) and a load generator producing
~150 msg/s, not the pipeline — and reasoning as though the object-storage
round trip were paid **per record**. It is paid **per batch**. At 100k-record
batches, 1M rec/s is **10 uploads/sec**, not a million.

The useful question is the marginal cost per record, and how much of the
per-batch cost is overlappable I/O.

## 2. Where a batch spent its time

Mean per stage over 432 batches, read from the engine's own `/metrics` at the
time #92 was investigated:

| stage | mean | kind |
|---|---|---|
| compress | 6.56 ms | CPU |
| checksum | 1.02 ms | CPU |
| object upload | 7.75 ms | **network wait** |
| manifest upload | 5.20 ms | **network wait** |
| verify | 2.55 ms | **network wait** |
| commit | 1.00 ms | I/O |
| **staged total** | **24.08 ms** | |

**15.5 ms of 24 ms — 64% — was blocking network I/O, not CPU.** A strictly
sequential pipeline leaves the processor idle for two thirds of every batch,
and that time is perfectly overlappable. This is the entire case for #87, and
the measured engine sat at 5.67% CPU while doing one batch at a time.

Observed `total_ms` (39–86 ms median) exceeded the staged total, so 15–60 ms
per batch sat outside the instrumented stages. #119 and #120 have since
attributed part of that to ledger writes and removed most of it.

## 3. The arithmetic for 1M records/sec

At the measured 148.8 bytes/record, 1M rec/s is **148.8 MB/s** of uncompressed
ingest. Per-core figures are from the codec benchmark in §4.

| resource | requirement | verdict |
|---|---|---|
| compression (zstd −1) | 148.8 ÷ 417 MB/s per core | **0.36 cores** |
| compression (gzip −6) | 148.8 ÷ 32 MB/s per core | 4.7 cores — the blocker |
| SHA-256 checksum | ~1–2 GB/s per core, hardware accelerated | ~0.1 cores |
| network out | 148.8 MB/s ÷ ~3.1× ratio | **48 MB/s** |
| object PUTs, 100k-record batches | 10 objects + 10 manifests | **20 PUT/s** |
| verify read-backs | 10/s | negligible |
| state transitions | ~6 per batch × 10 batches/s | 60 writes/s |

None of these are extreme. **Verify-before-commit is not what limits
throughput.** It costs ~15 ms of *waiting* per batch, and batches are large.
What limited throughput was doing all of it strictly sequentially with the
slowest codec available.

## 4. Compression: the measured part

From the 112-cell matrix in `benchmarks/results/matrix-*/`. Compression ratio
is a property of the data and the codec, so these numbers do not depend on how
busy the machine was — they are the most trustworthy figures in this document.

Mean ratio across 16 cells per codec (4 formats × 2 file sizes × 2 batch sizes):

| codec | mean ratio | range |
|---|---|---|
| gzip −1 | 3.62× | 3.21 – 4.12 |
| zstd −3 | 4.36× | 3.77 – 5.15 |
| zstd −1 | 4.44× | 3.73 – 5.33 |
| gzip −6 | 4.96× | 4.18 – 5.80 |
| gzip −9 | 4.99× | 4.16 – 5.87 |
| zstd −9 | **5.14×** | 4.24 – 6.36 |

Throughput, measured separately on 13 MB of the lab's own CEF traffic:

| codec | throughput | ratio |
|---|---|---|
| gzip −6 | 32 MB/s | 3.48× |
| gzip −1 | 114 MB/s | 2.90× |
| zstd −1 | **417 MB/s** | ~3.1× |
| zstd −9 | — | 3.68× |

zstd −1 is **13× gzip −6's throughput**; zstd −9 beats gzip −6 on ratio
outright. Compression was the largest CPU cost in the pipeline and it was
running the slowest codec available.

## 5. What has since shipped

Every structural item #92 identified is now closed:

| item | what it was | state |
|---|---|---|
| #85 | serial per-topic Kafka polling | closed |
| #87 | one batch at a time | closed — `max_concurrent_batches`, default 8 |
| #88 | gzip −6 as the pipeline's largest CPU cost | closed — `examples/config.yaml` sets `type: zstd`, `level: 3` |
| #89 | batches sealing on the 60s age timer | closed |
| #90 | no benchmark matrix | closed — the 112 cells used above |
| #96 | serial source reads in every adapter | closed — `buffer_unordered` in file and spool |
| #119 / #120 | unattributed per-batch time; SQLite durability cost | merged |
| #66 | engine unsafe as multiple replicas | closed |

One deliberate non-change: the **code** default is still gzip/6. Changing it
would change the output format for every config that omits the field, and
objects record the codec they were written with. `examples/config.yaml`
selects zstd; the default stays where an existing deployment left it.

## 6. What is still not known

This is the part that matters for anyone planning against these numbers.

- **No end-to-end demonstration.** Every cell of the matrix ran against
  `backend: mock` — in memory, no object store. It measures the pipeline up to
  the storage boundary and nothing across it.
- **The matrix's throughput and CPU columns are not usable.**
  `cpu_max_percent` is `0.0` in all 112 cells and `duration_seconds` is `0` in
  all 112: those columns were not captured. The MB/s figures were taken on a
  machine that was compiling at the time, and are withdrawn. Ratios and
  relative ordering survive; absolute rates do not.
- **The marginal per-record cost (~6.2 µs) came from two data points.** It
  wants a proper batch-size sweep before anyone plans against it.
- **Record size dominates the whole question.** 1M rec/s of 148-byte records is
  149 MB/s. Of 1 KB records it is 1 GB/s — a different problem with a different
  answer.
- **The consume side has never been demonstrated at 149 MB/s.** It may well be
  the real ceiling, and nothing here rules that out.
- **5M and 20M rec/s are not addressed by this arithmetic at all.** 5M of
  148-byte records is 744 MB/s ingest and ~240 MB/s egress after compression;
  20M is 3 GB/s in. Those cross from "one box, arranged better" into
  multi-instance territory, and the single-box arithmetic above says nothing
  about them.

## 7. The honest summary

Single-box component arithmetic shows **1M records/sec of 148-byte records is
not architecturally precluded**: 0.36 cores of compression, 48 MB/s of network,
20 PUTs/sec. The structural blockers that made it look impossible — a
sequential pipeline and the slowest codec — have both been removed.

**It has not been achieved, measured, or demonstrated.** The gap between "the
arithmetic permits it" and "the system does it" is exactly the end-to-end run
against real object storage that nobody has performed, and until someone does,
this document is a plan and not a result.

For 5M and 20M there is not even arithmetic yet.
