# VTOP Benchmark Framework

A self-contained harness for measuring the VTOP engine under many realistic
conditions — different input volumes, file sizes, formats, batch settings,
compression, checksums, upload backends, fault injection, and long-running
workloads. It **drives the compiled `vtopctl` binary** and never imports engine
code, so benchmark logic stays fully separate from the engine.

Output is plain **CSV + JSON** under `results/<run_id>/` for later analysis, and
runs **never overwrite** prior results.

## Requirements

- The built engine binary (`target/release/vtopctl`). The runner builds it
  automatically if missing, or set `VTOPCTL_BIN=/path/to/vtopctl`.
- Python 3.9+. Optional but recommended:
  ```bash
  pip install -r benchmarks/requirements.txt   # PyYAML + psutil
  ```
  Without them the framework still runs (a minimal YAML parser handles the flat
  scenario files, and system metrics fall back to `ps`).

## 1. Start the benchmark stack (only for the MinIO backend)

In-memory scenarios (`backend: mock` / `mock_fail`) need **no** services. For
the real-upload scenario (`backend: minio`):

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml up -d
# MinIO console: http://localhost:9001  (minioadmin / minioadmin)
```

Buckets are provisioned by the one-shot `minio-init` service; on the very
first boot of a fresh volume, let it exit before starting a run
(`docker compose -f benchmarks/docker-compose.benchmark.yml ps -a` shows it
`Exited (0)`) or the first upload can fail with `NoSuchBucket`. The buckets
live in the named volume from then on, so this is a first-boot wait only.

## 2. Generate seed data

Seed data is generated automatically per scenario, but you can also produce it
standalone:

```bash
python3 -c "import sys; sys.path.insert(0,'benchmarks'); \
from lib import seed; print(seed.generate_dataset('/tmp/seed','jsonl',1000,'small'))"
```

Size classes: `small` (1 KB–64 KB), `medium` (1 MB–10 MB), `large`
(100 MB–1 GB), `mixed`. Formats: `jsonl`, `csv`, `txt`/`log`, `cef`, `leef`,
`syslog`, `mixed`, `binary`.

## 3. Run one scenario

```bash
python3 benchmarks/run_benchmark.py benchmarks/scenarios/01-small-jsonl-gzip.yaml
```

Results land in `benchmarks/results/<run_id>/` (unique per run).

### Where the sender runs: `runner_mode` (#476)

Every scenario carries a `runner_mode` — `host` (the default, and the
behaviour of every bundled scenario: `target/release/vtopctl` as a host
process) or `container`, which execs the **same host-built binary**,
mounted read-only, inside the compose stack's `vtop-engine` service:

```bash
cargo build --release --bin vtopctl   # the binary is bind-mounted read-only
docker compose -f benchmarks/docker-compose.benchmark.yml --profile containerized up -d
python3 benchmarks/run_benchmark.py my-containerized-scenario.yaml   # runner_mode: container
```

Build the binary **before** `compose up`: the `vtop-engine` service
bind-mounts `target/release/vtopctl` read-only, and the mount is declared
with `create_host_path: false` so Compose refuses to start (rather than
silently mounting an empty directory in its place) if the binary is not
there yet. A prior host-mode run also leaves it built.

Container mode exists so something can later sit in the sender's L3 path
(#477's middlebox): `tc` shapes only the interfaces of the namespace it
runs in, and a host process reaching a published loopback port has no such
interface. The mode is recorded as a `runner_mode` column in
`metrics.csv`, `matrix.csv` and `summary.json` on **every** run, host mode
included — a container's veth and bridge hop are part of the measurement,
and a number must never be read without knowing which namespace produced
it.

Two constraints, stated rather than discovered:

- **Shaped containerized runs are Linux-only.** On macOS (and any Docker
  Desktop platform) the sender's traffic crosses a host-to-VM hop that sits
  outside `tc`, so the profile the engine experiences is not the profile a
  middlebox installs. Unshaped container runs work anywhere; numbers from
  them still carry the virtualization tax of that platform.
- **Container-mode credentials are environment credentials.** The keys,
  session token and SDK endpoint overrides cross the boundary by name;
  the PROFILE chain (`AWS_PROFILE`, `~/.aws/*`) does not — the hardened
  container mounts no home directory, deliberately. Export keys (or use
  the lab fallbacks) for container runs; profile-based identity is
  host-mode-only until someone decides mounting credentials into the lab
  container is worth it.
- **Resource metrics are INTERVAL-sampled** (`sys_sample_interval`,
  default 1 s): a host-mode engine subprocess that starts and exits
  between two samples — a fast one-shot mock scenario — can be missed
  entirely, reading near-zero cpu/memory. The numbers are meaningful for
  the sustained scenarios metrics exist for (the soaks run the engine
  continuously); for a one-shot drain, lower the interval or read the
  batch-stage timings instead.
- **Container-mode cpu/memory come from the container's own accounting**
  (`docker stats`), because the engine there is no descendant of the
  runner; thread and open-file counts are unavailable through that
  surface and read 0. Host-wide disk/network counters are unchanged.
- **Run directories must live under the mounted run root** (default
  `/tmp`, overridable with `VTOP_BENCH_RUN_ROOT` — export `TMPDIR` to
  match): the generated config names absolute paths, and the container
  mounts the run root at the same path so the config means the same thing
  in both namespaces.

**The committed host↔container pair** (scenario 12's knobs at
`duration_seconds: 60` — shortened from 300 under disk pressure; same
seeder, same store, both modes on the same machine back-to-back):
run ids `bp12-60s-host-20260908T093607Z-a6x5gf` and
`bp12-60s-container-20260908T093747Z-twlcfm` (local artifacts —
`results/` is never committed, so the numbers and conditions HERE are the
canonical record), Linux 5.14 (RHEL 9),
local MinIO via loopback (host) / compose bridge (container), 2026-09-08.
Host: 156.4 files/s, upload p95 29ms, 0 errors. Container: 121.0 files/s,
upload p95 17ms, 18 of ~40 cycle invocations returned nonzero with no
outcomes (their stderr is not yet surfaced — #499 tracks that gap).

That is a 23% throughput gap, which is LARGER than the 10% bar — so per
that bar, treat container-mode numbers as measurements of the mode until
this is explained and closed. What the pair already shows: the gap is not
the data path — the container's upload p95 is LOWER — but the per-cycle
launch, where host mode spawns a process and container mode runs
`docker compose exec` (~100–200ms each) against cycles that are
themselves short. A longer-duration soak amortizes it; so would a
resident in-container cycle runner, which is #477-adjacent work. Compare
container runs to container runs until then.

## 4. Run the full matrix

```bash
python3 benchmarks/run_matrix.py --all
# or a subset:
python3 benchmarks/run_matrix.py benchmarks/scenarios/01-small-jsonl-gzip.yaml \
                                 benchmarks/scenarios/02-medium-cef-zstd.yaml
```

This runs each scenario and writes `results/matrix-<stamp>/matrix.csv` +
`matrix.md` comparing them side by side.

## 5. Read the metrics files

Each `results/<run_id>/` contains:

| File | Granularity | Key columns |
|------|-------------|-------------|
| `metrics.csv` | one row per run | throughput, latency p50/p95/p99, compression, cpu/mem |
| `batch_metrics.csv` | one row per batch | per-stage durations, compression ratio, status |
| `state_transition_metrics.csv` | one row per state transition | `from_state→to_state`, duration |
| `upload_metrics.csv` | one row per object | object size, upload duration + speed, retries |
| `replay_metrics.csv` | one row per replay | failed state, replay duration, success |
| `backlog_metrics.csv` | one row per sustained-load cycle | bytes seeded vs archived, and the deficit between them |
| `system_metrics.csv` | one row per sample | cpu%, memory, disk, network |
| `summary.json` / `summary.md` | run rollup | everything above, aggregated + bottleneck notes |

All timestamps are ISO 8601 (UTC).

## 6. Compare results

- Across runs: open `matrix.csv` (from `run_matrix.py`) in any spreadsheet / pandas.
- Within a run: `summary.md` for a human view; CSVs for analysis.
- Example (pandas):
  ```python
  import pandas as pd, glob
  df = pd.concat(pd.read_csv(f) for f in glob.glob("benchmarks/results/*/metrics.csv"))
  df.groupby("scenario_name")[["throughput_mb_per_sec","p95_latency_ms"]].mean()
  ```

## 7. Long-duration tests

Set `duration_seconds` in a scenario (e.g. `06-longrun-5min.yaml` = 300 s,
also 1800 / 3600 for 30 min / 1 h). The runner re-seeds fresh files each cycle
to sustain load and samples system metrics throughout. Each run gets its own
`run_id` directory — long runs never clobber earlier ones.

### Sustained backpressure (#98)

Sustained load is not the same as a **backlog**. `process-once` drains
everything it can see before returning, so work added between cycles is work
the engine was never behind on: serially the deficit is zero at the end of
every cycle, at any volume. That is the property the original "1M records/sec
for 5 minutes" framing was really after — Kafka's producers do not wait — and
it comes from the *source*, not from the record rate.

`seed_concurrently: true` runs the seeder beside the engine, adding
`volume x backlog_multiplier` files every `seed_interval_seconds`. Above what
a cycle can drain, a real deficit accumulates — at whatever scale the disk can
afford, which is why `11-backpressure-soak.yaml` reaches the condition on an
ordinary machine instead of needing 45 GB and a producer rig.

Three outputs answer the questions the issue asks:

| output | reads on |
|--------|----------|
| `backlog_metrics.csv` | the deficit per cycle, in bytes seeded vs bytes archived. Whether it **plateaus or climbs** is the whole hypothesis; a shape needs samples, not an end-of-run total |
| `ledger_bytes` / `ledger_rows` / `ledger_bytes_per_batch` | ledger growth. Rows scale with BATCH count, so a low `batch_max_records` grows the ledger far faster per byte archived than a flood does — this is tested *better* small than large |
| `recovery_ms` | what it costs to open that ledger afterwards, with no work left to do (#77 loads the whole thing into memory at startup) |

### Bandwidth-shaped soak (#403)

Every soak above runs over a loopback with unbounded bandwidth, so nothing
measures how the engine degrades when the upload link — not the store — is
the bottleneck. That is the normal condition at an edge site archiving over a
WAN. `13-backpressure-soak-shaped.yaml` is scenario 12 with the upload path
routed through [toxiproxy](https://github.com/Shopify/toxiproxy): no root, no
`CAP_NET_ADMIN`, and a shape per scenario.

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml --profile shaped up -d
python3 benchmarks/run_benchmark.py benchmarks/scenarios/13-backpressure-soak-shaped.yaml
docker compose -f benchmarks/docker-compose.benchmark.yml --profile shaped down
```

The scenario names the shape with flat keys — `shaping_api_url` (empty means
unshaped), `shaping_proxy`, `shaping_bandwidth_kbps` (KB/s, each direction,
**per connection**: toxiproxy limits each connection it proxies, so the
aggregate is the shape times the uploads in flight — scenario 13 keeps
`max_concurrent_batches: 1`, which makes one connection the pipe),
`shaping_latency_ms` (round trip, split across the directions) and
`shaping_jitter_ms` — and points `endpoint_url` at the proxy on `:9100`. The
runner installs the toxics for exactly the measured block and removes them on
every way out of it, so a following unshaped run never inherits the pipe; the
shape is recorded in `summary.json` under `shaping`, as flat `shaping_*`
columns in `metrics.csv` and the matrix comparison, and as a row in
`summary.md`, so a p95 is never read without the link it was measured through.
Toxics are named per run (`vtop_<token>_…`) behind a `vtop_lock` claim toxic
that toxiproxy refuses to create twice, so of two runs racing to one proxy
exactly one shapes it; a proxy already carrying any other toxic — an
interrupted run's or a hand-made one — refuses the run rather than stack.

What the run answers, and where to read it:

| question | reads on |
|----------|----------|
| does a thin pipe grow the **deficit** rather than the process? | `backlog_metrics.csv` should climb while `memory_max_mb` in `metrics.csv` stays bounded — the #98 hypothesis-1 shape, now reachable on purpose. (The engine's own queue gauges, `inflight_batches` and `upload_throttled_total`, are not scraped by this harness; read them off the engine's `/metrics` if you run it with `VTOP_METRICS_ADDR`.) |
| is the wait attributed honestly? | `upload_p95_ms` in `summary.json` / `metrics.csv` — the p95 of the per-batch `object_upload_ms`, which `upload_metrics.csv` carries per object as `upload_duration_ms` — grows by the pipe; `p95_latency_ms` (the whole batch) grows by the same, not more |
| what does the controller see? | the raw signal #102's width controller consumes — the per-batch `object_upload_ms`, summarized as `upload_p50_ms` / `upload_p95_ms`, against the pipe. The controller itself lives in the engine process, and this harness runs one `process-once` per cycle, so its width resets every cycle: observing the back-off (and the throttle counter it reacts to) needs a long-lived `vtopctl run` against the same shaped stack with its `/metrics` scraped, which is where the #102 measurement belongs |
| what is the control? | the runner unshapes the pipe on every way out, so a following unshaped run of scenario 12 is the comparison — same seeder, same store, no pipe |

### The netem middlebox: loss, policing and a real queue (#477)

toxiproxy makes the pipe thin and long, and that is the whole of what it can
make it. It terminates TCP — bytes are read out of one connection and written
into another — so there is no packet to drop, no token bucket to overrun, and
no queue for two flows to share. Those are exactly the conditions under which
one transport beats another, so the egress work (#475) needs a second shaper
rather than a wider toxic. `shaping_driver` chooses which one a scenario gets:

| `shaping_driver` | what it is | what it can produce |
|---|---|---|
| `toxiproxy` (the default) | a TCP-terminating proxy, shaping each **connection** it proxies | rate, round-trip delay, jitter |
| `netem` | an L3 forwarding **middlebox** shaped with `tc`, on the **aggregate** path | the same three, plus packet **loss** (independent or bursty), a token-bucket **policer**, and a real bottleneck **queue** of a chosen depth |

toxiproxy stays the default for a reason that outlives the choice: rewriting
the bundled scenarios onto a new driver would make their recorded numbers
incomparable to their future ones. Naming the driver **is** the opt-in — a
netem scenario has no `shaping_api_url` to switch on — and a netem-only key
left on a toxiproxy scenario (`shaping_loss_pct`, `shaping_loss_model`,
`shaping_bottleneck_kbps`, `shaping_buffer_bdp`, `shaping_policer_kbps`) is
refused by name rather than ignored: a knob that is recorded and applied by
nothing is the failure this harness spends most of its refusals preventing.

**The two drivers are not comparable, and the matrix refuses rather than
labels.** A per-connection bandwidth toxic inside a proxy and an aggregate L3
token bucket with a queue in front of it are different links, and two rows
side by side in one table get compared whatever the `shaping_driver` column
says. `run_matrix.py` raises `IncomparableRuns` — before a single row is
written — when a comparison spans two shaped drivers. A shaped row beside an
*unshaped* one is fine: that is the comparison shaping exists for, and the
columns say which is which.

#### The middlebox, and why the shaping is not on the sender

The `netem` compose profile adds one container, `vtop-bench-netem`, which
forwards at L3 between the engine and the store. It is the only service in the
lab file that holds `CAP_NET_ADMIN`; the engine and the store keep
`cap_drop: [ALL]`. `netem` and `tbf` run **there** and never on the sender,
because a qdisc on the sender queues on the transmit side and reshapes the
very traffic being measured — the sender's own backpressure becomes part of
the number, and the result is a property of the harness rather than of the
link.

The box has two interfaces, which is what makes the directions separable
without inspecting a single packet:

- **Upload** — packets *arriving* on the engine-facing interface. A
  `tc police` action on that ingress hook drops everything above the policer's
  rate and holds nothing; what survives is redirected with `act_mirred` onto
  an `ifb` device, where the bottleneck (`tbf`: the rate and the queue depth)
  and the impairment (`netem`: half the delay, all of the loss) live.
- **Download** — packets *leaving* on that same interface: the other half of
  the round trip, carrying no loss and no rate limit.

Loss therefore lands on the **upload direction only**, so `shaping_loss_pct`
means exactly "loss on the data path" instead of a number whose end-to-end
effect depends on which way a packet happened to be going;
`summary.json` records `loss_direction: upload` beside it.

The engine reaches the store by dialling the middlebox at `http://netem:9200`,
which DNATs to MinIO on its own store-side network, so every byte crosses the
qdiscs. That port is deliberately **not** published on the host: Docker's
userland proxy for a published port terminates TCP, which would put a second
TCP-terminating hop in a path whose entire purpose is to be L3 — the defect
that makes toxiproxy unable to answer this question in the first place. So a
netem scenario is container-mode only, and `lib/netem.py` refuses
`runner_mode: host`, a backend that never dials `endpoint_url`, and an
`endpoint_url` (or a `VTOP_S3_ENDPOINT_URL` override) that does not name the
middlebox.

#### Running one

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml --profile netem up -d
```

That is the middlebox and its calibration probe. Every netem scenario is also
a **container-mode** scenario, so the engine needs the `containerized` profile
beside it — with its binary built first, and the run root mounted, exactly as
in §3:

```bash
cargo build --release --bin vtopctl
docker compose -f benchmarks/docker-compose.benchmark.yml \
  --profile netem --profile containerized up -d
python3 benchmarks/run_benchmark.py benchmarks/scenarios/14-lossy-wan.yaml
docker compose -f benchmarks/docker-compose.benchmark.yml \
  --profile netem --profile containerized down
```

The bundled stack needs no keys of your own: the lab credential fallback
follows the middlebox exactly as it follows the toxiproxy proxy —
`lib/engine.py` reads a netem scenario aimed at `netem:9200` as the lab's own
store, because that is the only store the box forwards to, and supplies the
same `MINIO_ROOT_*` values the stack itself honors (#81). Real environment
credentials still outrank them, and the rest of §3's container-mode rule
stands: keys and SDK endpoint overrides cross the boundary by name, the AWS
profile chain does not.

#### The keys, and their units

| key | unit | what it does |
|---|---|---|
| `shaping_driver` | — | `toxiproxy` (default) or `netem` |
| `shaping_latency_ms` | milliseconds | the **round trip**, split across the two directions |
| `shaping_jitter_ms` | milliseconds | spread on that round trip, normally distributed; needs a latency to spread |
| `shaping_loss_pct` | percent | loss on the upload direction; 100 or more is refused, since a run through it measures nothing |
| `shaping_loss_model` | — | `random` (independent per packet) or `gemodel` (Gilbert-Elliott: bursty, mean burst two packets); a non-random model with no loss is refused |
| `shaping_bottleneck_kbps` | **kilobits/s** | the `tbf` rate; requires a `shaping_buffer_bdp`, because tbf's queue *is* the bottleneck buffer and nobody should inherit a default one |
| `shaping_buffer_bdp` | multiples of the BDP | bottleneck queue depth; requires both a rate and a latency, since the bandwidth-delay product is their product |
| `shaping_policer_kbps` | **kilobits/s** | token-bucket policer on the ingress hook: drops above its rate, queues nothing |
| `shaping_competitor` | `<mode>:<seconds>` | a plain-TCP bulk flow in the same bottleneck queue, measured alone / with VTOP / alone again — see below. Absent means no competitor and no columns; needs `shaping_driver: netem` **and** a `shaping_bottleneck_kbps` |

> **The two rate keys disagree, and the disagreement is inherited.**
> `shaping_bandwidth_kbps` — the toxiproxy driver's — is **KILOBYTES** per
> second, because that is what toxiproxy's bandwidth toxic takes. Every netem
> rate key (`shaping_bottleneck_kbps`, `shaping_policer_kbps`) is **KILOBITS**
> per second, because that is what `tc` takes. The same 10 Mbit/s WAN is
> `shaping_bandwidth_kbps: 1250` on toxiproxy and
> `shaping_bottleneck_kbps: 10000` on netem — an eightfold difference in the
> spelling of one link. Each recorded shape carries its own `rate_unit` so the
> unit travels with the number; a reader comparing the raw columns has to
> convert deliberately.

The three bundled netem scenarios all shape the same 10 Mbit/s, 100 ms link,
so that they differ in what the middlebox does with a burst and in nothing
else. Their workload is scenario 13's (400 small JSONL files, 50-record
batches, a 1.5x concurrent seeder, 300 s), so the numbers can be read beside
it — the toxiproxy row's caveats included:

| scenario | the link | what it measures |
|---|---|---|
| `14-lossy-wan` | 0.5% bursty (`gemodel`) loss, one BDP of buffer | non-congestive loss, where a loss-based control law settles near the inverse square root of the loss rate. Acceptance criterion: `object_upload` p95 at least **3x** the same file at zero loss — and the zero-loss control is that file with `shaping_loss_pct: 0` **and the `shaping_loss_model` line removed**, which the driver requires |
| `15-policed-uplink` | a 10 Mbit/s policer, no queue at all | the edge-uplink case with **no queueing-delay signal**: the round trip stays flat while the link shreds bursts, so a delay-based control law degrades silently. The drops are the policer's, so the loss columns read 0 |
| `16-contended-bottleneck` | four BDP of buffer, no loss | queue depth: what a batch waits behind when the buffer is deep (4 BDP = 400 ms of standing queue at this rate). **Not** fairness or contention — nothing here starts a second flow; `18-competing-flow` is that half |
| `18-competing-flow` | one BDP of buffer, no loss, **a second flow** | what the upload costs its neighbour (#478): the engine runs at the shipped default of eight concurrent batches while a plain-TCP bulk flow shares the queue. One BDP and no loss, so it reads against `14-lossy-wan`'s zero-loss control with contention as the only new variable |

#### The calibration gate

An emulator that is not producing the link it was configured for files numbers
against a link that does not exist. So before a seed byte is written, a netem
run installs its shape, measures it with **iperf3 alone** — from a separate
probe container, across the same ingress hook the upload crosses, with no VTOP
traffic beside it — and tears the shape down again so the measured block
installs its own. The result is recorded as `emulator_validation_mbps` in
`metrics.csv` and `summary.json`, and it is a **gate**: outside ±10% of the
configured `shaping_bottleneck_kbps` the run is refused with a
`CalibrationError` rather than recorded. A scenario with no bottleneck — the
policer in scenario 15 — still records the probe but is not gated on it, since
a TCP flow through a policer lands well below the token rate by design and a
band around that rate would refuse every policed run.

#### The flow beside the upload (#478)

Raising upload concurrency wins throughput on a long link for a mechanical
reason: loss-based TCP's share is **per connection**, so N connections take
roughly N times a single flow's share of a shared bottleneck. VTOP already
makes that decision — `batching.max_concurrent_batches` defaults to 8, and its
AIMD width controller grows the width back on every cycle the store did not
refuse. A shaped WAN link never refuses: a thin pipe is a *rate*, not a 429, so
under exactly the conditions this lab measures the controller sees nothing to
back off from.

> **The number that authorises a concurrency or rate knob is the COMPETITOR's
> goodput, not VTOP's.** VTOP's own throughput going up is what a wider egress
> is *for*; it says nothing about whether the width is safe to ship. What
> decides that is what the flow beside it was left with. A scenario that adds
> `shaping_competitor` records that number, and it is the one to read.

`shaping_competitor: bulk:60` runs a plain-TCP bulk flow (`iperf3`, from the
calibration probe's container to the iperf3 server the middlebox already runs)
across the same ingress hook, the same policer and the same `tbf` queue as the
upload. Three windows, in this order and inside **one** installation of the
shape:

1. **competitor alone**, before the engine's clock starts;
2. **competitor with VTOP**, opened once the seed exists so the window covers
   the engine at work;
3. **competitor alone again**, as the run's shaped block closes.

The bracket is the point: a single solo measurement cannot tell a bottleneck
that drifted mid-run from an engine that took bandwidth, so the drift would be
absorbed into the fairness claim and reported as VTOP's doing. **If the two
solo windows disagree by more than 10% the run is refused, naming both
numbers** — an unstable bottleneck cannot support a fairness claim.

Five flat columns reach `metrics.csv`, `matrix.csv`, `summary.json` and the
`Competing flow` row of `summary.md` (blank for every run that started no
competitor):

| column | what it is |
|---|---|
| `competitor_goodput_mbps_with` | the competing flow's received rate over its **whole** window while VTOP was uploading — the answer to "what did the neighbour get" |
| `competitor_goodput_mbps_without` | the mean of the two solo windows — the same flow on the same link with VTOP idle |
| `competitor_goodput_mbps_over_vtop_window` | the same flow over the **span the index is taken on** (`vtop_window_seconds`), derived from the report's own per-second interval stream. One of the index's two inputs |
| `vtop_goodput_mbps_with` | VTOP's own share beside that flow: the bytes committed by the engine cycles the window **wholly contained**, over the span those cycles cover (see the refusal below). Committed object bytes, so framing, manifests and retries are **excluded** and the engine is understated. The index's other input |
| `competitor_jain_index` | Jain's fairness index over those two flows: 1.0 for an even split, 0.5 when one flow takes everything |

**Both of the index's rates cover one span, and it is not the competitor's
window.** VTOP's share can only be charged whole engine cycles, so it is a rate
over the cycles the window contained — nested inside the window. Pairing that
with the competitor's whole-window average would compare a slice against an
average, and a flow whose rate moves during the minute (a queue that fills, a
retransmission burst, the engine's own concurrency ramping) would then be
misreported by whatever the two spans differ by. So the competitor is
re-measured over that same span from its own `intervals` stream, and
`summary.json` states which span it was: `jain_span_seconds`,
`competitor_bytes_over_vtop_window` and `competitor_intervals_over_vtop_window`
beside `vtop_window_seconds`. iperf3's interval entries are the **sender's**,
so they supply only the shape — how the flow's bytes were spread across the
window — while the receiver's total supplies the magnitude, for the same reason
`sum_sent` is refused everywhere else here.

**The index never appears without the per-flow numbers beside it,** and a test
asserts that over the headers themselves. An index is symmetric: 0.61 is
equally true of an engine that starved its neighbour and a neighbour that
starved the engine, so it cannot say which flow won and must never be quoted
alone. Note also that `vtop_goodput_mbps_with` understates VTOP — it counts committed
object bytes, not wire bytes — and that this does NOT bias the index in one
direction. Jain's index peaks at equality, so understating the engine moves the
number **towards** equality when the engine's true share is the larger, and
**away** from it when the competitor's is: the error flatters the engine in
exactly the case worth catching, and penalises it in the harmless one. Read a
marginal index as a reason to look at the two per-flow numbers, which is why
they are never allowed to appear without it.

Every other way this measurement can go wrong is a refusal rather than a
number, because each of them produces a plausible result:

- `shaping_competitor` with `shaping_driver: toxiproxy` (or on an unshaped
  run) is **refused by name**: a per-connection bandwidth toxic meters each
  connection inside the proxy, so the two flows never queue behind one another
  and the index would come out near 1.0 on a link where VTOP could be taking
  everything. A second refusal, with the same reasoning, covers a netem shape
  that sets no `shaping_bottleneck_kbps`: no `tbf`, no shared queue. A policer
  does not qualify either — it drops above its rate and holds nothing.
- A competitor that cannot be started fails the run, carrying iperf3's own
  words.
- A window that outlives the engine's loop fails the run: the tail of it would
  be a solo measurement under a contended name. The arithmetic is checked
  against `duration_seconds` before a seed byte is written, and against what
  actually happened when the block closes.
- The **received** rate is recorded, never the sent one, and the sender's view
  is not used as a fallback. At the far end of a token bucket `sum_sent`
  counts bytes still sitting in the queue, so a competitor read that way looks
  less harmed than it was.
- A contended window that contained no **whole** engine cycle is refused
  rather than recorded. The engine's committed-byte total moves in one step
  when a `process_once` returns, so a cycle that began before the window
  opened, or returned after it closed, carries bytes from outside the window
  that cannot be separated from the bytes inside — it is charged to neither
  side, because splitting it would mean inventing the moment each byte left.
  If every cycle straddled an edge, VTOP's share is **unknown, not zero**, and
  a recorded zero would credit the competitor with a fair-share result it
  never had to fight for. Lengthen the window so it spans at least one whole
  cycle, or shorten the cycle. `summary.json` records what was attributed:
  `vtop_attributed_cycles`, `vtop_window_seconds` (the span those cycles
  cover, nested inside the competitor's window rather than equal to it) and
  `vtop_bytes_across_competitor_window` — the engine's total movement between
  the window's two edges, which is never divided by anything and whose gap
  from the attributed bytes is how much of it could not be placed.
- **Where the competitor's window sat on the run's clock is derived from its
  own report, not from a timer.** Nothing samples a boundary while the flow is
  running. The report says how long the window it measured was; the run knows
  when the flow's command was launched and when its report was in hand. A
  window of that length cannot have opened before the launch plus the ramp
  iperf3 omits, and cannot still have been open once the report came back, so
  cycles are attributed only to the stretch that every admissible position of
  the window contains. The wall clock left over — the setup at one end,
  iperf3's closing exchange and exit at the other — is a single quantity the
  run cannot split between the ends, so it is taken off the attributable span
  and recorded as `competitor_window_unaccounted_seconds`. Both edges follow
  from that one rule: a cycle that ran while `docker exec` was still starting
  or connecting is charged to neither side, and so is one that returned in the
  tail between iperf3's last measured second and the moment its report was
  collected.
- An iperf3 report with **no `intervals` stream** is refused, at the first solo
  window rather than after all three have run. Without it the competing flow
  has only a whole-window average, and the index would be back to comparing
  rates over two different spans. So is a report whose stream is entirely the
  omitted ramp, one with an entry this module cannot read (refused rather than
  skipped, because a skipped entry leaves a hole and the receiver's bytes would
  then be spread across a window the stream no longer describes), and one whose
  stream leaves the engine's own span uncovered — the competitor's rate over
  that span would have to be invented. **Uncovered, not untouched:** a stream
  with a hole under the span still overlaps it at both ends, and accepting that
  would treat the missing seconds as seconds in which the competitor received
  nothing while the rate's denominator still covered them — an understated
  flow, and an index that flatters VTOP by exactly the missing stretch. Two
  intervals that meet at a boundary are contiguous, to within the rounding
  iperf3 prints its interval bounds at.

A contended run costs roughly `3 x (window + 3 s)` of extra wall clock. Those
solo seconds are the harness's rather than the engine's — VTOP is deliberately
idle for both of them — so `duration_seconds` excludes them and the engine's
throughput columns stay comparable with an uncontended run's. Each phase's own
window is recorded in `summary.json` under `competitor`.

This issue builds the instrument and sets **no fairness target**. What VTOP may
do to a neighbour becomes a shipping gate in the egress-ceiling work, where a
knob is actually added.

#### What netem itself cannot tell you

Absolute measurement is a **non-goal** here, and the emulator is the reason:

- it is bounded by **kernel timer granularity** — delay and rate are enforced
  at the resolution the timer offers, and the finer the interval a link
  implies, the more the emulator quantizes it;
- rate throttling **compresses packets artificially**: throttled traffic is
  released in bursts that a real link of the same average rate would have
  spaced out, so the arrival pattern is emulator-shaped even when the average
  is right;
- published testbeds run netem from roughly **100 Mbit/s upward**, and the
  upper end of that range is not clearly sound — the faster the configured
  link, the more of the result belongs to the emulator;
- and this is **one host**: the middlebox, the engine and the store share a
  CPU, a scheduler and a clock with each other and with whatever else runs.

So a netem number is a **comparison and never an absolute**: this scenario
against that one, on the same driver, on the same machine, close together in
time. It does not say what the engine would do over a real WAN of the same
nominal rate, and the ±10% calibration band bounds the emulator's error, not
the claim.

## 8. Clean benchmark data

```bash
rm -rf benchmarks/results/*        # results/ is git-ignored
```

MinIO objects live in the named volume `bench-minio-data`, which survives
`docker compose ... down`. Drop them wholesale by removing the volume:

```bash
docker compose -f benchmarks/docker-compose.benchmark.yml down -v
```

or delete a single run — every run namespaces its objects under its `run_id`.
The compose stack's `mc` alias lives only inside the ephemeral init
container, so point one at the published port first, with the same
`MINIO_ROOT_*` overrides the stack itself honors (#81). `${VAR:-default}`
below reads only the shell — if your overrides live in `benchmarks/.env`,
read the two values out and plug them in (`grep -E
'^MINIO_ROOT_(USER|PASSWORD)=' benchmarks/.env`); `.env` is compose DATA,
not shell code, so it is parsed, never sourced — the same rule the smoke
scripts follow, and sourcing would also let a filed value clobber an
exported one, inverting compose's shell-wins precedence. The bucket is
whatever the scenario filed (`vtop-bench-soak` for the soak):

```bash
mc alias set local http://localhost:9000 \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin}"   # once per host
mc rm -r --force "local/<bucket>/<run_id>/"
```

The prefix makes runs separable; only the volume or prefix deletion above
bounds growth — nothing expires benchmark objects automatically.

## 9. Add a new scenario

Copy any file in `scenarios/`, change the knobs, drop it in `scenarios/`.
Every parameter is configurable (see `lib/scenario.py` `DEFAULTS`):
volume, file_size, format, batch_max_records/bytes/age, compression(+level),
checksum, backend, duration_seconds, fault, sys_sample_interval, bucket,
endpoint_url, and the `shaping_*` keys (§7). `run_matrix.py --all`
automatically picks it up.

## Benchmark matrix coverage

| Dimension | Supported now | Notes |
|-----------|---------------|-------|
| File volume | ✅ 1k–1M (configurable) | very large volumes need disk + time |
| File sizes | ✅ small / medium / large / mixed | |
| Batch size | ✅ by count, by bytes, by time window | `batch_max_records/bytes/age` |
| Formats | ✅ jsonl, csv, txt, cef, leef, syslog, mixed, binary | binary/compressed sources via whole-file (`whole_file`) mode — scenario `10-binary-localfs` |
| Compression | ✅ none / gzip / zstd | |
| Checksum | ✅ sha256 / blake3 / disabled | all three engine modes (protocol §10) — scenarios `08-blake3-jsonl`, `09-checksum-disabled` |
| Upload backend | ✅ MinIO, in-memory mock, localfs; AWS S3 via endpoint+creds | `localfs` (VTOP-LocalFS profile) driven by scenario `10-binary-localfs` |
| Failure conditions | ✅ verification failure, replay/recovery | `backend: mock_fail`, `fault: replay` |
| Runtime duration | ✅ any (`duration_seconds`) | 5 min / 30 min / 1 h presets easy to add |
| Sustained backpressure | ✅ `seed_concurrently` + `backlog_multiplier` | a real deficit, not just sustained load — scenario `11-backpressure-soak` (#98) |
| Bandwidth-shaped upload | ✅ toxiproxy on the `shaped` profile, `shaping_*` keys | the upload link as the bottleneck — scenario `13-backpressure-soak-shaped` (#403) |
| Lossy / policed / queued link | ✅ the netem middlebox on the `netem` profile | loss, a policer and a real bottleneck queue, which a TCP-terminating proxy cannot produce — scenarios `14-lossy-wan`, `15-policed-uplink`, `16-contended-bottleneck` (#477) |
| What the upload costs a neighbour | ✅ `shaping_competitor`, netem only | one plain-TCP bulk flow in the same queue, measured alone / with VTOP / alone again — scenario `18-competing-flow` (#478). The competitor's goodput, not VTOP's, is what authorises a concurrency or rate knob |

## Native segment write amp / proof overhead (#189)

The Python scenarios above drive the **archive** `vtopctl` path. Native
segment write amplification and proof-carrying overhead are measured in-process
by the Rust harness (see
[`docs/WRITE_AMP_PROOF_OVERHEAD.md`](../docs/WRITE_AMP_PROOF_OVERHEAD.md)):

```bash
mkdir -p benchmarks/results/native-write-amp
VTOP_WRITE_AMP_JSON=benchmarks/results/native-write-amp/summary.json \
  cargo test -p vtop-log --test write_amp_proof_harness --locked -- --nocapture
```

That report complements matrix issues #92 / #98 / #130; it does not claim
Kafka superiority.

## Native fetch I/O research (#190)

Native fetch I/O strategy research (buffered page cache vs Linux
`sendfile`/`splice` vs experimental `O_DIRECT`, with an explicit gate before
`io_uring`) is measured in-process by the Rust harness (see
[`docs/FETCH_IO_RESEARCH.md`](../docs/FETCH_IO_RESEARCH.md)):

```bash
mkdir -p benchmarks/results/native-fetch-io
VTOP_FETCH_IO_JSON=benchmarks/results/native-fetch-io/summary.json \
  cargo test -p vtop-log --test fetch_io_research_harness --locked -- --nocapture
```

This is a research harness — it does not ship three production fetch engines.
It complements matrix issues #92 / #98 / #130 and does not claim Kafka
superiority.

## Native metadata saturation research (#192)

Single three-node metadata Raft saturation and sharding-trigger criteria are
measured in-process by the Rust harness (see
[`docs/METADATA_SATURATION_RESEARCH.md`](../docs/METADATA_SATURATION_RESEARCH.md)):

```bash
mkdir -p benchmarks/results/native-meta-saturation
VTOP_META_SATURATION_JSON=benchmarks/results/native-meta-saturation/summary.json \
  cargo test -p vtop-meta --test metadata_saturation_harness --locked -- --nocapture
```

This is a research harness — it does **not** implement multi-group metadata
sharding (epic #93). Multi-hour dedicated soaks remain deferred.

## Design principles

- Benchmark logic is **separate** from engine logic (drives the binary only).
- **No hardcoded paths** — output dirs, seed dirs, and the binary are configurable.
- **Every parameter** is scenario-configurable.
- Results are **reproducible** (seedable generators) and **never overwritten**.
- Simple **CSV/JSON** output for later analysis.
- Local **Docker Compose** first; structure is extensible toward Kubernetes.

## Known limitations

- **System metrics** are best with `psutil`; the `ps` fallback reports CPU%/RSS
  of the process tree only (disk/network show 0).
- **Mid-flight restart** is approximated via the fault/replay path
  (`mock_fail` → failed batches → recovery), not a hard kill at a random instant.
- Very large volumes (100k–1M files) are supported but bounded by local disk and
  time; start small and scale up.
