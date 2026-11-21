# Cluster soak validation

The live-chaos scenarios each inject one fault and check one outcome, in
minutes. This soak runs the cluster plane, three metadata nodes and three data
nodes, for **seven days under continuous produce with a fault every hour**,
and decides pass or fail from what the run recorded. It is criterion A3 of the
production-readiness bar (#714), and it is required only if the cluster plane
is called production.

A maintainer starts it on a dedicated host; no pull request and no scheduled
job runs the seven-day profile. A `smoke` profile runs the same machinery in
about fifteen minutes and proves the harness works. Issue #727 is the record
of the requirement; it closes when a maintainer attaches the evidence of a
passing seven-day run.

The harness is `scripts/live-chaos/soak/`. It is built from the live-chaos
library (`scripts/live-chaos/lib.sh`: the node configs, certificates, the
candidate bring-up of scenario 14, the lease reads), the operation history and
its checker (#728, [Operation histories](LIVE_CHAOS_VALIDATION.md)), and the
engine soak's profile plan, trend fit and criterion record
([Engine soak validation](SOAK_VALIDATION.md), #726), so the two soaks plan
and judge the same way.

## What a run does

| | |
|---|---|
| Cluster | Three metadata nodes (Raft) and three data nodes in candidate mode, as scenario 14: the range's leader is whichever candidate holds its lease, and it changes without a restart. TLS on every plane. Every node runs in its own network namespace on one bridge, inside an unprivileged user namespace. |
| Data | One range, v2 segments of 4 MiB, retention on at 64 MiB (`retention: max_total_bytes`), each data node's directory on its own 256 MiB tmpfs. |
| Load | 5 records per second of 512 bytes, quorum produce, without a stop. Each producer session is a new producer epoch on whichever node holds the lease; when a fault ends a session, the next one starts on the current holder. Every batch is recorded (`produce --history`). |
| Reads | Every 15 minutes the range is read from the leader's log start to its committed high watermark, and every record it holds is recorded (`verify --dump --from-offset`). |
| Faults | One every hour from hour 1, cycling through six kinds, none in the last hour: 167 in all, 27 or 28 of each kind. |
| Sampled | Every 30 s, per node: whether its process is up, its resident memory and open files, which process lifetime it is, its Raft term and role (metadata) or its lease, epoch, log start and offsets (data), and a data node's directory size and free disk. |
| At the end | The producer stops, the range is read once more, the followers take the tail, every node is stopped with `SIGTERM`, and every segment of every replica is checked with `vtopctl segment verify` (the active tail is sealed first). |

The rate is low on purpose. The soak is about time and faults, not
throughput, and the history checker holds every acknowledged record in memory:
about 3 million records over seven days is a few gigabytes of RAM for the
check. At this rate retention keeps about six and a half hours of records, so
every record is read several times before retention reclaims it.

## The faults

Each fault proves it landed before it counts; one that cannot show its effect
fails the run.

| Fault | What is done | How it is proven | Live-chaos counterpart |
|---|---|---|---|
| `range_leader_kill` | `SIGKILL` the range's leaseholder; start it again 30 s later from its own directory | the lease names a holder, and it is the process killed | scenario 14 |
| `meta_leader_kill` | `SIGKILL` the metadata Raft leader; start it again 30 s later | the node said it led (`vtop_meta_raft_state`) | scenario 01, 02 |
| `partition` | drop everything on one node's interface for 120 s: the range leader, the metadata leader and a range follower in turn | its metrics endpoint does not answer across the bridge while it is cut | scenario 06 |
| `pause` | `SIGSTOP` the range leader, then the metadata leader in turn, for 60 s, then `SIGCONT` | the process is in the stopped state | scenario 23's freeze |
| `disk_full` | fill a data node's tmpfs to the last byte for 300 s: a follower, then the leader in turn | under 1 MiB is left | scenario 05 |
| `clock_step` | stop the range leader, then the metadata leader in turn, and start it under scenario 07's shim with its wall clock 3,600 s ahead (then behind, alternating) for 300 s, then on the true clock | the shim records the time it gives the node, which must be the host's plus the step | scenario 07 |

The clock shim is compiled from scenario 07's own source, read out of the
scenario by the engine soak's `extract_skew_shim_source`, so there is one copy.

After each fault the run waits for recovery: every node up and the producer
acknowledging again. A node that exits when no fault stopped it is recorded as
an unplanned exit and started again, so one finding does not end the run.

## Pass or fail

`cluster_soak_eval.py` decides. A criterion with nothing to judge fails.

| Criterion | Pass when |
|---|---|
| The run completed | `run_end` recorded and samples reach the full duration |
| Every node was sampled throughout | six nodes, each at least 90% of its samples, no gap over 300 s |
| Every planned fault landed and the cluster recovered | all 167, each landed, each recovered |
| Recovery is within the timeout | the slowest at most 600 s |
| No unplanned exit | no node exited unless a fault stopped it |
| **No acknowledged record was lost** | `check_history.py` finds the history valid: every acknowledged record seen at its offset (below the last read's log start, by the reads before it), none twice, none reordered, nothing visible that nobody wrote, nothing an earlier read saw rewritten later, the high watermark never back |
| The producer kept acknowledging | at least half the offered records acknowledged |
| The range was read throughout | reads at least half as often as scheduled, no gap over an hour, and the final read |
| No read found the leader serving what the log forbids | no read stopped on a record served at or above the committed high watermark, offsets going backwards, or the watermark going back mid-read (`verify` gives up before it records such a read, so the driver marks it) |
| **One leader per epoch** | no fencing epoch led by two data nodes (every `role=leader` the nodes logged, and every sample in which one said it leads), and no Raft term led by two metadata nodes (sampled) |
| **Bounded memory and disk** | no growth trend in any node's memory or open files, or in a data directory, over the last 80% of the run (Theil-Sen within each process lifetime, the engine soak's fit; growth over both 5% and 32 MiB for memory, 5% and 16 files, 10% and 16 MiB for the data directory), and every data directory under 96 MiB and every node under 2 GiB |
| **`vtopctl segment verify` clean on every replica** | every segment of all three data nodes verifies |

The history check is the one #728 built for the live-chaos scenarios, run over
the whole week. Under retention a read starts at the leader's log start
(`verify --from-offset`), and the checker reads a reclaimed prefix from the
reads that saw it before it went: a record retention reclaimed before any read
saw it is unproven, and counts as lost. The profile refuses a retention window
shorter than four read intervals for that reason.

## Running it

On a Linux host (x86_64 or arm64) with unprivileged user, mount and network
namespaces (on Ubuntu 24.04, `sysctl kernel.apparmor_restrict_unprivileged_userns=0`),
`iptables`, `iproute2`, `gcc`, `python3` 3.9 or newer, `curl` and `procps`:

```bash
cargo build --release -p vtop-node --bin vtop-node
cargo build --release -p vtop-cli --no-default-features --bin vtopctl
scripts/live-chaos/soak/cluster-soak.sh smoke /var/lib/vtop-cluster-soak/smoke-1   # first: about 15 minutes
scripts/live-chaos/soak/cluster-soak.sh 7d /var/lib/vtop-cluster-soak/7d-1
```

Run it under `nohup`, `tmux` or a systemd unit: the run is the driver's
process. The host needs about 1 GiB of RAM for the tmpfs data directories and
the nodes, and a few gigabytes for the history check at the end; the run
directory grows to a few gigabytes (the history and the logs). The exit status
is the verdict's: 0 pass, 1 fail, 2 the run could not start.

To judge an archived run again:

```bash
python3 scripts/live-chaos/history/check_history.py RUN/history/*.jsonl > RUN/history-check.json
python3 scripts/live-chaos/soak/cluster_soak_eval.py --profile RUN/profile.json --run-dir RUN
```

## What a run leaves

| File | What it is |
|---|---|
| `profile.json` | the profile the run used |
| `events.jsonl` | the run's events: node starts and exits, each fault's start, end and recovery, producer sessions, reads, the end |
| `samples.jsonl` | the per-node samples |
| `history/produce.jsonl`, `history/reads.jsonl` | the operation history |
| `history-check.json` | the history checker's summary |
| `leaders.jsonl` | every leadership a data node logged, with its epoch |
| `finals.json` | `vtopctl segment verify` for every replica |
| `verdict.json`, `verdict.txt` | the evaluator's verdict, with the SHA-256 of each input |
| `work/logs/` | every node lifetime's log, the producer's and the reader's |

The data directories live on tmpfs inside the run's mount namespace and go
with it; what the soak needs from them, the segment verification, is taken
before the run ends.

## What to attach to #727

The host (CPU, memory, kernel, distribution), the commit the binaries were
built from, and the run directory without `work/` except `work/logs/`,
compressed, with `verdict.txt` quoted in the comment.

## Not covered

- One range and one producer. Nothing here speaks to many ranges, range
  splits, or competing producers.
- The clock is stepped at a restart (the node starts into a skewed clock), not
  under a running process.
- A disk is filled, not failed: `fsync` errors are scenario 05b's.
- The meta-plane check is sampled every 30 s: two leaders of one Raft term
  that both lasted less than a sample would not be seen. Lease grants are not
  checked for linearizability (#728's open item).
- The trend limits were chosen before any seven-day run existed; the first
  real run is what tells whether they are right.
