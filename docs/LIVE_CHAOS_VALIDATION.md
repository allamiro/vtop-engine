# Live cluster chaos validation

The `scripts/live-chaos` harness runs metadata and native data-plane nodes as
real operating-system processes with real filesystem durability and mTLS TCP
transport. It complements the deterministic simulated-disk and fault-router
suites; it does not replace them.

## Build and run

```bash
cargo build --release -p vtop-node -p vtop-cli --no-default-features
scripts/live-chaos/run-all.sh
```

For a faster local correctness pass, build the same packages without
`--release` and run with `CHAOS_PROFILE=debug`.

The Linux host needs `bash`, `openssl`, `curl`, a C compiler, `unshare`, `ip`,
and `iptables`. Unprivileged user and mount namespaces must be enabled. Each
scenario owns a fresh `$TMPDIR/vtop-chaos.*` directory (`/tmp` when `TMPDIR`
is unset) and removes it on exit; `CHAOS_KEEP=1` retains generated evidence.
A caller-supplied `CHAOS_WORKDIR` is never deleted automatically.
For `run-all.sh`, that value is treated as a root and each scenario receives
its own named subdirectory; for a directly invoked scenario it is the exact
work directory.

Every scenario preflights its required tools, binary executability, scratch
writability, and free space before starting nodes. Namespace-dependent
scenarios also test the exact user/mount/network namespace capability they
need. Failures include a remediation, such as selecting a larger local disk,
moving `CARGO_TARGET_DIR` to an exec-enabled filesystem, or enabling the
required Linux namespaces. A scratch filesystem mounted `noexec` is supported:
compiled fault-injection shims are kept under `target/live-chaos`, and the
harness prints a notice explaining `TMPDIR`, `CHAOS_WORKDIR`, and
`VTOP_TEST_EXEC_TMPDIR` alternatives. If `target/live-chaos` is not writable,
set `CHAOS_SHIM_DIR` to a writable local directory. Override the conservative
256 MiB free-space check with `CHAOS_MIN_FREE_MIB` only after reviewing
scenario sizes.

### Environment overrides

All deployment-sensitive harness values have defaults and can be overridden
without editing a scenario:

| Setting | Default | Purpose |
|---|---:|---|
| `CHAOS_WORKDIR` | unset | Exact directory for one scenario; root of per-scenario directories under `run-all.sh`. |
| `CHAOS_TMPDIR` | `$TMPDIR`, then `/tmp` | Parent directory for automatically generated, unique work directories; created when missing. |
| `CHAOS_SHIM_DIR` | `target/live-chaos` | Writable directory for compiled `LD_PRELOAD` fault shims. |
| `CHAOS_MIN_FREE_MIB` | `256` | Scratch-space preflight floor. |
| `CHAOS_PROFILE` | `release` | Cargo target profile directory containing `vtop-node` and `vtopctl`. |
| `CHAOS_META_HOST`, `CHAOS_DATA_HOST` | `127.0.0.1` | Metadata and native/replica listen hosts. |
| `CHAOS_META_PEER_BASE_PORT` | `9100` | Metadata peer ports are this base plus node ID. |
| `CHAOS_META_ADMIN_BASE_PORT` | `9200` | Metadata admin ports are this base plus node ID. |
| `CHAOS_REPLICA_BASE_PORT` | `9300` | Replica ports are this base plus replica number. |
| `CHAOS_NATIVE_PORT` | `9400` | Native producer/fetch port. |
| `CHAOS_KAFKA_BASE_PORT` | `9420` | Kafka gateway listeners (#225): kafka-0 for scenario 16's standalone, kafka-1 for scenario 17's leader, kafka-2 for scenario 18's, kafka-3 for scenario 19's. |
| `CHAOS_META_METRICS_BASE_PORT` | `9500` | Metadata `/metrics` ports are this base plus node ID (#224). |
| `CHAOS_DATA_METRICS_BASE_PORT` | `9600` | Data-plane `/metrics` ports: base+0 for the leader, base+1/2 for followers. |
| `CHAOS_TOPIC_UUID` | `aaaaaaaa-…-b1` | Metadata's UUID for the topic, distinct from the wire-level topic name. |
| `CHAOS_LEASE_DURATION_MS` | `6000` | Range-lease TTL (#223). Short so a scenario need not sit through a production-length lease. |
| `CHAOS_LEASE_RENEW_MS` | `2000` | Renewal interval — a third of the TTL, so two renewals can fail without a failover. |
| `CHAOS_LEASE_POLL_MS` | `500` | How often a non-holder re-checks for a lapsed lease. |
| `CHAOS_READY_TIMEOUT_SECONDS` | `20` | Process readiness deadline. |
| `CHAOS_ELECTION_TIMEOUT_SECONDS` | `30` | Election and convergence deadline. |
| `CHAOS_PROGRESS_TIMEOUT_SECONDS` | `30` | Producer-progress deadline. |
| `CHAOS_STOP_TIMEOUT_SECONDS` | `10` | Hard-stop observation deadline. |
| `CHAOS_CLUSTER_ID`, `CHAOS_RANGE_ID`, `CHAOS_SEGMENT_ID` | deterministic UUIDs | Fixture identities. |
| `CHAOS_PRINCIPAL_ID`, `CHAOS_LEADER_UUID`, `CHAOS_FOLLOWER1_UUID`, `CHAOS_FOLLOWER2_UUID` | deterministic UUIDs | TLS/data-plane fixture identities. |
| `CHAOS_TOPIC`, `CHAOS_FENCING_EPOCH` | `chaos.v1`, `18` | Native range and fencing fixture values. |

Scenario workloads are parameterized too: `CHAOS_BRINGUP_*`, `CHAOS_GROW_*`,
`CHAOS_KILL9_*`, `CHAOS_DISKFULL_*`, `CHAOS_FSYNC_*`, and `CHAOS_CLOCK_*`.
The defaults in the scenario table are the minimum reference workloads used
for the documented assertions. Invalid values, port collisions, and a
disk-full workload too small to exhaust its tmpfs fail during preflight with
an actionable message.

## Readiness gating

Node startup is gated on `GET /readyz` (#224), not on a stdout ready marker.
The marker proves a node reached the end of its startup path exactly once; the
endpoint reports whether it is servable *right now*, which is a different and
stronger claim — a marker cannot go back to false when a leader is fenced, a
partition heals, or a disk fills. Scenarios that need the current state call
`await_ready` / `await_not_ready`, and a failed gate prints the reason the node
served rather than only an HTTP code.

Nodes started inside their own network namespace (scenario `06`) are not
reachable from the harness, so the marker remains their only available signal.
Everywhere else the health gate is authoritative.

Because those gates are now load-bearing for every other scenario, the
endpoints themselves get a scenario: `08-operational-surface`. A gate nobody
checks is a gate that silently stops working.

## Scenarios and assertions

| Scenario | Live fault or transition | Assertions |
|---|---|---|
| `00-bringup` | Three metadata voters and three native replicas start cold | Metadata election/commit converges; 5,000 quorum-acknowledged records read back byte-exactly; every replica seals and passes `vtopctl segment verify`. |
| `01-meta-membership-grow` | Metadata membership grows 3 → 5 during committed metadata proposals and a concurrent 100,000-record native producer | No proposal is lost; all five voters converge and catch up; native produce/fetch remains byte-exact; all native replicas independently verify. |
| `02-meta-membership-shrink-fenced-leader` | Metadata membership shrinks 5 → 3 with the current leader removed | A survivor takes leadership and commits; the removed voter leaves membership and refuses direct proposals. |
| `03-meta-voter-replacement` | A fresh metadata learner catches up and replaces one voter | Replacement catches up before promotion; the retired voter can be killed without losing metadata availability. |
| `04-leader-kill9-durability` | Native leader receives `SIGKILL` during quorum production | Every acknowledged record survives recovery; fetch stays below the reported committed HWM; both followers contain the acknowledged floor; all recovered artifacts independently verify. |
| `05-follower-diskfull` | One follower writes into an 8 MiB tmpfs until `ENOSPC` | Leader plus healthy follower continue quorum commits; the full follower stays alive and reports only its durable prefix; its recovered prefix and both healthy artifacts verify. |
| `05b-follower-fsync-failure` | One follower's live `fsync`/`fdatasync` calls return `EIO` | The failing follower remains fail-closed at its durable prefix while the healthy quorum commits; all recovered artifacts verify. |
| `06-partition-meta-leader` | Per-node network namespaces plus `iptables` isolate the metadata leader's peer traffic | Survivors elect and commit in a higher term; the isolated leader cannot commit; after healing it steps down, converges, and refuses direct proposals. |
| `07-clock-skew` | `CLOCK_REALTIME` on one metadata node is shifted +1 hour while monotonic time stays honest | The shim proves the applied clock offset; exactly one leader is observed; proposals commit and the skewed member converges. |
| `09-range-leader-failover` | A range leader is `SIGKILL`ed mid-flight under sustained quorum produce, and the follower holding the acknowledged floor is restarted as a lease-driven leader over the data it already replicated (#223) | The follower acquires the lease within the TTL at a strictly higher fencing epoch; every acknowledged record is still readable byte-exactly once the new quorum settles; the restarted old leader (on its own ports) never reports ready and is refused with a `Fenced` error under its stale epoch; every surviving replica artifact verifies offline. |
| `10-colocated-node` | `vtop-node node` hosts a metadata voter and a data replica in ONE process (#215), which is then `SIGKILL`ed and restarted | One `/metrics` scrape carries both planes; `/readyz` opens only as the conjunction of both roles and both then serve (admin RPCs, produce); every acknowledged record survives the shared-fate restart byte-exactly and the sealed artifact verifies offline. |
| `08-operational-surface` | Every node's `/metrics`, `/healthz`, and `/readyz` under a live cluster (#224) | The metric names the dashboards query are published on every role; exactly one node claims Raft leadership over `/metrics`; the committed offset agrees between `/metrics` and `vtopctl node status`; the endpoint answers `GET` only; a killed node stops answering its health gate while survivors stay ready. |
| `11-admin-authorization` | Admin transport authorization enforced by the running binary over real mTLS (#238) | An operator certificate retains the full surface; a data-node certificate is refused membership changes (and the refusal says so), may still read, and can still acquire its **own** range lease — so the policy does not break the failover path. |
| `12-data-replica-replacement` | A data replica is lost permanently; a fresh node joins, is repaired from a survivor, and the dead replica is retired (#242) | The replica set committed is the one placement computed; `vtopctl node repair` populates the newcomer byte-for-byte and the artifact verifies independently; retirement is **refused before** the replacement proof commits and accepted after; every previously acknowledged record remains readable. |
| `13-orderly-shutdown` | The range leader and a follower are stopped with SIGTERM instead of SIGKILL (#280) | The signal is handled within a deadline (an ignored SIGTERM fails the scenario); the departing leader releases its range lease so the replacement acquires it without waiting out the lease deadline; every acknowledged record is readable after the handoff; both stopping nodes write their final commit boundary; the stopped leader's sealed artifact verifies offline. |
| `14-candidate-failover` | Three identical candidate configs; the range leader is SIGKILLed mid-hold (#284) | Exactly one candidate takes the range from ONE config shape — the winner is not scripted; quorum produce lands at the winner's own native address; after the kill a SURVIVOR takes the range with no process started, no config rewritten, and no port moved — the promotion appears in its log as a role change, not a restart; quorum produce resumes at the new epoch on the survivor's own address; every record acknowledged before the kill is intact. |
| `15-plaintext-lab` | The whole cluster on loopback without a certificate: every plane `transport: plaintext` (#294) | The lab boots and serves; a leased replicated leader is refused on a plaintext replica plane, naming the knob to turn (`replica_transport: tls`); a plaintext listener off loopback is refused, naming the way out; the warnings are on the log. |
| `16-kafka-gateway` | A standalone node serves Kafka's wire protocol beside its native plane (#225), exercised with nothing but bash and `/dev/tcp` | ApiVersions v0 answers with the served keys; Metadata v1 names the range's topic behind the gateway; a version outside the served range is refused by closing the connection; the ready line names the listener and the node warns that the protocol carries no vtop identity. |
| `17-kafka-stock-client` | librdkafka (`kcat`) against the gateway on a leader with two followers; one follower is `SIGKILL`ed while ONE paced producer is mid-stream (#225 acceptance) | 20,000 keyed records produced with `acks=all` by one process, every delivery a quorum acknowledgement; the kill lands after a watermark floor and before the stream ends (the scenario fails if the producer had already finished); the same process finishes with no failed delivery and the watermark reaches 20,000; the same client reads everything back byte-exact and in order from `-o beginning`; the surviving follower's committed offset covers every acknowledgement; the leader's and the follower's sealed segments pass `vtopctl segment verify`. |
| `18-kafka-idempotent-producer` | librdkafka (`kcat`) with `enable.idempotence=true` against the gateway on a leader with two followers; InitProducerId mints the id, and BOTH followers are `SIGSTOP`ped mid-stream past the client's request timeout so it retries batches the leader still holds; every retry is answered with its original offset (#457 slice 1 acceptance) | 20,000 keyed records produced with `acks=all` and `enable.idempotence=true` by one process; the stall (both followers `SIGSTOP`ped) lands after a watermark floor and before the stream ends (the scenario fails if the producer had already finished); the same process finishes with no failed delivery and no fatal error, and the watermark reaches 20,000 — not one more; the leader's log names at least one retried set acknowledged with its original offset; the same client reads everything back byte-exact, in order and with no key twice from `-o beginning`; follower 1's committed offset covers every acknowledgement; the leader's and follower 1's sealed segments pass `vtopctl segment verify`. |
| `19-kafka-consumer-group` | librdkafka's consumer group (`kcat -G`) against the gateway on a leader with two followers (#457 slice 2): FindCoordinator, JoinGroup, SyncGroup, Heartbeat, OffsetFetch and LeaveGroup, the client-side assignor assigning the range's one partition; auto-commit on at a short interval, the durable store being the next slice | 5,000 keyed records produced; the group consumer's rebalance line names `TOPIC [0]`; it reads all 5,000 byte-exact and in order from the earliest offset and exits at the end; every commit is refused BY NAME on the leader's log (no store yet), the refusal is on the client's log, and the consumer reads everything and exits clean with no other error; the leader's sealed segment passes `vtopctl segment verify`. |
| `20-kafka-offsets-resume` | a group's committed offset is a cursor on the metadata plane (#457 slice 2b): on a three-node data plane under a lease from a three-node metadata plane, a stock group consumer (`kcat -G`) reads the first half of the records and leaves, committing; `vtopctl meta group-cursor` reads the position back from the plane as an unpinned cursor at exactly that offset; a second consumer under the same group resumes at record N/2+1 and reads the rest byte-exact; the cursor then stands at N, and the leader's log names no refused commit. | the durable-store acceptance the issue asks for: commit, restart, resume — with the position living on the plane between the two consumers, not in the gateway |

The data scenarios stop each process before offline sealing. Recovery first
truncates any tail beyond the durable commit boundary, then `vtopctl segment
verify` checks the resulting sealed bundle. For the disk-full follower, the
quiesced namespace-visible active file and commit boundary are copied out
before its private tmpfs disappears.

## Scope boundaries

This harness is an integration foundation for issue #215, not the final
production cluster daemon.

- Metadata and native data-plane processes are assembled separately. The
  native broker does not yet consume metadata membership or lease changes
  from the live Raft group.
- Scenario 03 validates metadata voter replacement, and only that. The
  #180/#181 replacement-proof lifecycle for a sealed DATA replica is
  exercised end to end by scenario 12 — operator-driven, through
  `vtopctl node repair --seal-tail`, which seals the leader's active tail
  into the transferable prefix and asserts the repaired replica holds the
  full acknowledged floor (the closure #306 tracked) — so the one thing
  still unproven here is automated repair with no operator in the loop.
- The native data plane has quorum replication but no live leader election.
  Scenario 04 therefore proves acknowledged durability and same-directory
  recovery, not automatic data-plane failover.
- Durable consumer-group cursor proposals are covered by the metadata state
  machine and deterministic Raft tests. They are not yet wired to the live
  native fetch client, so this harness does not claim end-to-end cursor
  continuity across membership changes.
- Offline segment verification applies to scenarios that create data-plane
  artifacts. Metadata-only scenarios have no segment files to verify.

These boundaries must remain explicit in pull-request and issue updates;
passing this suite alone is not grounds to close every remaining #215 item.
