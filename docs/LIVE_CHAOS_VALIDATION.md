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
| `CHAOS_KAFKA_BASE_PORT` | `9420` | Kafka gateway listeners (#225): kafka-0 for scenario 16's standalone and scenario 26's, kafka-1 for scenario 17's leader, kafka-2 for scenario 18's, kafka-3 for scenarios 19 and 20, kafka-4 and kafka-5 for the two partitions of scenarios 21 and 24, kafka-5 again for scenario 25's, kafka-6 to kafka-8 for the three candidates of scenarios 27, 28 and 30 to 33 (#751), in either shape. |
| `CHAOS_KAFKA_RECORDS`, `CHAOS_KAFKA_KILL_AFTER_ACKS`, `CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS` | `4000`, `800`, `60` (scenario 27); `3000`, `600`, `60` (scenario 28) | The records streamed, the acknowledgements after which the leader is killed, and the longest the client may wait for an acknowledgement after the kill (#751). |
| `CHAOS_KAFKA_PACE_EVERY`, `CHAOS_KAFKA_PACE_SLEEP` | `10`, `0.1` | Scenario 27's pace: a pause of that many seconds after every that many records, 100 records a second. Scenario 28 sets `CHAOS_KAFKA_RATE` (default `100`), records a second, instead. |
| `CHAOS_KAFKA_HOME`, `CHAOS_KAFKA_IMAGE`, `CHAOS_KAFKA_STRATEGIES` | unset, unset, `rebootstrap none` | Scenario 28's (and 31's) client: an unpacked Apache Kafka distribution, or an `apache/kafka` image to copy `/opt/kafka` out of with `docker`; with neither the scenario skips. The `metadata.recovery.strategy` values to run, one failover each. A JVM (17 or newer) must be on `PATH`. |
| `CHAOS_KAFKA_SHAPE` | `split` | The processes the Kafka-candidate scenarios (27, 28, 32, 33) run on (#751): `split` is three `vtop-node meta` processes beside three `vtop-node data` candidates; `colocated` is three `vtop-node node` processes, each a metadata voter and a candidate, whose lease asks its own voter first and lists all three in `admin_peers`. Scenarios 30 and 31 set `colocated` and run 27 and 28. |
| `CHAOS_KAFKA_KILL_AFTER_CONSUMED`, `CHAOS_KAFKA_COMMIT_INTERVAL_MS`, `CHAOS_KAFKA_GROUP_CLIENTS` | `800`, `500`, `librdkafka jvm` | Scenario 33: the deliveries after which the leader (and the group's coordinator) is killed, the JVM consumer's `auto.commit.interval.ms`, and the consumers to run, each on a fresh cluster (the JVM one needs `CHAOS_KAFKA_HOME` or `CHAOS_KAFKA_IMAGE`, and is left out with a log line without them). `kcat` commits at librdkafka's own 5 s: its `-X` tries the topic configuration first, and `auto.commit.interval.ms` is also the name of a legacy topic property, which is where it lands. |
| `CHAOS_KAFKA_PAUSE_AFTER_ACKS`, `CHAOS_KAFKA_RESUME_ACKS`, `CHAOS_KAFKA_PAUSE_LIMIT_SECONDS` | `800`, `200`, `120` | Scenario 32: the acknowledgements after which the leader is paused, how many more must arrive through the new leader before the old one is resumed, and the longest the client may wait for an acknowledgement after the pause. A paused leader keeps its socket open, so librdkafka waits out its own timeouts before it looks elsewhere: 60 to 71 s in CI under either lease, so the bound is 120 s (it was 90 when it assumed the 30 s request timeout alone). |
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
`CHAOS_KILL9_*`, `CHAOS_DISKFULL_*`, `CHAOS_FSYNC_*`, `CHAOS_CLOCK_*`, and
scenario 23's `CHAOS_611_SEED`, `CHAOS_611_SHARED`, `CHAOS_611_BATCH` and
`CHAOS_611_SUFFIX_MAX`.
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
| `04-leader-kill9-durability` | Native leader receives `SIGKILL` during quorum production | Every acknowledged record survives recovery; fetch stays below the reported committed HWM; both followers contain the acknowledged floor; all recovered artifacts independently verify; the producer's and the recovery read's [operation history](#operation-histories-728) passes the checker. |
| `05-follower-diskfull` | One follower writes into an 8 MiB tmpfs until `ENOSPC` | Leader plus healthy follower continue quorum commits; the full follower stays alive and reports only its durable prefix; its recovered prefix and both healthy artifacts verify. |
| `05b-follower-fsync-failure` | One follower's live `fsync`/`fdatasync` calls return `EIO` | The failing follower remains fail-closed at its durable prefix while the healthy quorum commits; all recovered artifacts verify. |
| `06-partition-meta-leader` | Per-node network namespaces plus `iptables` isolate the metadata leader's peer traffic | Survivors elect and commit in a higher term; the isolated leader cannot commit; after healing it steps down, converges, and refuses direct proposals. |
| `07-clock-skew` | `CLOCK_REALTIME` on one metadata node is shifted +1 hour while monotonic time stays honest | The shim proves the applied clock offset; exactly one leader is observed; proposals commit and the skewed member converges. |
| `09-range-leader-failover` | A range leader is `SIGKILL`ed mid-flight under sustained quorum produce, and the follower holding the acknowledged floor is restarted as a lease-driven leader over the data it already replicated (#223) | The follower acquires the lease within the TTL at a strictly higher fencing epoch; every acknowledged record is still readable byte-exactly once the new quorum settles; the restarted old leader (on its own ports) never reports ready and is refused with a `Fenced` error under its stale epoch; every surviving replica artifact verifies offline; the [operation history](#operation-histories-728) of both producers, the fenced probe and every read passes the checker. |
| `10-colocated-node` | `vtop-node node` hosts a metadata voter and a data replica in ONE process (#215), which is then `SIGKILL`ed and restarted | One `/metrics` scrape carries both planes; `/readyz` opens only as the conjunction of both roles and both then serve (admin RPCs, produce); every acknowledged record survives the shared-fate restart byte-exactly and the sealed artifact verifies offline. |
| `08-operational-surface` | Every node's `/metrics`, `/healthz`, and `/readyz` under a live cluster (#224) | The metric names the dashboards query are published on every role; exactly one node claims Raft leadership over `/metrics`; the committed offset agrees between `/metrics` and `vtopctl node status`; the endpoint answers `GET` only; a killed node stops answering its health gate while survivors stay ready. |
| `11-admin-authorization` | Admin transport authorization enforced by the running binary over real mTLS (#238) | An operator certificate retains the full surface; a data-node certificate is refused membership changes (and the refusal says so), may still read, and can still acquire its **own** range lease — so the policy does not break the failover path. |
| `12-data-replica-replacement` | A data replica is lost permanently; a fresh node joins, is repaired from a survivor, and the dead replica is retired (#242) | The replica set committed is the one placement computed; `vtopctl node repair` populates the newcomer byte-for-byte and the artifact verifies independently; retirement is **refused before** the replacement proof commits and accepted after; every previously acknowledged record remains readable. |
| `13-orderly-shutdown` | The range leader and a follower are stopped with SIGTERM instead of SIGKILL (#280) | The signal is handled within a deadline (an ignored SIGTERM fails the scenario); the departing leader releases its range lease so the replacement acquires it without waiting out the lease deadline; every acknowledged record is readable after the handoff; both stopping nodes write their final commit boundary; the stopped leader's sealed artifact verifies offline. |
| `14-candidate-failover` | Three identical candidate configs; the range leader is SIGKILLed mid-hold (#284) | Exactly one candidate takes the range from ONE config shape — the winner is not scripted; quorum produce lands at the winner's own native address; after the kill a SURVIVOR takes the range with no process started, no config rewritten, and no port moved — the promotion appears in its log as a role change, not a restart; quorum produce resumes at the new epoch on the survivor's own address; every record acknowledged before the kill is intact; the survivor serves within 3 s (`CHAOS_PROMOTION_SERVE_LIMIT_MS`) of the lease changing hands, at a write quorum of replica streams, and logs that it serves with 1 of 2 streams connected (#750: it used to wait its whole 10 s deadline for the killed leader's stream). The producers, every read and who led are recorded, and both history checkers pass (#728). |
| `15-plaintext-lab` | The whole cluster on loopback without a certificate: every plane `transport: plaintext` (#294) | The lab boots and serves; a leased replicated leader is refused on a plaintext replica plane, naming the knob to turn (`replica_transport: tls`); a plaintext listener off loopback is refused, naming the way out; the warnings are on the log. |
| `16-kafka-gateway` | A standalone node serves Kafka's wire protocol beside its native plane (#225), exercised with nothing but bash and `/dev/tcp` | ApiVersions v0 answers with the served keys; Metadata v1 names the range's topic behind the gateway; a version outside the served range is refused by closing the connection; the ready line names the listener and the node warns that the protocol carries no vtop identity. |
| `17-kafka-stock-client` | librdkafka (`kcat`) against the gateway on a leader with two followers; one follower is `SIGKILL`ed while ONE paced producer is mid-stream (#225 acceptance) | 20,000 keyed records produced with `acks=all` by one process, every delivery a quorum acknowledgement; the kill lands after a watermark floor and before the stream ends (the scenario fails if the producer had already finished); the same process finishes with no failed delivery and the watermark reaches 20,000; the same client reads everything back byte-exact and in order from `-o beginning`; the surviving follower's committed offset covers every acknowledgement; the leader's and the follower's sealed segments pass `vtopctl segment verify`. |
| `18-kafka-idempotent-producer` | librdkafka (`kcat`) with `enable.idempotence=true` against the gateway on a leader with two followers; InitProducerId mints the id, and BOTH followers are `SIGSTOP`ped mid-stream past the client's request timeout so it retries batches the leader still holds; every retry is answered with its original offset (#457 slice 1 acceptance) | 20,000 keyed records produced with `acks=all` and `enable.idempotence=true` by one process; the stall (both followers `SIGSTOP`ped) lands after a watermark floor and before the stream ends (the scenario fails if the producer had already finished); the same process finishes with no failed delivery and no fatal error, and the watermark reaches 20,000 — not one more; the leader's log names at least one retried set acknowledged with its original offset; the same client reads everything back byte-exact, in order and with no key twice from `-o beginning`; follower 1's committed offset covers every acknowledgement; the leader's and follower 1's sealed segments pass `vtopctl segment verify`. |
| `19-kafka-consumer-group` | librdkafka's consumer group (`kcat -G`) against the gateway on a leader with two followers (#457 slice 2): FindCoordinator, JoinGroup, SyncGroup, Heartbeat, OffsetFetch and LeaveGroup, the client-side assignor assigning the range's one partition; auto-commit on at a short interval, the durable store being the next slice | 5,000 keyed records produced; the group consumer's rebalance line names `TOPIC [0]`; it reads all 5,000 byte-exact and in order from the earliest offset and exits at the end; every commit is refused BY NAME on the leader's log (no store yet), the refusal is on the client's log, and the consumer reads everything and exits clean with no other error; the leader's sealed segment passes `vtopctl segment verify`. |
| `20-kafka-offsets-resume` | a group's committed offset is a cursor on the metadata plane (#457 slice 2b): on a three-node data plane under a lease from a three-node metadata plane, a stock group consumer (`kcat -G`) reads the first half of the records and leaves, committing; `vtopctl meta group-cursor` reads the position back from the plane as an unpinned cursor at exactly that offset; a second consumer under the same group resumes at record N/2+1 and reads the rest byte-exact; the cursor then stands at N, and the leader's log names no refused commit. | the durable-store acceptance the issue asks for: commit, restart, resume — with the position living on the plane between the two consumers, not in the gateway |
| `21-kafka-two-partitions` | two members of one group over two partitions, one each (#457 slice 4b): two standalone gateways, each leading one Kafka partition of the same topic and naming the other in `kafka.partitions`; a stock group (`kcat -G`, range assignor) of two members finds the one coordinator the topology elects, is assigned one partition each, and each member reads only the records produced to its partition — byte-exact, no overlap. Standalone: commits are refused by name (no metadata plane), as scenario 19; the assignment is the criterion. | the remaining in-flight acceptance of #457: two members, two partitions, one each |
| `22-meta-rolling-upgrade` | a rolling upgrade of the metadata tier (#554): three voters start on the PREVIOUS release (`CHAOS_OLD_VTOP_NODE`) and are replaced one at a time by this build; after each replacement a write must commit within the progress deadline and every voter must reach the leader's applied index; with two voters replaced and the third still on the previous release, a leader on this build refuses `enable-features --level 8` and the refusal names the old voter as closing the connection without answering the version question (#706; while the old voter leads it is restarted, still on the previous release, until a node on this build does); once all are upgraded `vtopctl meta enable-features --level 4` raises the feature level, a voter restarted on the raised log reopens, and a lower level leaves 4 in force. Skips (exit 77, reported as SKIP by `run-all.sh`) without an old binary; CI's upgrade job runs it against the newest release tag. Against v0.6.0, raising the level with one voter still old leaves that voter unable to append (`applied 5, the leader 7`) - the failure this scenario catches, and the reason the leader gates new kinds. Then (#634) `vtopctl meta node-versions --level 8` lists the registered nodes and finds none a raise waits for (placeholders with no range and no report are not counted), `enable-features --level 7` raises the report level on the metadata tier alone, and a static range runs with one follower on the previous release and one on this build behind a leader on this build: the leader opens each session with a version hello, the old follower answers it as every release before the handshake does, and both followers must reach every acknowledged record, with no protocol error in the old follower's log. | the mixed-version check #554 asks for: the gate holds against a real previous release, not only a simulated one; and #634's: a new leader falls back to the old replica protocol against a real previous-release follower |
| `23-acked-suffix-failover` | The #611 shape on three candidates over TLS: one follower C is SIGKILLed after a shared prefix, so the leader L and the other follower R alone acknowledge the next records; L is SIGKILLed mid-stream; R is frozen (SIGSTOP) and C restarted so C takes the lease first; R is thawed to answer C's fence; then L restarts so R has a follower that holds its tail (a new leader does not stream a lagging follower up from its log, #341). Both followers' committed offsets must cover the shared prefix before C is killed, so what C lacks is exactly the suffix. `CHAOS_611_SEED` picks the lagging follower and how much suffix is acknowledged before L dies, in [`CHAOS_611_BATCH`, `CHAOS_611_SUFFIX_MAX`] (defaults 20 and 2000); `CHAOS_611_SHARED` (default 200) sizes the prefix. | C, which lacks acknowledged records, never logs a role change to leader, and R takes the range at an epoch newer than both L's and the one C was refused at; C's refusal by R's answer (Raft §5.4.1) is reported; quorum produce resumes on the candidate that serves; every record acknowledged before the kill reads back byte-exact from it. When L returns holding records past R's tail that nobody acknowledged, nothing fences it and no quorum forms (#641): the scenario reports it and exits 77 (SKIP), never PASS. It also exits 77 when C comes back holding every acknowledged record, since the shape was then not reached. Candidates carry `lease.admin_peers` for all three metadata nodes, so a metadata leader change mid-run does not strand the lease agents. The producers, every read and who led are recorded, and both history checkers pass (#728). |
| `24-kafka-replicated-partitions` | Two Kafka partitions of one topic, each a range with a leader and two followers, the leaders on different hosts and every host a replica of both under one certificate (#596: the shape of the Kafka comparison's multi-partition cells). Each leader lists both partitions in `kafka.partitions`. ONE idempotent librdkafka producer (`enable.idempotence=true`, `acks=all`) lets its partitioner spread keyed records over both, then a `kcat -G` group consumer reads them. No metadata plane, as in the benchmark. | Both partitions are written, and their watermarks sum to the records produced. librdkafka's own debug lines show it acquired exactly one producer id from one leader and wrote every batch of BOTH partitions under it, so the other leader accepted an id it did not mint. The group reads exactly the input, each record once, each partition in produced order. A follower of each range committed every acknowledged record (`replica-status`). |
| `25-group-commit-under-kill` | Group commit on (`group_commit: {enabled: true}`, #752) on a leader with two followers; FOUR idempotent librdkafka producers (`kcat`, `enable.idempotence=true`, `acks=all`) stream keyed records through the gateway concurrently; the leader is `SIGKILL`ed mid-stream and restarted from the same config. `CHAOS_GROUP_COMMIT_DELAY_MS` (default 2) sets the window so sharing is reliable on a loopback lab; `CHAOS_GC_RECORDS` (default 4,000 per producer) sizes the streams. | The leader logs that group commit is on and exports `vtop_broker_group_commit_enabled` 1. Before the kill `vtop_broker_group_commit_requests_total` exceeds `vtop_broker_group_commits_total` (requests shared barriers). Every producer finishes with no failed delivery and no fatal error; the watermark is exactly the records sent; each producer's records read back byte-exact and in the order it sent them, no key twice and none missing; the restarted leader seals groups again; both followers committed every record. |
| `26-kafka-compressed-produce` | librdkafka (`kcat -z`) produces through the gateway on a standalone node with each codec the gateway decompresses, gzip and zstd (#596), with librdkafka's message debug on | Every record reads back byte-exact and in order, and the watermark after each codec's stream equals the records produced. librdkafka's own debug lines say which batches it compressed: zstd must arrive compressed, so the gateway decompressed it and Fetch served it uncompressed. Against this gateway librdkafka 2.8.0 logs "Broker does not support compression type gzip: not compressing batch" and sends gzip, snappy and lz4 uncompressed, because its feature check wants protocol versions below the gateway's Produce v3 floor; the scenario reports that for gzip and does not count it as coverage. Batches the pinned Java producer compressed with gzip and zstd are decoded by the gateway's unit tests (`crates/vtop-kafka/tests/fixtures`). A codec the local kcat was built without is skipped by name (`kcat -V` features); with no codec compressed the scenario exits 77 (SKIP). |
| `27-kafka-client-follows-failover` | Three candidates (#284), each serving the Kafka gateway and listing all three as brokers in `kafka.brokers`. ONE idempotent librdkafka producer (`kcat`, `enable.idempotence=true`, `acks=all`, stock timeouts) bootstrapped from all three streams 4,000 keyed records at 100 a second; after 800 acknowledgements the range's leader is `SIGKILL`ed. The client is not restarted or reconfigured and no survivor is restarted or re-rendered (#751). | Exactly one candidate reports `vtop_kafka_gateway_serving` 1, and the two that follow list all three brokers and name the holder as the partition's leader. A survivor takes the range at a newer epoch with a role-change line, not a start, and its gateway logs that it is serving at that epoch. The same producer process finishes all 4,000 records with no failed delivery and no fatal idempotence error. The same client reads everything back through the new leader byte-exact, in order, with no key twice. The high watermark is at most 4,000 plus one boundary marker per epoch. The time from the kill to the first acknowledgement after it, and to the end of the longest silence that ends after it, are printed and bounded by `CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS` (60). |
| `28-kafka-jvm-client-failover` | The same failover with the JVM client of an Apache Kafka distribution (`kafka-verifiable-producer`, idempotent, `acks=all`, 3,000 records at 100 a second, the leader killed after 600 acknowledgements), once with `metadata.recovery.strategy=rebootstrap` and once with `none`, each on a fresh cluster. Skips without `CHAOS_KAFKA_HOME` or `CHAOS_KAFKA_IMAGE`; CI sets the pinned `apache/kafka` image (#751). | For each strategy: a survivor takes the range in place and its gateway says it is serving; the producer finishes all 3,000 sends with none failed; `kafka-console-consumer` reads partition 0 back and every acknowledged value is at exactly the offset it was acknowledged at, once, and in produced order; the kill-to-first-acknowledgement and kill-to-resume times are printed. |
| `30-kafka-failover-colocated` | Scenario 27 with `CHAOS_KAFKA_SHAPE=colocated`: three `vtop-node node` processes, each a metadata voter and a candidate serving the Kafka gateway, the shape the Helm chart renders for a replicated topology and `benchmarks/kafka_compare/vtop/ha-node-*.yaml` run. The `SIGKILL` of the range's leader also removes a Raft voter, which may be the metadata leader (#751). | Every assertion of 27. The lease is read from the two survivors only, so a run that killed the metadata leader waits for an election rather than reading a dead node; the result line names the shape, the profile, the lease and whether the killed process hosted the metadata leader (`killed_meta_leader`). |
| `31-kafka-jvm-failover-colocated` | Scenario 28 with `CHAOS_KAFKA_SHAPE=colocated` (the JVM client, both `metadata.recovery.strategy` values, each on a fresh co-located trio). Skips as 28 does (#751). | Every assertion of 28, in the co-located shape; under `none` the client still finds the new leader through a surviving member's metadata. |
| `32-kafka-paused-leader` | Three candidates serving Kafka (as 27); one idempotent librdkafka producer streams; after 800 acknowledgements the leader is `SIGSTOP`ped. While it sleeps a survivor takes the range, one raw Kafka Produce (the request a client holding pre-failover metadata sends) is queued in the paused leader's socket, and the producer resumes through the new leader; then the old leader is `SIGCONT`ed (#751). | Safety: the queued Produce is not acknowledged, whichever of the old gateway and its lease agent runs first; no record sent to the old leader appears in the new leader's log; every acknowledged record reads back once and in order. Liveness: the old leader's `vtop_kafka_gateway_serving` goes to 0 within one renew plus one poll interval (plus 2 s for the scrape) of the resume, it logs a role change to follower, its Metadata names the new leader, and a second raw Produce to it is answered `NOT_LEADER_OR_FOLLOWER`; the producer finishes every record with no failed delivery, and the pause-to-resume time is printed and bounded by `CHAOS_KAFKA_PAUSE_LIMIT_SECONDS`. Control: the same raw Produce sent to the new leader is acknowledged, so the refusals are the old leader's answer and not a malformed request. |
| `33-kafka-consumer-group-failover` | Three candidates serving Kafka (as 27). ONE stock group consumer with auto-commit on, bootstrapped from all three (`kcat -G` at librdkafka's 5 s interval, then, with a Kafka distribution, `kafka-console-consumer --group` every 500 ms, each on a fresh cluster), reads while one idempotent librdkafka producer streams 3,000 records; after 800 deliveries the group's committed offset is read from the metadata plane and the range's leader, which is also the group's coordinator, is `SIGKILL`ed. Group membership is not carried across a failover; the committed offset is (#751). | The same consumer process delivers every produced record at least once, with its produced value, at one offset, first delivered in produced order. Every rewind goes back no further than the offset committed before the kill, so only records after the last commit are delivered again; they are counted, not bounded by a number, because librdkafka keeps fetching through the failover while its commits wait for a coordinator, and everything it delivers before it rejoins comes again. The committed offset kept up before the kill (it covers what was delivered one commit interval and 2 s earlier). No offset reset (`OFFSET_OUT_OF_RANGE`). The group's committed offset on the plane reaches the high watermark, and a fresh member of the group reads nothing. The time from the kill to the end of the longest silence in the deliveries (the rejoin and resume) is printed and bounded by `CHAOS_KAFKA_FAILOVER_LIMIT_SECONDS`; for `kcat` so is the time until it reports the partition assigned again. |
| `29-leader-ahead-after-full-restart` | A whole replica set killed at once and restarted over a PLAINTEXT plane, on a range in the default (v1) format (#599): after a shared prefix both followers are `SIGKILL`ed, the leader alone appends and fsyncs one batch no quorum can acknowledge (the client is told `quorum not reached`), then the leader is `SIGKILL`ed and all three nodes restart, so the leader's log is strictly longer than both followers' and its retransmission buffer is empty. `CHAOS_AHEAD_SHARED`, `CHAOS_AHEAD_LEADER_ONLY` and `CHAOS_AHEAD_AFTER` (defaults 1,000, 100 and 1,000) size the three stretches and `CHAOS_AHEAD_RECOVERY_SECONDS` (default 60) bounds the wait. | A quorum produce succeeds within the bound with no operator action; every record, the batch only the leader held included, reads back byte-exact and in order; both followers' committed offset equals the whole log; all three sealed segments pass `vtopctl segment verify`. |

Scenario 07 skews a Raft **voter**. Skew on a lease **proposer** (a data node)
is a different path and is covered deterministically, not live, in
`crates/vtop-meta/tests/raft_three_node.rs`: the metadata leader refuses a
lease request whose clock is further from its own than the tolerance below
(#555), and that test drives a proposer an hour fast and an hour slow through
the real propose path, asserting neither can take a lease, push a deadline out,
or renew; that a follower redirects instead of reporting skew; that a retry of
an already-committed request is answered from the dedup table; and that
takeover follows the replicated deadline (refused one millisecond before it,
granted at it). A live scenario that skews a lease-holding data node would build on
`09-range-leader-failover` and has not been written.

What the guard does and does not do. It **bounds** the error; it does not
remove it. An accepted request keeps the proposer's timestamp, and the
tolerance (`lease_skew_bound_ms`) is the greater of a twentieth of the lease and one second,
capped at a tenth of the lease, so a deadline can be off by up to that much in either
direction, and a holder slow by it facing a rival fast by it loses twice it from the lease.
The lease agent's pacing validation subtracts that twice-the-tolerance from the lease before
checking that one lost round fits; the cap keeps it to a fifth of any lease, which is what
lets this suite's 6 s / 2 s / 0.5 s pacing pass (4.5 s needed, 4.8 s left). The leader's check
is also made on a snapshot of its role immediately before the proposal, not
inside the proposal: if leadership moves to another node in that window, one
skewed request can be replicated without being judged. That request is a
single lease deadline, and the next acquire or renewal is judged by the new
leader; closing the window outright would need the check inside a
leader-serialized proposal path, which this change does not attempt. The test
covers an hour-fast renewal and an hour-slow acquisition; it does not skew a
holder after a successful renewal, so takeover timing against a holder whose
clock drifts mid-lease is exercised only for the replicated deadline itself.

The data scenarios stop each process before offline sealing. Recovery first
truncates any tail beyond the durable commit boundary, then `vtopctl segment
verify` checks the resulting sealed bundle. For the disk-full follower, the
quiesced namespace-visible active file and commit boundary are copied out
before its private tmpfs disappears.

## Kafka clients and a failover (#751)

Until #751 a Kafka client could not follow a VTOP failover: a candidate refused
the gateway, and the one shape that fails over without a restart is every node
a candidate. Scenarios 27 and 28 are the live proof that it can now, with the
two Kafka clients that matter, and they measure what the client sees.

Both start three candidates with `kafka.brokers` on every one (the same list,
each at its own broker id), bootstrap the client from all three, and
`SIGKILL` the lease holder while the client is mid-stream. The client is the
same process before and after, with no restart and no configuration change.

Scenarios 30 and 31 run 27 and 28 unchanged on the co-located shape: three
`vtop-node node` processes, each a metadata voter and a candidate, as the Helm
chart deploys a replicated topology. There the kill also takes a Raft voter,
and when that voter led the metadata plane the survivors elect a new leader
before the lease can move; each result line says whether that happened
(`killed_meta_leader`). Scenario 32 pauses the leader instead of killing it,
which is the case a lease exists for: the old leader keeps its sockets, wakes
up still believing it leads, and must acknowledge nothing and refuse by name.

**What is measured.** Two times, from the kill, both in milliseconds, taken from
the clock the client's acknowledgements are stamped with:

- *kill to first acknowledgement after it*: the first acknowledgement the
  client reports after the kill. It can be a straggler the dead leader sent
  just before it died (in two runs it arrived 1 ms after the kill), so it is not
  the outage.
- *kill to resume*: the end of the longest silence between acknowledgements
  that ends after the kill. This is the outage the client lived through, and
  the number to read.

kcat prints a delivery report when its main loop next polls, which at this pace
is once per input burst (about every 0.1 s), so its times run late by up to
that. Its first acknowledgement after the kill (about 0.34 s in every run) can
only be one the leader sent before it died, since no node serves until the
survivor does. The JVM tool prints each one from the client's own callback. In
run 3 of scenario 27 the new leader logged that its gateway was serving 5.5 s
after the kill, and the client's first acknowledgement after the silence
followed 0.3 s later.

**Measured**, four runs of each scenario (run 4 on the tree with main merged in), every record of every run read back:

| Run | Client | Strategy | Kill to first ack | Kill to resume | Longest silence |
|---|---|---|---:|---:|---:|
| 1 | librdkafka 2.8.0 (kcat 1.7.1) | stock | 341 ms | 5,739 ms | 5,393 ms |
| 2 | librdkafka | stock | 341 ms | 6,558 ms | 6,208 ms |
| 3 | librdkafka | stock | 338 ms | 5,747 ms | 5,400 ms |
| 4 | librdkafka | stock | 283 ms | 6,493 ms | 6,202 ms |
| 1 | Kafka client 4.4.0 (JVM) | `rebootstrap` | 5,645 ms | 5,645 ms | 5,673 ms |
| 2 | Kafka client 4.4.0 | `rebootstrap` | 5,677 ms | 5,677 ms | 5,677 ms |
| 3 | Kafka client 4.4.0 | `rebootstrap` | 1 ms | 5,500 ms | 5,499 ms |
| 4 | Kafka client 4.4.0 | `rebootstrap` | 1 ms | 5,769 ms | 5,767 ms |
| 1 | Kafka client 4.4.0 | `none` | 5,724 ms | 5,724 ms | 5,738 ms |
| 2 | Kafka client 4.4.0 | `none` | 5,634 ms | 5,634 ms | 5,661 ms |
| 3 | Kafka client 4.4.0 | `none` | 5,591 ms | 5,591 ms | 5,630 ms |
| 4 | Kafka client 4.4.0 | `none` | 5,687 ms | 5,687 ms | 5,705 ms |

Every run: all records acknowledged, none failed, all read back through the
new leader once and in order, and under 7 s from the kill to the client
acknowledged again. One earlier attempt of scenario 27 stopped on a harness
check after the failover (a follower that never changed role was asked for a
role-change line) and is not counted. In the four runs of scenario 27 the new
leader's log names no retried set it acknowledged with an original offset, so
the duplicate check was not exercised by these kills; whether a retry meets it
depends on where the kill lands.

**Conditions.** One host: an Apple M5 Max under Docker Desktop 4.100.0, the
scenarios run in one `rust:latest` Linux container capped at 2 CPUs and 6 GB,
with debug binaries (as the CI job runs them), the three metadata nodes, the
three candidates and the client all in that container. The lease is the
harness's: 6 s long, renewed every 2 s, polled every 500 ms
(`CHAOS_LEASE_*`). The clients keep their stock timeouts and retry settings,
except that the Kafka tool sends with no retries unless told, and an
idempotent producer is refused with none, so scenario 28 sets
`retries=2147483647`, the client's own default. `enable.idempotence=true` and
`acks=all` throughout; one partition.

**What it says, and what it does not.** The outage tracks the lease:
5.5 s to 6.6 s against a 6 s lease. A killed leader's lease is still on
record until it lapses, so the wait is the rest of the lease, the poll, a
survivor opening the range at a write quorum (#750), and one metadata refresh.
With the chart's default lease of 15 s the same failover is expected to take
correspondingly longer; that was not measured, and these numbers are the
harness's short lease, on one laptop. They are not comparable with the failover
report's numbers, which use each system's shipped timeouts and a different
client for VTOP
([Kafka failover](benchmarks/kafka-failover.md)). Under `rebootstrap` and `none`
alike the JVM client found the new leader through its ordinary metadata
refresh: `rebootstrap` acts only when no broker the client knows answers for
`metadata.recovery.rebootstrap.trigger.ms` (5 minutes by default), which never
happened here because every member is listed in `kafka.brokers` and two of them
stayed up, so the two strategies behaved alike.

Group consumers are not exercised by these two scenarios. A group's membership
is in memory per gateway, so after a failover the members rejoin; committed
offsets survive (scenario 20 covers a commit and a resume, not across a
failover).

## Operation histories (#728)

The assertions above each check one outcome a scenario chose in advance. A
history checks the run as a whole: the clients record what they did and what
they were told, and a checker decides afterwards whether any single log could
have produced that. It is the Jepsen approach, scaled to what the native data
plane promises.

Recording. `vtop-node produce --history FILE` appends one JSON line before
each batch goes out (its producer, epoch and sequences) and one when it is
answered: `ok` with the offsets the broker answered with (a resent batch it already holds is answered with its original offsets), `fail` for a refusal made before
anything was appended (`WrongRange`, a sequence conflict), or `info` when the
outcome is unknown: no answer, a dropped connection, or an answer that does not
settle it. `Overloaded` ("quorum not reached") is `info`, because the leader
appended the batch before its quorum wait expired, so the records may still
surface; so is `Fenced`, because the broker checks the lease again after
appending and replicating, before it publishes the commit, and answers the
same way. `--fenced-probe` records every refusal as `fail`: the caller says
the target is a leader it fenced, whose records must never surface. `vtop-node verify --dump FILE` records what one read found
up to the committed high watermark, compressed into runs of consecutive
offsets and sequences. `lib.sh` puts each client's file under
`$WORKDIR/history/` (`history_file NAME`); `CHAOS_READ_HISTORY` makes
`await_verified_floor` record every attempt; `assert_fenced_produce` records
its probe (`--fenced-probe`) when given a history file, at a producer sequence
(2^40) no stream reaches, so the probe's record could not pass for one of the
stream's own.

Checking. `check_history` runs
`scripts/live-chaos/history/check_history.py` over every file and fails the
scenario on any violation, keeping the JSON summary in
`logs/history-check.json`. Against the last read, and every earlier read
against it:

| Check | What fails it |
|---|---|
| `lost` | an acknowledged record missing from its acknowledged offset, or a different record there (the #611 and #261 shapes) |
| `duplicate` | one offset acknowledged for two different records, one record acknowledged at two offsets, or an acknowledged or unanswered record visible a second time. A record is its producer's id, epoch and sequence: a retry from a new process sends the same records again, so an unanswered attempt cannot account for a copy of what its retry was acknowledged for, and a resent batch the broker answers with its original offsets is one copy |
| `order` | a producer's records out of sequence order: per producer id and epoch, across its processes, a higher sequence acknowledged at a lower offset (an unanswered batch is only accepted where it keeps that order) |
| `phantom` | a visible record that no acknowledged batch claims and no whole unanswered batch accounts for; a record of a refused (`fail`) batch, which is what a fenced leader's writes would be (the #641 shape). An unanswered batch accounts only for records no read showed before it was sent |
| `rewritten` | a record an earlier read saw, below the last read's high watermark, that the last read sees differently |
| `missed` | a record the last read holds below an earlier read's high watermark that the earlier read did not see: a committed record is visible to every read that covers it |
| `hwm` | a later read reporting a lower committed high watermark |
| `unkeyed` | a visible record whose key is not a producer sequence |

An unanswered batch may surface once, whole, or not at all; the checker
does not count either outcome as a violation. Matching unanswered batches to
the visible records they explain is a search: it backtracks, so a phantom is
reported only when no assignment of whole batches accounts for the records. Offsets the consumer never sees
(a promotion's boundary marker, #240) are gaps, not records, and are not
counted against anything; `verify --max-offset-gaps` still bounds them.

Known limits of the checker:

- **The search has a budget.** When matching unanswered batches to a visible
  run takes more than `SEARCH_BUDGET` (100,000) steps, the run is reported as
  unexplained, a `phantom`, even if some assignment would account for it.
  Example: thousands of overlapping unanswered batches from many producers
  under one long visible run. Live scenarios leave a handful of unanswered
  batches.
- **A record is first seen when its read ends.** Each record is stamped with
  the end of the first read that showed it, not with the moment that read
  fetched it, so an unanswered batch sent while a read was running may
  account for records that read had already fetched. Example: a read fetches
  sequences 0..9 at offsets 10..19, then a producer sends 0..9 under a new
  epoch, gets no answer, and the read ends. The copy is accepted as that
  batch.

- **Whole batches are judged against the last read only.** An unanswered
  batch must surface whole in the last read; an earlier read that showed
  part of it is not held to that. Example: sequences 0..9 acknowledged, an
  unanswered 10..19, a read showing 0..14 at watermark 15, then a read showing
  0..19 at watermark 20: valid.
- **An unanswered attempt's time is borrowed without placing it whole.** A
  record an acknowledged retry was answered for is accepted as earlier than
  the first read that showed it when any unanswered attempt of the same
  producer and epoch covers its sequence, whether or not that attempt could
  account for the whole read. Example: an unanswered 0..9, a read showing
  only 0..4 at watermark 5, then a retry of 0..4 acknowledged at 0..4: valid.

- **A read that `verify` rejects leaves nothing in the history.** `verify
  --dump` writes a read only when it completes, so a read stopped by a content
  mismatch, an offset going back or too many gaps is absent, and
  `await_verified_floor` retries it and overwrites its log. A later good read
  then passes the scenario and the checker. Example: scenario 09's read meets
  a corrupted acknowledged record once, and the retry reads it intact. The
  cluster soak marks such reads itself and fails on them.

- **Acknowledgements are judged against the last read only.** A history
  records when a read ended, not when it started, so a read that began after
  an acknowledgement and did not show it is not held to it. Example:
  sequences 0..9 acknowledged at 0..9, a read showing nothing at watermark 0,
  then a read showing all ten at watermark 10: valid.

The checker's own tests (`test_check_history.py`, run in the live-chaos job
before the scenarios) build histories by hand, clean ones and ones shaped like
#611, #261 and #641, and pin the verdict on each.

Scenarios 04, 09, 14 and 23 record and check data-plane histories.

### Who led: control-plane histories

The data-plane history says what the clients wrote and read. A second
recorder, `scripts/live-chaos/history/record_control.py`, says who led. It
polls until the scenario checks the history, and stamps every observation
with the wall-clock instants it was asked for and answered (`invoke_ms`,
`complete_ms`), so observations can be ordered in real time:

- `meta_status`: `vtopctl meta status` against each metadata node: its Raft
  term, its server state, and its vote (term, voted-for, committed);
- `lease_read`: `vtopctl meta range-lease`, the linearizable read of the
  range's lease, rotated over the metadata nodes: the range's fencing epoch,
  the lease's holder, epoch and deadline, and the applied index it was served
  at;
- `node_lease`: each data node's own `/metrics`: whether it is the authorized
  leaseholder (`vtop_broker_lease_active`) and at which epoch.

A failed observation (a killed node, a frozen one, a metadata group
mid-election) is recorded as failed and is evidence of nothing.
`control_history_start` in `lib.sh` starts it, the scenario's exit stops it,
and `check_history` runs `check_control.py` over what it has written so far
(a scenario that goes on is checked again later), keeping the summary in
`logs/control-check.json`:

| Check | What fails it |
|---|---|
| `meta_leader` | two metadata nodes Leader in one Raft term, or a committed vote naming another leader for the term |
| `lease_holder` | one lease epoch read with two holders |
| `lease_order` | a lease read that began after another ended reporting an older range fencing epoch, or "not found" for a range a read already found. The epoch only grows, so this real-time order is exactly what a linearizable history of reads allows |
| `meta_index` | the same for the applied index a read was served at: a linearizable metadata read never goes back in the log |
| `epoch_leader` | two data nodes acting as the leaseholder of one epoch, or a node acting as the leaseholder of an epoch metadata granted to another |
| `unknown_grant` | a node acting on an epoch that a lease read begun afterwards does not reach: a grant metadata never made, or lost |
| `stale_leader` | a deposed leader still acting at its epoch after a read that ended earlier reported a newer one, past the latest deadline any read reported for its lease, and still saying so 2 s later. A data node's gauges keep their last value while a scrape cannot take its lease view, so a single late observation can be a cached one |
| `coverage` | a scenario that started the recorder ends up with no answered observation of one kind (Raft status, lease read, data node), or the recorder stopped early: a recorder that saw nothing checked nothing |

Its limits: it sees what it polls, by default every 400 ms for a data node,
500 ms for a lease read and 2 s for a node's Raft status
(`CHAOS_CONTROL_NODE_MS`, `CHAOS_CONTROL_LEASE_MS`, `CHAOS_CONTROL_STATUS_MS`;
each `vtopctl` call is a process and a TLS handshake, and the scenarios run on
two CPUs), so a violation shorter than that can fall between two
observations. A lease with no deadline (an
operator's grant) is not held to `stale_leader`, and an operator's
revocation of a live lease is not modelled; no scenario does either.
`stale_leader` compares a data node's deadline with the recorder's clock,
which is sound on one host and not across hosts with skewed clocks.

Scenarios 14 and 23 record the control plane from the moment the candidates
start. In 23, should the candidate that lacks acknowledged records lead after
all, the scenario reads what it serves into the history and runs the
checkers before it fails, so the history is the first to judge the run.

### The checkers against the bugs they exist for

The unit tests feed the checkers hand-built histories shaped like each bug.
`scripts/live-chaos/history/reintroduce/run.sh <bug> [runs]` does the live
half: it copies the tree, applies a patch from that directory that undoes
the bug's fix, builds `vtop-node` and `vtopctl` from the copy, runs the
scenarios that reach the bug's shape against that build with this tree's
scenarios and checkers, and runs both checkers over each run's histories
whatever the scenario's own verdict. It exits 0 when a checker flagged a
run. The patches never ship: they are applied to a copy, and only this
script runs the build they make.

| Bug | The patch undoes | Scenarios |
|---|---|---|
| #611 | #616: a fence discards a suffix the candidate merely lacks | 23 |
| #641 | #649: a returning deposed leader's sessions open unfenced | 23 |
| #261 | #263: a diverged replica is not reconciled at the fence, and acks the new leader's records over its own | 23 |
| #640 | #648: a follower files caught-up records under its own adopted epoch | 14, 23 |

What the runs showed, on 2026-11-22 (debug builds of this branch, in a
two-CPU Linux container): **no run reached a bug's shape, so the checkers
have not yet been shown to fail on a live run with a bug re-introduced.**
That part of #728 stays open.

| Bug | Runs | What happened |
|---|---|---|
| #611 | 13 of scenario 23 | 11 passed; 2 stopped at the producer's 30-second wait for a long suffix, before any read. The #611 window is an acknowledged record above the surviving follower's known high-water mark, and in every run R had learned the mark of every acknowledged record before L died, so the patched fence discarded nothing acknowledged. |
| #641 | 12 of scenario 23 | Every run passed. The shape needs L to return holding records past R's tail; in these runs L's tail equalled R's every time, so there was nothing for an unfenced session to keep. (An unpatched run that day did reach it, with seed 604: L 20 records past R, and the scenario skipped as #641's guard requires.) |
| #261 | 12 of scenario 23 | As #641: the diverged suffix the patch leaves unreconciled never existed. |
| #640 | 3 each of scenarios 14 and 23 | Every run passed. The shape needs a follower to catch up records an older epoch wrote after adopting a newer one, then a second failover; neither scenario produces that sequence. With `--stall-ms`, the control checker would report the stall it causes. |

What would close it: a scenario that holds a follower's commit marks back
while the leader acknowledges (a delivery shim on the replica plane, for
#611), one that returns the deposed leader with an in-flight suffix on
purpose rather than by timing (#641, #261), and two failovers around a
catch-up (#640); or many runs of the existing ones, which the campaign
workflow's `reintroduce` input runs.

### The long campaign

`scripts/live-chaos/history/campaign.sh` runs the scenarios that hold a
nemesis (04 and 09 kill, 05 fills a disk, 06 partitions, 07 skews a clock,
14 kills, 23 kills and freezes) one after another with a fresh seed each,
until `CAMPAIGN_HOURS` (default 24) is up. Each run is judged by its own
assertions and, where it records histories, by both checkers; the campaign
writes one line per run to `runs.jsonl` and a `summary.json`, keeps the work
directory of every run that failed, and exits 1 if any did.

It runs from `.github/workflows/chaos-campaign.yml`, on `workflow_dispatch`
only: never on a pull request or a schedule. A hosted job stops at six
hours, so the total is split into legs that run side by side (24 hours as six
legs of four hours by default), and a report job sums them. The same
workflow, given `reintroduce`, runs the re-introduction runner instead. No
24-hour campaign has been run yet: that is the maintainer's to start.

## Engine crash sweep (#725)

The scenarios above drive the cluster plane. The archive engine (`vtopctl
run`: a source, batches, the object store, the ledger) has its own harness,
`scripts/live-chaos/engine/run-sweep.sh`, which sources the same `lib.sh`. It
kills the engine with `SIGKILL` at every failpoint, against a real source
(a Kafka broker, a file, or a syslog spool file), the lab's MinIO and a
ledger (SQLite, or PostgreSQL), restarts it and checks what the restart made
of it. Seven legs
run the same script with different knobs (see "The legs" below).

**System model.** One crash-recovery engine process; a partially synchronous
network; honest processes. The fault is the `SIGKILL`; two legs also hold a
second fault (the store down, the work directory full) while the engine runs
towards the failpoint.

**How the kill lands.** A failpoint, not a timer: a build with the cargo
feature `failpoints` reads `VTOP_FAILPOINT=<point>` (or `<point>:<n>` for the
nth hit) and, on reaching that line, sends itself `SIGKILL`: no destructor
runs and nothing is flushed. The feature is off by default; without it the
hook is an empty inline function and the variable is not read. No release
archive, container image or chart enables it, and
`scripts/check-failpoints-absent.sh` (the `failpoints absent from release
builds` job) fails if the workspace's default features, `release.yml` or the
Dockerfile ever would. The same script, given a built binary, looks for the
line the failpoint prints before the kill: ci.yml's test job runs it on its
release build of `vtopctl` (it must be absent), and the sweep runs it on its
own build (it must be present, the positive control). An unknown point refuses to start and lists the valid
ones; the sweep reads its point list from that refusal, so the list lives in
one place (`crates/vtop-cli/src/failpoints.rs`) and a full run fails if it
did not run every entry.

**The points.** Ledger points are placed by a decorator around the state
store, so every path that writes a transition (the pipeline, recovery, the
settle pass) has them; the others are explicit calls.

| Point | Where the engine is when it dies | Ledger row at the kill |
|---|---|---|
| `before_insert`, `after_insert` | around the row's first write | none / `BATCHING` |
| `before_ledger_<S>`, `after_ledger_<S>` for S in `sealed`, `compressed`, `checksummed`, `object_uploaded`, `manifest_uploaded`, `verified`, `source_committed` | around the transition to S | the state before S / S |
| `mid_object_put` | a multipart upload has stored a part and is not complete (the sweep sends this leg's objects as multipart) | `CHECKSUMMED` |
| `after_object_put_before_ledger` | the object is in the store, the ledger does not say so | `CHECKSUMMED` |
| `after_manifest_put_before_ledger` | the manifest is in the store, the ledger does not say so | `OBJECT_UPLOADED` |
| `before_source_commit` | VERIFIED is in the ledger, the source's position is not committed | `VERIFIED` |
| `after_source_commit_before_ledger` | the source's position is committed (the Kafka offset on the broker; a file's cursor in the engine), SOURCE_COMMITTED is not written | `VERIFIED` |

`crates/vtop-cli/tests/failpoints.rs` pins this table on every build of the
feature: each point kills a real `vtopctl process-once` (exit by signal 9),
the ledger is in the state above, and a restart archives every record of a
local file into a local store.

**The legs.** The issue asks for "SQLite and PostgreSQL, Kafka and file
sources, and with a store outage and ENOSPC". Each leg is the whole script with
`ENGINE_SWEEP_BACKEND`, `ENGINE_SWEEP_SOURCE` and `ENGINE_SWEEP_FAULT` set, so
every leg is held to every invariant below.

| Leg | Ledger | Source | Fault while the engine runs towards the point |
|---|---|---|---|
| `sqlite-kafka` | SQLite | Kafka | none (the baseline, and the only leg a pull request runs) |
| `postgres-kafka` | PostgreSQL, one database per point, schema applied by `vtopctl migrate` | Kafka | none |
| `sqlite-file` | SQLite | one file, read through the file source | none |
| `sqlite-spool` | SQLite | one spool file, read through the syslog spool source (its own adapter: cursor, markers, replay); its commit rule is read from the ledger's cursor, as for a file | none |
| `sqlite-kafka-store-outage` | SQLite | Kafka | MinIO is stopped (`docker stop`) for up to `ENGINE_SWEEP_FAULT_SECONDS` (12), then started again; the engine is not restarted in between |
| `sqlite-kafka-enospc` | SQLite | Kafka | the engine's work directory is a 16 KiB tmpfs with no free page, for the same time, then the filler is deleted; the engine is not restarted in between |
| `postgres-kafka-host-loss` | PostgreSQL | Kafka | none; the kill takes the host (#739): the dead engine's work directory is deleted and the restart is a standby with a new one, on the same ledger, group and bucket ([Losing the engine's host](deploy/engine-host-loss.md)) |

In the two fault legs a point the engine reaches while the fault is on is
killed there (a kill with the store down, or the disk full); a point that
waits on the store or the disk is reached after the fault lifts, so the
engine has to recover from the failed attempts without a restart, and then
dies at the point. Either way the restart and every check that follows are
the same as without a fault.

**What is checked, per point.** Each point has its own ledger (a SQLite file
or a PostgreSQL database), work directory and object prefix; a Kafka leg also
gives each point its own consumer group over one seeded topic, and the file
leg reads one file written once.

| Invariant | Kind | How |
|---|---|---|
| The commit rule at the source: the source's committed position never passes the end of the highest range the ledger records as VERIFIED | safety | Kafka: the group's committed offset, read from the broker (`kafka-consumer-groups.sh`), against the ledger, at the kill and again after the restart. "No committed offset" counts only when the broker said so (the offsets table without one, or the group not found); a query that failed or timed out, which the tool reports with exit status 0, is retried three times and then fails the check. File: the file's committed position is in the engine and the ledger is its durable record, so the position is the ledger's cursor (the highest SOURCE_COMMITTED end byte); that comparison cannot fail by itself, so the next two rows are what bites for a file |
| The commit rule in the ledger: no SOURCE_COMMITTED row lacks what the VERIFIED write leaves (the verification class, which only `mark_verified` sets, the object and the manifest) | safety | the ledger read at the kill and after the restart: SQLite opened read-only, so the WAL the kill left is not checkpointed; PostgreSQL through `psql` in the server's container |
| The order, whatever the source: at the kill a ledger row is in the state the failpoint sits beside (the table above), the newest live one when batches did not overlap, so the engine reached the source commit only with the batch VERIFIED | safety | `KILL_STATE` in `sweep_checks.py`, a unit test holds it equal to the state table in `tests/failpoints.rs` and to the points the engine defines |
| No hole | safety | `vtopctl audit-coverage --check-storage` passes, `vtopctl scrub` is clean, and every produced record is found in the objects the store holds, copied out with `mc` and counted by the harness, not by the engine |
| Duplicates only where the at-least-once window is open | safety | the extra copies are counted against the table below |
| The restart commits every record and stops cleanly | liveness | bounded poll on the group's committed offset and the ledger, then `SIGTERM` and exit 0 |

| Points | Extra record copies allowed | Why |
|---|---|---|
| `before_insert` to `after_ledger_checksummed`, and `mid_object_put` | 0 | no object of the batch is in the store yet; the replay builds it from the source |
| every later point | one batch (`ENGINE_SWEEP_BATCH_RECORDS`, 20) | the store holds the batch's object while its range is uncommitted; a replay that cannot rebuild the same bytes writes a second object ([ARCHITECTURE](ARCHITECTURE.md) section 13, item 3) |

The sweep runs one batch at a time through upload and verification
(`batching.max_concurrent_batches: 1`). Batches stay in flight across cycles
([ARCHITECTURE](ARCHITECTURE.md) section 5.2), so a batch read after the one
the kill interrupts can already be VERIFIED and waiting for it to commit; the
order check therefore accepts the failpoint's state on any row, not only the
newest. Such a waiting row is committed by recovery, not replayed, so the
extra copies allowed stay at one batch.

**Running it.**

```bash
cargo build -p vtop-cli --features failpoints --bin vtopctl
docker compose build minio && docker compose up -d --wait minio
# a broker that advertises an address the engine can reach, for example the
# kafka service of the `engine crash sweep` workflow, then:
CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
ENGINE_SWEEP_POINTS=before_source_commit CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
```

The other legs add knobs to the same script:

```bash
# PostgreSQL ledger: a build with the postgres feature, and a running server
cargo build -p vtop-cli --features failpoints,postgres --bin vtopctl
ENGINE_SWEEP_BACKEND=postgres ENGINE_SWEEP_PG_CONTAINER=<container> CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
# file source, or the syslog spool source
ENGINE_SWEEP_SOURCE=file CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
ENGINE_SWEEP_SOURCE=syslog_spool CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
# host loss (PostgreSQL ledger and Kafka source only)
ENGINE_SWEEP_FAULT=host_loss ENGINE_SWEEP_BACKEND=postgres ENGINE_SWEEP_PG_CONTAINER=<container> CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
# store outage / ENOSPC (the latter mounts a tmpfs: root or passwordless sudo)
ENGINE_SWEEP_FAULT=store_outage CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
ENGINE_SWEEP_FAULT=enospc CHAOS_PROFILE=debug scripts/live-chaos/engine/run-sweep.sh
```

Knobs (`ENGINE_SWEEP_POINTS`, `ENGINE_SWEEP_BACKEND`, `ENGINE_SWEEP_SOURCE`,
`ENGINE_SWEEP_FAULT`, `ENGINE_SWEEP_FAULT_SECONDS`, `ENGINE_SWEEP_RECORDS`,
`ENGINE_SWEEP_BATCH_RECORDS`, `ENGINE_SWEEP_POINT_TIMEOUT_SECONDS`, the Kafka,
PostgreSQL and MinIO containers, the S3 endpoint and bucket) are documented
and validated in the script header. `CHAOS_VTOPCTL` points at the failpoints
build when it is not `target/$CHAOS_PROFILE/vtopctl`.

**Where it runs.** The `engine crash sweep` workflow
(`.github/workflows/engine-crash-sweep.yml`): every point on every leg
nightly at 06:37 UTC, and on dispatch (which can name points, a record count
and legs); the five commit-boundary points (`mid_object_put`,
`after_object_put_before_ledger`, `after_manifest_put_before_ledger`,
`before_source_commit`, `after_source_commit_before_ledger`) of the
`sqlite-kafka` leg on a pull request that changes the engine, the ledger, the
upload path, the adapters or the harness (and every leg, with those five
points, on a pull request that changes the workflow itself, since the nightly
runs only from `main`). The job is a matrix over the legs;
its check names are `engine crash sweep (real Kafka, MinIO) (<leg>)`. The
PostgreSQL image is the one `postgres-matrix.yml` pins for major 18.

**Measured.** Local runs, all 21 points, 50 records each, batches of 20, a
debug build in a container capped at two CPUs, `apache/kafka:4.3.1` and the
lab's MinIO capped at one CPU each (and PostgreSQL 18 at one for its leg), all
on one laptop. Every leg passed, with no duplicate at any point:

| Leg | Time | Per point |
|---|---|---|
| `sqlite-kafka` | 262 s | 12 to 13 s |
| `postgres-kafka` | 285 s | 13 to 14 s |
| `sqlite-file` | 98 s | 4 to 5 s |
| `sqlite-spool` | 74 s | 3 to 5 s (measured later, with the MinIO image only: no broker running) |
| `postgres-kafka-host-loss` | 244 s | 10 to 15 s; 8 to 12 s from the kill to every record committed by the standby (measured later, Kafka, MinIO and PostgreSQL 18 at under a CPU each) |
| `sqlite-kafka-store-outage` | 616 s | 22 s for a point reached before the store is touched, 33 to 35 s for the rest (12 s of outage, then the recovery) |
| `sqlite-kafka-enospc` | 495 s | 12 to 13 s, then 25 to 28 s once the staging write needs space |

The five pull-request points of the `sqlite-kafka` leg take about 55 s.

**What a broken commit order looks like.** With `mark_verified` moved behind
the source commit (a deliberate mutation of the engine: the ledger's
transitions stay legal, so neither the state machine nor the database's
trigger can see it), the commit-boundary points fail on every leg that has a
ledger and a source to compare it with:

| Leg and point | Result |
|---|---|
| `sqlite-kafka`, `after_source_commit_before_ledger` | "COMMIT RULE VIOLATED: the consumer group's committed position is 20, but in the ledger no range is recorded as VERIFIED or SOURCE_COMMITTED, so positions 0..19 were committed at the source before they were verified. Ledger states: {"MANIFEST_UPLOADED": 1}" |
| `postgres-kafka`, `after_source_commit_before_ledger` | the same message, read from PostgreSQL |
| `sqlite-kafka`, `postgres-kafka` and `sqlite-file`, `before_source_commit` | "the ledger holds a MANIFEST_UPLOADED row, but a kill at this point must find a VERIFIED row: the engine reached the point out of order" |
| `sqlite-file`, `after_source_commit_before_ledger` | the same: a file's committed position is in the engine's memory, so the broker comparison has nothing to read, and the ledger state at the kill is what shows the commit running ahead |
| `sqlite-spool`, `before_source_commit` | "the ledger holds a MANIFEST_UPLOADED row, but a kill at this point must find a VERIFIED row: the engine reached the point out of order" (run later, on this leg alone) |

The three store-write points (`mid_object_put`,
`after_object_put_before_ledger`, `after_manifest_put_before_ledger`) pass
under the mutation on every leg, as they should: it changes nothing before the
source commit. With the mutation reverted, all fifteen runs (those three and
the two above, on three legs) pass. The order check at the
kill (`KILL_STATE`) is also what fails `before_source_commit` on the
`sqlite-kafka` leg, where the broker's offset has not moved yet and the
offset comparison could not.

**Not covered.** Power loss, which
`kill -9` does not model (the page cache survives it), stays with
`crates/vtop-cli/tests/ledger_power_loss.rs`; PostgreSQL with the file source
(the two knobs are independent, but the nightly runs the PostgreSQL ledger
only with Kafka); a PostgreSQL outage while the engine runs; and a kill with
the store down or the disk full combined with the file source or PostgreSQL
(the fault legs run the `sqlite-kafka` shape).

## Scope boundaries

This harness is an integration foundation for issue #215, not the final
production cluster daemon.

- Metadata and native data-plane processes are assembled separately, except
  in scenario 10, which hosts both in one process, and scenarios 30 and 31,
  which host a voter and a candidate in each of three. Range leadership on the
  data plane follows metadata leases from the live Raft group: scenarios 09,
  14, 23, 27, 28, 30 and 31 kill a range leader and assert that another node
  takes the range under a lease (09 also asserts that the restarted old leader
  is refused as fenced), 32 pauses one past its lease and asserts that it
  comes back deposed, and 33 kills one under a consumer group. Scenarios 30 and 31 host a metadata voter and a candidate
  in each of three processes, so the kill also removes a voter. Membership changes (scenarios 01 to 03) and data replica-set
  changes (scenario 12) are driven by the operator; the harness does not
  claim that a data node reacts to a membership change on its own.
- Scenario 03 validates metadata voter replacement, and only that. The
  #180/#181 replacement-proof lifecycle for a sealed DATA replica is
  exercised end to end by scenario 12 — operator-driven, through
  `vtopctl node repair --seal-tail`, which seals the leader's active tail
  into the transferable prefix and asserts the repaired replica holds the
  full acknowledged floor (the closure #306 tracked) — so the one thing
  still unproven here is automated repair with no operator in the loop.
- Scenario 04 proves acknowledged durability and same-directory recovery;
  it does not fail over. Lease-driven failover is scenarios 09, 14, 23, 27,
  28, 30, 31 and 33; 27, 28 and 30 to 33 are the ones a Kafka client rides
  through.
- Consumer-group cursors are exercised live through the Kafka gateway:
  scenario 20 commits a group's offset as a cursor on the metadata plane, and
  a second consumer resumes from it, and scenario 33 carries a running group
  through a kill of its coordinator, resuming from the committed offset with
  no restart (scenarios 19 and 21 run groups on nodes with no metadata plane,
  where every commit is refused by name). Group membership itself is not
  carried across a failover; members rejoin. The native
  protocol's own cursor commit is covered by the metadata state machine and
  deterministic Raft tests, not by this harness, so it claims no end-to-end
  cursor continuity across membership changes.
- Offline segment verification applies to scenarios that create data-plane
  artifacts. Metadata-only scenarios have no segment files to verify.

These boundaries must remain explicit in pull-request and issue updates;
passing this suite alone is not grounds to close every remaining #215 item.
