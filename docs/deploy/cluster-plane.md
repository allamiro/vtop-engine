# Native cluster plane

The cluster plane is VTOP's own replicated log, built alongside the archive
engine and not needed by it. This page runs it as processes you manage; on
Kubernetes, use [Kubernetes with Helm](kubernetes.md). What it is:
[Architecture](../concepts/architecture.md#the-cluster-plane-optional). Its
design is the [native broker architecture](../NATIVE_BROKER_ARCHITECTURE.md).

!!! warning "Prototype"

    Read the known limitations of the release you deploy in the
    [roadmap](../ROADMAP.md) before depending on it.

## Prerequisites

- `vtop-node` and `vtopctl` from the same release.
- Three or five hosts (or ports) for the metadata voters.
- A CA and per-node leaf certificates for each plane, with the CN rules on
  [Kubernetes with Helm](kubernetes.md#tls-secrets).
- A YAML config per node. Every config struct rejects unknown keys, so a
  misspelled key is a startup error. The quickest way to see a complete,
  valid config is to render the chart: `helm template` writes each node's
  config into the ConfigMap. The live-chaos harness,
  [`scripts/live-chaos/lib.sh`](../../scripts/live-chaos/lib.sh), writes and
  starts real three-node clusters and is the reference for hand-written
  configs.

## Steps

1. **Start each node** in its role:

    ```bash
    vtop-node meta --config meta.yaml   # a metadata Raft voter
    vtop-node data --config data.yaml   # a data-plane replica
    vtop-node node --config node.yaml   # both roles in one process
    ```

2. **Bootstrap the metadata group once**, with every listed voter running and
   empty: `vtopctl meta init --members 1,2,3 --config admin.yaml`.

3. **Register topics, ranges and nodes** through `vtopctl meta`
   (`create-topic`, `register-node`, and the rest; `vtopctl meta --help` lists
   them).

The ports the Helm chart uses for each plane:

| Port (chart default) | Plane |
|---|---|
| 9100 | Metadata peer (Raft) |
| 9200 | Metadata admin (`vtopctl meta`) |
| 9300 | Replica plane |
| 9400 | Native produce and fetch |
| 9500 | `/metrics`, `/healthz`, `/readyz` (unauthenticated) |

### The Kafka gateway

The gateway is a `kafka:` block in a data node's config. A leader, a
standalone node or a candidate may serve it (a static follower may not), and
its listener stays on loopback unless the config sets `any_interface`. The
Helm chart renders it for a replicated range when `data.kafka.enabled` is set
([Helm](../install/helm.md#the-kafka-gateway-on-a-replicated-range)).

| Key | Default | Meaning |
|---|---|---|
| `kafka.brokers` | - | Candidates only, and required there. Every member of `peers` as a Kafka broker, `{ node_uuid, node_id, host, port }`, this node included at its own `kafka.node_id`; the same list on every member. Metadata lists them all and names whichever member holds the lease as the partition's leader, and FindCoordinator names it as the group coordinator, so a client bootstrapped from any member follows a failover with no restart. Ids are unique in 0..=255 and endpoints are distinct. A candidate does not take `kafka.partitions` or `kafka.topics`. |

#### What a Kafka client sees through a failover

On a replicated range every candidate serves the gateway and lists every
member in `kafka.brokers`, so a client that bootstrapped from any one of them
is never told only about a broker that is gone.

| Moment | What the member answers | What the client does |
|---|---|---|
| Steady state | The member holding the lease serves Produce, Fetch and ListOffsets. A member that follows answers each `NOT_LEADER_OR_FOLLOWER`, and its Metadata lists every member and names the lease holder as the partition's leader, at its epoch. | A client that asked a follower is sent to the holder by its next metadata refresh. |
| The holder dies | Its connections reset. Until its lease lapses the survivors still name it as the leader; once it has lapsed they name nobody (`LEADER_NOT_AVAILABLE`) until a survivor takes it. | The client's requests fail and it refreshes metadata from a member it knows. Under `metadata.recovery.strategy=rebootstrap` or `none` alike, it still knows the two survivors. |
| A survivor takes the lease | Its gateway installs a bridge over the broker it just opened. The INFO line `kafka gateway for range ... is now serving at epoch N` says so. The other survivor's Metadata names it. | The next refresh finds the new leader. The client retries what it had not been acknowledged for. |
| A retry | The new leader finds the client's producer in the log it inherited (the identity is derived from the range, the client's producer id and epoch) and acknowledges a batch it already holds with its original offsets. | An idempotent producer sees no error and no duplicate. |

What the operator does: nothing. The failover needs no restart, no config
change and no client change. The client's outage is the lease's lapse, the
election of a survivor, the survivor opening the range at a write quorum
([#750](https://github.com/allamiro/vtop-engine/issues/750)), and one metadata
refresh. Live-chaos scenarios 27 and 28 measure it for librdkafka and for the
JVM client, with `metadata.recovery.strategy` both ways, and 30 and 31 in this
chart's co-located shape: with release binaries and the chart's default lease
(15 s) a producer resumed 12.5 s to 15.9 s after the leader was killed, on one
GitHub-hosted runner (2 vCPUs, every process on one host)
([Kafka clients and a failover](../LIVE_CHAOS_VALIDATION.md#kafka-clients-and-a-failover-751)).
A leader that stalls past its lease and comes back acknowledges nothing and
refuses by name (scenario 32).

Known limits. A group's membership is held in memory by each gateway, so after
a failover the members rejoin; committed offsets are on the metadata plane and
survive. Measured (scenario 33, same conditions): a stock consumer group, the
same process, read on 13.0 s to 14.4 s after the kill from its committed
offset, and the records it had consumed after its last commit came again (41
for a JVM consumer committing every 500 ms; 369 to 1,761 for librdkafka, which
commits every 5 s and reads on through the failover before it rejoins). Size
`auto.commit.interval.ms` for what a consumer may read twice. `kafka.brokers` must name what clients dial: a client that cannot
reach or resolve the host a member advertises cannot follow a failover to it.

How it
compares with Apache Kafka, with every condition, is in the
[Kafka comparison](../benchmarks/kafka-comparison.md) and
[Kafka failover](../benchmarks/kafka-failover.md) reports.

## Verify it works

- `vtopctl meta status --config admin.yaml`: membership and a leader.
- `vtopctl node status --config node-client.yaml`: each replica's committed
  offset and lag.
- `/readyz` on each node's observability address.
- [Live chaos validation](../LIVE_CHAOS_VALIDATION.md) describes the
  scenarios the harness runs, and what they do not prove yet.

## What to monitor

The node metrics and readiness rules are in
[Node endpoints and metrics](../NODE_OBSERVABILITY.md), and the provisioned
cluster alerts on [Metrics and alerts](../operate/monitoring.md#alerts).

## Upgrade notes

Upgrade every node first, then raise the feature level with
`vtopctl meta node-versions` and `vtopctl meta enable-features`. Raising is
permanent; see [Upgrades](../operate/upgrades.md#2-then-raise-the-cluster-feature-level).
