# Cluster plane

The cluster plane is VTOP's own replicated log, built alongside the archive
engine. It is optional: the archive engine does not need it. Its design is
[the native broker architecture](../NATIVE_BROKER_ARCHITECTURE.md) and its
release-by-release status, including what each release does not do, is the
[roadmap](../ROADMAP.md).

!!! warning "Prototype"

    Read the known limitations of the release you deploy in the
    [roadmap](../ROADMAP.md) before depending on the cluster plane.

## Parts

| Part | What it does |
|---|---|
| **Metadata Raft group** | Three or five voters holding cluster metadata: topics, ranges, node registrations, leases, placement and the feature level. Administered with `vtopctl meta`. |
| **Data plane** | Range replicas that store sealed, proof-carrying segments. A range is led by the holder of its metadata lease; followers replicate, and a write is acknowledged on a quorum. Inspected with `vtopctl node`. |
| **Promotion** | When the lease holder fails, a candidate takes the range after establishing the committed boundary from a quorum of fenced replicas. |
| **Kafka gateway** | A Kafka wire-compatible listener in front of a range, so Kafka clients can produce and fetch. |
| **Cold tier** | `vtopctl tier` copies sealed segments to object storage, verifies them and records the evidence in metadata. |

## The `vtop-node` process

`vtop-node` runs one of three roles, each from a YAML config:

```bash
vtop-node meta --config meta.yaml   # a metadata Raft voter
vtop-node data --config data.yaml   # a data-plane replica
vtop-node node --config node.yaml   # both roles in one process
```

Every config struct rejects unknown keys, so a misspelled key is a startup
error. The quickest way to see a complete, valid config is to render the Helm
chart: `helm template` writes each node's config into the ConfigMap.

| Port (chart default) | Plane |
|---|---|
| 9100 | Metadata peer (Raft) |
| 9200 | Metadata admin (`vtopctl meta`) |
| 9300 | Replica plane |
| 9400 | Native produce and fetch |
| 9500 | `/metrics`, `/healthz`, `/readyz` (unauthenticated) |

Every plane uses mutual TLS unless the config says otherwise by name.

## Deploying it

- **Kubernetes:** the [Helm chart](kubernetes.md), co-located or separated
  tiers, standalone or replicated ranges.
- **Processes on hosts:** the live-chaos harness in
  [`scripts/live-chaos/`](../../scripts/live-chaos/lib.sh) starts real
  three-node clusters and is the reference for hand-written configs; what it
  covers is in [Live chaos validation](../LIVE_CHAOS_VALIDATION.md).

## The Kafka gateway

The gateway is a `kafka:` block in a data node's config. Only a leader or a
standalone node may serve it, and its listener stays on loopback unless the
config sets `any_interface`. The Helm chart does not render this block.

How the gateway compares with Apache Kafka on the same machine, with every
condition of the measurement, is in the
[Kafka comparison](../benchmarks/kafka-comparison.md) and
[Kafka failover](../benchmarks/kafka-failover.md) benchmarks.

## Feature levels

Metadata commands are gated by a cluster feature level, raised with
`vtopctl meta enable-features` once every node runs a release that supports
it. Raising is permanent. The procedure is on [Upgrades](../operate/upgrades.md).
