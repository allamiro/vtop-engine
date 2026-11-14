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

The gateway is a `kafka:` block in a data node's config. Only a leader or a
standalone node may serve it, and its listener stays on loopback unless the
config sets `any_interface`. The Helm chart does not render this block. How it
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
