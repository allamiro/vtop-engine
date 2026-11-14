# Deploy

Choose the smallest shape that meets your durability needs. The tiers follow
[Production HA §9](../PRODUCTION_HA.md#9-deployment-topologies), which also
lists the shapes that are designed but not built yet.

| Shape | State ledger | Status | Guide |
|---|---|---|---|
| One engine, one host | SQLite file | Available | [Single engine with SQLite](single-node.md) |
| One engine, durable external ledger | PostgreSQL | Available from a source build with `--features postgres` | [PostgreSQL ledger](../POSTGRES_DEPLOYMENT.md) |
| Several engines sharing a Kafka consumer group | PostgreSQL | **Not built yet.** One engine per state store is enforced at startup. | [Production HA](../PRODUCTION_HA.md) |
| Native replicated log on Kubernetes | Metadata Raft (not the engine ledger) | Available, prototype | [Kubernetes with Helm](kubernetes.md), [Cluster plane](cluster-plane.md) |

!!! note "The Helm chart deploys the cluster plane"

    `helm/vtop` runs `vtop-node`, the native log. It does not run the archive
    engine. On Kubernetes, run the engine from the
    [container image](../install/container.md) with your own manifests.

Before production, read:

1. [Production HA](../PRODUCTION_HA.md), Part I: what is done and what is not.
2. [Security model](../SECURITY_MODEL.md): credentials, TLS, least privilege.
3. [Known limitations](../reference/limitations.md).
