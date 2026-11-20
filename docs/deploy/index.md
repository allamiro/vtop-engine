# Choose a deployment

Start with the smallest shape that meets your durability needs. Every
deployment page follows the same order: prerequisites, steps, how to verify
it, what to monitor, and upgrade notes. The status of each shape comes from
[Production HA](../PRODUCTION_HA.md), which also covers the designs not built
yet.

<div class="comparison" markdown>

| Option | What it gives | What it needs | When it fails | When to use it |
|---|---|---|---|---|
| [Single engine, SQLite](single-node.md) | The full archive path, with the ledger in a local file. | One host, a bucket, `vtopctl`. | Archiving stops while the engine is down and data waits at the source; a restart resumes from the ledger. Losing the host's disk loses the ledger. | A first deployment, an edge site, one host you already back up. |
| [Single engine, PostgreSQL](postgresql.md) | The same, with a ledger that outlives the engine's host. | `vtopctl` built with `--features postgres`; PostgreSQL with `sslmode=verify-full`; migration and runtime identities. | As above, but the ledger survives the host: start the engine again against the same database. | When the ledger must survive the engine host. The recommended production shape today. |
| [Several engines, one PostgreSQL ledger](ha.md) | Not available yet. | Fleet mode (Production HA Phase 5), not built. | Host-local locks cannot see an engine on another host; the engine warns at every start on PostgreSQL. | Not yet. Run one engine per ledger. |
| [Kubernetes with Helm](kubernetes.md) | The cluster plane (`vtop-node`) as a StatefulSet. Not the archive engine. | Kubernetes 1.25 or later, TLS Secrets you mint, cluster identities. | One voter may be down at a time; with `replicated`, a surviving pod takes over the range in place. | Evaluating the cluster plane on Kubernetes. |
| [Native cluster plane on hosts](cluster-plane.md) | The same cluster plane, as processes you run. | A config and certificates per node; three or five metadata voters. | Raft keeps metadata while a majority of voters is up; a range fails over to a fenced quorum. | Evaluating the cluster plane outside Kubernetes. |

</div>

!!! note "The archive engine on Kubernetes"

    The Helm chart does not run the archive engine; an engine chart is
    [Phase 7](../PRODUCTION_HA.md#phase-7--engine-kubernetes-deployment--not-started),
    not started. To run the engine on Kubernetes today, use the
    [container image](../install/container.md) in your own manifests, as a
    single replica with its ledger on a persistent volume or in PostgreSQL.

<div class="grid cards" markdown>

-   **Single engine, SQLite**

    ---

    [:octicons-arrow-right-24: Deploy](single-node.md)

-   **Single engine, PostgreSQL**

    ---

    [:octicons-arrow-right-24: Deploy](postgresql.md)

-   **Several engines (HA)**

    ---

    [:octicons-arrow-right-24: Status](ha.md)

-   **Kubernetes with Helm**

    ---

    [:octicons-arrow-right-24: Deploy](kubernetes.md)

-   **Native cluster plane**

    ---

    [:octicons-arrow-right-24: Deploy](cluster-plane.md)

</div>
