# Kubernetes with Helm

The [Helm chart](../install/helm.md) deploys the **cluster plane**: a
StatefulSet of `vtop-node` processes, each a metadata Raft voter and a data
replica. It does not run the archive engine (see the note on
[Choose a deployment](index.md)). What the cluster plane is:
[Architecture](../concepts/architecture.md#the-cluster-plane-optional). Every
value is in the [chart README](../../helm/vtop/README.md#values); this page is
the order of operations.

## Prerequisites

- Kubernetes 1.25 or later, and the chart from a checkout of the release
  ([Helm chart](../install/helm.md#get-the-chart)).
- `vtopctl` on your workstation, for bootstrap.
- Two TLS Secrets you mint (below). The chart ships no credentials.
- Your identities: `cluster.id`, `data.nodeUuids`, `data.range.topic`,
  `data.range.rangeId`, `data.segmentId`, `data.principalId`. None has a
  default, because each must match a certificate.

### Choose the shape

| Value | Options | Default |
|---|---|---|
| `deployment.mode` | `colocated`: one StatefulSet, every pod runs both roles, sized by `replicaCount`. `separated`: a metadata tier and a data tier, sized by `deployment.meta.replicaCount` and `deployment.data.replicaCount`. | `colocated` |
| `data.topology` | `standalone`: every pod serves its own independent range. `replicated`: one range across the pods, led by whoever holds the metadata lease. | `standalone` |
| `replicaCount` | Prefer an odd count; 3 is the smallest metadata group that survives one failure. | `3` |

`replicated` also requires `data.lease.enabled: true`, `data.lease.topicUuid`,
`data.fencingEpoch: 0` and an odd replica count; the chart refuses to render
without them.
[`ci/replicated-values.yaml`](../../helm/vtop/ci/replicated-values.yaml) is a
complete example.

A replicated range can also serve Kafka clients from every pod, so a client
follows a failover with no restart: set `data.kafka.enabled: true` and
`data.kafka.acknowledgeUnauthenticated: true`
([`ci/replicated-kafka-values.yaml`](../../helm/vtop/ci/replicated-kafka-values.yaml)
is the overlay). The gateway has no identity check, so restrict its port
(`ports.kafka`, 9092) with `networkPolicy.clientFrom` or the mesh. See
[Helm: the Kafka gateway on a replicated range](../install/helm.md#the-kafka-gateway-on-a-replicated-range).

### TLS Secrets

Two Secrets (they may be the same Secret) in the release namespace, each
holding:

| Key | Content |
|---|---|
| `ca.pem` | The cluster CA bundle |
| `node-<ordinal>.pem` | The leaf certificate for pod `<fullname>-<ordinal>` |
| `node-<ordinal>-key.pem` | Its private key |

- **Metadata Secret** (`tls.metaSecretName`): the leaf's CN is the metadata
  node id, `ordinal + 1`. `node-0.pem` carries `CN=1`.
- **Data Secret** (`tls.dataSecretName`): the CN is `data.nodeUuids[ordinal]`.
- Every leaf needs the `serverAuth` and `clientAuth` extended key usages and a
  SAN for the pod's headless FQDN,
  `<fullname>-<ordinal>.<fullname>-headless.<namespace>.svc.cluster.local`.

[`scripts/live-chaos/gen-certs.sh`](../../scripts/live-chaos/gen-certs.sh)
shows how to mint them (ECDSA P-256).

## Steps

1. **Create the Secrets**, for example:

    ```bash
    kubectl -n vtop create secret generic vtop-meta-tls \
      --from-file=ca.pem=ca.pem \
      --from-file=node-0.pem=meta-1.pem --from-file=node-0-key.pem=meta-1-key.pem \
      --from-file=node-1.pem=meta-2.pem --from-file=node-1-key.pem=meta-2-key.pem \
      --from-file=node-2.pem=meta-3.pem --from-file=node-2-key.pem=meta-3-key.pem
    ```

2. **Render, then install:**

    ```bash
    helm template vtop helm/vtop -f my-values.yaml > /dev/null
    helm install vtop helm/vtop --namespace vtop --create-namespace -f my-values.yaml
    kubectl -n vtop get pods -w
    ```

    On a cold install the first pods often restart once or twice: a node exits
    if a peer's name does not resolve yet. It settles; do not loosen the probes.

3. **Bootstrap the metadata group once.** Forward a specific pod, not the
   Service, because each pod's certificate names only its own FQDN:

    ```bash
    kubectl -n vtop port-forward pod/vtop-0 9200:9200 &
    vtopctl meta init --members 1,2,3 --config admin.yaml
    vtopctl meta status --config admin.yaml
    ```

    `admin.yaml` must set `server_name` to that pod's FQDN; the default,
    `localhost`, never matches:

    ```yaml
    endpoint: localhost:9200
    server_name: vtop-0.vtop-headless.vtop.svc.cluster.local
    ca_cert: ca.pem
    client_cert: operator.pem
    client_key: operator-key.pem
    ```

    The Common Name of `operator.pem` must be in
    `meta.adminAuthorization.operatorCommonNames`. Admin authorization is on
    by default since v0.11.0, and the chart refuses to render without an
    operator named ([chart README](../../helm/vtop/README.md#admin-authorization-on-by-default-732)).

4. **Under `replicated`, register the topic, range and nodes.** Every pod
   stays not ready until they exist, which is expected. The chart's
   post-install notes print the exact commands for your values:
   `helm get notes vtop -n vtop`.

## Verify it works

| Question | How |
|---|---|
| Is each pod live and ready? | `/healthz` and `/readyz` on port 9500. `/readyz` returns `503 not ready: <reason>`; read the reason. |
| Does the metadata group have a leader? | `vtopctl meta status --config admin.yaml`, or `vtop_meta_raft_state{state="leader"}`. |
| Who holds a replicated range? | `vtopctl --json meta range-lease --topic-uuid <uuid> --range-uuid <uuid> --config admin.yaml` |
| How far has each replica got? | `vtopctl node status --config node-client.yaml`, with a node client config naming the range, the replicas and PEM paths ([details](../NODE_OBSERVABILITY.md#vtopctl-node-status)). |

[`scripts/k8s-smoke.sh`](../../scripts/k8s-smoke.sh) installs the chart into
a live cluster and checks bootstrap, durability and, for `replicated`,
failover.

## What to monitor

- The endpoint on port 9500. It is unauthenticated: enable
  `networkPolicy.enabled` where your CNI enforces NetworkPolicy, or give it
  TLS with `observability.tls.secretName`.
- `metrics.serviceMonitor.enabled` for the Prometheus Operator.
- `metrics.prometheusRule.enabled` installs the engine's alert rules as a
  `PrometheusRule` (it needs the Operator's CRDs, like the ServiceMonitor).
  This chart runs the cluster plane, not the engine: the rules fire only for
  engines your Prometheus also scrapes. `metrics.prometheusRule.labels` adds
  the labels your Prometheus's rule selector matches, and
  `metrics.prometheusRule.runbookBaseUrl` points the `runbook_url` links at
  your own copy of the [alert runbooks](../operate/runbooks.md).
- The cluster alert rules on [Metrics and alerts](../operate/monitoring.md#alerts),
  and the node metrics in [Node endpoints and metrics](../NODE_OBSERVABILITY.md).
- A pod that goes not ready because metadata fenced it is behaving correctly.

Operating notes: the PodDisruptionBudget allows one pod down at a time; the
default anti-affinity is only preferred, so set required anti-affinity or zone
spreading; on SIGTERM a leader releases its range lease so failover starts at
once (`terminationGracePeriodSeconds`, default 60).

## Secrets from an external manager

The chart mounts its own TLS Secrets with `defaultMode: 0440` and the pod's
`fsGroup: 10001`. To add a secret file from a manager (External Secrets
Operator, the Secrets Store CSI driver, Vault Agent) to the chart's pods, use
`extraVolumes` and `extraVolumeMounts`; mount the whole volume at a directory,
never with `subPath`, and use `defaultMode: 0400`, which the kubelet turns
into `0440` because of `fsGroup`.
[`ci/external-secret-values.yaml`](../../helm/vtop/ci/external-secret-values.yaml)
is a rendered example; the [chart README](../../helm/vtop/README.md#secrets-from-an-external-manager)
has the details.

The archive engine, which this chart does not run, checks the files that hold
its secrets and its ledger before reading them, and refuses ones other users
can reach. When you run it in Kubernetes yourself, mount its manifest keys
and its `engine.state_store` secret the same way: a Secret volume with
`defaultMode: 0400`, a pod `fsGroup` the engine's user belongs to, no
`subPath`, and the file paths in `config.yaml`. The resulting `0440` files
are accepted as a Kubernetes Secret mount. Keep its ledger and audit file on
a volume with `fsGroupChangePolicy: OnRootMismatch`: under the default
policy the kubelet makes every file on a writable volume group-writable at
each mount, which the engine refuses. The rule, the override and the
recipes for each manager are on
[Secret files and their permissions](../operate/secrets.md).

## Upgrade notes

Upgrade every node first, by `helm upgrade` with the new release's chart (or
`image.tag`), then raise the cluster feature level with
`vtopctl meta enable-features`. Raising is permanent; see
[Upgrades](../operate/upgrades.md#2-then-raise-the-cluster-feature-level).
