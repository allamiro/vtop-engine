# Helm chart

The chart in [`helm/vtop`](../../helm/vtop/README.md) deploys the **cluster
plane**: `vtop-node` processes as a StatefulSet, each hosting a metadata Raft
voter and a data-plane replica. It does **not** deploy the archive engine
(`vtopctl run`); an engine chart is
[Phase 7 of the HA roadmap](../PRODUCTION_HA.md#phase-7--engine-kubernetes-deployment--not-started).
To run the engine, use the [container image](container.md).

| Chart field | Value |
|---|---|
| Chart name | `vtop` |
| Chart version | `0.6.3` |
| `appVersion` (the default image tag) | `0.10.4` |
| Kubernetes | `>=1.25.0-0` |
| Image | `ghcr.io/allamiro/vtop-engine`, tag defaults to `appVersion` |

The values come from [`Chart.yaml`](../../helm/vtop/Chart.yaml) at the time of
writing; the chart in the checkout you install from is authoritative.

## Get the chart

The chart is not published to a chart repository. Install it from a checkout
of the release you are deploying:

```bash
git clone --branch v0.10.4 https://github.com/allamiro/vtop-engine.git
cd vtop-engine
```

## Prerequisites

- **TLS Secrets.** The chart ships no credentials and refuses to render
  until you name a Secret for every plane left at TLS (the default). The
  Secret layout and the certificate CN rules are in
  [Kubernetes with Helm](../deploy/kubernetes.md#tls-secrets).
- **Identities.** `cluster.id`, `data.nodeUuids`, `data.range.topic`,
  `data.range.rangeId`, `data.segmentId` and `data.principalId` have no
  defaults, because they must match the certificates you mint.
- **The Prometheus Operator CRDs**, only if you set
  `metrics.serviceMonitor.enabled: true`.

## Render before you install

Rendering shows every node's exact config and catches a missing value before
anything reaches the cluster. Without values, the chart stops at the first
required identity:

```console
$ helm template vtop helm/vtop
Error: execution error at (vtop/templates/statefulset.yaml:35:28):

cluster.id is required: the cluster UUID shared by every node. The chart does not default identities — they must match the certificates you minted.
```

[`helm/vtop/ci/default-values.yaml`](../../helm/vtop/ci/default-values.yaml)
is a complete example that renders; copy it and replace the identities with
your own.

```bash
helm lint helm/vtop -f my-values.yaml
helm template vtop helm/vtop -f my-values.yaml
```

## Install, upgrade, remove

```bash
helm install vtop helm/vtop --namespace vtop --create-namespace -f my-values.yaml
helm upgrade vtop helm/vtop --namespace vtop -f my-values.yaml
helm uninstall vtop --namespace vtop
```

`helm uninstall` leaves the StatefulSet's PersistentVolumeClaims in place, as
Kubernetes does for every StatefulSet; delete them yourself to discard the
data.

After the first install, bootstrap the metadata Raft group once. The steps are
printed by the chart's post-install notes and described in
[Kubernetes with Helm](../deploy/kubernetes.md#steps).

## The Kafka gateway on a replicated range

With `data.topology: replicated` and `data.kafka.enabled: true` every pod
serves the Kafka gateway and lists every data pod as a broker, so a Kafka
client that bootstrapped from any pod follows a failover to the pod that took
the range, with no restart and no config change. The gateway has no identity
check, so the render is refused until `data.kafka.acknowledgeUnauthenticated`
is `true`; restrict the port with `networkPolicy.clientFrom` or the mesh.

```bash
helm template vtop helm/vtop \
  -f helm/vtop/ci/replicated-values.yaml \
  -f helm/vtop/ci/replicated-kafka-values.yaml
```

What a client sees through a failover, and the measured times, are in
[Live chaos validation](../LIVE_CHAOS_VALIDATION.md#kafka-clients-and-a-failover-751).
The values are in the
[chart README](../../helm/vtop/README.md#the-kafka-gateway-on-a-replicated-range).

Every value, its default and its meaning is in the
[chart README](../../helm/vtop/README.md#values).
