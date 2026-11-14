# Kubernetes with Helm

This page deploys the **cluster plane** with the [Helm chart](../install/helm.md):
a StatefulSet of `vtop-node` processes, each a metadata Raft voter and a data
replica. For what the cluster plane is, read [Cluster plane](cluster-plane.md)
first. Every value is documented in the
[chart README](../../helm/vtop/README.md#values); this page is the order of
operations.

## Choose the shape

| Value | Options | Default |
|---|---|---|
| `deployment.mode` | `colocated`: one StatefulSet, every pod runs both roles, sized by `replicaCount`. `separated`: a metadata tier and a data tier as two StatefulSets, sized by `deployment.meta.replicaCount` and `deployment.data.replicaCount`. | `colocated` |
| `data.topology` | `standalone`: every pod serves its own independent range. `replicated`: one range across the pods, and the role follows the metadata lease. | `standalone` |
| `replicaCount` | Prefer an odd count; 3 is the smallest metadata group that survives one failure. | `3` |

`replicated` also requires `data.lease.enabled: true`, `data.lease.topicUuid`,
`data.fencingEpoch: 0` and an odd replica count. The chart refuses to render
without them. [`ci/replicated-values.yaml`](../../helm/vtop/ci/replicated-values.yaml)
is a complete example.

## TLS Secrets

Every plane uses mutual TLS by default, and the chart ships no certificates.
Create two Secrets (they may be the same Secret) in the release namespace,
each holding:

| Key | Content |
|---|---|
| `ca.pem` | The cluster CA bundle |
| `node-<ordinal>.pem` | The leaf certificate for pod `<fullname>-<ordinal>` |
| `node-<ordinal>-key.pem` | Its private key |

The node checks the certificate's Common Name:

- **Metadata Secret** (`tls.metaSecretName`): CN is the metadata node id,
  `ordinal + 1`. `node-0.pem` carries `CN=1`.
- **Data Secret** (`tls.dataSecretName`): CN is `data.nodeUuids[ordinal]`.
- Every leaf needs the `serverAuth` and `clientAuth` extended key usages and a
  SAN for the name peers dial: by default the pod's headless FQDN,
  `<fullname>-<ordinal>.<fullname>-headless.<namespace>.svc.cluster.local`.

[`scripts/live-chaos/gen-certs.sh`](../../scripts/live-chaos/gen-certs.sh)
shows how to mint them (ECDSA P-256). Then, for example:

```bash
kubectl -n vtop create secret generic vtop-meta-tls \
  --from-file=ca.pem=ca.pem \
  --from-file=node-0.pem=meta-1.pem --from-file=node-0-key.pem=meta-1-key.pem \
  --from-file=node-1.pem=meta-2.pem --from-file=node-1-key.pem=meta-2-key.pem \
  --from-file=node-2.pem=meta-3.pem --from-file=node-2-key.pem=meta-3-key.pem
```

A plane can be switched to plaintext with `transport.<plane>: plaintext`, and
the chart then requires `transport.acknowledgePlaintext: true` as well. A
plaintext plane has no peer authentication and no confidentiality; see the
[chart README](../../helm/vtop/README.md#transport-tls-by-default-plaintext-when-said-twice-294).

## Install

```bash
helm install vtop helm/vtop --namespace vtop --create-namespace -f my-values.yaml
kubectl -n vtop get pods -w
```

On a cold install the first pods often restart once or twice: a node exits
if a peer's DNS name does not resolve yet, and settles once the headless
Service has endpoints. Do not loosen the probes to hide it.

## Bootstrap the metadata group

Run this once, after the pods are up. Forward a **specific pod**, not the
Service: each pod's certificate names only its own FQDN.

```bash
kubectl -n vtop port-forward pod/vtop-0 9200:9200 &
vtopctl meta init --members 1,2,3 --config admin.yaml
vtopctl meta status --config admin.yaml
```

`admin.yaml` is the metadata admin client config. It must set `server_name`
to the forwarded pod's FQDN, because the default, `localhost`, never matches:

```yaml
endpoint: localhost:9200
server_name: vtop-0.vtop-headless.vtop.svc.cluster.local
ca_cert: ca.pem
client_cert: operator.pem
client_key: operator-key.pem
```

Under `replicated`, every pod stays not ready until the topic, range and node
registrations exist in metadata. That is expected. The chart's post-install
notes print the exact commands for your values; `helm get notes vtop -n vtop`
shows them again.

## Check it

| Question | How |
|---|---|
| Is each pod live and ready? | `/healthz` and `/readyz` on port 9500. `/readyz` returns `503 not ready: <reason>`; read the reason. |
| Does the metadata group have a leader? | `vtopctl meta status --config admin.yaml`, or `vtop_meta_raft_state{state="leader"}`. |
| Who holds a replicated range? | `vtopctl --json meta range-lease --topic-uuid <uuid> --range-uuid <uuid> --config admin.yaml` |
| How far has each replica got? | `vtopctl node status --config node-client.yaml`, with a node client config naming the range, the replicas and PEM paths. See [Node endpoints and metrics](../NODE_OBSERVABILITY.md#vtopctl-node-status). |

A pod that goes not ready because metadata fenced it is behaving correctly.
The probe semantics are in the
[chart README](../../helm/vtop/README.md#probes-wired-honestly) and the metrics
in [Node endpoints and metrics](../NODE_OBSERVABILITY.md).

## Operating notes

- **Disruption.** The chart's PodDisruptionBudget allows one pod down at a
  time. With three voters, losing two loses the metadata quorum.
- **Scheduling.** The default anti-affinity is only *preferred*. Set required
  anti-affinity or zone spreading so one Kubernetes node cannot take two
  voters with it.
- **Shutdown.** `terminationGracePeriodSeconds` defaults to 60. On SIGTERM a
  leader releases its range lease so failover starts at once.
- **Network.** The observability port is unauthenticated. Enable
  `networkPolicy.enabled` where your CNI enforces NetworkPolicy.
- **Smoke test.** [`scripts/k8s-smoke.sh`](../../scripts/k8s-smoke.sh) installs
  the chart into a live cluster and checks bootstrap, durability and, for
  `replicated`, failover.
