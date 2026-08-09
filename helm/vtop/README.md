# vtop Helm chart

Deploys a cluster of **co-located VTOP nodes** (`vtop-node node`, #215/#237):
a StatefulSet where every pod hosts a metadata Raft voter **and** a data-plane
replica in one process, sharing one runtime, one observability endpoint, and
one fate. Default: 3 replicas — the smallest quorum that survives one failure.

## What the chart renders

| Object | Purpose |
|---|---|
| StatefulSet `<fullname>` | The co-located nodes; two PVCs per pod (meta + data). |
| ConfigMap `<fullname>` | One fully rendered `node-<ordinal>.yaml` per replica. |
| Service `<fullname>-headless` | Stable per-pod DNS for Raft peers and per-pod client access (`publishNotReadyAddresses: true` so a fresh quorum can form). |
| Service `<fullname>` | ClusterIP for the admin + observability planes only (the native plane is per-pod; see below). |
| ServiceAccount | `automountServiceAccountToken: false` — the binary never calls the Kubernetes API. |
| PodDisruptionBudget | `maxUnavailable: 1` — quorum protection for voluntary disruption. |
| NetworkPolicy (optional) | Default-deny with the topology's exact allowances. |
| ServiceMonitor (optional) | Requires the Prometheus Operator CRDs; render fails clearly without them. |

## How a pod knows who it is (ordinal → identity)

A StatefulSet pod's hostname is `<statefulset>-<ordinal>`. The container's
command derives the ordinal from `$HOSTNAME` and execs:

```
vtop-node node --config /etc/vtop/config/node-<ordinal>.yaml
```

Each `node-<ordinal>.yaml` is rendered **at template time**, so `helm
template` shows every node's exact config and the pod template's
`checksum/config` annotation rolls the set on any config change. From the
ordinal the chart fixes:

- **meta `node_id` = ordinal + 1** (Raft ids are 1-based, matching upstream);
- **data `node_uuid` = `cluster.nodeUuids[ordinal]`**;
- the TLS leaf filenames `node-<ordinal>.pem` / `node-<ordinal>-key.pem`;
- the peer list: every pod's headless FQDN, identical on all pods (the binary
  ignores its own entry).

The rendered config is exactly the co-located schema the binary accepts
(`deny_unknown_fields`): `meta:` + `data:` + **one top-level
`observability:`** block. Per-role observability blocks are rejected by the
binary, and this chart never emits them.

Each pod's data role runs an independent **standalone** range — the same
shape the upstream co-located harness runs, because replicated ranges under
co-location wait on follower-side epoch propagation upstream. The chart does
not offer a topology it cannot back.

## TLS: required, never defaulted

The chart ships **no credentials of any kind** and refuses to render until
you name your Secrets (a lab compose that shipped default credentials is how
issue #81 happened):

```
helm template vtop helm/vtop
# Error: ... TLS is required and never defaulted: set tls.metaSecretName ...
```

### Secret contract

Two Secrets (they may be the same object), each with:

| Key | Content |
|---|---|
| `ca.pem` | Cluster CA bundle |
| `node-<ordinal>.pem` | Leaf certificate for pod `<fullname>-<ordinal>` |
| `node-<ordinal>-key.pem` | Its private key |

CN rules the binary enforces:

- **`tls.metaSecretName` leaves:** CN = the **decimal meta node id**, i.e.
  `ordinal + 1` (`node-0.pem` carries `CN=1`, `node-1.pem` carries `CN=2`, …).
- **`tls.dataSecretName` leaves:** CN = `cluster.nodeUuids[ordinal]`.
- Every leaf needs `serverAuth` + `clientAuth` EKUs and a SAN matching the
  name peers dial: by default each pod's headless FQDN
  (`<fullname>-<ordinal>.<fullname>-headless.<ns>.svc.<clusterDomain>`), or
  the single shared `tls.serverName` if you set one.

`scripts/live-chaos/gen-certs.sh` in the repository shows the exact minting
recipe (ECDSA P-256). Example:

```bash
kubectl create secret generic vtop-meta-tls \
  --from-file=ca.pem=ca.pem \
  --from-file=node-0.pem=meta-1.pem --from-file=node-0-key.pem=meta-1-key.pem \
  --from-file=node-1.pem=meta-2.pem --from-file=node-1-key.pem=meta-2-key.pem \
  --from-file=node-2.pem=meta-3.pem --from-file=node-2-key.pem=meta-3-key.pem
```

**cert-manager** can issue these (one `Certificate` per ordinal with the
`commonName`/`dnsNames` above, keys copied/renamed into this layout), but the
chart deliberately does not depend on it — it consumes plain Secrets from
wherever you issue them.

## First start: expect one or two restarts

Nodes resolve their peers' DNS names at startup and **exit** if resolution
fails. `podManagementPolicy: Parallel` and `publishNotReadyAddresses: true` are
both set so peers can find each other before any of them is Ready — but at the
instant the first container runs, its peers may not have endpoints yet.

So on a cold install the first pods typically restart once or twice, back off,
and settle. Observed on a fresh 3-replica install: two pods restarted twice,
one never restarted, and all three reached Ready inside a minute.

This is a startup-ordering cost, not a fault. It resolves itself, and nothing
is lost — but do not read `RESTARTS 2` on a fresh install as a broken deploy,
and do not loosen the probes to hide it. Signal-aware startup retry is tracked
alongside #280.

## Verifying a real deployment

`scripts/k8s-smoke.sh` installs the chart against a live cluster and asserts
what rendering cannot: pods reach Ready, the Raft group bootstraps and elects a
leader, records stream and the committed offset matches what was produced, each
pod is an independent range, and every record survives a force-deleted pod.

```
docker build -f docker/Dockerfile -t vtop-engine:local .
scripts/k8s-smoke.sh              # namespace vtop-smoke, release vtop
```

## Probes — wired honestly

| Probe | Endpoint | Semantics |
|---|---|---|
| startup | `/healthz` | The endpoint binds before segment recovery finishes; `periodSeconds × failureThreshold` (default 5 min) is the cold-recovery budget for large ranges. |
| readiness | `/readyz` | A **level with a reason** (`503 not ready: <why>`), the **conjunction of both roles** on a co-located node. |
| liveness | `/healthz` | Process liveness only; always 200 while the accept loop turns. |

A node that goes unready because metadata fenced it is **correct**: every
write sent to it from that moment is one it must refuse. Do not "fix" this
with looser probes — port-forward and read the reason in the `/readyz` body.
Also correct: fresh pods are Ready before a Raft leader exists (bootstrap
arrives over the very admin endpoint being gated; requiring leadership would
deadlock bringup). Leadership is observable as
`vtop_meta_raft_state{state="leader"}` instead.

## Image

`image.repository` defaults to `ghcr.io/allamiro/vtop-engine`, and the image
tag defaults to the chart's `appVersion`. Since 0.2.1 that image ships **both**
`vtopctl` and `vtop-node`, so a default install runs the published artifact and
needs no override. Set `image.binary` only if you point at an image that keeps
`vtop-node` somewhere unusual.

This section previously told you to build your own image, because the 0.2.0
image shipped only `vtopctl` and the StatefulSet's command was therefore not
in it. That is fixed, and CI now asserts both binaries are present in the
image it builds.

## Install

```bash
helm install vtop helm/vtop \
  --namespace vtop --create-namespace \
  --set tls.metaSecretName=vtop-meta-tls \
  --set tls.dataSecretName=vtop-data-tls \
  --set cluster.id=11111111-2222-3333-4444-555555555555 \
  --set 'cluster.nodeUuids={aaaaaaaa-0000-0000-0000-0000000000a1,aaaaaaaa-0000-0000-0000-0000000000a2,aaaaaaaa-0000-0000-0000-0000000000a3}' \
  --set data.range.topic=telemetry \
  --set data.range.rangeId=aaaaaaaa-0000-0000-0000-0000000000c1 \
  --set data.segmentId=aaaaaaaa-0000-0000-0000-0000000000d1 \
  --set data.principalId=aaaaaaaa-0000-0000-0000-0000000000ce
```

Then bootstrap the Raft group once (see the post-install NOTES):

```
kubectl -n <ns> port-forward pod/<release>-0 9200:9200 &
vtopctl meta init --members 1,2,3 --config admin.yaml
```

Forward a **specific pod**, not the Service. Each pod presents a certificate
whose SAN is that pod's own headless FQDN, so a load-balanced endpoint gives
the client no name it can verify and TLS fails before `vtopctl` sends
anything. `admin.yaml` therefore needs both an endpoint and a matching
server name — the latter defaults to `localhost`, which never matches:

```yaml
endpoint: localhost:9200
server_name: <release>-0.<release>-headless.<ns>.svc.cluster.local
ca_cert: ...
client_cert: ...
client_key: ...
```

The client Service deliberately does **not** publish the admin port for this
reason. `service.exposeMetaAdmin=true` puts it back, and is safe only if your
certificates also carry that Service's DNS name as a shared SAN.

## Values

| Key | Default | Description |
|---|---|---|
| `replicaCount` | `3` | Co-located nodes. Prefer odd counts (a 4th voter adds no fault tolerance). |
| `image.repository` | `ghcr.io/allamiro/vtop-engine` | Must contain `vtop-node` — see the caveat above. |
| `image.tag` | `""` (appVersion) | Never use a moving tag in production. |
| `image.pullPolicy` | `IfNotPresent` | |
| `image.binary` | `vtop-node` | Name on PATH or absolute path inside the image. |
| `imagePullSecrets` | `[]` | |
| `nameOverride` / `fullnameOverride` | `""` | |
| `clusterDomain` | `cluster.local` | For building peer FQDNs. |
| `cluster.id` | — **required** | Cluster UUID shared by every node. |
| `cluster.nodeUuids` | — **required** | One broker UUID per replica, indexed by ordinal; each must equal the CN of that pod's data-plane leaf. |
| `tls.metaSecretName` | — **required** | Existing Secret, metadata plane (contract above). |
| `tls.dataSecretName` | — **required** | Existing Secret, data/replica plane (contract above). |
| `tls.serverName` | `""` | Shared dial name; empty uses each peer's own FQDN. |
| `meta.timers.electionTimeoutMinMs` | `300` | Mirrors the binary default. |
| `meta.timers.electionTimeoutMaxMs` | `600` | |
| `meta.timers.heartbeatIntervalMs` | `60` | |
| `meta.adminAuthorization.enabled` | `false` | `false` = block absent: authenticate-only, node warns at startup. `true` + empty list = **nobody** may run cluster-scoped commands (enforced as written). |
| `meta.adminAuthorization.operatorCommonNames` | `[]` | Certificate CNs allowed cluster-scoped admin commands. |
| `data.fencingEpoch` | `1` | Static epoch; only a floor when the lease drives leadership. |
| `data.range.topic` | — **required** | Wire-level topic name. |
| `data.range.topicEpoch` | `1` | |
| `data.range.rangeId` | — **required** | Range UUID. |
| `data.range.rangeGeneration` | `0` | |
| `data.segmentId` | — **required** | Segment UUID. |
| `data.principalId` | — **required** | The one client principal accepted (= client cert CN). Never defaulted. |
| `data.lease.enabled` | `false` | Metadata-driven leadership (#223); credential is the node's own data cert. |
| `data.lease.adminEndpoint` | `127.0.0.1:9200` | Local admin listener — legitimate under co-location. |
| `data.lease.serverName` | `""` | Falls back to `tls.serverName`, then the pod's own FQDN. |
| `data.lease.topicUuid` | — **required when enabled** | Metadata's topic UUID (not the wire name). |
| `data.lease.leaseDurationMs` / `renewIntervalMs` / `pollIntervalMs` | `15000`/`5000`/`2000` | Mirrors binary defaults; keep the poll short. |
| `ports.metaPeer` / `metaAdmin` / `replica` / `native` / `observability` | `9100`/`9200`/`9300`/`9400`/`9500` | Same well-known ports on every pod. |
| `logFormat` | `json` | `VTOP_LOG_FORMAT`; `""` keeps pretty logs. |
| `service.type` / `service.annotations` | `ClusterIP` / `{}` | Client Service (admin + obs only). |
| `persistence.meta.size` / `storageClass` / `accessModes` | `10Gi` / `""` / `[ReadWriteOnce]` | Raft log volume. |
| `persistence.data.size` / `storageClass` / `accessModes` | `50Gi` / `""` / `[ReadWriteOnce]` | Segment volume. |
| `resources` | 500m/512Mi → 2/2Gi | Conservative defaults. |
| `probes.startup.*` | `enabled`, 5s × 60 | Cold-recovery budget. |
| `probes.readiness.*` | 5s, threshold 3 | Do not loosen to mask fencing. |
| `probes.liveness.*` | 10s, threshold 3 | |
| `podDisruptionBudget.enabled` / `maxUnavailable` | `true` / `1` | Quorum — do not raise with 3 replicas. |
| `networkPolicy.enabled` | `false` | Enable where the CNI enforces policy. |
| `networkPolicy.metricsFrom` | `[]` | Peers allowed to scrape; **empty allows all sources on that one port**. |
| `networkPolicy.clientFrom` | `[]` | Peers allowed the admin + native planes; empty = none (mTLS still applies everywhere). |
| `networkPolicy.extraEgress` | `[]` | Appended verbatim. |
| `metrics.prometheusAnnotations` | `true` | `prometheus.io/*` pod annotations. |
| `metrics.serviceMonitor.enabled` | `false` | Fails clearly if the CRD is absent; offline template needs `--api-versions monitoring.coreos.com/v1`. |
| `metrics.serviceMonitor.interval` / `scrapeTimeout` / `labels` | `30s` / `""` / `{}` | |
| `serviceAccount.create` / `name` / `annotations` | `true` / `""` / `{}` | Token automount is off. |
| `podSecurityContext` | uid/gid/fsGroup 10001, `RuntimeDefault` seccomp | Matches the image's unprivileged `vtop` user. |
| `containerSecurityContext` | no-escalation, read-only rootfs, drop ALL | Data dirs are volumes, so the rootfs stays read-only. |
| `podAnnotations` / `podLabels` / `nodeSelector` / `tolerations` / `topologySpreadConstraints` | `{}`/`{}`/`{}`/`[]`/`[]` | |
| `affinity` | `{}` | Empty gets the chart's default **preferred** anti-affinity; set required anti-affinity for real quorum safety. |
| `priorityClassName` | `""` | |
| `terminationGracePeriodSeconds` | `60` | Clean-shutdown fsync budget. |
| `extraEnv` / `extraVolumes` / `extraVolumeMounts` | `[]` | Escape hatches. |

## Observability

`/metrics`, `/healthz`, `/readyz` on the observability port. The endpoint is
**unauthenticated by design** (#78) with a 16-connection cap and 10 s
deadline; keep it off public networks (`networkPolicy.enabled=true` is the
in-cluster control). Metric semantics — including why a frozen offset gauge
is a signal and not an exporter bug — are documented in
`docs/NODE_OBSERVABILITY.md`.
