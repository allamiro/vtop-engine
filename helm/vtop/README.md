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
| PrometheusRule (optional) | The engine's alert rules from `files/vtop-rules.yaml` (a CI-checked copy of `observability/prometheus/vtop-rules.yaml`), behind `metrics.prometheusRule.enabled`; same CRD requirement. |

## Deployment mode: co-located, or separated tiers (#287)

`deployment.mode` selects which processes run:

- **`colocated`** (default) — one StatefulSet of `vtop-node node` processes,
  each hosting a metadata voter AND a data replica. Sized by `replicaCount`;
  both tiers are the same size by construction. Everything below describes
  this shape unless it says otherwise.
- **`separated`** — a metadata tier and a data tier as TWO StatefulSets,
  sized independently under `deployment.meta.replicaCount` (odd; even counts
  are refused) and `deployment.data.replicaCount`. This is the split KRaft
  made for the same reason: a Raft quorum wants to stay small and odd, a
  data plane grows with the data, and co-location cannot grow one without
  growing the other.

Under `separated` the chart renders:

| Object | Purpose |
|---|---|
| StatefulSet `<fullname>-meta` | `vtop-node meta` voters; PVC `meta`; TLS from `tls.metaSecretName`. |
| StatefulSet `<fullname>-data` | `vtop-node data` replicas; PVC `data`; TLS from `tls.dataSecretName`. |
| ConfigMap `<fullname>` | `meta-<ordinal>.yaml` and `data-<ordinal>.yaml`, one per pod of each tier. |
| Services `<fullname>-meta-headless`, `<fullname>-data-headless` | Per-tier stable pod DNS (`<fullname>-<tier>-<ordinal>.<fullname>-<tier>-headless.<ns>.svc.<domain>`). |
| Services `<fullname>-meta`, `<fullname>-data` | Per-tier client Services (observability; the admin plane on the meta one behind `service.exposeMetaAdmin`). |
| PodDisruptionBudgets `<fullname>-meta`, `<fullname>-data` | One budget per tier, so a drain cannot take a voter and a replica together while reporting one unavailable. |

Identity follows the same ordinal rule per tier: meta pod `<i>` is Raft node
`i+1` with leaf `node-<i>.pem` in the metadata Secret; data pod `<i>` is
`data.nodeUuids[i]` with leaf `node-<i>.pem` in the data Secret (the list is
indexed by DATA-tier ordinal and must be exactly `deployment.data.replicaCount`
long). Every leaf's SAN is that pod's own tier FQDN, as above.

The data tier reaches metadata through the metadata tier's first pod
(`data.lease.adminEndpoint` at its co-located default is replaced by that
pod's FQDN, so the certificate has a name to verify) and carries every
metadata pod in `admin_peers` so a redirect to the Raft leader is followed.
Set `adminEndpoint` to anything else and `data.lease.serverName` becomes
yours to set to match it.

Release names are capped at 49 characters under `separated` (the tier
Services append `-data-headless` inside the 63-character label budget);
`fullnameOverride` shortens one that does not fit.

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
- **data `node_uuid` = `data.nodeUuids[ordinal]`**;
- the TLS leaf filenames `node-<ordinal>.pem` / `node-<ordinal>-key.pem`;
- the peer list: every pod's headless FQDN, identical on all pods (the binary
  ignores its own entry).

The rendered config is exactly the co-located schema the binary accepts
(`deny_unknown_fields`): `meta:` + `data:` + **one top-level
`observability:`** block. Per-role observability blocks are rejected by the
binary, and this chart never emits them.

## Topology: one range, or one range per pod

`data.topology` selects what the pods' data roles are. Both shapes are
exercised by `scripts/k8s-smoke.sh` against a live cluster.

| Value | What it renders | What you get |
|---|---|---|
| `standalone` (default) | `role: standalone` on every pod, no replica peers | Each pod serves an **independent** range: three replicas are three separate logs. Quorum durability is refused, and a pod nobody produced to stays empty. The default because it needs no coordination and cannot half-work. |
| `replicated` | `role: candidate` on every pod, one shared peer list (self included — the binary skips its own entry, and identical lists keep the rendered configs diffable) | **One** range across the pods (#284). The role follows the metadata lease inside the binary: whichever pod acquires the range leads and the rest follow it. When the holder dies a surviving candidate takes the range **in place** — no re-render, no `helm upgrade`, no restart. |

`replicated` requires three other things, all enforced at render time rather
than left to fail in the cluster:

- **`data.lease.enabled: true`** (with `data.lease.topicUuid`) — candidates
  take the range *through* the lease, so without it no pod would ever lead.
- **`data.fencingEpoch: 0`** — grants are minted from 1, so a static floor at
  or above the first grant refuses the very grant that makes a candidate lead.
- **An odd replica count** (#562). A write is acknowledged on a majority of the
  replicas, so an even count survives no more failures than the odd count
  below it while raising the majority it must gather: 4 replicas need 3
  acknowledgements and survive 1 failure, exactly as 3 do, and 2 replicas need
  both and survive none (any follower loss stops quorum produce, any leader
  loss leaves no majority to promote from). Co-located mode already forces an
  odd `replicaCount` through the metadata voters; in `separated` mode the
  data tier is sized on its own, so `deployment.data.replicaCount` is refused
  when even. `standalone` pods serve independent ranges, have no quorum, and
  are not subject to it.

`data.leaderOrdinal` is **retired** and now fails the render in *every*
topology. The chart used to freeze `role: leader` on one ordinal, which made
failover a re-render and a restart; the role now lives in the binary, and a
value that used to steer it must not be silently ignored.

`ci/replicated-values.yaml` in this chart is a complete, copyable example —
it is what CI renders and what the smoke test installs.

### Bootstrap order under `replicated`

Candidates learn everything from the metadata plane, so **every pod stays NOT
READY until `vtopctl meta init` has run and the topic, range, and node
registrations exist.** That is correct, not a broken install: a candidate that
has never completed a metadata exchange does not know the current epoch, and a
range metadata says does not exist withholds readiness for as long as that is
true. Bootstrap first and wait for Ready after — waiting first deadlocks
against the very steps that open the gate. The post-install NOTES print the
exact commands, including registering each node at its **own** headless FQDN.

Which pod holds the range is an election's outcome, never a rendered fact, so
ask metadata for the holder rather than assuming an ordinal:

```bash
vtopctl --json meta range-lease \
  --topic-uuid <data.lease.topicUuid> --range-uuid <data.range.rangeId> \
  --config admin.yaml
```

then aim produce/fetch at that holder's own headless FQDN on the native port.

### The Kafka gateway on a replicated range (#751)

A Kafka client cannot be pointed at "the holder": which pod that is changes
when the holder dies. With `data.kafka.enabled: true` (replicated only) every
pod serves the Kafka gateway on `ports.kafka` and lists **every** data pod as a
broker, so a stock client that bootstrapped from any pod finds the holder, and
follows a failover to the survivor that took the range, with no restart and no
config change:

- A pod that does not hold the range answers a produce or a fetch
  `NOT_LEADER_OR_FOLLOWER`. Its Metadata names the lease holder as the
  partition's leader, so the client's next metadata refresh goes there. A
  lapsed or absent lease names nobody (`LEADER_NOT_AVAILABLE`) until a
  survivor takes the range.
- Broker ids are the data ordinal (0 to 255, so at most 256 data pods).
  Each pod advertises its own headless FQDN, which resolves inside the cluster
  only; set `data.kafka.advertisedHosts` (one address per data pod, in
  ordinal order) when clients are outside, for example per-pod NodePort or
  LoadBalancer addresses, each reaching that pod's `ports.kafka`.
- **The gateway has no identity check.** It listens on every interface of the
  pod and admits whoever reaches the port, so the render is refused until
  `data.kafka.acknowledgeUnauthenticated` is `true`, and the port belongs
  behind `networkPolicy.clientFrom` (which then opens it) or the mesh.
- Group membership is held in memory by each gateway, so after a failover a
  group's members rejoin; committed offsets are on the metadata plane and
  survive.
- Not rendered: `kafka.partitions` and `kafka.topics` (a candidate refuses both
  by name), and a load-balanced Service for the port: a client must reach the
  pod it is told about, so the headless Service carries the port and clients
  dial the pods.

`ci/replicated-kafka-values.yaml` is the overlay CI renders on top of
`ci/replicated-values.yaml` and on `ci/separated-values.yaml`.

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
- **`tls.dataSecretName` leaves:** CN = `data.nodeUuids[ordinal]`.
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

### Secrets from an external manager

The chart mounts its TLS Secrets with `defaultMode: 0440`, and
`podSecurityContext.fsGroup: 10001` gives the unprivileged `vtop` user group
access to them. Secrets from a manager reach the pods the same way:

- **External Secrets Operator** syncs into an ordinary Secret: name it in
  `tls.metaSecretName` / `tls.dataSecretName`, or mount it with
  `extraVolumes`.
- **Any other secret file** goes in through `extraVolumes` and
  `extraVolumeMounts`. Mount the whole volume at a directory, never with
  `subPath`. For a Secret, set `defaultMode: 0400`: the kubelet adds the
  group read bit because the pod sets `fsGroup`, so the file is `0440` on
  disk, owned by root and group 10001. A Secret's own default mode is
  `0644`, readable by every user in the container. A Secrets Store CSI
  volume (`readOnly: true`) has no `defaultMode`; its file mode comes from
  the provider, so set it there.

[`ci/external-secret-values.yaml`](ci/external-secret-values.yaml) mounts a
Secret this way and is rendered by the chart's CI lint:

```yaml
extraVolumes:
  - name: external-secret
    secret:
      secretName: vtop-external-secret
      defaultMode: 0400
extraVolumeMounts:
  - name: external-secret
    mountPath: /etc/vtop/external
    readOnly: true
```

This chart runs `vtop-node`. The archive engine (`vtopctl run`), which you
deploy yourself, refuses a manifest key file or `engine.state_store` secret
file other local users can read; the `0440` result above is the form it
accepts. The rule and the recipes for each secret manager are in
[docs/operate/secrets.md](../../docs/operate/secrets.md).

## Transport: TLS by default, plaintext when said twice (#294)

Each plane's transport is a value: `transport.peer`, `transport.admin`,
`transport.replica`, `transport.native`, each `tls` (the default, the mutual
TLS above) or `plaintext`. Pods have no loopback peers, so a plaintext plane
is rendered as the node's spelled-out `plaintext-on-any-interface`, and the
node prints its warning at every start. On the wire that plane then has **no
peer authentication and no confidentiality** — so the chart refuses to render
it until `transport.acknowledgePlaintext: true` is also set. A downgrade is
typed twice, never inherited from one line.

What each plaintext plane costs, in the node's own words: the Raft sender is
self-asserted (`peer`); every reachable peer may change membership and grant
leases, and `meta.adminAuthorization` cannot apply, so the chart refuses the
combination and asks for `meta.adminAuthorization.enabled: false` by name
(`admin`); fencing and promotion are refused, so only a
**standalone** range serves it — a replicated topology is refused at render,
because its candidates would be refused by the node (`replica`); the
configured principal is admitted on its declaration alone (`native`). The
lease dials the admin plane and follows `transport.admin`; under a plaintext
admin dial a custom `data.lease.adminEndpoint` needs no certificate name.

A Secret is required only for the planes left at `tls`: with every plane
plaintext the chart renders without `tls.metaSecretName` or
`tls.dataSecretName`, mounts nothing, and `helm/vtop/ci/plaintext-values.yaml`
is that shape.

## Admin authorization: on by default (#732)

The metadata admin plane authorizes every cluster-scoped command by the
caller's certificate Common Name: `meta init`, membership changes,
administrative lease grants. Only the CNs in
`meta.adminAuthorization.operatorCommonNames` may run them. A data node may
still act on its own range lease, report its own version and keep its own
groups' cursors under its own certificate; reads are open to any certificate
the CA signed.

The policy is **on by default since v0.11.0**. Before, the chart left it off,
and every certificate the CA signed, every data node's included, could change
membership and grant leases.

| Values | Rendered policy |
|---|---|
| `operatorCommonNames: [vtop-admin]` (you supply the names) | Only those CNs are operators. |
| `operatorCommonNames: []` (the default, unchanged) | **Refused at render**: no certificate could bootstrap the cluster. The message names the three ways out. |
| `operatorCommonNames: []` and `allowNoOperators: true` | Nobody may run cluster-scoped commands: a cluster administered out of band. Enforced as written. |
| `enabled: false` | **Lab opt-out.** No policy: every certificate the CA signed is an operator. The node warns at every start, and the install notes print a WARNING. |

A plaintext admin plane (`transport.admin: plaintext`) carries no certificate,
so no policy can apply there: the chart refuses `enabled: true` with it and
asks for the opt-out by name (`helm/vtop/ci/plaintext-values.yaml` is that
shape).

Nothing falls back to an open admin plane: a missing operator list is a
render error, never an unauthorized cluster.

## Observability TLS (#294)

`observability.tls.secretName` names a Secret with `cert.pem` and `key.pem`,
and the `/metrics`, `/healthz`, `/readyz` endpoint serves TLS 1.3, server-only.
The probes switch to `scheme: HTTPS` (the kubelet does not verify), the scrape
annotations say `https`, and the ServiceMonitor scrapes `https` with
`metrics.serviceMonitor.tlsConfig` verbatim (a CA reference and `serverName`,
or `insecureSkipVerify` for a lab). The mutual form — a client CA, only a
scraper the CA vouches for served — is not rendered by the chart: kubelet
httpGet probes cannot present a certificate. A deployment that wants it writes
`observability.tls.client_ca` into a hand-written config and uses exec probes.

Two things to know operating it. The node reads the certificate and key at
**start** and has no hot reload yet, so a rotated Secret takes effect on the
next restart: rotate with `kubectl rollout restart statefulset/<name>` (or a
Secret-watching restarter) before the old certificate expires — the chart
cannot add a checksum annotation for a Secret it does not own. And an
annotation-driven scraper (`metrics.prometheusAnnotations`) that follows
`prometheus.io/scheme: https` verifies the certificate against nothing it
knows when the CA is private: give it the CA (or skip verification in a lab),
or turn the annotations off and use the ServiceMonitor, whose `tlsConfig` is
where the CA reference goes.

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
what rendering cannot. **Standalone:** pods reach Ready, the Raft group
bootstraps and elects a leader, records stream and the committed offset matches
what was produced, each pod is an independent range, and every record survives
a force-deleted pod. **Replicated:** quorum durability — refused outright on a
standalone range — succeeds, every replica converges on the produced offset,
and deleting the pod that holds the lease moves the range: the test asks
metadata who holds it now, resumes producing against that holder while the
deleted pod is still coming back, and requires all three replicas (the
recreated one included) to converge on the full total.

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
  --set 'data.nodeUuids={aaaaaaaa-0000-0000-0000-0000000000a1,aaaaaaaa-0000-0000-0000-0000000000a2,aaaaaaaa-0000-0000-0000-0000000000a3}' \
  --set data.range.topic=telemetry \
  --set data.range.rangeId=aaaaaaaa-0000-0000-0000-0000000000c1 \
  --set data.segmentId=aaaaaaaa-0000-0000-0000-0000000000d1 \
  --set data.principalId=aaaaaaaa-0000-0000-0000-0000000000ce \
  --set 'meta.adminAuthorization.operatorCommonNames={vtop-admin}'
```

The last line names the Common Name of the certificate your `vtopctl` uses
for the admin plane (see "Admin authorization" below). Then bootstrap the Raft
group once with that certificate (see the post-install NOTES):

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
| `image.repository` | `ghcr.io/allamiro/vtop-engine` | Must contain `vtop-node`; the published image has since 0.2.1, so the default needs no override. |
| `image.tag` | `""` (appVersion) | Never use a moving tag in production. |
| `image.pullPolicy` | `IfNotPresent` | |
| `image.binary` | `vtop-node` | Name on PATH or absolute path inside the image. |
| `imagePullSecrets` | `[]` | |
| `nameOverride` / `fullnameOverride` | `""` | |
| `clusterDomain` | `cluster.local` | For building peer FQDNs. |
| `cluster.id` | — **required** | Cluster UUID shared by every node. |
| `data.nodeUuids` | — **required** | One broker UUID per replica, indexed by ordinal; each must equal the CN of that pod's data-plane leaf. (Moved from `cluster.nodeUuids`, #287.) |
| `tls.metaSecretName` | — **required** | Existing Secret, metadata plane (contract above). |
| `tls.dataSecretName` | — **required** | Existing Secret, data/replica plane (contract above). |
| `tls.serverName` | `""` | Shared dial name; empty uses each peer's own FQDN. |
| `meta.timers.electionTimeoutMinMs` | `300` | Mirrors the binary default. |
| `meta.timers.electionTimeoutMaxMs` | `600` | |
| `meta.timers.heartbeatIntervalMs` | `60` | |
| `meta.adminAuthorization.enabled` | `true` | On since v0.11.0 (#732). `false` is the **lab opt-out**: no policy, every CA-signed certificate is an operator, the node warns at startup and NOTES.txt prints a WARNING. Required for `transport.admin: plaintext`. |
| `meta.adminAuthorization.operatorCommonNames` | `[]` — **required** while enabled | Certificate CNs allowed cluster-scoped admin commands (`meta init`, membership, administrative lease grants). Empty while enabled is refused at render unless `allowNoOperators` is set. |
| `meta.adminAuthorization.allowNoOperators` | `false` | `true` renders an enabled policy with no operators: **nobody** may run cluster-scoped commands (a cluster administered out of band). |
| `meta.transitionMacKey.secretName` | `""` | Secret holding the 32-byte hex key that signs leadership-transition statements (#240). Injected as `VTOP_TRANSITION_MAC_KEY` into every metadata process and named to its config; empty means unsigned. A named Secret missing at startup is a hard error. |
| `meta.transitionMacKey.secretKey` | `key` | The key within that Secret. |
| `data.topology` | `standalone` | `standalone` = one independent range per pod; `replicated` = one range, every pod a candidate, role from the lease (section above). |
| `data.leaderOrdinal` | — **retired** | Setting it fails the render in every topology; the role lives in the binary now. |
| `data.fencingEpoch` | `1` | Static epoch; only a floor when the lease drives leadership. **Must be `0` under `replicated`** (grants are minted from 1). |
| `data.range.topic` | — **required** | Wire-level topic name. |
| `data.range.topicEpoch` | `1` | |
| `data.range.rangeId` | — **required** | Range UUID. |
| `data.range.rangeGeneration` | `0` | |
| `data.segmentId` | — **required** | Segment UUID. |
| `data.principalId` | — **required** | The one client principal accepted (= client cert CN). Never defaulted. |
| `data.kafka.enabled` | `false` | Serve the Kafka gateway from every pod of a replicated range (#751; section above). Refused outside `replicated`. |
| `data.kafka.acknowledgeUnauthenticated` | `false` | **Required when enabled.** The gateway carries no vtop identity and listens on every interface. |
| `data.kafka.advertisedHosts` | `[]` | One address per data pod, in ordinal order, that clients dial for it; empty advertises each pod's headless FQDN (in-cluster only). |
| `data.lease.enabled` | `false` | Metadata-driven leadership (#223); credential is the node's own data cert. **Required under `replicated`.** |
| `data.lease.adminEndpoint` | `127.0.0.1:9200` | Local admin listener — legitimate under co-location. |
| `data.lease.serverName` | `""` | Falls back to `tls.serverName`, then the pod's own FQDN. |
| `data.lease.topicUuid` | — **required when enabled** | Metadata's topic UUID (not the wire name). |
| `data.lease.leaseDurationMs` / `renewIntervalMs` / `pollIntervalMs` | `15000`/`5000`/`2000` | Mirrors binary defaults; keep the poll short. |
| `ports.metaPeer` / `metaAdmin` / `replica` / `native` / `observability` | `9100`/`9200`/`9300`/`9400`/`9500` | Same well-known ports on every pod. |
| `ports.kafka` | `9092` | The Kafka gateway, when `data.kafka.enabled`. |
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
| `networkPolicy.clientFrom` | `[]` | Peers allowed the admin + native planes, and the Kafka port when `data.kafka.enabled`; empty = none. mTLS still applies on the admin and native planes; the Kafka port has no identity check, so this is its only gate. |
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
| `terminationGracePeriodSeconds` | `60` | Drain budget for the SIGTERM handler (#280): listeners close, the lease is released, the final commit boundary lands. |
| `extraEnv` / `extraVolumes` / `extraVolumeMounts` | `[]` | Escape hatches. |

## Observability

`/metrics`, `/healthz`, `/readyz` on the observability port. The endpoint is
**unauthenticated by design** (#78) with a 16-connection cap and 10 s
deadline; keep it off public networks (`networkPolicy.enabled=true` is the
in-cluster control). Metric semantics — including why a frozen offset gauge
is a signal and not an exporter bug — are documented in
`docs/NODE_OBSERVABILITY.md`.
