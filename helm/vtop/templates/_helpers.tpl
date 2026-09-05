{{/*
Chart name.
*/}}
{{- define "vtop.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
Fully qualified app name.

Truncated to 54, not 63, and the 9 characters held back are load-bearing:

  * the headless Service appends "-headless" (9 chars). Truncating THAT to 63
    after building it from a 63-char base yields the same string as the client
    Service, so a long release name silently produces two Services with one
    name and the chart fails to install.
  * StatefulSet pod names append "-<ordinal>", and the
    statefulset.kubernetes.io/pod-name label is itself capped at 63.

Reserving the space up front is the only way both stay inside the limit; a
second trunc after concatenating cannot recover characters already dropped.
*/}}
{{- define "vtop.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 54 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 54 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 54 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "vtop.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{/*
The headless Service that gives every pod a stable DNS identity for Raft
peer addressing and per-pod client access.
*/}}
{{- define "vtop.headlessServiceName" -}}
{{- printf "%s-headless" (include "vtop.fullname" .) | trimSuffix "-" -}}
{{- end }}

{{/*
Kubernetes recommended labels.
*/}}
{{- define "vtop.labels" -}}
helm.sh/chart: {{ include "vtop.chart" . }}
{{ include "vtop.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/component: node
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels only — these are immutable on a StatefulSet, so they must
never include anything that changes between upgrades (version, chart).
*/}}
{{- define "vtop.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vtop.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "vtop.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "vtop.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/*
Stable FQDN of the pod at a given ordinal, via the headless Service.
Expects (dict "root" $ "ordinal" <int>).
*/}}
{{- define "vtop.podFqdn" -}}
{{- $root := .root -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "vtop.fullname" $root) (int .ordinal) (include "vtop.headlessServiceName" $root) $root.Release.Namespace $root.Values.clusterDomain -}}
{{- end }}

{{/*
rustls server name used when dialing the pod at a given ordinal: the shared
tls.serverName override when set, otherwise that pod's own FQDN (which the
certificate must then carry as a SAN). Expects (dict "root" $ "ordinal" <int>).
*/}}
{{- define "vtop.peerServerName" -}}
{{- if .root.Values.tls.serverName -}}
{{- .root.Values.tls.serverName -}}
{{- else -}}
{{- include "vtop.podFqdn" . -}}
{{- end -}}
{{- end }}

{{/*
TLS Secret names. `required` (not defaults) is the point: this chart ships no
credentials and refuses to render without yours — a lab compose that shipped
default credentials is exactly how issue #81 happened.
*/}}
{{- define "vtop.metaSecretName" -}}
{{- required "\n\nTLS is required and never defaulted: set tls.metaSecretName to an existing Secret holding the metadata-plane mTLS material (keys: ca.pem, node-<ordinal>.pem, node-<ordinal>-key.pem; leaf CN = the decimal meta node id, ordinal+1). This chart ships no default credentials (issue #81). See helm/vtop/README.md for the full Secret contract." .Values.tls.metaSecretName -}}
{{- end }}

{{- define "vtop.dataSecretName" -}}
{{- required "\n\nTLS is required and never defaulted: set tls.dataSecretName to an existing Secret holding the data/replica-plane mTLS material (keys: ca.pem, node-<ordinal>.pem, node-<ordinal>-key.pem; leaf CN = data.nodeUuids[ordinal]). This chart ships no default credentials (issue #81). See helm/vtop/README.md for the full Secret contract." .Values.tls.dataSecretName -}}
{{- end }}

{{/*
Deployment-wide guards (#287), included once from the StatefulSet so every
render passes through them. Three refusals live here rather than scattered:

  - deployment.mode is the colocated/separated switch. `colocated` is the
    default and renders exactly what this chart always rendered; `separated`
    is slice 2 and REFUSES until the two-tier rendering exists, because a
    mode that silently rendered the colocated shape would deploy the wrong
    processes and look like success.
  - cluster.nodeUuids is RETIRED in favour of data.nodeUuids. Metadata
    identities are Raft ids derived from the pod ordinal; data identities
    are broker UUIDs. They shared one list only because the co-located
    tiers are the same size, and separated mode sizes them independently —
    a shared index cannot name both.
  - the metadata voter count refuses even numbers. An even quorum buys no
    additional fault tolerance over the odd count below it (four voters
    tolerate one failure, exactly as three do, while raising the quorum
    majority) — accepting it would render a cluster that pays for a voter
    it gets nothing from.
*/}}
{{- define "vtop.deploymentGuards" -}}
{{- $v := .Values -}}
{{- $mode := include "vtop.deploymentMode" . -}}
{{- if and (ne $mode "colocated") (ne $mode "separated") -}}
{{- fail (printf "\n\ndeployment.mode must be colocated or separated; got %q." $mode) -}}
{{- end -}}
{{- if $v.cluster.nodeUuids -}}
{{- fail "\n\ncluster.nodeUuids moved to data.nodeUuids (#287). Metadata identities are Raft node ids derived from the pod ordinal (ordinal+1, the meta certificate CN) while these are broker UUIDs (the data certificate CN) — two different identities that shared one list only while the tiers were the same size. Move the list unchanged: data.nodeUuids." -}}
{{- end -}}
{{- if eq $mode "colocated" -}}
{{- if eq (mod (int $v.replicaCount) 2) 0 -}}
{{- fail (printf "\n\nreplicaCount %d is even, and the metadata plane is a Raft quorum: an even voter count tolerates no more failures than the odd count below it while raising the majority it must gather. Use %d or %d." (int $v.replicaCount) (sub (int $v.replicaCount) 1) (add1 (int $v.replicaCount))) -}}
{{- end -}}
{{- else -}}
{{- /* SEPARATED (#287 slice 2). The tiers are sized independently and each
       size is REQUIRED: guessing either would defeat the mode. The voter
       count keeps the odd-only rule; the data count must match the identity
       list it indexes; a replicated range needs a follower; and the tier
       names must fit the DNS label budget with their suffixes attached. */ -}}
{{- $metaCount := int (include "vtop.metaReplicaCount" .) -}}
{{- $dataCount := int (include "vtop.dataReplicaCount" .) -}}
{{- if lt $metaCount 1 -}}
{{- fail "\n\ndeployment.mode is separated but deployment.meta.replicaCount is unset: the metadata tier is sized independently under this mode, and the chart refuses to guess a quorum size. Use 3 or 5." -}}
{{- end -}}
{{- if eq (mod $metaCount 2) 0 -}}
{{- fail (printf "\n\ndeployment.meta.replicaCount %d is even, and the metadata plane is a Raft quorum: an even voter count tolerates no more failures than the odd count below it while raising the majority it must gather. Use %d or %d." $metaCount (sub $metaCount 1) (add1 $metaCount)) -}}
{{- end -}}
{{- if lt $dataCount 1 -}}
{{- fail "\n\ndeployment.mode is separated but deployment.data.replicaCount is unset: the data tier is sized independently under this mode, and the chart refuses to guess. Size it by the data (one per data.nodeUuids entry)." -}}
{{- end -}}
{{- if ne (len $v.data.nodeUuids) $dataCount -}}
{{- fail (printf "\n\ndata.nodeUuids has %d entries but deployment.data.replicaCount is %d: under separated mode the list is indexed by DATA-tier pod ordinal, one broker UUID per data replica, each equal to the CN of that pod's data-plane certificate." (len $v.data.nodeUuids) $dataCount) -}}
{{- end -}}
{{- if and (eq $v.data.topology "replicated") (lt $dataCount 2) -}}
{{- fail (printf "\n\ndata.topology is \"replicated\" but deployment.data.replicaCount is %d: a replicated range needs at least one follower. Use topology \"standalone\" for a single data node." $dataCount) -}}
{{- end -}}
{{- if gt (len (include "vtop.fullname" .)) 49 -}}
{{- fail (printf "\n\nthe release name renders a fullname of %d characters; under separated mode the tier Services append \"-data-headless\" (14) and the label budget is 63. Use fullnameOverride to shorten it to 49 or fewer." (len (include "vtop.fullname" .))) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
The data-role refusals both render paths share (review: one copy, so the
messages cannot drift between the co-located and the separated shapes).
No output; only failures. Expects the root context.

  - a retired key is retired in every shape, checked BEFORE the topology
    branch: a standalone values file still carrying `leaderOrdinal` would
    otherwise render happily and only fail the day somebody switched it to
    `replicated` — the moment they can least afford a surprise.
  - candidates take the range FROM the lease; without it no pod would ever
    lead. Renders-then-cannot-work is the worst configuration error, so it
    fails at render time.
  - grants are minted from 1; a static floor at or above the first grant
    would refuse the very grant that makes a candidate lead.
*/}}
{{- define "vtop.dataGuards" -}}
{{- $v := .Values -}}
{{- if hasKey $v.data "leaderOrdinal" -}}
{{- fail "\n\ndata.leaderOrdinal is retired (#284): \"replicated\" renders every pod as a CANDIDATE and the role follows the metadata lease, so failover no longer needs a re-render. Remove the value." -}}
{{- end -}}
{{- if eq $v.data.topology "replicated" -}}
{{- if not $v.data.lease.enabled -}}
{{- fail "\n\ndata.topology \"replicated\" requires data.lease.enabled: candidates acquire the range through the metadata lease (#284). Without it no pod would ever lead." -}}
{{- end -}}
{{- if ne (int $v.data.fencingEpoch) 0 -}}
{{- fail (printf "\n\ndata.fencingEpoch is %d but must be 0 under \"replicated\": candidates learn their epoch from lease grants (minted from 1), and a static floor at or above the first grant refuses it." (int $v.data.fencingEpoch)) -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Deployment mode (#287): colocated unless deployment.mode says otherwise.
*/}}
{{- define "vtop.deploymentMode" -}}
{{- $mode := "colocated" -}}
{{- with .Values.deployment -}}{{- $mode = default "colocated" .mode -}}{{- end -}}
{{- $mode -}}
{{- end }}

{{- define "vtop.metaReplicaCount" -}}
{{- $count := 0 -}}
{{- with .Values.deployment -}}{{- with .meta -}}{{- $count = int (default 0 .replicaCount) -}}{{- end -}}{{- end -}}
{{- $count -}}
{{- end }}

{{- define "vtop.dataReplicaCount" -}}
{{- $count := 0 -}}
{{- with .Values.deployment -}}{{- with .data -}}{{- $count = int (default 0 .replicaCount) -}}{{- end -}}{{- end -}}
{{- $count -}}
{{- end }}

{{/*
Tier objects (#287 separated mode). Each tier is its own StatefulSet with its
own headless Service, so a pod's stable name is
<fullname>-<tier>-<ordinal>.<fullname>-<tier>-headless.<ns>.svc.<domain>.
Expects (dict "root" $ "tier" "meta"|"data" [ "ordinal" <int> ]).
*/}}
{{- define "vtop.tierName" -}}
{{- printf "%s-%s" (include "vtop.fullname" .root) .tier -}}
{{- end }}

{{- define "vtop.tierHeadlessServiceName" -}}
{{- printf "%s-%s-headless" (include "vtop.fullname" .root) .tier -}}
{{- end }}

{{- define "vtop.tierPodFqdn" -}}
{{- $root := .root -}}
{{- printf "%s-%d.%s.%s.svc.%s" (include "vtop.tierName" .) (int .ordinal) (include "vtop.tierHeadlessServiceName" .) $root.Release.Namespace $root.Values.clusterDomain -}}
{{- end }}

{{/*
rustls server name for a tier pod: the shared tls.serverName when set,
otherwise the pod's own FQDN — the same rule as vtop.peerServerName.
*/}}
{{- define "vtop.tierServerName" -}}
{{- if .root.Values.tls.serverName -}}
{{- .root.Values.tls.serverName -}}
{{- else -}}
{{- include "vtop.tierPodFqdn" . -}}
{{- end -}}
{{- end }}

{{/*
Selector labels for one tier: the chart's selector labels plus the tier, so
each StatefulSet, headless Service and PodDisruptionBudget selects exactly
its own pods while a chart-wide selector (NetworkPolicy, ServiceMonitor)
still spans both.
*/}}
{{- define "vtop.tierSelectorLabels" -}}
{{ include "vtop.selectorLabels" .root }}
vtop.allamiro.io/tier: {{ .tier }}
{{- end }}

{{/*
The METADATA TIER config for one meta-tier pod ordinal (#287 separated): the
MetaNodeConfig `vtop-node meta` deserializes, with its own observability
endpoint — the standalone command owns one, unlike the co-located wrapper.
Peers are the metadata tier's own pods. Expects (dict "root" $ "ordinal" <int>).
*/}}
{{/*
The transition MAC key reference must be whole (#240 item 5): a Secret name
without a key within it renders an env var Kubernetes cannot resolve, and
the pod would sit in CreateContainerConfigError saying so less clearly.
*/}}
{{- define "vtop.transitionMacKeyGuard" -}}
{{- if and .Values.meta.transitionMacKey.secretName (not .Values.meta.transitionMacKey.secretKey) -}}
{{- fail "\n\nmeta.transitionMacKey.secretName is set but meta.transitionMacKey.secretKey is empty: name the key within the Secret that holds the 32-byte hex MAC key." -}}
{{- end -}}
{{- /* The Secret-backed entry must be the only VTOP_TRANSITION_MAC_KEY: a
       later extraEnv entry of the same name would win inside the container,
       and the metadata process would sign with a key the chart never
       named (review). */ -}}
{{- if .Values.meta.transitionMacKey.secretName -}}
{{- range .Values.extraEnv -}}
{{- if eq (toString .name) "VTOP_TRANSITION_MAC_KEY" -}}
{{- fail "\n\nextraEnv sets VTOP_TRANSITION_MAC_KEY while meta.transitionMacKey.secretName is set: the chart injects that variable from the Secret, and a second entry would override it. Remove it from extraEnv, or unset meta.transitionMacKey.secretName to leave transitions unsigned — the config field that makes the node read the variable is rendered only from the Secret reference." -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{- define "vtop.metaTierConfig" -}}
{{- $root := .root -}}
{{- $i := int .ordinal -}}
{{- $v := $root.Values -}}
{{- include "vtop.transitionMacKeyGuard" $root -}}
{{- $metaCount := int (include "vtop.metaReplicaCount" $root) -}}
{{- $clusterId := required "\n\ncluster.id is required: the cluster UUID shared by every node. The chart does not default identities — they must match the certificates you minted." $v.cluster.id -}}
# Metadata-tier config for pod {{ include "vtop.tierName" (dict "root" $root "tier" "meta") }}-{{ $i }}.
# Rendered by Helm; the container selects this file by its hostname ordinal.
# Raft node id = pod ordinal + 1; the peer transport authenticates it against
# the meta certificate's CN, so node-{{ $i }}.pem in tls.metaSecretName must
# carry CN={{ add1 $i }}.
node_id: {{ add1 $i }}
cluster_id: {{ $clusterId }}
data_dir: /var/lib/vtop/meta
peer_listen: "0.0.0.0:{{ $v.ports.metaPeer }}"
admin_listen: "0.0.0.0:{{ $v.ports.metaAdmin }}"
# The full voter set, identical on every pod: the binary ignores a node's own
# entry, and one shared list keeps the rendered configs diffable.
peers:
{{- range $j := until $metaCount }}
  - { id: {{ add1 $j }}, addr: "{{ include "vtop.tierPodFqdn" (dict "root" $root "tier" "meta" "ordinal" $j) }}:{{ $v.ports.metaPeer }}", server_name: "{{ include "vtop.tierServerName" (dict "root" $root "tier" "meta" "ordinal" $j) }}" }
{{- end }}
tls:
  ca: /etc/vtop/tls/meta/ca.pem
  cert: /etc/vtop/tls/meta/node-{{ $i }}.pem
  key: /etc/vtop/tls/meta/node-{{ $i }}-key.pem
timers:
  election_timeout_min_ms: {{ int $v.meta.timers.electionTimeoutMinMs }}
  election_timeout_max_ms: {{ int $v.meta.timers.electionTimeoutMaxMs }}
  heartbeat_interval_ms: {{ int $v.meta.timers.heartbeatIntervalMs }}
{{- if $v.meta.adminAuthorization.enabled }}
# Present-but-empty is the STRICTEST policy (nobody may run cluster-scoped
# commands), absent is authenticate-only — the chart emits exactly what
# meta.adminAuthorization asked for.
admin_authorization:
  operator_common_names: {{ toJson $v.meta.adminAuthorization.operatorCommonNames }}
{{- end }}
{{- if $v.meta.transitionMacKey.secretName }}
# The key itself is in the environment, from meta.transitionMacKey (#240).
transition_mac_key_env: VTOP_TRANSITION_MAC_KEY
{{- end }}
# This process's own endpoint. Unauthenticated (#78): keep it off public
# networks (see networkPolicy).
observability:
  listen: "0.0.0.0:{{ $v.ports.observability }}"
{{- end }}

{{/*
The DATA TIER config for one data-tier pod ordinal (#287 separated): the
DataNodeConfig `vtop-node data` deserializes, with its own observability
endpoint. Peers are the data tier's own pods; the lease reaches the METADATA
tier — its first pod by default, every pod in admin_peers, each dialled at
its own FQDN with its own certificate name, never a Service name (the same
per-pod SAN rule the co-located render keeps). Expects (dict "root" $
"ordinal" <int>).
*/}}
{{- define "vtop.dataTierConfig" -}}
{{- $root := .root -}}
{{- $i := int .ordinal -}}
{{- $v := $root.Values -}}
{{- $metaCount := int (include "vtop.metaReplicaCount" $root) -}}
{{- $dataCount := int (include "vtop.dataReplicaCount" $root) -}}
{{- $clusterId := required "\n\ncluster.id is required: the cluster UUID shared by every node. The chart does not default identities — they must match the certificates you minted." $v.cluster.id -}}
{{- /* The deployment guards run here too (review): the ConfigMap renders
       before the StatefulSet, and an unsized tier would otherwise die on an
       index error inside this template instead of on the guard that names
       the problem. */ -}}
{{- include "vtop.deploymentGuards" $root -}}
{{- include "vtop.dataGuards" $root -}}
{{- if ne (len (uniq $v.data.nodeUuids)) (len $v.data.nodeUuids) -}}
{{- fail (printf "\n\ndata.nodeUuids contains duplicates: %v. Each data pod ordinal needs its OWN broker UUID — two pods sharing one identity would present the same identity to the metadata plane and race the same range lease." $v.data.nodeUuids) -}}
{{- end -}}
{{- $nodeUuid := index $v.data.nodeUuids $i -}}
# Data-tier config for pod {{ include "vtop.tierName" (dict "root" $root "tier" "data") }}-{{ $i }}.
# Rendered by Helm; the container selects this file by its hostname ordinal.
{{- if eq $v.data.topology "replicated" }}
role: candidate
# The SAME list on every data pod, self included — the binary skips its own
# entry. Each peer is dialled at its OWN FQDN (never a Service name).
peers:
{{- range $ordinal := until $dataCount }}
  - node_uuid: {{ index $v.data.nodeUuids $ordinal }}
    addr: "{{ include "vtop.tierPodFqdn" (dict "root" $root "tier" "data" "ordinal" $ordinal) }}:{{ $v.ports.replica }}"
    server_name: "{{ include "vtop.tierServerName" (dict "root" $root "tier" "data" "ordinal" $ordinal) }}"
{{- end }}
{{- else }}
role: standalone
{{- end }}
# Must equal the CN of node-{{ $i }}.pem in the data-plane Secret.
node_uuid: {{ $nodeUuid }}
cluster_id: {{ $clusterId }}
data_dir: /var/lib/vtop/data
fencing_epoch: {{ int $v.data.fencingEpoch }}
{{- if $v.data.segmentFormat }}
{{- if not (has $v.data.segmentFormat (list "v1" "v2")) }}
{{- fail (printf "\n\ndata.segmentFormat must be \"v1\" or \"v2\" (got %q): it is the on-disk format a NEW range is created in, and the binary refuses anything else." $v.data.segmentFormat) }}
{{- end }}
segment_format: {{ $v.data.segmentFormat }}
{{- end }}
range:
  topic: {{ required "\n\ndata.range.topic is required: the wire-level topic name this range serves." $v.data.range.topic | quote }}
  topic_epoch: {{ int $v.data.range.topicEpoch }}
  range_id: {{ required "\n\ndata.range.rangeId is required: the range UUID (protocol-visible identity; the chart does not invent one)." $v.data.range.rangeId }}
  range_generation: {{ int $v.data.range.rangeGeneration }}
segment_id: {{ required "\n\ndata.segmentId is required: the segment UUID (protocol-visible identity; the chart does not invent one)." $v.data.segmentId }}
native_listen: "0.0.0.0:{{ $v.ports.native }}"
replica_listen: "0.0.0.0:{{ $v.ports.replica }}"
replica_tls:
  ca: /etc/vtop/tls/data/ca.pem
  cert: /etc/vtop/tls/data/node-{{ $i }}.pem
  key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
native_tls:
  ca: /etc/vtop/tls/data/ca.pem
  cert: /etc/vtop/tls/data/node-{{ $i }}.pem
  key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
# The one client principal the produce/fetch authorizer accepts; never
# defaulted by the chart (a default principal is a baked-in credential).
principal_id: {{ required "\n\ndata.principalId is required: the UUID of the one client principal the produce/fetch authorizer accepts (must equal your client certificate's CN). The chart never defaults credentials (issue #81)." $v.data.principalId }}
{{- if $v.data.lease.enabled }}
{{- /* Under separated mode the co-located default (this process's own
       loopback admin listener) names nothing: there is no metadata process
       in this pod. The default is therefore replaced by the metadata tier's
       first pod, at its own FQDN so the certificate has a name to verify.
       Any other value is the operator's explicit choice and is honoured as
       written — with serverName then theirs to set to match it. */ -}}
{{- $metaZero := (dict "root" $root "tier" "meta" "ordinal" 0) -}}
{{- $endpoint := $v.data.lease.adminEndpoint -}}
{{- $serverName := $v.data.lease.serverName -}}
{{- if or (not $endpoint) (eq $endpoint "127.0.0.1:9200") -}}
{{- /* The endpoint is the metadata tier's first pod, so the name is that
       pod's (or the shared tls.serverName) and nothing else: a serverName
       carried over from a co-located values file would name a pod that is
       not being dialled, and the lease would fail its handshake in
       production (review). Refused rather than silently overridden. */ -}}
{{- if $serverName -}}
{{- fail (printf "\n\ndata.lease.serverName is %q but data.lease.adminEndpoint is at its default: under separated mode the default endpoint is the metadata tier's first pod, whose certificate name the chart derives (tls.serverName, else the pod's own FQDN). Remove data.lease.serverName, or set adminEndpoint explicitly to the endpoint that name belongs to." $serverName) -}}
{{- end -}}
{{- $endpoint = printf "%s:%d" (include "vtop.tierPodFqdn" $metaZero) (int $v.ports.metaAdmin) -}}
{{- $serverName = include "vtop.tierServerName" $metaZero -}}
{{- else if not $serverName -}}
{{- fail (printf "\n\ndata.lease.adminEndpoint is %q under separated mode: set data.lease.serverName to the name that endpoint's certificate carries (or leave adminEndpoint at its default to address the metadata tier's first pod)." $endpoint) -}}
{{- end }}
# Metadata-driven leadership (#223). The credential is this node's OWN
# data-plane certificate (CN = node_uuid): the lease names this broker as
# holder, and admin authorization (#238) refuses any other identity.
lease:
  admin_endpoint: "{{ $endpoint }}"
  server_name: "{{ $serverName }}"
  topic_uuid: {{ required "\n\ndata.lease.topicUuid is required when data.lease.enabled: metadata's UUID for the topic (NOT data.range.topic, which is the wire name)." $v.data.lease.topicUuid }}
  tls:
    ca: /etc/vtop/tls/data/ca.pem
    cert: /etc/vtop/tls/data/node-{{ $i }}.pem
    key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
  lease_duration_ms: {{ int $v.data.lease.leaseDurationMs }}
  renew_interval_ms: {{ int $v.data.lease.renewIntervalMs }}
  poll_interval_ms: {{ int $v.data.lease.pollIntervalMs }}
  # EVERY metadata node, so a redirect can be followed (#292): only the Raft
  # leader serves a lease read or proposal, and which pod leads is an
  # election's outcome. Ids are 1-based meta-tier ordinals, matching the
  # CNs in the metadata Secret.
  admin_peers:
{{- range $ordinal := until $metaCount }}
    - node_id: {{ add1 $ordinal }}
      endpoint: "{{ include "vtop.tierPodFqdn" (dict "root" $root "tier" "meta" "ordinal" $ordinal) }}:{{ $v.ports.metaAdmin }}"
      server_name: "{{ include "vtop.tierServerName" (dict "root" $root "tier" "meta" "ordinal" $ordinal) }}"
{{- end }}
{{- end }}
# This process's own endpoint. Unauthenticated (#78): keep it off public
# networks (see networkPolicy).
observability:
  listen: "0.0.0.0:{{ $v.ports.observability }}"
{{- end }}

{{/*
The co-located node config for one pod ordinal — the exact YAML shape
`vtop-node node` deserializes with deny_unknown_fields:

  meta:          MetaNodeConfig, WITHOUT a per-role observability block
  data:          DataNodeConfig, WITHOUT a per-role observability block
  observability: the ONE endpoint for the whole process

Per-role observability blocks are rejected by the binary (even empty ones),
so this template never emits them. Expects (dict "root" $ "ordinal" <int>).
*/}}
{{- define "vtop.nodeConfig" -}}
{{- $root := .root -}}
{{- $i := int .ordinal -}}
{{- $v := $root.Values -}}
{{- include "vtop.transitionMacKeyGuard" $root -}}
{{- $clusterId := required "\n\ncluster.id is required: the cluster UUID shared by every node. The chart does not default identities — they must match the certificates you minted." $v.cluster.id -}}
{{- $nodeUuid := "" -}}
{{- if gt (len $v.data.nodeUuids) $i -}}
{{- /* Distinct per ordinal. Two pods sharing a broker UUID share an IDENTITY:
       with data.lease.enabled they present the same one to the metadata plane
       and race each other for the same range lease, so the StatefulSet is not
       a cluster of replicas but two claimants wearing one name. The
       certificate CN convention makes it worse — one certificate would be
       valid for both. */ -}}
{{- if ne (len (uniq $v.data.nodeUuids)) (len $v.data.nodeUuids) -}}
{{- fail (printf "\n\ndata.nodeUuids contains duplicates: %v. Each pod ordinal needs its OWN broker UUID — two pods sharing one identity would present the same identity to the metadata plane and race the same range lease." $v.data.nodeUuids) -}}
{{- end -}}
{{- $nodeUuid = index $v.data.nodeUuids $i -}}
{{- else -}}
{{- fail (printf "\n\ndata.nodeUuids has %d entries but replicaCount is %d: provide one broker UUID per replica, indexed by pod ordinal. Each must equal the CN of that pod's data-plane certificate." (len $v.data.nodeUuids) (int $v.replicaCount)) -}}
{{- end -}}
# Co-located node config for pod {{ include "vtop.fullname" $root }}-{{ $i }}.
# Rendered by Helm; the container selects this file by its hostname ordinal.
meta:
  # Raft node id = pod ordinal + 1 (ids are 1-based, matching the upstream
  # harness). The peer transport authenticates this id against the meta
  # certificate's CN, so node-{{ $i }}.pem must carry CN={{ add1 $i }}.
  node_id: {{ add1 $i }}
  cluster_id: {{ $clusterId }}
  data_dir: /var/lib/vtop/meta
  peer_listen: "0.0.0.0:{{ $v.ports.metaPeer }}"
  admin_listen: "0.0.0.0:{{ $v.ports.metaAdmin }}"
  # The full voter set, identical on every pod: the binary ignores a node's
  # own entry, and one shared list keeps the rendered configs diffable.
  peers:
{{- range $j := until (int $v.replicaCount) }}
    - { id: {{ add1 $j }}, addr: "{{ include "vtop.podFqdn" (dict "root" $root "ordinal" $j) }}:{{ $v.ports.metaPeer }}", server_name: "{{ include "vtop.peerServerName" (dict "root" $root "ordinal" $j) }}" }
{{- end }}
  tls:
    ca: /etc/vtop/tls/meta/ca.pem
    cert: /etc/vtop/tls/meta/node-{{ $i }}.pem
    key: /etc/vtop/tls/meta/node-{{ $i }}-key.pem
  timers:
    election_timeout_min_ms: {{ int $v.meta.timers.electionTimeoutMinMs }}
    election_timeout_max_ms: {{ int $v.meta.timers.electionTimeoutMaxMs }}
    heartbeat_interval_ms: {{ int $v.meta.timers.heartbeatIntervalMs }}
{{- if $v.meta.adminAuthorization.enabled }}
  # Present-but-empty is the STRICTEST policy (nobody may run cluster-scoped
  # commands), absent is authenticate-only — the chart emits exactly what
  # meta.adminAuthorization asked for.
  admin_authorization:
    operator_common_names: {{ toJson $v.meta.adminAuthorization.operatorCommonNames }}
{{- end }}
{{- if $v.meta.transitionMacKey.secretName }}
  transition_mac_key_env: VTOP_TRANSITION_MAC_KEY
{{- end }}
data:
  {{- /* TOPOLOGY. The binary supports candidate/leader/follower/standalone,
         and the live-chaos harness configures all of them; this value is how
         the chart reaches them.

         standalone (default): every pod serves an INDEPENDENT range. Three
         replicas are three separate logs, which is why quorum durability is
         refused and why a pod nobody produced to stays empty.

         replicated: ONE range across the pods, every pod a CANDIDATE (#284).
         The role follows the metadata lease inside the binary: whichever pod
         acquires the range leads, the rest follow it, and when the holder
         dies a survivor takes the range in place — no re-render, no restart.
         The chart used to encode `role: leader`/`follower` from a
         `leaderOrdinal` value, which made failover a helm upgrade; that value
         is retired, and setting it now fails the render rather than being
         silently ignored. */ -}}
  {{- include "vtop.dataGuards" $root -}}
  {{- if eq $v.data.topology "replicated" }}
  {{- if lt (int $v.replicaCount) 2 }}
  {{- fail (printf "\n\ndata.topology is \"replicated\" but replicaCount is %d: a replicated range needs at least one follower. Use topology \"standalone\" for a single node." (int $v.replicaCount)) }}
  {{- end }}
  role: candidate
  peers:
    {{- range $ordinal := until (int $root.Values.replicaCount) }}
    {{- /* The SAME list on every pod, self included — the binary skips its
           own entry, and one shared list keeps the rendered configs diffable
           (the metadata peer list above follows the same convention).

           addr is the pod's OWN FQDN, never `vtop.peerServerName`: that
           helper returns tls.serverName when a shared SAN is configured, and
           using it here would make a leading candidate dial one shared name
           for every peer — a load-balanced endpoint, or the same pod
           repeatedly — instead of each specific replica. The socket
           destination and the name verified on the certificate are different
           questions; only the second may be shared.

           This comment must NOT close with a right-chomping delimiter. A
           right-chomp swallows the newline and indent that follow, so the
           first list item lands on the "peers:" line as "peers:- node_uuid:"
           and every later one glues onto its predecessor — YAML that renders
           without error and that the binary refuses to parse. */}}
    - node_uuid: {{ index $v.data.nodeUuids $ordinal }}
      addr: "{{ include "vtop.podFqdn" (dict "root" $root "ordinal" $ordinal) }}:{{ $v.ports.replica }}"
      server_name: "{{ include "vtop.peerServerName" (dict "root" $root "ordinal" $ordinal) }}"
    {{- end }}
  {{- else }}
  role: standalone
  {{- end }}
  # Must equal the CN of node-{{ $i }}.pem in the data-plane Secret.
  node_uuid: {{ $nodeUuid }}
  cluster_id: {{ $clusterId }}
  data_dir: /var/lib/vtop/data
  fencing_epoch: {{ int $v.data.fencingEpoch }}
  {{- if $v.data.segmentFormat }}
  {{- if not (has $v.data.segmentFormat (list "v1" "v2")) }}
  {{- fail (printf "\n\ndata.segmentFormat must be \"v1\" or \"v2\" (got %q): it is the on-disk format a NEW range is created in, and the binary refuses anything else." $v.data.segmentFormat) }}
  {{- end }}
  segment_format: {{ $v.data.segmentFormat }}
  {{- end }}
  range:
    topic: {{ required "\n\ndata.range.topic is required: the wire-level topic name this range serves." $v.data.range.topic | quote }}
    topic_epoch: {{ int $v.data.range.topicEpoch }}
    range_id: {{ required "\n\ndata.range.rangeId is required: the range UUID (protocol-visible identity; the chart does not invent one)." $v.data.range.rangeId }}
    range_generation: {{ int $v.data.range.rangeGeneration }}
  segment_id: {{ required "\n\ndata.segmentId is required: the segment UUID (protocol-visible identity; the chart does not invent one)." $v.data.segmentId }}
  native_listen: "0.0.0.0:{{ $v.ports.native }}"
  # Status-only handler on the leader/standalone: lets `vtopctl node status`
  # measure lag against this replica's boundary. Write paths refuse.
  replica_listen: "0.0.0.0:{{ $v.ports.replica }}"
  replica_tls:
    ca: /etc/vtop/tls/data/ca.pem
    cert: /etc/vtop/tls/data/node-{{ $i }}.pem
    key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
  native_tls:
    ca: /etc/vtop/tls/data/ca.pem
    cert: /etc/vtop/tls/data/node-{{ $i }}.pem
    key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
  # The one client principal the produce/fetch authorizer accepts; never
  # defaulted by the chart (a default principal is a baked-in credential).
  principal_id: {{ required "\n\ndata.principalId is required: the UUID of the one client principal the produce/fetch authorizer accepts (must equal your client certificate's CN). The chart never defaults credentials (issue #81)." $v.data.principalId }}
{{- if $v.data.lease.enabled }}
  # Metadata-driven leadership (#223). The credential is this node's OWN
  # data-plane certificate (CN = node_uuid): the lease names this broker as
  # holder, and admin authorization (#238) refuses any other identity.
  lease:
    admin_endpoint: "{{ $v.data.lease.adminEndpoint }}"
    server_name: "{{ default (include "vtop.peerServerName" (dict "root" $root "ordinal" $i)) $v.data.lease.serverName }}"
    topic_uuid: {{ required "\n\ndata.lease.topicUuid is required when data.lease.enabled: metadata's UUID for the topic (NOT data.range.topic, which is the wire name)." $v.data.lease.topicUuid }}
    tls:
      ca: /etc/vtop/tls/data/ca.pem
      cert: /etc/vtop/tls/data/node-{{ $i }}.pem
      key: /etc/vtop/tls/data/node-{{ $i }}-key.pem
    lease_duration_ms: {{ int $v.data.lease.leaseDurationMs }}
    renew_interval_ms: {{ int $v.data.lease.renewIntervalMs }}
    poll_interval_ms: {{ int $v.data.lease.pollIntervalMs }}
    {{- /* EVERY metadata node, so a redirect can be followed (#292).

           `admin_endpoint` above is where to ask FIRST — under co-location that
           is this pod's own metadata process, which is the cheapest hop. It is
           not necessarily the RAFT LEADER, though, and only the leader can
           serve a lease read or a lease proposal. Without somewhere else to go,
           every pod that did not happen to co-locate the leader failed closed
           forever and never became ready, which made the replicated topology
           unusable and made WHICH pod worked depend on an election.

           Rendered unconditionally rather than behind a topology check: a
           standalone range with a lease has exactly the same problem, and a
           single-replica install renders a one-entry list that changes nothing.

           Metadata node ids are 1-based ordinals, matching the peer list
           above and the CNs in the metadata Secret. */}}
    admin_peers:
      {{- range $ordinal := until (int $root.Values.replicaCount) }}
      - node_id: {{ add1 $ordinal }}
        endpoint: "{{ include "vtop.podFqdn" (dict "root" $root "ordinal" $ordinal) }}:{{ $v.ports.metaAdmin }}"
        server_name: "{{ include "vtop.peerServerName" (dict "root" $root "ordinal" $ordinal) }}"
      {{- end }}
{{- end }}
# ONE endpoint for the whole process — top level, never per role. It is
# unauthenticated (#78): keep it off public networks (see networkPolicy).
observability:
  listen: "0.0.0.0:{{ $v.ports.observability }}"
{{- end }}
