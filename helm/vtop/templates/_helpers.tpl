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
{{- required "\n\nTLS is required and never defaulted: set tls.dataSecretName to an existing Secret holding the data/replica-plane mTLS material (keys: ca.pem, node-<ordinal>.pem, node-<ordinal>-key.pem; leaf CN = cluster.nodeUuids[ordinal]). This chart ships no default credentials (issue #81). See helm/vtop/README.md for the full Secret contract." .Values.tls.dataSecretName -}}
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
{{- $clusterId := required "\n\ncluster.id is required: the cluster UUID shared by every node. The chart does not default identities — they must match the certificates you minted." $v.cluster.id -}}
{{- $nodeUuid := "" -}}
{{- if gt (len $v.cluster.nodeUuids) $i -}}
{{- /* Distinct per ordinal. Two pods sharing a broker UUID share an IDENTITY:
       with data.lease.enabled they present the same one to the metadata plane
       and race each other for the same range lease, so the StatefulSet is not
       a cluster of replicas but two claimants wearing one name. The
       certificate CN convention makes it worse — one certificate would be
       valid for both. */ -}}
{{- if ne (len (uniq $v.cluster.nodeUuids)) (len $v.cluster.nodeUuids) -}}
{{- fail (printf "\n\ncluster.nodeUuids contains duplicates: %v. Each pod ordinal needs its OWN broker UUID — two pods sharing one identity would present the same identity to the metadata plane and race the same range lease." $v.cluster.nodeUuids) -}}
{{- end -}}
{{- $nodeUuid = index $v.cluster.nodeUuids $i -}}
{{- else -}}
{{- fail (printf "\n\ncluster.nodeUuids has %d entries but replicaCount is %d: provide one broker UUID per replica, indexed by pod ordinal. Each must equal the CN of that pod's data-plane certificate." (len $v.cluster.nodeUuids) (int $v.replicaCount)) -}}
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
data:
  # Standalone: each pod serves an INDEPENDENT range. Three replicas here are
  # three separate logs, not three copies of one — which is why
  # `--durability quorum` is refused ("Quorum durability requires a configured
  # replica set") and why a pod nobody produced to stays empty.
  #
  # This used to say replicated ranges were waiting on follower-side epoch
  # propagation upstream. That landed (#239), along with the rest of the
  # recovery arc (#240), so the engine is no longer the constraint. The chart
  # is: `role` is a static per-node field, so a replicated range means
  # rendering ordinal 0 as leader with a followers list and the rest as
  # followers — and a StatefulSet cannot re-render a role when leadership
  # moves. Tracked with the shape that would work in #284.
  role: standalone
  # Must equal the CN of node-{{ $i }}.pem in the data-plane Secret.
  node_uuid: {{ $nodeUuid }}
  cluster_id: {{ $clusterId }}
  data_dir: /var/lib/vtop/data
  fencing_epoch: {{ int $v.data.fencingEpoch }}
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
{{- end }}
# ONE endpoint for the whole process — top level, never per role. It is
# unauthenticated (#78): keep it off public networks (see networkPolicy).
observability:
  listen: "0.0.0.0:{{ $v.ports.observability }}"
{{- end }}
