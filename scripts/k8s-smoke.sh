#!/usr/bin/env bash
#
# End-to-end smoke test for the Helm chart against a real cluster.
#
# The chart's CI job lints, renders, and checks its refusal contracts. All of
# that is STATIC: it proves the chart emits valid YAML, not that a cluster
# comes up. This is the other half — it installs the chart, bootstraps the
# metadata group, streams records, and kills a pod to check they survive.
#
# It exists because rendering proved nothing about two real bugs. The image
# shipped without `vtop-node` at all (#281), so every pod would have
# CrashLoopBackOff'd, and nothing in the static checks could have noticed.
#
# Three shapes, in one run, each in its own namespace:
#
#   $NS               the STANDALONE default — three independent ranges
#   ${NS}-neighbour   a second release, to prove namespaces are isolated
#   ${NS}-replicated  ONE range across three pods: the only shape that
#                     exercises replication, fencing and promotion
#

# Usage:  scripts/k8s-smoke.sh [namespace] [release]
#
# Requires: a reachable cluster, kubectl, helm, openssl, and a locally built
# image tagged vtop-engine:local (docker build -f docker/Dockerfile).

set -euo pipefail

NS="${1:-vtop-smoke}"
REL="${2:-vtop}"
IMAGE_REPO="${IMAGE_REPO:-vtop-engine}"
IMAGE_TAG="${IMAGE_TAG:-local}"
DOMAIN="${CLUSTER_DOMAIN:-cluster.local}"
NEIGHBOUR_NS="${NS}-neighbour"
REPLICATED_NS="${NS}-replicated"
SEPARATED_NS="${NS}-separated"
WORK="$(mktemp -d)"
CERTS="$WORK/certs"
HEADLESS="${REL}-headless"

CLUSTER_ID=11111111-2222-3333-4444-555555555555
PRINCIPAL=aaaaaaaa-0000-0000-0000-0000000000ce
RANGE_ID=aaaaaaaa-0000-0000-0000-0000000000c1
# Metadata's identity for the topic, distinct from the wire name "telemetry".
# Must match data.lease.topicUuid in helm/vtop/ci/replicated-values.yaml.
TOPIC_UUID=aaaaaaaa-0000-0000-0000-0000000000f1
UUID_0=aaaaaaaa-0000-0000-0000-0000000000a1
UUID_1=aaaaaaaa-0000-0000-0000-0000000000a2
UUID_2=aaaaaaaa-0000-0000-0000-0000000000a3

log()  { printf '[k8s-smoke] %s\n' "$*"; }
fail() { printf '[k8s-smoke] FAIL: %s\n' "$*" >&2; exit 1; }

cleanup() {
  # Port-forwards are children of this shell; kill the whole group's strays.
  for pid in ${FORWARDS:-}; do kill "$pid" 2>/dev/null || true; done
  if [ "${KEEP:-0}" != "1" ]; then
    helm uninstall "$REL" -n "$NS" >/dev/null 2>&1 || true
    kubectl delete namespace "$NS" --wait=false >/dev/null 2>&1 || true
    helm uninstall "$REL" -n "$NEIGHBOUR_NS" >/dev/null 2>&1 || true
    kubectl delete namespace "$NEIGHBOUR_NS" --wait=false >/dev/null 2>&1 || true
    helm uninstall "$REL" -n "$REPLICATED_NS" >/dev/null 2>&1 || true
    kubectl delete namespace "$REPLICATED_NS" --wait=false >/dev/null 2>&1 || true
    helm uninstall "$REL" -n "$SEPARATED_NS" >/dev/null 2>&1 || true
    kubectl delete namespace "$SEPARATED_NS" --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

FORWARDS=""
forward_ns() { # namespace pod localport remoteport
  kubectl -n "$1" port-forward "pod/$2" "$3:$4" >/dev/null 2>&1 &
  FORWARDS="$FORWARDS $!"
  disown 2>/dev/null || true
  sleep 4
}

forward() { # pod localport remoteport
  kubectl -n "$NS" port-forward "pod/$1" "$2:$3" >/dev/null 2>&1 &
  FORWARDS="$FORWARDS $!"
  # Job control off for this shell, so tearing the forwards down at exit does
  # not print a "Terminated" line per child over the test's own output.
  disown 2>/dev/null || true
  sleep 4
}

# Wait for the StatefulSet to CREATE its pods before waiting for them to be
# ready.
#
# `kubectl wait` does not wait for resources to appear: given a label selector
# that currently matches nothing it fails immediately with "no matching
# resources found". `helm upgrade --install` returns once the objects are
# accepted by the API server, not once the StatefulSet controller has produced
# pods, so there is a window — usually a fraction of a second, occasionally
# longer — where the selector matches nothing and the readiness wait dies
# instantly with a message that reads like the pods will never come:
#
#     error: no matching resources found
#     vtop-0   0/1   Pending   0   0s
#
# Timing-dependent, so it passes almost always and fails for reasons unrelated
# to the thing under test. Found by running the whole suite against a freshly
# built image rather than by reading.
await_pods_exist() { # namespace count
  # A FAILED QUERY IS NOT ZERO PODS — the same rule the capacity wait below
  # states, and which the first version of this helper broke in the very next
  # function. Retrying is right either way, so the gap is not behaviour but
  # ATTRIBUTION: if the API server stopped answering, reading 0 for two minutes
  # and then blaming the StatefulSet accuses a component that was never
  # observed doing anything. `seen` records whether any query ever succeeded,
  # so the two failures get the two different messages they deserve.
  seen=0
  for _ in $(seq 1 60); do
    if pods="$(kubectl -n "$1" get pods -l "app.kubernetes.io/instance=${REL}" \
      --no-headers 2>/dev/null)"; then
      seen=1
      have="$(printf '%s' "$pods" | grep -c . || true)"
      [ "${have:-0}" -ge "$2" ] && return 0
    fi
    sleep 2
  done
  kubectl -n "$1" get pods || true
  if [ "$seen" = "0" ]; then
    fail "namespace $1 never answered a pod query in 120s; the cluster stopped responding, so \
whether the StatefulSet created anything is unknown"
  fi
  fail "namespace $1 never produced $2 pod(s) in 120s; the StatefulSet did not create them"
}

# The kube-DNS readiness gate (#416). Pod readiness proves a pod can serve;
# it proves nothing about whether that pod's PEERS can spell its name. Three
# CI failures in one day showed both shapes the gap takes on kind: headless
# names that never resolve before readiness (a rejoin that times out), and a
# Ready pod whose record its peers still cannot look up — replication to it
# silently stalls at whatever it last applied, and the verify downstream
# blames the replica. So every readiness wait for this release is followed by
# this gate, which asks the only authority that matters — glibc inside each
# pod, through the same resolv.conf the node process resolves with — and asks
# EVERY pod about EVERY peer, because the record one pod's query finds says
# nothing about the cache another pod's query lands on.
# Settled means resolved to the pod's CURRENT address, not merely resolved
# (review): across a pod recreation a cached positive answer can hand a peer
# the DEAD pod's IP with a success exit code, which is precisely the
# occurrence-three stall this gate exists to catch — so each answer is
# compared against the target's live status.podIP.
#
# Attribution, the `await_pods_exist` rule applied three ways (review): a
# hung LOOKUP is a DNS verdict — the resolver stalling is a way DNS fails —
# so the lookup is bounded REMOTELY and reports its own exit code through
# the sentinel; an exec that dies without delivering the remote shell's
# output at all is the API path's failure, not DNS's; and a target with no
# podIP yet is a pod that is not scheduled or started, which is neither.
# Three lists, three names in the report, no component blamed unobserved.
#
# Wall-clock bounded, not round-counted (review): with every probe hanging,
# sixty rounds of six 15-second timeouts would run ~90 minutes — past the CI
# job's own ceiling, so the diagnostics below would never print. The
# deadline keeps the worst case inside the budget the surrounding readiness
# waits already set.
await_peer_dns() { # namespace [pod-prefix] [headless-service]
  # The prefix and headless name are the co-located ones unless told
  # otherwise: the separated shape (#287) gates each tier's own DNS.
  peer_ns="$1"
  peer_prefix="${2:-${REL}-}"
  peer_headless="${3:-$HEADLESS}"
  unresolved=""
  unprobed=""
  unaddressed=""
  gate_deadline=$((SECONDS + 240))
  while [ "$SECONDS" -lt "$gate_deadline" ]; do
    unresolved=""
    unprobed=""
    unaddressed=""
    # Success needs a COMPLETE round (review): the mid-round deadline breaks
    # below leave the lists exactly as far as the round got, and empty lists
    # from a round that skipped its remaining probes are absence of
    # evidence, not evidence — a gate that returned 0 there would wave
    # through the very failure it was still measuring.
    round_complete=1
    for name in 0 1 2; do
      # Re-checked inside the round too (review): with every remote call
      # hanging, one full round costs ~105s of bounded timeouts, and a
      # deadline probed only between rounds would overrun its own claim by
      # most of a round. Past the deadline the round stops where it stands
      # — at most one bounded probe late.
      [ "$SECONDS" -lt "$gate_deadline" ] || { round_complete=0; break; }
      want="$(timeout 15 kubectl -n "$peer_ns" get pod "${peer_prefix}${name}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || true)"
      if [ -z "$want" ]; then
        unaddressed="$unaddressed ${peer_prefix}${name}"
        continue
      fi
      peer_fqdn="${peer_prefix}${name}.${peer_headless}.${peer_ns}.svc.${DOMAIN}"
      for probe in 0 1 2; do
        [ "$probe" = "$name" ] && continue
        [ "$SECONDS" -lt "$gate_deadline" ] || { round_complete=0; break; }
        answer="$(timeout 15 kubectl -n "$peer_ns" exec "${peer_prefix}${probe}" -- \
          sh -c "timeout 10 getent hosts ${peer_fqdn} || echo miss:\$?" 2>/dev/null || true)"
        case "$answer" in
          "")
            unprobed="$unprobed ${peer_prefix}${probe}->${peer_prefix}${name}"
            ;;
          miss:*)
            # miss:2 is name-not-found; miss:124 is the remote lookup's own
            # timeout — a hung resolver, still a DNS verdict.
            unresolved="$unresolved ${peer_prefix}${probe}->${peer_prefix}${name}(${answer})"
            ;;
          *)
            got="${answer%%[[:space:]]*}"
            [ "$got" = "$want" ] \
              || unresolved="$unresolved ${peer_prefix}${probe}->${peer_prefix}${name}(stale:${got}!=${want})"
            ;;
        esac
      done
    done
    [ "$round_complete" = 1 ] && [ -z "${unresolved}${unprobed}${unaddressed}" ] && return 0
    sleep 2
  done
  # Self-diagnosing on failure, per #324's rule: each list under its own
  # name, plus the two components that could be responsible, so a CI
  # recurrence carries its own evidence instead of a bare timeout. The dumps
  # ride the same bound as the probes — kubectl's own --request-timeout
  # defaults to never, and a diagnostic that hangs eats the attribution it
  # exists to print (review).
  timeout 15 kubectl -n "$peer_ns" get endpointslices 2>/dev/null || true
  timeout 15 kubectl -n kube-system get pods -l k8s-app=kube-dns 2>/dev/null || true
  # No failed lookup means NO DNS VERDICT (review): if everything that ran
  # resolved correctly, blaming DNS would accuse the one component that was
  # observed working. The bare-DNS message speaks only when a lookup failed.
  [ -n "$unresolved" ] || fail "peer DNS in ${peer_ns} could not be OBSERVED before the gate's \
deadline: every lookup that completed resolved correctly, so this is an API or scheduling \
verdict, not a DNS one (#416):\
${unaddressed:+ targets with no podIP:${unaddressed};}\
${unprobed:+ probes that never completed:${unprobed};}"
  fail "headless DNS never settled in ${peer_ns} within its deadline (#416):\
${unresolved:+ unresolved lookups:${unresolved};}\
${unaddressed:+ targets with no podIP — not scheduled/started, or the query failed:${unaddressed};}\
${unprobed:+ probes that never completed — the exec/API path, not a DNS verdict:${unprobed};}"
}

dump_pod_evidence() { # <namespace> <pod...> — what a timed-out wait saw (#416)
  local ns="$1"; shift
  echo "--- pods in $ns"
  timeout 20 kubectl -n "$ns" get pods -o wide 2>&1 | head -20 || true
  local pod
  for pod in "$@"; do
    echo "--- describe $pod"
    timeout 20 kubectl -n "$ns" describe pod "$pod" 2>&1 | tail -40 || true
    echo "--- last log lines of $pod"
    timeout 30 kubectl -n "$ns" logs "$pod" --tail=60 --request-timeout=20s --limit-bytes=100000 2>&1 || true
  done
  echo "--- kube-dns"
  timeout 15 kubectl -n kube-system get pods -l k8s-app=kube-dns -o wide 2>&1 || true
}

await_pod_running() { # <namespace> <pod> <seconds> [old-uid] — a NEW pod, Running, judged on the clock
  # Not the pod that was deleted (its uid) and not one on its way out (a
  # terminating pod still reports Running), on a WALL-CLOCK budget: the bound
  # of each query is what remains, and the pause between queries is taken
  # only when it fits (review). A query the API server did not answer is told
  # apart from a pod that is not there, so the timeout blames what it saw.
  local ns="$1" pod="$2" budget="$3" old_uid="${4:-}"
  local deadline=$((SECONDS + budget)) answered=0 remaining bound seen="" rc errf
  local uid="" phase="" deleting=""
  errf="$(mktemp)"
  while :; do
    remaining=$((deadline - SECONDS))
    [ "$remaining" -gt 0 ] || break
    bound=$(( remaining < 15 ? remaining : 15 ))
    seen="$(timeout "$bound" kubectl -n "$ns" get pod "$pod" \
      -o jsonpath='{.metadata.uid}{"|"}{.status.phase}{"|"}{.metadata.deletionTimestamp}' 2>"$errf")"; rc=$?
    if [ "$rc" -eq 0 ]; then
      answered=1
      IFS='|' read -r uid phase deleting <<< "$seen"
      if [ "$phase" = "Running" ] && [ -z "$deleting" ] && [ "$uid" != "$old_uid" ]; then
        rm -f "$errf"; return 0
      fi
    elif grep -qi 'not found' "$errf"; then
      answered=1; uid=""; phase="absent"; deleting=""
    fi
    remaining=$((deadline - SECONDS))
    [ "$remaining" -ge 3 ] || break
    sleep 3
  done
  rm -f "$errf"
  if [ "$answered" -eq 1 ]; then
    local why="phase '${phase:-absent}'"
    [ -n "$deleting" ] && why="$why, terminating"
    [ -n "$old_uid" ] && [ "$uid" = "$old_uid" ] && why="$why, still the deleted pod's uid"
    echo "pod $pod is not a new Running pod after ${budget}s: $why" >&2
  else
    echo "the API server did not answer one query about pod $pod in ${budget}s" >&2
  fi
  return 1
}

# ---------------------------------------------------------------------------
# Identities. The chart refuses to render without them by design (#81), so a
# smoke test has to mint a real CA and per-ordinal leaves with the CNs and SANs
# the binary enforces — meta leaves carry the decimal node id, data leaves the
# broker UUID, and every leaf needs the pod's headless FQDN as a SAN.
# ---------------------------------------------------------------------------
mkdir -p "$CERTS"
mkcert() { # cn san out
  openssl ecparam -name prime256v1 -genkey -noout -out "$CERTS/$3-key.pem" 2>/dev/null
  openssl req -new -key "$CERTS/$3-key.pem" -subj "/CN=$1" -out "$CERTS/$3.csr" 2>/dev/null
  printf 'basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:%s\n' "$2" > "$CERTS/$3.ext"
  openssl x509 -req -in "$CERTS/$3.csr" -CA "$CERTS/ca.pem" -CAkey "$CERTS/ca-key.pem" \
    -CAcreateserial -out "$CERTS/$3.pem" -days 1 -sha256 -extfile "$CERTS/$3.ext" 2>/dev/null
}

log "minting a CA and per-ordinal leaves"
openssl ecparam -name prime256v1 -genkey -noout -out "$CERTS/ca-key.pem" 2>/dev/null
openssl req -x509 -new -key "$CERTS/ca-key.pem" -sha256 -days 1 \
  -subj "/CN=vtop-smoke-ca" -out "$CERTS/ca.pem" 2>/dev/null

i=0
for uuid in "$UUID_0" "$UUID_1" "$UUID_2"; do
  fqdn="${REL}-${i}.${HEADLESS}.${NS}.svc.${DOMAIN}"
  mkcert "$((i+1))" "$fqdn" "meta-node-${i}"
  mkcert "$uuid" "$fqdn" "data-node-${i}"
  i=$((i+1))
done
mkcert "operator" "operator" "operator"
mkcert "$PRINCIPAL" "${REL}-0.${HEADLESS}.${NS}.svc.${DOMAIN}" "client"

# A CN that did not take would produce a cert the binary rejects at handshake
# time, which surfaces as an unhelpful TLS error much later.
openssl x509 -in "$CERTS/data-node-0.pem" -noout -subject | grep -q "$UUID_0" \
  || fail "data leaf CN is not the broker UUID; certificate minting is broken"

# ---------------------------------------------------------------------------
log "installing the chart into namespace $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

for plane in meta data; do
  kubectl -n "$NS" create secret generic "${REL}-${plane}-tls" \
    --from-file=ca.pem="$CERTS/ca.pem" \
    --from-file=node-0.pem="$CERTS/${plane}-node-0.pem" --from-file=node-0-key.pem="$CERTS/${plane}-node-0-key.pem" \
    --from-file=node-1.pem="$CERTS/${plane}-node-1.pem" --from-file=node-1-key.pem="$CERTS/${plane}-node-1-key.pem" \
    --from-file=node-2.pem="$CERTS/${plane}-node-2.pem" --from-file=node-2-key.pem="$CERTS/${plane}-node-2-key.pem" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

helm upgrade --install "$REL" helm/vtop -n "$NS" \
  -f helm/vtop/ci/default-values.yaml \
  --set "tls.metaSecretName=${REL}-meta-tls" \
  --set "tls.dataSecretName=${REL}-data-tls" \
  --set "image.repository=${IMAGE_REPO}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --timeout 5m >/dev/null

# Deliberately NOT `helm --wait`. Nodes resolve their peers at startup and
# exit if DNS has not caught up yet, so the first pods can restart once or
# twice before settling — see the note in the chart README. Waiting on
# readiness directly reports the state that matters instead of failing on a
# transient one.
log "waiting for all three pods to become Ready"
await_pods_exist "$NS" 3
kubectl -n "$NS" wait --for=condition=ready pod -l "app.kubernetes.io/instance=${REL}" \
  --timeout=180s >/dev/null || {
    kubectl -n "$NS" get pods
    kubectl -n "$NS" logs "${REL}-0" --tail=30 || true
    fail "pods never became Ready"
  }
log "all pods Ready"
log "gating on headless DNS: every pod must resolve every peer (#416)"
await_peer_dns "$NS"
log "peer DNS settled"

# ---------------------------------------------------------------------------
log "bootstrapping the metadata Raft group"
forward "${REL}-0" 19200 9200
cat > "$WORK/admin.yaml" <<EOF
endpoint: localhost:19200
server_name: ${REL}-0.${HEADLESS}.${NS}.svc.${DOMAIN}
ca_cert: $CERTS/ca.pem
client_cert: $CERTS/operator.pem
client_key: $CERTS/operator-key.pem
EOF

vtopctl meta init --members 1,2,3 --config "$WORK/admin.yaml" >/dev/null \
  || fail "meta init failed"

# Captured, never piped into `grep -q`. A short-circuiting grep closes the pipe
# while vtopctl is still writing, and it dies on SIGPIPE — which reports as a
# panic and hides whatever the actual status was.
#
# Retried because election takes a moment after init: asserting immediately
# tests the timing, not the bootstrap.
leader=""
for _ in $(seq 1 20); do
  status="$(vtopctl meta status --config "$WORK/admin.yaml" 2>/dev/null || true)"
  if printf '%s' "$status" | grep -q "server_state:.*Leader"; then leader="yes"; break; fi
  sleep 2
done
[ -n "$leader" ] || { printf '%s\n' "${status:-<no status>}"; fail "no Raft leader after bootstrap"; }
log "Raft group bootstrapped with a leader"

# ---------------------------------------------------------------------------
log "streaming records into ${REL}-0"
forward "${REL}-0" 19400 9400
cat > "$WORK/client.yaml" <<EOF
cluster_id: $CLUSTER_ID
principal_id: $PRINCIPAL
producer_id: $PRINCIPAL
producer_epoch: 1
fencing_epoch: 1
range:
  topic: telemetry
  topic_epoch: 1
  range_id: $RANGE_ID
  range_generation: 0
server_name: "${REL}-0.${HEADLESS}.${NS}.svc.${DOMAIN}"
tls: { ca: $CERTS/ca.pem, cert: $CERTS/client.pem, key: $CERTS/client-key.pem }
EOF

# A fresh producer epoch per round. Sequence state is keyed on
# (producer_id, producer_epoch), so replaying epoch 1 would be correctly
# DEDUPLICATED and the offset would not move — which looks like a stall and is
# actually idempotency working.
ROUNDS=6
PER_ROUND=50
for epoch in $(seq 1 "$ROUNDS"); do
  sed -i.bak "s/^producer_epoch: .*/producer_epoch: $epoch/" "$WORK/client.yaml"
  vtop-node produce --client-config "$WORK/client.yaml" \
    --addr "127.0.0.1:19400" --records "$PER_ROUND" --batch 10 \
    --durability local-fsync >/dev/null || fail "produce round $epoch failed"
  sleep 1
done
EXPECTED=$((ROUNDS * PER_ROUND))
log "streamed $EXPECTED records"

# ---------------------------------------------------------------------------
# An unreachable endpoint is an EMPTY answer here, never a dead script.
#
# `set -euo pipefail` is on, so a bare `curl -sf` that fails takes the whole
# pipeline down with it, and a caller that meant to RETRY never gets the chance —
# the script exits mid-loop with no message, which is the same silent abort the
# port allocator above documents. Callers compare the result to an expected
# value and fail with it in the message, so an empty string reports "the endpoint
# did not answer" exactly where it happened.
committed_offset() {
  { curl -sf "localhost:$1/metrics" || true; } \
    | awk '/^vtop_broker_local_committed_offset\{/ { print $NF }' | head -1
}

forward "${REL}-0" 19500 9500
got="$(committed_offset 19500)"
[ "$got" = "$EXPECTED" ] || fail "expected $EXPECTED committed records, metrics report '${got:-none}'"
log "committed offset is $got, as produced"

# Each pod is an INDEPENDENT standalone range under the chart's defaults, so a
# pod nobody produced to must still be empty. If this ever reports records, the
# deployment is not the shape the chart documents.
forward "${REL}-1" 19501 9500
other="$(curl -sf localhost:19501/metrics | awk '/^vtop_broker_next_offset\{/ { print $NF }' | head -1)"
[ "${other:-0}" = "0" ] || fail "pod 1 holds $other records; ranges are not independent as documented"
log "pod 1 is empty, confirming per-pod independent ranges"

# ---------------------------------------------------------------------------
# The durability claim, on Kubernetes. Force-deleted with no grace period —
# deliberately the SIGKILL path, NOT the orderly SIGTERM drain #280 added:
# durability must never depend on the clean path being taken.
log "force-deleting ${REL}-0 and checking the records survive"
killed_uid="$(kubectl -n "$NS" get pod "${REL}-0" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
kubectl -n "$NS" delete pod "${REL}-0" --grace-period=0 --force >/dev/null 2>&1
sleep 5
# The recreated pod's readiness depends on its peers resolving its new name
# and it resolving theirs (#416): gate DNS again once the pod is back, and
# only then judge readiness, on a fresh budget. A wait that times out says
# what it saw.
await_pod_running "$NS" "${REL}-0" 120 "$killed_uid" \
  || { dump_pod_evidence "$NS" "${REL}-0"; fail "the killed pod did not come back as a new Running pod within 120s"; }
log "gating on headless DNS again: the recreated pod must resolve its peers, and they it (#416)"
await_peer_dns "$NS"
kubectl -n "$NS" wait --for=condition=ready "pod/${REL}-0" --timeout=180s >/dev/null \
  || { dump_pod_evidence "$NS" "${REL}-0"; fail "pod did not come back Ready within 180s of DNS settling after being killed"; }

forward "${REL}-0" 19502 9500
survived="$(committed_offset 19502)"
[ "$survived" = "$EXPECTED" ] || fail "expected $EXPECTED records after the kill, got '${survived:-none}'"
log "all $survived records survived a hard kill"

# ---------------------------------------------------------------------------
# CROSS-NAMESPACE ISOLATION.
#
# Every peer address the chart renders carries the namespace, so two releases
# in different namespaces SHOULD be independent by construction. "Should" is
# what this exercise keeps disproving, so it is checked: a second release in
# its own namespace, produced to, while the first must not move.
#
# One replica, because the question is whether the namespaces are separated,
# not whether three pods work — that is already established above.
log "installing a neighbour release in $NEIGHBOUR_NS"
kubectl create namespace "$NEIGHBOUR_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

n_fqdn="${REL}-0.${HEADLESS}.${NEIGHBOUR_NS}.svc.${DOMAIN}"
mkcert "1" "$n_fqdn" "n-meta-node-0"
mkcert "$UUID_0" "$n_fqdn" "n-data-node-0"
mkcert "$PRINCIPAL" "$n_fqdn" "n-client"
for plane in meta data; do
  kubectl -n "$NEIGHBOUR_NS" create secret generic "${REL}-${plane}-tls" \
    --from-file=ca.pem="$CERTS/ca.pem" \
    --from-file=node-0.pem="$CERTS/n-${plane}-node-0.pem" \
    --from-file=node-0-key.pem="$CERTS/n-${plane}-node-0-key.pem" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

helm upgrade --install "$REL" helm/vtop -n "$NEIGHBOUR_NS" \
  -f helm/vtop/ci/default-values.yaml \
  --set replicaCount=1 \
  --set "data.nodeUuids={$UUID_0}" \
  --set "tls.metaSecretName=${REL}-meta-tls" \
  --set "tls.dataSecretName=${REL}-data-tls" \
  --set "image.repository=${IMAGE_REPO}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --timeout 5m >/dev/null

await_pods_exist "$NEIGHBOUR_NS" 1
kubectl -n "$NEIGHBOUR_NS" wait --for=condition=ready pod -l "app.kubernetes.io/instance=${REL}" \
  --timeout=180s >/dev/null || fail "the neighbour release never became Ready"

forward_ns "$NEIGHBOUR_NS" "${REL}-0" 19600 9400
sed "s|^server_name: .*|server_name: \"${n_fqdn}\"|; s|cert: $CERTS/client.pem|cert: $CERTS/n-client.pem|; s|key: $CERTS/client-key.pem|key: $CERTS/n-client-key.pem|; s/^producer_epoch: .*/producer_epoch: 1/" \
  "$WORK/client.yaml" > "$WORK/neighbour-client.yaml"

vtop-node produce --client-config "$WORK/neighbour-client.yaml" \
  --addr "127.0.0.1:19600" --records 20 --batch 10 --durability local-fsync >/dev/null \
  || fail "producing to the neighbour namespace failed"

forward_ns "$NEIGHBOUR_NS" "${REL}-0" 19601 9500
n_offset="$(committed_offset 19601)"
[ "$n_offset" = "20" ] || fail "neighbour should hold 20 records, reports '${n_offset:-none}'"

# And the original must be untouched. This is the assertion that matters: a
# shared peer name, a shared range id, or a Service selector that crossed
# namespaces would show up here as records appearing where nobody produced them.
forward "${REL}-0" 19602 9500
still="$(committed_offset 19602)"
[ "$still" = "$EXPECTED" ] || fail "producing to $NEIGHBOUR_NS changed $NS from $EXPECTED to $still; the namespaces are not isolated"
log "namespaces are isolated: neighbour holds $n_offset, $NS still holds $still"

# ---------------------------------------------------------------------------
# RECLAIM THE NODE before the last shape.
#
# Everything above has been asserted and has nothing left to prove, and it is
# holding four pods. The chart requests 500m CPU per pod, so seven pods want 3.5
# CPU — more than a two-vCPU CI runner has once system pods are counted. The
# third replicated pod then sits Pending forever, its FQDN never resolves, and
# the leader exits with "failed to lookup address information".
#
# That failure reads as DNS and is really scheduling, which is why it survived a
# retry budget being tripled: waiting cannot schedule a pod the node has no room
# for. Tearing down what is finished is both the fix and the honest shape for a
# sequential test — each topology is verified, then released.
log "releasing the standalone and neighbour namespaces before the replicated shape"
helm uninstall "$REL" -n "$NS" >/dev/null 2>&1 || true
helm uninstall "$REL" -n "$NEIGHBOUR_NS" >/dev/null 2>&1 || true
kubectl delete namespace "$NS" "$NEIGHBOUR_NS" --wait=true --timeout=180s >/dev/null 2>&1 || true
# Deleting the namespace returns before the kubelet has finished releasing the
# pods, and it is the RELEASE that frees the capacity. Wait for the node to
# actually have it back rather than assuming.
#
# A FAILED QUERY IS NOT ZERO PODS. `|| true` was the right shape for reading an
# offset — an empty answer there means "unknown", and the caller compares it and
# fails with it. Here the natural default is actively wrong: 0 means "all clear",
# so swallowing a kubectl failure would report reclaimed capacity that was never
# measured, and the run would walk straight into the Pending-pod failure this
# exists to prevent. Same technique, opposite consequence.
#
# So a query failure keeps `remaining` unknown and the loop retries, and running
# out of attempts FAILS with the count actually observed rather than announcing
# success on a deadline.
remaining="unknown"
for _ in $(seq 1 60); do
  if pods="$(kubectl get pods -A -l "app.kubernetes.io/instance=${REL}" --no-headers 2>/dev/null)"; then
    # `grep -c .` counts non-empty lines and exits 1 when there are none, which
    # is a count of zero and not an error.
    remaining="$(printf '%s' "$pods" | grep -c . || true)"
    [ "$remaining" = "0" ] && break
  else
    remaining="unknown"
  fi
  sleep 2
done
[ "$remaining" = "0" ] \
  || fail "capacity was not reclaimed after 120s: $remaining pod(s) still present (\"unknown\" means the cluster stopped answering). Installing the replicated shape now would leave a pod Pending on a node that has no room for it."
log "node capacity reclaimed"

# ---------------------------------------------------------------------------
# THE REPLICATED TOPOLOGY, run TWICE (#287 slice 3): first as the co-located
# shape (one StatefulSet, each pod a metadata voter AND a data candidate),
# then as the SEPARATED shape (a metadata tier and a data tier as two
# StatefulSets). The assertions are the same because the claims are the same;
# what differs is which pod carries which plane, and the naming helpers below
# are the only place that knows. Rendering the separated shape is not
# evidence it works — the lesson of #281, restated for #287.
# ---------------------------------------------------------------------------
meta_pod()  { printf '%s%s' "$META_POD" "$1"; }   # metadata voter <ordinal>
data_pod()  { printf '%s%s' "$DATA_POD" "$1"; }   # data candidate <ordinal>
meta_fqdn() { printf '%s.%s.%s.svc.%s' "$(meta_pod "$1")" "$META_HEADLESS" "$R_NS" "$DOMAIN"; }
data_fqdn() { printf '%s.%s.%s.svc.%s' "$(data_pod "$1")" "$DATA_HEADLESS" "$R_NS" "$DOMAIN"; }
shape_pods() { # every pod of the shape, one per line
  local o
  for o in 0 1 2; do meta_pod "$o"; echo; done
  if [ "$META_POD" != "$DATA_POD" ]; then
    for o in 0 1 2; do data_pod "$o"; echo; done
  fi
}
await_shape_dns() { # each tier's peers must resolve each other (#416)
  await_peer_dns "$R_NS" "$DATA_POD" "$DATA_HEADLESS"
  if [ "$META_POD" != "$DATA_POD" ]; then
    await_peer_dns "$R_NS" "$META_POD" "$META_HEADLESS"
  fi
}
bounded_logs() { # <pod> <tail> [kubectl logs flags...] — one bounded fetch (review)
  # Bounded on every axis a hung API path can stretch: the client wait, the
  # server-side request, and the response size — so a failure report is
  # reached rather than waited for.
  local pod="$1" tail="$2"; shift 2
  timeout 30 kubectl -n "$R_NS" logs "$pod" --tail="$tail" --request-timeout=20s \
    --limit-bytes=200000 "$@" 2>&1 || true
}
await_capacity_reclaimed() { # <what comes next> — the same wait the standalone release does
  local remaining="unknown" pods
  for _ in $(seq 1 60); do
    if pods="$(kubectl get pods -A -l "app.kubernetes.io/instance=${REL}" --no-headers 2>/dev/null)"; then
      remaining="$(printf '%s' "$pods" | grep -c . || true)"
      [ "$remaining" = "0" ] && break
    else
      remaining="unknown"
    fi
    sleep 2
  done
  [ "$remaining" = "0" ] \
    || fail "capacity was not reclaimed after 120s: $remaining pod(s) still present (\"unknown\" means the cluster stopped answering). Installing $1 now would leave a pod Pending on a node that has no room for it."
  log "node capacity reclaimed"
}

run_replicated_shape() {
# ---------------------------------------------------------------------------
# THE REPLICATED TOPOLOGY.
#
# Everything above runs the STANDALONE default, where three replicas are three
# separate logs. That shape cannot exercise replication, fencing or promotion —
# it is why `--durability quorum` is refused there and why pod 1 stays empty.
#
# So the entire #240 epoch arc was reachable only from the live-chaos harness,
# and #286 gave the chart a `replicated` topology that nothing ever deployed.
# It rendered. Rendering is not evidence; that is the lesson #281 taught, where
# a chart rendered perfectly and every pod CrashLoopBackOff'd because the image
# had no engine in it.
#
# The assertions here are chosen to be the INVERSE of the standalone ones
# above, because that is what proves the topology took effect rather than
# merely being accepted: quorum durability succeeds where it was refused, and
# a follower nobody produced to holds the records instead of staying empty.
log "installing the REPLICATED topology (${SHAPE} shape, ${R_VALUES}) into $R_NS"
kubectl create namespace "$R_NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# Per-tier leaves: in the co-located shape a pod carries both, so the two
# FQDNs coincide; in the separated shape each tier's pod has its own.
i=0
for uuid in "$UUID_0" "$UUID_1" "$UUID_2"; do
  mkcert "$((i+1))" "$(meta_fqdn "$i")" "r-meta-node-${i}"
  mkcert "$uuid" "$(data_fqdn "$i")" "r-data-node-${i}"
  i=$((i+1))
done
mkcert "operator" "operator" "r-operator"
mkcert "$PRINCIPAL" "$(data_fqdn 0)" "r-client"

for plane in meta data; do
  kubectl -n "$R_NS" create secret generic "${REL}-${plane}-tls" \
    --from-file=ca.pem="$CERTS/ca.pem" \
    --from-file=node-0.pem="$CERTS/r-${plane}-node-0.pem" --from-file=node-0-key.pem="$CERTS/r-${plane}-node-0-key.pem" \
    --from-file=node-1.pem="$CERTS/r-${plane}-node-1.pem" --from-file=node-1-key.pem="$CERTS/r-${plane}-node-1-key.pem" \
    --from-file=node-2.pem="$CERTS/r-${plane}-node-2.pem" --from-file=node-2-key.pem="$CERTS/r-${plane}-node-2-key.pem" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
done

helm upgrade --install "$REL" helm/vtop -n "$R_NS" \
  -f "helm/vtop/ci/${R_VALUES}" \
  --set "tls.metaSecretName=${REL}-meta-tls" \
  --set "tls.dataSecretName=${REL}-data-tls" \
  --set "image.repository=${IMAGE_REPO}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --timeout 5m >/dev/null

# Bootstrap FIRST, wait for readiness AFTER — the order is the design (#284).
# Candidates learn everything from the metadata plane: /readyz stays closed
# until a pod has completed a metadata exchange that FINDS the range, and the
# range cannot exist until `meta init` and `create-topic` have run. Waiting
# for Ready here would deadlock against the very steps that open the gate. A
# winning candidate can also exit fail-stop if its peers' DNS has not caught
# up when it builds its replica set, so a cold install may still restart a
# few times while the headless Service settles — deliberate, not a symptom.
#
# RUNNING, not Ready, and the distinction is the whole point. The standalone
# install above can wait for Ready because a standalone range needs no lease
# and no metadata at all.
log "waiting for pod 0's metadata process to be up, then bootstrapping the group"
for _ in $(seq 1 60); do
  phase="$(kubectl -n "$R_NS" get "pod/$(meta_pod 0)" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
  [ "$phase" = "Running" ] && break
  sleep 3
done
[ "${phase:-}" = "Running" ] \
  || { kubectl -n "$R_NS" get pods; fail "metadata pod 0 ($(meta_pod 0)) never reached Running (phase='${phase:-none}')"; }

# The forward is re-established on EVERY attempt, which the standalone path
# does not need to do. Two things are moving here that are settled there: the
# admin listener binds a moment after the process starts (a single early
# attempt fails as `tls handshake eof`, which reads like a certificate problem
# and is not one), and a cold replicated install restarts a few times while
# follower DNS settles — every restart kills the port-forward for good, so a
# retry through a dead forward just repeats `connection refused` until it gives
# up. A fresh local port per attempt avoids racing the previous one's teardown.
# ONE monotonic allocator for every port in this section. The init retry loop
# below allocates dynamically, and the produce / metrics checks after it used to
# use FIXED ports that fell inside the range the loop had already walked. When
# the loop happened to land on one of them, the later `port-forward` failed to
# bind and the request went to the surviving init forward — pod-0:9200 instead of
# :9400 — which reads as a confusing produce failure or, worse, a false pass.
# Nothing here may pick a port by hand.
#
# Sets `r_port`; it does NOT echo it. A `$(next_port)` command substitution runs
# in a SUBSHELL, so the increment inside it is discarded and every call returns
# the same number — which reintroduced the very collision this allocator exists
# to prevent, and did it silently: two forwards on one port means `curl -sf` hits
# the wrong listener, fails, and `set -e` aborts the script with no message at
# all. Caught by running the test rather than by reading it.
r_port="$R_PORT_BASE"
alloc_port() {
  r_port=$((r_port + 1))
}

# 90 attempts, not 30. Each costs the 4s forward settle plus the attempt, so
# this is roughly six minutes of patience against roughly two before.
#
# The budget is not arbitrary and the earlier one was tuned to the wrong
# cluster. A cold replicated install can restart while peer DNS settles — a
# winning candidate exits fail-stop with "failed to lookup address
# information" until the headless Service publishes every peer, which the
# chart documents as expected. On
# Docker Desktop that resolves in well under a minute, so 30 attempts looked
# generous; on kind it does not, and CI failed here with `Connection refused`
# while the pod was still in that loop. Waiting for `phase=Running` cannot help:
# a crash-looping pod reports Running almost immediately and keeps restarting,
# killing each forward as it goes.
#
# So the loop is the wait. It re-establishes the forward every attempt and keeps
# going until the process stays up long enough to answer, which is the actual
# condition being waited for.
r_init=""
for _ in $(seq 1 90); do
  r_port=$((r_port + 1))
  cat > "$WORK/r-admin.yaml" <<EOF
endpoint: localhost:${r_port}
server_name: $(meta_fqdn 0)
ca_cert: $CERTS/ca.pem
client_cert: $CERTS/r-operator.pem
client_key: $CERTS/r-operator-key.pem
EOF
  forward_ns "$R_NS" "$(meta_pod 0)" "$r_port" 9200
  if vtopctl meta init --members 1,2,3 --config "$WORK/r-admin.yaml" >/dev/null 2>&1; then
    r_init="yes"; break
  fi
done
[ -n "$r_init" ] || {
  vtopctl meta init --members 1,2,3 --config "$WORK/r-admin.yaml" || true
  kubectl -n "$R_NS" get pods || true
  bounded_logs "$(meta_pod 0)" 20
  fail "meta init never succeeded in the replicated namespace"
}

# A forward per metadata pod: needed to find WHICH one leads, and then to reach
# the leader after a redirect. Allocated before the probe below because that
# probe queries every pod by port.
peer_ports=()
for ordinal in 0 1 2; do
  alloc_port
  peer_ports+=("$r_port")
  forward_ns "$R_NS" "$(meta_pod "$ordinal")" "$r_port" 9200
done

# Find WHICH pod leads, and then deliberately aim everything at one that does
# NOT.
#
# Waiting for pod 0 to report Leader and then sending every command to pod 0 was
# a test that could not fail: the first hop always reached the leader, so
# removing redirect support entirely would still have passed, and a run that
# elected a different leader would have failed before reaching the assertion.
# The redirect is the thing under test, so the primary endpoint has to be a
# follower.
leader_ordinal=""
for _ in $(seq 1 30); do
  for ordinal in 0 1 2; do
    cat > "$WORK/probe-${ordinal}.yaml" <<EOF
endpoint: localhost:${peer_ports[$ordinal]}
server_name: $(meta_fqdn "$ordinal")
ca_cert: $CERTS/ca.pem
client_cert: $CERTS/r-operator.pem
client_key: $CERTS/r-operator-key.pem
EOF
    status="$(vtopctl meta status --config "$WORK/probe-${ordinal}.yaml" 2>/dev/null || true)"
    if printf '%s' "$status" | grep -q "server_state:.*Leader"; then
      leader_ordinal="$ordinal"
      break
    fi
  done
  [ -n "$leader_ordinal" ] && break
  sleep 2
done
[ -n "$leader_ordinal" ] || fail "no Raft leader in the replicated namespace"

# Any ordinal that is not the leader. With three nodes there is always one.
follower_ordinal=$(( (leader_ordinal + 1) % 3 ))
log "metadata leader is pod $leader_ordinal; aiming every admin write at pod $follower_ordinal so the redirect is exercised"

# THE RANGE MUST EXIST IN METADATA BEFORE ANYONE CAN HOLD A LEASE ON IT.
#
# `meta init` only bootstraps the Raft group. A lease-driven leader asks
# metadata for a lease on a specific range, and a range metadata has never
# heard of cannot be leased — so the leader never becomes ready and reports
# `not ready: metadata lease released; range is fenced at epoch 1`, which names
# the symptom and not the cause. The live-chaos harness does these two steps
# explicitly (scenarios/09-range-leader-failover.sh) and says why; a chart
# install needs exactly the same two, which is what makes this a documented
# post-install step rather than something the chart can do for you.
#
# `topic_uuid` is metadata's identity for the topic and must match the
# `data.lease.topicUuid` the pods were rendered with; `root-range-uuid` is
# `data.range.rangeId`. Getting either wrong produces a lease request for a
# range nobody created, which fails the same indistinguishable way.
#
# Tried against every pod rather than assuming pod 0. These are writes, so they
# must reach the RAFT leader, and which pod that is depends on an election —
# a non-leader answers "has to forward request to", so a run that happened to
# elect node 2 would fail for a reason that reads like a configuration error.
# ONE endpoint, with every peer listed so a redirect can be followed.
#
# This used to try all three pods in turn, because `vtopctl` built a
# single-endpoint client: a write must reach the RAFT LEADER, a non-leader
# refuses, and with nowhere to go the command failed roughly two times in three
# depending on which node won the election. That workaround is gone now the CLI
# follows the redirect itself. The commands below are aimed at a pod that is NOT
# the leader, and the assertion after them is that a redirect was actually
# observed — not merely that the writes succeeded, which they would have done
# anyway if the endpoint happened to lead.
{
  cat <<EOF
endpoint: localhost:${peer_ports[$follower_ordinal]}
server_name: $(meta_fqdn "$follower_ordinal")
ca_cert: $CERTS/ca.pem
client_cert: $CERTS/r-operator.pem
client_key: $CERTS/r-operator-key.pem
peers:
EOF
  for ordinal in 0 1 2; do
    cat <<EOF
  - node_id: $((ordinal + 1))
    endpoint: localhost:${peer_ports[$ordinal]}
    server_name: $(meta_fqdn "$ordinal")
EOF
  done
} > "$WORK/r-admin-multi.yaml"

# OBSERVES the redirect rather than inferring it from a role snapshot.
#
# Picking a follower up front is necessary but not sufficient: leadership can
# move between the probe and the request, and then the "follower" is the leader,
# the command succeeds on its first hop, and the test passes without exercising
# anything. `vtopctl` reports the redirects it actually followed, so this checks
# what happened instead of what was arranged.
redirects_seen=0
meta_admin() { # description -- args...
  what="$1"; shift
  # stderr carries the note; stdout is the command's own output.
  if ! note="$(vtopctl "$@" --config "$WORK/r-admin-multi.yaml" 2>&1 >/dev/null)"; then
    printf '%s\n' "$note"
    fail "$what failed even though every metadata peer was listed and reachable"
  fi
  if printf '%s' "$note" | grep -q "followed .* leader redirect"; then
    redirects_seen=$((redirects_seen + 1))
  fi
  log "$what"
}

meta_admin "created the topic and its root range in metadata" \
  meta create-topic --name telemetry \
  --topic-uuid "$TOPIC_UUID" --root-range-uuid "$RANGE_ID"

# Each node registered at its OWN address: with candidates (#284) any of the
# three may end up holding the range, and the registration should say where
# that node actually lives rather than pointing every identity at pod 0.
ordinal=0
for uuid in "$UUID_0" "$UUID_1" "$UUID_2"; do
  meta_admin "registered data node $uuid" \
    meta register-node --node-uuid "$uuid" --addr "$(data_fqdn "$ordinal"):9300"
  ordinal=$((ordinal + 1))
done

# THE ASSERTION. Without it this suite would pass against a `vtopctl` that
# cannot follow a redirect at all, provided the endpoint happened to be the
# leader — which is how the first version of this test was wrong.
[ "$redirects_seen" -gt 0 ] \
  || fail "no admin command followed a leader redirect, so redirect support was never exercised: \
either the endpoint was the leader after all (leadership moved between the probe and the request) \
or vtopctl is not following redirects"
log "observed $redirects_seen leader redirect(s): admin writes reached the leader from a follower endpoint"

log "waiting for the whole replicated range to become Ready"
await_pods_exist "$R_NS" "$R_PODS"
kubectl -n "$R_NS" wait --for=condition=ready pod -l "app.kubernetes.io/instance=${REL}" \
  --timeout=240s >/dev/null || {
    kubectl -n "$R_NS" get pods
    for pod in $(shape_pods); do echo "--- $pod ---"; bounded_logs "$pod" 30; done
    fail "the replicated range never became Ready"
  }
log "gating on the replicated range's headless DNS (#416)"
await_shape_dns
log "replicated peer DNS settled"

# WHICH POD HOLDS THE RANGE IS AN ELECTION'S OUTCOME, NOT A RENDERED FACT.
# The chart used to freeze the leader at ordinal 0, so this test could aim
# everything at pod 0 and be right by construction. Candidates (#284) take
# the range through the metadata lease, so the holder must be ASKED for —
# aiming at pod 0 now would fail two runs in three, looking like a produce
# bug and actually being an assumption the topology no longer honours.
lease_state() { # [per-attempt-bound] — echoes "<holder> <epoch>", empty when no lease
  # Bounded per attempt (#411, the meta_admin_read precedent in
  # live-chaos lib.sh): this runs inside deadline-checked retry loops whose
  # only clock is between iterations, so one vtopctl read hanging on a
  # wedged forward would spend the whole produce window in a single call
  # and the loop's deadline could never fire. A caller near its own
  # deadline passes the smaller bound it can still afford (review).
  local bound="${1:-10}"
  timeout --kill-after=1 "$bound" \
    vtopctl --json meta range-lease --config "$WORK/r-admin-multi.yaml" \
    --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_ID" 2>/dev/null \
    | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
lease = d.get("lease") or {}
holder = lease.get("holder_node_uuid") or ""
epoch = lease.get("fencing_epoch")
if holder and epoch is not None:
    print(holder, epoch)
' 2>/dev/null || true
}

ordinal_of() { # <node-uuid> -> pod ordinal
  case "$1" in
    "$UUID_0") echo 0 ;;
    "$UUID_1") echo 1 ;;
    "$UUID_2") echo 2 ;;
    *) fail "lease holder $1 is not one of the rendered candidates" ;;
  esac
}

await_holder() { # [min-epoch] — echoes "<holder> <epoch>"
  # Deadline-polled, and the epoch floor is the whole point: after a holder
  # goes away its unexpired lease is still on record — correctly, that is
  # what a lease means — so waiting for "any holder" right after a delete
  # reads back the corpse and proves nothing (the live-chaos suite learned
  # this the hard way).
  #
  # A FLOOR ON THE EPOCH, not an exclusion on the UUID, because the thing
  # being waited for is a NEW GRANT and the epoch is what a grant mints.
  # Metadata mints them monotonically, so "epoch above the one we saw" is
  # exactly "somebody has been granted the range since" — and it stays true
  # whether a survivor took over or the recreated pod won its own range back
  # (a legitimate outcome an exclusion would wrongly reject, and one this
  # test must not turn into a flake).
  # Wall-clock bounded, not iteration-counted (review): lease_state's own
  # per-attempt timeout means an iteration can cost twelve seconds against a
  # wedged forward, and ninety of those is eighteen minutes wearing a
  # 180-second label. The deadline is the claim; the loop merely fills it —
  # and the FINAL probe is bounded by what remains of the claim (review,
  # round two), so a probe started just inside the deadline cannot stretch
  # the whole wait a further probe-plus-sleep past it.
  local floor="${1:-0}" holder="" epoch="" left holder_deadline=$((SECONDS + 180))
  while [ "$SECONDS" -lt "$holder_deadline" ]; do
    left=$((holder_deadline - SECONDS))
    # `timeout 0` means UNBOUNDED (review): a tick landing between the
    # loop condition and this arithmetic makes `left` zero and would turn
    # the tightest probe into the only unlimited one.
    [ "$left" -ge 1 ] || break
    [ "$left" -gt 10 ] && left=10
    read -r holder epoch <<< "$(lease_state "$left")" || true
    if [ -n "$holder" ] && [ -n "$epoch" ] && [ "$epoch" -gt "$floor" ]; then
      printf '%s %s\n' "$holder" "$epoch"
      return 0
    fi
    # The sleep only runs when a whole interval fits (review): a probe
    # returning with a second to spare must not buy two more.
    [ $((holder_deadline - SECONDS)) -ge 2 ] || break
    sleep 2
  done
  return 1
}

# CAPTURED, THEN TESTED. `read ... <<< "$(cmd)"` does NOT propagate cmd's
# exit status, and a here-string appends a newline — so on a failed
# `await_holder` the read sees an empty LINE, returns 0, and `|| fail` never
# fires. The variables are then empty and the failure surfaces later as
# "lease holder  is not one of the rendered candidates", blaming an
# unrecognised candidate for a UUID that was never there. Observed on a real
# CI run before this was written.
held="$(await_holder)" || held=""
[ -n "$held" ] || fail "no candidate acquired the range within 180s"
read -r HOLDER EPOCH <<< "$held"
holder_ordinal="$(ordinal_of "$HOLDER")"
log "candidate $holder_ordinal ($HOLDER) holds the range at epoch $EPOCH — an election decided that, not the chart"

# Point the produce client at the pod that actually leads, at the epoch the
# lease actually granted. producer_epoch is bumped per produce round for the
# same reason the standalone phase bumps it: sequence state is keyed on
# (producer_id, producer_epoch), and replaying an epoch is correctly
# deduplicated — a stall that is actually idempotency working.
r_produce() { # <ordinal> <fencing-epoch> <producer-epoch> <records>
  local ordinal="$1" fencing="$2" producer="$3" records="$4"
  alloc_port; local pport="$r_port"
  # A FORWARD PER ATTEMPT, torn down when the attempt ends (review). Fresh
  # because a pod that restarted takes its forward with it and a reused one
  # only repeats `connection refused` — the same reasoning the init retry
  # loop above records. Torn down because this call sits in a retry loop:
  # sixty abandoned port-forwards would be sixty kubectl processes and sixty
  # API streams held open for the length of the wait.
  forward_ns "$R_NS" "$(data_pod "$ordinal")" "$pport" 9400
  local fwd="${FORWARDS##* }"
  local fqdn
  fqdn="$(data_fqdn "$ordinal")"
  sed "s|^server_name: .*|server_name: \"${fqdn}\"|; s|cert: $CERTS/client.pem|cert: $CERTS/r-client.pem|; s|key: $CERTS/client-key.pem|key: $CERTS/r-client-key.pem|; s/^producer_epoch: .*/producer_epoch: ${producer}/; s/^fencing_epoch: .*/fencing_epoch: ${fencing}/" \
    "$WORK/client.yaml" > "$WORK/r-client.yaml"
  # BOUNDED, so the retry deadline outside means what it says: the client
  # waits on a socket, and a forward that accepts and then stops answering
  # would hang this attempt forever while the loop below never got another
  # chance to check the clock (review).
  local rc=0
  timeout 60 vtop-node produce --client-config "$WORK/r-client.yaml" \
    --addr "127.0.0.1:${pport}" --records "$records" --batch 10 \
    --durability quorum > "$WORK/r-produce.log" 2>&1 || rc=$?
  kill "$fwd" 2>/dev/null || true
  return "$rc"
}

# THE ASSERTION THIS VARIANT EXISTS FOR. Quorum durability is refused
# outright on a standalone range — "Quorum durability requires a configured
# replica set" — so a produce that SUCCEEDS with it is proof the holder
# really has followers and really reached a majority of them. Nothing else
# in CI establishes that.
R_EXPECTED=60
log "producing with QUORUM durability, which a standalone range refuses"
r_produce "$holder_ordinal" "$EPOCH" 1 "$R_EXPECTED" \
  || fail "quorum produce failed; the range is not actually replicated"
log "quorum produce accepted $R_EXPECTED records on candidate $holder_ordinal"

# Every pod converges on the same offset — the holder because it acked, the
# other two because quorum only proves a majority: a perfectly healthy write
# returns while the third replica is still catching up, so this is POLLED
# with a deadline rather than asserted once (an immediate assertion tests
# the timing, not the replication, and flakes).
#
# A BOUNDED band plus convergence, not exact equality (#240): every promotion
# appends one boundary marker of its epoch to the log — a record the consumer
# never sees but every replica durably holds — and how many elections a run
# holds is not deterministic. What IS known is the current lease epoch: epochs
# are minted from 1 and a marker exists only for an epoch that was promoted,
# so the surplus over the records produced is at most that epoch (review: a
# floor alone would admit a deterministic leader-side duplication that every
# replica converged on). So the offset lies in [records, records + epoch],
# and every replica reports the SAME offset: the band says nothing was lost
# and nothing beyond the markers was invented, the agreement says
# replication reached every replica.
await_all_committed() { # <expected-records> <lease-epoch>
  local expected="$1" epoch="$2" ordinal offset
  local ceiling=$((expected + epoch))
  local -a mports=() offsets=()
  for ordinal in 0 1 2; do
    alloc_port; mports[ordinal]="$r_port"
    forward_ns "$R_NS" "$(data_pod "$ordinal")" "${mports[ordinal]}" 9500
  done
  local settled=0
  for _ in $(seq 1 45); do
    settled=1
    for ordinal in 0 1 2; do
      offset="$(committed_offset "${mports[ordinal]}")"
      offsets[ordinal]="${offset:-0}"
      [ "${offsets[ordinal]}" -ge "$expected" ] || settled=0
      [ "${offsets[ordinal]}" -le "$ceiling" ] || settled=0
    done
    if [ "$settled" = 1 ]; then
      [ "${offsets[0]}" = "${offsets[1]}" ] && [ "${offsets[1]}" = "${offsets[2]}" ] || settled=0
    fi
    [ "$settled" = 1 ] && break
    sleep 2
  done
  for ordinal in 0 1 2; do
    [ "${offsets[ordinal]}" -ge "$expected" ] \
      || fail "pod $ordinal has durably applied '${offsets[ordinal]}' of at least $expected records after 90s; replication is not reaching it"
    [ "${offsets[ordinal]}" -le "$ceiling" ] \
      || fail "pod $ordinal reports offset ${offsets[ordinal]}, above the $expected records plus at most $epoch boundary marker(s) the lease epoch permits: something was applied that nobody produced"
  done
  [ "$settled" = 1 ] \
    || fail "the replicas never converged after 90s: offsets ${offsets[0]}/${offsets[1]}/${offsets[2]} (expected $expected records plus at most $epoch boundary marker(s))"
  log "every pod has durably applied all $expected records (offset ${offsets[0]}, within the $epoch marker(s) the lease epoch permits) and the three agree"
}
await_all_committed "$R_EXPECTED" "$EPOCH"
log "replication verified in Kubernetes: quorum durability works and every replica holds the data"

# THE FAILOVER THE TOPOLOGY EXISTS FOR (#284). Delete the holder's pod —
# gracefully, so the #280 drain runs and the lease is RELEASED rather than
# left to lapse — and the range must move (or provably come back) without
# anyone re-rendering anything. Which surviving candidate wins is an
# election's outcome; whichever it is, produce must resume against it and
# every previously acknowledged record must still be there.
log "deleting the holder's pod (candidate $holder_ordinal) to force a failover"
holder_uid="$(kubectl -n "$R_NS" get pod "$(data_pod "$holder_ordinal")" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
kubectl -n "$R_NS" delete pod "$(data_pod "$holder_ordinal")" >/dev/null

# The recreated pod races the survivors for the vacated lease, and it CAN
# legitimately win — StatefulSets restart fast, and a recovered holder
# resuming its range is #284 working, not a test failure. The assertion is
# that the range is HELD and SERVING again, not who holds it; scenario 14 in
# the live-chaos suite already proves the takeover-by-a-survivor path with a
# kill no orchestrator softens.
# Same capture-then-test as above: the `||` on a read fed by a here-string
# guards nothing.
moved="$(await_holder "$EPOCH")" || moved=""
[ -n "$moved" ] || {
  # evidence for a grant that never came (#416): the lease as metadata
  # shows it, every data and metadata pod's last lines, and kube-dns — the
  # election is not gated on DNS, because the deleted holder's name cannot
  # resolve until it is back and the survivors must elect without it.
  echo "--- lease view (holder epoch): $(lease_state 20 2>/dev/null || true)"
  pods=()
  for o in 0 1 2; do pods+=("$(data_pod "$o")"); done
  for o in 0 1 2; do pods+=("$(meta_pod "$o")"); done
  dump_pod_evidence "$R_NS" "${pods[@]}"
  fail "no grant above epoch $EPOCH within 180s of deleting the holder: the range \
did not move, so either the lease never came free or no candidate could take it"
}
read -r NEW_HOLDER NEW_EPOCH <<< "$moved"
new_ordinal="$(ordinal_of "$NEW_HOLDER")"
if [ "$NEW_HOLDER" = "$HOLDER" ]; then
  log "the recreated pod won its own range back at epoch $NEW_EPOCH, above the pre-delete $EPOCH (a legitimate outcome of the race — the grant is new either way)"
else
  log "candidate $new_ordinal ($NEW_HOLDER) took the range at epoch $NEW_EPOCH — in place, no re-render, no upgrade"
fi

R_TOTAL=$((R_EXPECTED + 30))

# PRODUCE FIRST, while the deleted pod is still coming back: with three
# members a quorum is the holder plus one, so the range must keep serving
# through the outage rather than waiting for the full set. Proving that is a
# stronger claim than producing into a healed cluster, and it is the claim
# operators actually care about.
#
# DEADLINE-POLLED, never one-shot. A freshly granted holder has to establish
# its replication streams before any quorum write can land, so the first
# attempts legitimately fail — this is the same retry-until-deadline shape
# live-chaos scenario 09 and 14 use after every promotion, and the reason is
# recorded there too. A single attempt tests the timing, not the failover
# (which is exactly how this step first failed in CI).

# Ready, un-terminating members. Echoes the count; FAILS when the API server
# did not answer, so a caller can tell "nobody is Ready" from "nobody was
# asked" — the same attribution rule `await_pods_exist` states above.
ready_members() { # [label selector] — Ready, un-terminating pods; the release's by default
  local pods selector="${1:-app.kubernetes.io/instance=${REL}}"
  pods="$(kubectl -n "$R_NS" get pods -l "$selector" \
    -o jsonpath='{range .items[*]}{.metadata.deletionTimestamp}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' \
    2>/dev/null)" || return 1
  printf '%s' "$pods" | grep -c '^|True$' || true
}

log "producing 30 more records against the post-failover holder, retrying until it lands"
produce_deadline=$((SECONDS + 180))
while :; do
  # SAMPLED PER ATTEMPT, immediately before the produce it describes (review).
  # The claim worth making is "the range served during the outage", and whether
  # the replacement was still absent when the write landed is a race this test
  # does not control — a fast StatefulSet restart is not a failure. So the
  # member count is captured and NARRATED rather than asserted. Sampling it
  # ONCE before the loop measured the wrong attempt: the retry deadline is
  # 180s, long enough for the replacement to become Ready between the reading
  # and the write, which would have reported a bare quorum that no write ever
  # ran on. What survives the loop is the reading taken at the start of the
  # attempt that actually landed.
  # The DATA tier's members (review): in the separated shape the metadata
  # voters are pods too, and a count that included them would report a
  # six-pod figure as the state of a three-replica quorum.
  members_at_write="$(ready_members "$DATA_SELECTOR" || true)"
  r_produce "$new_ordinal" "$NEW_EPOCH" 2 30 && break
  [ "$SECONDS" -lt "$produce_deadline" ] || {
    kubectl -n "$R_NS" get pods || true
    # THE ENGINE'S OWN ACCOUNT, not just the client's error. This failure has
    # now been diagnosed twice from the outside and misattributed twice, both
    # times because the only evidence was `tls handshake eof` — which says the
    # holder is not serving and nothing about why. The decisions that matter
    # are the engine's: whether it refused promotion, whether its quorum probe
    # answered, whether it stood aside, whether it restarted. Dumping them here
    # costs nothing on a passing run and is the difference between a diagnosis
    # and a guess on a failing one.
    for o in 0 1 2; do
      echo "--- $(data_pod "$o") (last 120 lines) ---"
      # ONE bounded fetch per container, filtered from the copy (review):
      # an unbounded `kubectl logs` against a wedged API path is how a
      # failure report never arrives.
      view="$(bounded_logs "$(data_pod "$o")" 120)"
      printf '%s\n' "$view" \
        | grep -iE 'promot|quorum|lease|epoch|fenc|stand|slot|listener|panic|error' \
        || printf '%s\n' "$view" | tail -40
      echo "--- $(data_pod "$o") previous container, if it restarted ---"
      bounded_logs "$(data_pod "$o")" 60 --previous | tail -20
    done
    echo "--- lease as metadata sees it ---"
    lease_state || true
    fail "produce never resumed against candidate $new_ordinal within 180s of the \
failover, last aimed at epoch $NEW_EPOCH: $(tail -3 "$WORK/r-produce.log" 2>/dev/null)"
  }
  # RE-READ THE HOLDER AFTER A FAILURE (review). The first grant above the
  # pre-delete epoch is not necessarily the last: a candidate can win the
  # race and then fail to stand its leader up — resolving or connecting to
  # the peer that was just deleted — and lapse the lease to somebody else at
  # a higher epoch. Pinning the target for all 180 seconds would then spend
  # the whole budget producing at an epoch metadata has moved past, and fail
  # a failover that actually succeeded. Retarget only when the epoch
  # ADVANCES: monotonic minting makes that the one unambiguous signal that
  # this is a different grant and not a stale read.
  if read -r seen_holder seen_epoch <<< "$(lease_state)" \
    && [ -n "${seen_epoch:-}" ] && [ "$seen_epoch" -gt "$NEW_EPOCH" ]; then
    NEW_HOLDER="$seen_holder"
    NEW_EPOCH="$seen_epoch"
    new_ordinal="$(ordinal_of "$NEW_HOLDER")"
    log "the range moved again while retrying: candidate $new_ordinal now holds it at \
epoch $NEW_EPOCH — retargeting"
  fi
  sleep 3
done
if [ "${members_at_write:-x}" -ge 1 ] 2>/dev/null && [ "$members_at_write" -lt "$R_DATA_PODS" ]; then
  log "produce resumed at epoch $NEW_EPOCH; the attempt that landed began with only \
${members_at_write} of $R_DATA_PODS data replicas Ready — the range served through the outage on a bare quorum"
elif [ "${members_at_write:-x}" -eq "$R_DATA_PODS" ] 2>/dev/null; then
  log "produce resumed at epoch $NEW_EPOCH; the replacement was already back when the \
winning attempt began, so this run did not exercise the degraded-quorum path (a fast restart, \
not a failure)"
else
  # Zero Ready members cannot be true of a cluster that just accepted a
  # quorum write, so this is the readiness QUERY having failed. Say that,
  # rather than reporting a member count nobody measured.
  log "produce resumed at epoch $NEW_EPOCH; how many pods were Ready at the time is \
unknown (the readiness query did not answer)"
fi

# NOW the returning pod must rejoin, because the convergence assertion below
# is about it: with only two pods up, "a majority acked" and "everyone acked"
# are the same claim and checking all three would prove nothing extra.
#
# Polled rather than `kubectl wait`, which can match the pod that is still
# TERMINATING under the same name and return instantly — the recreated pod is
# a different object, and waiting on the name alone is how this wait sailed
# through in 4 seconds while the replacement had not started.
# A FAILED QUERY IS NOT AN UNREADY POD — the rule `await_pods_exist` states
# above, and which this loop broke on its first draft (review). Retrying is
# right either way; the difference is ATTRIBUTION. Blaming the recreated pod
# after four minutes in which the API server never answered accuses a
# component nobody observed doing anything.
# The recreated pod first has to exist and run; then the data tier's DNS is
# gated again — its new name for its peers, theirs for it — and only then is
# readiness judged, on the full budget (#416).
deleted_pod="$(data_pod "$holder_ordinal")"
await_pod_running "$R_NS" "$deleted_pod" 120 "$holder_uid" \
  || { dump_pod_evidence "$R_NS" "$deleted_pod"; fail "the deleted holder's pod did not come back as a new Running pod within 120s"; }
log "gating the data tier on DNS again: the recreated pod must resolve its peers, and they it (#416)"
await_peer_dns "$R_NS" "$DATA_POD" "$DATA_HEADLESS"
returned=""
answered=0
ready=""
for _ in $(seq 1 80); do
  if ready="$(ready_members)"; then
    answered=1
    [ "${ready:-0}" -eq "$R_PODS" ] && { returned="yes"; break; }
  fi
  sleep 3
done
[ -n "$returned" ] || {
  not_ready=()
  while IFS= read -r name; do [ -n "$name" ] && not_ready+=("$name"); done < <(
    timeout 20 kubectl -n "$R_NS" get pods -l "app.kubernetes.io/instance=${REL}" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.conditions[?(@.type=="Ready")].status}{"\n"}{end}' 2>/dev/null \
      | grep -v '|True$' | cut -d'|' -f1 || true)
  dump_pod_evidence "$R_NS" "${not_ready[@]}"
  if [ "$answered" -eq 1 ]; then
    fail "the deleted pod never came back: only ${ready:-0} of $R_PODS pods are Ready and \
un-terminating 240s after DNS settled"
  else
    fail "could not read pod readiness for 240s: the API server never answered, so \
nothing is known about the recreated pod"
  fi
}
log "the deleted pod rejoined; all $R_PODS pods are Ready again"

# The second gate is the one occurrence three taught: the recreated pod was
# Ready, but its peers could not resolve its NEW record, so replication to it
# stalled and the convergence check below blamed the replica. Readiness and
# resolvability move independently across a pod recreation — gate on both.
log "gating on post-failover headless DNS: peers must resolve the recreated pod (#416)"
await_shape_dns
log "post-failover peer DNS settled"

# Every replica — INCLUDING the recreated pod — converges on the full total:
# the 60 pre-delete records survived the failover, the 30 post-failover
# records replicated, and the returned pod caught up from whatever it missed.
await_all_committed "$R_TOTAL" "$NEW_EPOCH"
log "failover verified in Kubernetes: the range moved (or recovered) without a re-render, produce resumed, and all $R_TOTAL records are on every replica"

}

SHAPE=colocated
R_NS="$REPLICATED_NS"
R_VALUES=replicated-values.yaml
META_POD="${REL}-"
DATA_POD="${REL}-"
META_HEADLESS="$HEADLESS"
DATA_HEADLESS="$HEADLESS"
R_PODS=3
R_DATA_PODS=3
DATA_SELECTOR="app.kubernetes.io/instance=${REL}"
R_PORT_BASE=19700
run_replicated_shape

log "releasing the co-located replicated namespace before the separated shape"
helm uninstall "$REL" -n "$REPLICATED_NS" >/dev/null 2>&1 || true
kubectl delete namespace "$REPLICATED_NS" --wait=true --timeout=180s >/dev/null 2>&1 || true
await_capacity_reclaimed "the separated shape"

# The SEPARATED shape (#287): three metadata voters in one StatefulSet, three
# data candidates in another, the lease crossing between them. Six pods, two
# headless Services, and a fresh port range so nothing collides with the
# forwards the first pass left behind.
SHAPE=separated
R_NS="$SEPARATED_NS"
R_VALUES=separated-values.yaml
META_POD="${REL}-meta-"
DATA_POD="${REL}-data-"
META_HEADLESS="${REL}-meta-headless"
DATA_HEADLESS="${REL}-data-headless"
R_PODS=6
R_DATA_PODS=3
DATA_SELECTOR="app.kubernetes.io/instance=${REL},vtop.allamiro.io/tier=data"
R_PORT_BASE=21700
run_replicated_shape

log "PASS"
