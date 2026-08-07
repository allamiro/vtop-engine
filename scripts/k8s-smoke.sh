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
WORK="$(mktemp -d)"
CERTS="$WORK/certs"
HEADLESS="${REL}-headless"

CLUSTER_ID=11111111-2222-3333-4444-555555555555
PRINCIPAL=aaaaaaaa-0000-0000-0000-0000000000ce
RANGE_ID=aaaaaaaa-0000-0000-0000-0000000000c1
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
kubectl -n "$NS" wait --for=condition=ready pod -l "app.kubernetes.io/instance=${REL}" \
  --timeout=180s >/dev/null || {
    kubectl -n "$NS" get pods
    kubectl -n "$NS" logs "${REL}-0" --tail=30 || true
    fail "pods never became Ready"
  }
log "all pods Ready"

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
committed_offset() {
  curl -sf "localhost:$1/metrics" \
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
# The durability claim, on Kubernetes. Force-deleted with no grace period,
# which is the SIGKILL path every stop takes today (#280) — so this is the
# ordinary shutdown behaviour, not an extreme.
log "force-deleting ${REL}-0 and checking the records survive"
kubectl -n "$NS" delete pod "${REL}-0" --grace-period=0 --force >/dev/null 2>&1
sleep 5
kubectl -n "$NS" wait --for=condition=ready "pod/${REL}-0" --timeout=180s >/dev/null \
  || fail "pod did not come back Ready after being killed"

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
  --set "cluster.nodeUuids={$UUID_0}" \
  --set "tls.metaSecretName=${REL}-meta-tls" \
  --set "tls.dataSecretName=${REL}-data-tls" \
  --set "image.repository=${IMAGE_REPO}" \
  --set "image.tag=${IMAGE_TAG}" \
  --set image.pullPolicy=IfNotPresent \
  --timeout 5m >/dev/null

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

log "PASS"
