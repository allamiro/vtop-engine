#!/usr/bin/env bash
# Mint the mTLS material for the live chaos harness (#215): one CA, meta-node
# leaves (CN = decimal Raft node id — the peer transport parses it), data-node
# leaves (CN = broker UUID — the replica transport parses it), plus admin and
# native-client leaves. ECDSA P-256, SAN DNS:localhost throughout.
set -euo pipefail

CERT_DIR="${1:?usage: gen-certs.sh <output-dir> <meta-ids...> -- <data-uuids...>}"
shift
META_IDS=()
DATA_UUIDS=()
seen_sep=0
for arg in "$@"; do
  if [[ "$arg" == "--" ]]; then seen_sep=1; continue; fi
  if [[ $seen_sep -eq 0 ]]; then META_IDS+=("$arg"); else DATA_UUIDS+=("$arg"); fi
done

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

ext_file() {
  cat > leaf.ext <<'EOF'
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = DNS:localhost
EOF
}

if [[ ! -f ca.pem ]]; then
  openssl ecparam -name prime256v1 -genkey -noout -out ca-key.pem 2>/dev/null
  openssl req -x509 -new -key ca-key.pem -sha256 -days 30 \
    -subj "/CN=vtop-live-chaos-ca" -out ca.pem 2>/dev/null
fi
ext_file

mint() { # mint <basename> <common-name>
  local base="$1" cn="$2"
  [[ -f "$base.pem" ]] && return 0
  openssl ecparam -name prime256v1 -genkey -noout -out "$base-key.pem" 2>/dev/null
  openssl req -new -key "$base-key.pem" -subj "/CN=$cn" -out "$base.csr" 2>/dev/null
  openssl x509 -req -in "$base.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
    -days 30 -sha256 -extfile leaf.ext -out "$base.pem" 2>/dev/null
  rm -f "$base.csr"
}

for id in "${META_IDS[@]}"; do
  mint "meta-$id" "$id"
done
index=1
for uuid in "${DATA_UUIDS[@]}"; do
  mint "data-$index" "$uuid"
  index=$((index + 1))
done
mint admin "vtop-admin"
mint client "vtop-harness-client"

rm -f leaf.ext ca.srl
echo "certs_ready dir=$PWD meta=${#META_IDS[@]} data=${#DATA_UUIDS[@]}"
