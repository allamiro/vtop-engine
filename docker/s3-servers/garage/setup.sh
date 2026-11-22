#!/usr/bin/env bash
# Garage's one-time node setup for the S3 compatibility matrix (#722), run
# by scripts/s3-lab.sh up before the shared bootstrap creates the buckets.
# Garage's image has no shell, so this drives its /garage binary through
# `docker compose exec`: give the single node a role in the cluster layout
# (Garage serves nothing until it has one), then import the lab key and let
# it create buckets. Idempotent: a second run on the same volumes changes
# nothing.
#
#   setup.sh <compose project> <compose file>
set -euo pipefail

[ $# -eq 2 ] || { echo "usage: $0 <compose project> <compose file>" >&2; exit 2; }
garage=(docker compose -p "$1" -f "$2" exec -T -e RUST_LOG=warn server /garage)
key_id=GK0123456789abcdef01234567
secret=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

# The daemon answers its own CLI once its RPC is up.
for _ in $(seq 1 60); do
    if node=$("${garage[@]}" node id -q 2>/dev/null); then
        break
    fi
    sleep 1
done
node=${node%%@*}
node=${node//$'\r'/}
[ -n "${node:-}" ] || { echo "garage setup: the node never answered 'garage node id'" >&2; exit 1; }

if "${garage[@]}" layout show 2>/dev/null | grep -q "dc1"; then
    echo "garage setup: layout already applied"
else
    "${garage[@]}" layout assign -z dc1 -c 1G "$node" >/dev/null
    "${garage[@]}" layout apply --version 1 >/dev/null
    echo "garage setup: node ${node:0:16} given a 1G role in zone dc1"
fi

if "${garage[@]}" key info "$key_id" >/dev/null 2>&1; then
    echo "garage setup: key $key_id exists"
else
    "${garage[@]}" key import --yes -n vtoplab "$key_id" "$secret" >/dev/null
    echo "garage setup: imported key $key_id"
fi
"${garage[@]}" key allow --create-bucket "$key_id" >/dev/null
