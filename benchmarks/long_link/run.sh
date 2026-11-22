#!/usr/bin/env bash
# One drain of the rig's backlog by one engine build:
#
#   benchmarks/long_link/run.sh NAME BINARY TOPIC MAX_BYTES SLOW_START_AFTER_IDLE [CHECKSUM]
#
# BINARY is a Linux vtopctl for the host's architecture (built in a container
# from the tree under test); TOPIC is p1 or p4; SLOW_START_AFTER_IDLE is the
# engine namespace's net.ipv4.tcp_slow_start_after_idle (1 is Linux's
# default). The run ends when every record of the topic is in a committed
# batch; WORK/runs/NAME holds the config, the engine log and result.txt.
# analyze.py reads them.
set -euo pipefail
NAME="$1" BIN="$2" TOPIC="$3" MAX_BYTES="$4" SSAI="$5" CHECKSUM="${6:-sha256}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${LONGLINK_WORK:-$HERE/work}"
SUBNET="${SUBNET:-10.251.77.0/24}"
R="$WORK/runs/$NAME"
rm -rf "$R"
mkdir -p "$R/state" "$R/work"
STRONG=true
[ "$CHECKSUM" = none ] && STRONG=false
sed -e "s/@NAME@/$NAME/g" -e "s/@TOPIC@/$TOPIC/g" -e "s/@MAX_BYTES@/$MAX_BYTES/g" \
  -e "s/@CHECKSUM@/$CHECKSUM/g" -e "s/@STRONG@/$STRONG/g" "$HERE/config.yaml.in" > "$R/config.yaml"
cp "$HERE/engine-run.sh" "$R/engine-run.sh"
docker run --rm --name vtop-longlink-engine --network vtop-longlink-net --ip "${SUBNET%.*}.30" \
  --cpus=2 --memory=4g --sysctl "net.ipv4.tcp_slow_start_after_idle=$SSAI" \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e NO_COLOR=1 \
  -v "$R:/r" -v "$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN"):/usr/local/bin/vtopctl:ro" \
  "${ENGINE_IMAGE:-debian:stable-slim}" bash /r/engine-run.sh "$(cat "$WORK/records")" "$(id -u):$(id -g)"
cat "$R/result.txt"
