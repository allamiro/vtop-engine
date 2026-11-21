#!/usr/bin/env bash
# Kill the engine with SIGKILL KILLS times at random points of a drain, check
# the commit rule at every kill (check_commit_rule.py), restart it, and at the
# end check that everything committed, that audit-coverage --check-storage
# finds no hole and that scrub reads every committed object back clean.
#
#   benchmarks/long_link/crash.sh NAME BINARY TOPIC KILLS
#
# KILL_MIN and KILL_SPAN (seconds, defaults 6 and 10) choose when each kill
# lands after a start.
set -euo pipefail
NAME="$1" BIN="$2" TOPIC="$3" KILLS="$4"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${LONGLINK_WORK:-$HERE/work}"
SUBNET="${SUBNET:-10.251.77.0/24}"
R="$WORK/runs/$NAME"
rm -rf "$R"
mkdir -p "$R/state" "$R/work"
sed -e "s/@NAME@/$NAME/g" -e "s/@TOPIC@/$TOPIC/g" -e "s/@MAX_BYTES@/16777216/g" \
  -e "s/@CHECKSUM@/sha256/g" -e "s/@STRONG@/true/g" "$HERE/config.yaml.in" > "$R/config.yaml"
cat > "$R/loop.sh" <<'LOOP'
while true; do
  if [ -f /r/go ]; then
    rm -f /r/go
    vtopctl run --config /r/config.yaml >> /r/engine.log 2>&1 &
    echo $! > /r/engine.pid
    wait
  fi
  sleep 0.2
done
LOOP
docker run -d --name vtop-longlink-engine --network vtop-longlink-net --ip "${SUBNET%.*}.30" \
  --cpus=2 --memory=4g -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e NO_COLOR=1 \
  -v "$R:/r" -v "$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN"):/usr/local/bin/vtopctl:ro" \
  "${ENGINE_IMAGE:-debian:stable-slim}" bash /r/loop.sh >/dev/null
failed=0
for i in $(seq 1 "$KILLS"); do
  touch "$R/go"
  t=$(( RANDOM % ${KILL_SPAN:-10} + ${KILL_MIN:-6} ))
  sleep "$t"
  # The engine image has no pkill: signal the PID the loop recorded.
  docker exec vtop-longlink-engine bash -c 'kill -9 "$(cat /r/engine.pid)"'
  sleep 1
  echo "kill $i after ${t}s: $(python3 "$HERE/check_commit_rule.py" "$R" "g-$NAME" || echo FAILED)"
done
touch "$R/go"
lag=-1
for _ in $(seq 1 300); do
  lag=$(docker exec vtop-longlink-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group "g-$NAME" 2>/dev/null | awk -v g="g-$NAME" '$1==g && $6 ~ /^[0-9]+$/ {s+=$6; n++} END{print (n?s:-1)}')
  [ "$lag" = 0 ] && break
  sleep 2
done
echo "drained: lag $lag"
docker exec vtop-longlink-engine bash -c 'kill -TERM "$(cat /r/engine.pid)"' || true
sleep 3
python3 "$HERE/check_commit_rule.py" "$R" "g-$NAME" || failed=1
if docker exec vtop-longlink-engine vtopctl audit-coverage --check-storage --config /r/config.yaml \
  > "$R/audit.txt" 2>&1; then
  echo "audit-coverage: no hole"
else
  echo "audit-coverage FAILED (see $R/audit.txt)"
  failed=1
fi
if docker exec vtop-longlink-engine vtopctl scrub --config /r/config.yaml > "$R/scrub.txt" 2>&1; then
  echo "scrub: clean"
else
  echo "scrub FAILED (see $R/scrub.txt)"
  failed=1
fi
docker rm -f vtop-longlink-engine >/dev/null
[ "$lag" = 0 ] || failed=1
exit "$failed"
