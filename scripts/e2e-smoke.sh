#!/usr/bin/env bash
# End-to-end smoke test for the VTOP lab.
#
# Brings up the shipped docker-compose stack, feeds all three source types, and
# asserts telemetry actually reaches object storage THROUGH the verified path.
#
# This exists because unit and integration tests never run the real stack: the
# Kafka ingestion P0 shipped with CI fully green precisely because nothing
# assembled the whole thing. It also validates the compose file itself, which is
# what users actually run.
#
# What it asserts (each would have caught a real bug we have already had):
#   1. every source type commits              - the Kafka stall produced zero commits
#   2. objects AND manifests land per format  - a manifest-less object is unusable
#   3. Kafka lag reaches 0                    - offsets advance only post-verify
#   4. the engine's own metrics agree         - commits <= verified (the invariant)
#   5. no errors/panics in the engine log
#
# Usage:
#   scripts/e2e-smoke.sh            # up, assert, tear down
#   KEEP=1 scripts/e2e-smoke.sh     # leave the stack running for debugging
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

TIMEOUT="${TIMEOUT:-240}"
KEEP="${KEEP:-0}"
COMPOSE=(docker compose)

pass() { printf '  \033[32mPASS\033[0m  %s\n' "$1"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$1"; FAILED=$((FAILED + 1)); }
info() { printf '\n\033[1m%s\033[0m\n' "$1"; }
FAILED=0

# shellcheck disable=SC2329  # invoked via `trap` below
cleanup() {
  if [ "$KEEP" = "1" ]; then
    echo
    echo "KEEP=1: stack left running. Tear down with: docker compose down -v"
    return
  fi
  echo
  echo "--- tearing down ---"
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
info "1/6  Starting from a clean slate"
# ---------------------------------------------------------------------------
# A previous run's state store would let the engine skip already-committed data,
# so the test could pass without ingesting anything. Wipe it.
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
rm -rf data/state data/work data/input data/spool
mkdir -p data/state data/work data/input data/spool

# Seed the file and syslog sources BEFORE start-up so the first cycle sees them.
bash docker/seed-events.sh json   200 > data/input/smoke.log
bash docker/seed-events.sh cef    200 > data/input/smoke-cef
bash docker/seed-events.sh syslog 200 > data/spool/smoke.log
pass "seeded file + syslog sources (600 records)"

# ---------------------------------------------------------------------------
info "2/6  Bringing up the lab"
# ---------------------------------------------------------------------------
# kafka-ui is excluded: it binds 8080, which collides on many machines and is
# irrelevant to whether telemetry flows.
if ! "${COMPOSE[@]}" up -d --wait kafka minio minio-init kafka-init vtop-engine >/dev/null 2>&1; then
  # --wait fails if a one-shot init container exits, which is expected; only a
  # missing engine is fatal.
  "${COMPOSE[@]}" up -d kafka minio minio-init kafka-init vtop-engine >/dev/null 2>&1 || true
fi
if [ -z "$("${COMPOSE[@]}" ps -q vtop-engine 2>/dev/null)" ]; then
  fail "engine container did not start"
  "${COMPOSE[@]}" logs --tail=30 vtop-engine || true
  exit 1
fi
pass "stack is up"

# ---------------------------------------------------------------------------
info "3/6  Waiting for every source type to commit"
# ---------------------------------------------------------------------------
# The assertion is source_committed - NOT "the engine logged something". Only a
# commit proves the batch went all the way through verification.
# Plain parallel arrays, NOT associative ones: `declare -A` needs bash 4+ and
# macOS still ships bash 3.2, so this script would work in CI and fail on every
# developer's Mac.
SOURCES="file syslog kafka"
# Match each ADAPTER's commit confirmation, which names the real source. The
# generic `source_committed` line carries only the sanitized batch_id
# (`_data_input_smoke_log`), so grepping it for a raw path silently never
# matches - a false negative that would report a healthy pipeline as broken.
pattern_for() {
  case "$1" in
    file)   echo "file source progress committed" ;;
    syslog) echo "syslog spool progress committed" ;;
    kafka)  echo "kafka offset committed" ;;
  esac
}

deadline=$((SECONDS + TIMEOUT))
seen=""
while [ $SECONDS -lt $deadline ]; do
  logs=$("${COMPOSE[@]}" logs vtop-engine 2>&1 || true)
  for k in $SOURCES; do
    case " $seen " in *" $k "*) continue ;; esac
    pat="$(pattern_for "$k")"
    if grep -qF "$pat" <<<"$logs"; then
      seen="$seen $k"
      pass "committed: $k"
    fi
  done
  complete=1
  for k in $SOURCES; do
    case " $seen " in *" $k "*) ;; *) complete=0 ;; esac
  done
  [ "$complete" = "1" ] && break
  sleep 5
done
for k in $SOURCES; do
  case " $seen " in
    *" $k "*) ;;
    *) fail "no committed batch for source type: $k (waited ${TIMEOUT}s)" ;;
  esac
done

# ---------------------------------------------------------------------------
info "4/6  Asserting objects AND manifests landed in MinIO"
# ---------------------------------------------------------------------------
# An object without its manifest is unusable: the manifest carries the checksum a
# consumer verifies against. Assert BOTH, per format.
NET="$("${COMPOSE[@]}" ps --format json vtop-engine 2>/dev/null | head -1 | python3 -c 'import json,sys
try: print(json.loads(sys.stdin.read()).get("Networks",""))
except Exception: print("")' 2>/dev/null || true)"
NET="${NET:-vtop-engine_default}"

listing=$(docker run --rm --network "$NET" --entrypoint /bin/sh minio/mc:latest -c \
  'mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null 2>&1; mc ls --recursive local' 2>/dev/null || true)

# Count DATA objects only: a manifest ends in `.manifest.json`, so a naive
# extension match counts it twice and makes objects == 2x manifests.
objects=$(grep -vc 'manifest\.json' <<<"$listing" || true)
manifests=$(grep -c 'manifest\.json' <<<"$listing" || true)
if [ "${objects:-0}" -gt 0 ]; then pass "objects in MinIO: $objects"; else fail "no data objects in MinIO"; fi
if [ "${manifests:-0}" -gt 0 ]; then pass "manifests in MinIO: $manifests"; else fail "no manifests in MinIO"; fi
if [ "${objects:-0}" -ne "${manifests:-0}" ]; then
  fail "objects ($objects) != manifests ($manifests): every object must have a manifest"
else
  pass "every object has a manifest"
fi

# Per-format buckets prove the format detection actually routed data.
for b in telemetry-cef telemetry-jsonl telemetry-syslog; do
  n=$(grep -c "$b" <<<"$listing" || true)
  if [ "${n:-0}" -gt 0 ]; then pass "bucket $b: $n files"; else fail "bucket $b is empty"; fi
done

# ---------------------------------------------------------------------------
info "5/6  Asserting Kafka offsets advanced (only happens post-verify)"
# ---------------------------------------------------------------------------
# Lag reaching 0 is the end-to-end proof: VTOP commits offsets ONLY after
# VERIFIED, so a drained consumer group means the whole verified path worked.
groups=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group vtop-engine 2>/dev/null || true)
if grep -qE '^vtop-engine' <<<"$groups"; then
  lag=$(awk '/^vtop-engine/ {print $6}' <<<"$groups" | grep -E '^[0-9]+$' | awk '{s+=$1} END {print s+0}')
  committed=$(awk '/^vtop-engine/ {print $4}' <<<"$groups" | grep -cE '^[0-9]+$' || true)
  if [ "${committed:-0}" -gt 0 ]; then pass "kafka: $committed partition(s) have committed offsets"; else fail "kafka: no committed offsets"; fi
  if [ "${lag:-1}" -eq 0 ]; then pass "kafka: total lag = 0"; else fail "kafka: total lag = $lag (expected 0)"; fi
else
  fail "kafka: consumer group 'vtop-engine' not found"
fi

# ---------------------------------------------------------------------------
info "6/6  Asserting the engine's own metrics agree"
# ---------------------------------------------------------------------------
# The engine measures itself; if its metrics disagree with the logs, one of them
# is lying. Most importantly this re-checks the invariant from a second angle.
metrics=$(docker run --rm --network "$NET" curlimages/curl:latest -s \
  http://vtop-engine:9090/metrics 2>/dev/null || true)
if grep -q '^vtop_' <<<"$metrics"; then
  pass "engine serves /metrics"
  # awk alone, no grep pipeline: with `set -o pipefail` a grep that matches
  # nothing fails the pipeline and `set -e` kills the script mid-assertion -
  # which is exactly what a HEALTHY run does, because failure counters only
  # exist after the first failure. awk always prints a number.
  sum_metric() { awk -v p="^$1" '$0 ~ p {s+=$2} END {print s+0}' <<<"$metrics"; }
  v=$(sum_metric vtop_verified_total)
  c=$(sum_metric vtop_commits_total)
  f=$(sum_metric vtop_verification_failures_total)
  if [ "${c:-0}" -gt 0 ]; then pass "metrics: commits_total = $c"; else fail "metrics: commits_total = 0"; fi
  if [ "${c:-0}" -le "${v:-0}" ]; then
    pass "metrics: INVARIANT holds (commits $c <= verified $v)"
  else
    fail "metrics: INVARIANT VIOLATED (commits $c > verified $v)"
  fi
  if [ "${f:-0}" -eq 0 ]; then pass "metrics: zero verification failures"; else fail "metrics: $f verification failures"; fi
else
  fail "engine /metrics did not respond (VTOP_METRICS_ADDR set?)"
fi

# A panic is NEVER acceptable.
panics=$("${COMPOSE[@]}" logs vtop-engine 2>&1 | grep -c 'panicked' || true)
if [ "${panics:-0}" -eq 0 ]; then pass "no panics"; else
  fail "$panics panic(s) in the engine log"
  "${COMPOSE[@]}" logs vtop-engine 2>&1 | grep 'panicked' | head -3
fi

# Errors are filtered, NOT ignored. A brand-new broker creates __consumer_offsets
# lazily on the first group operation, so the first reads legitimately get
# NotCoordinator; the engine skips that cycle and retries, which is why every
# topic still committed with lag 0 above. That transient is expected and
# self-healing. ANY OTHER error is not, and still fails this test - the point is
# to be precise, not lenient.
other_errors=$("${COMPOSE[@]}" logs vtop-engine 2>&1 \
  | sed 's/\x1b\[[0-9;]*m//g' \
  | grep -E '\bERROR\b' \
  | grep -vc 'NotCoordinator' || true)
if [ "${other_errors:-0}" -eq 0 ]; then
  pass "no unexpected errors (transient NotCoordinator on a fresh broker is expected)"
else
  fail "$other_errors unexpected error line(s) in the engine log"
  "${COMPOSE[@]}" logs vtop-engine 2>&1 | sed 's/\x1b\[[0-9;]*m//g' \
    | grep -E '\bERROR\b' | grep -v 'NotCoordinator' | head -5
fi

# ---------------------------------------------------------------------------
echo
if [ "$FAILED" -eq 0 ]; then
  printf '\033[32mE2E SMOKE PASSED\033[0m — telemetry flowed from all three sources to verified objects\n'
  exit 0
fi
printf '\033[31mE2E SMOKE FAILED\033[0m — %d assertion(s)\n' "$FAILED"
echo "--- engine log (tail) ---"
"${COMPOSE[@]}" logs --tail=40 vtop-engine 2>&1 || true
exit 1
