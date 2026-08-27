#!/usr/bin/env bash
# The topic-count dimension of the benchmark matrix (#130).
#
# WHY THIS IS NOT A SWEEP CELL. Every other dimension — format, compression,
# size, batch — is a property of the DATA, so `run_matrix.py --sweep` can vary
# it by writing a scenario and pointing the file harness at it. Topic count is
# a property of the SOURCE SET, and the cost it is suspected of carrying is
# paid in the read loop rather than in any batch: `source_poll_wait_ms` is
# spent PER SOURCE, serially, and an empty source burns the whole window
# before returning nothing. A cycle over N idle topics therefore costs up to
# N * that window — 100 topics at the default 250 ms is 25 seconds of a cycle
# spent waiting on nothing.
#
# That is a hypothesis with an arithmetic prediction, which is worth measuring
# rather than reasoning about. This drives the compose lab, produces into a
# controlled number of topics, and reads the engine's own `read_cycle_profile`
# back out.
#
# Usage:  benchmarks/topic_sweep.sh [counts...]      (default: 1 8 28 100)
#
# Requires: docker compose, and the lab's images already buildable.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

COUNTS=("$@")
[ ${#COUNTS[@]} -eq 0 ] && COUNTS=(1 8 28 100)

# Records produced per count, held CONSTANT so the only variable is how many
# topics they are spread across. Varying both would measure their product and
# attribute it to whichever one the reader had in mind.
TOTAL_RECORDS="${TOTAL_RECORDS:-4000}"
# How long the engine is observed at each count, after the load has landed.
OBSERVE_SECONDS="${OBSERVE_SECONDS:-60}"
OUT_DIR="${OUT_DIR:-benchmarks/results/topic-sweep-$(date -u +%Y%m%dT%H%M%SZ)}"

log()  { printf '[topic-sweep] %s\n' "$*"; }
fail() { printf '[topic-sweep] FAIL: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || fail "docker is required"
docker compose version >/dev/null 2>&1 || fail "docker compose plugin is required"

mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/topic_sweep.csv"
echo "topics,records_total,observe_seconds,cycles,records_read,avg_cycle_ms,avg_empty_wait_pct,archived_objects" > "$CSV"

kafka() { docker compose exec -T kafka /opt/kafka/bin/"$@"; }

cleanup() {
  log "tearing the lab down"
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "bringing up kafka + minio + engine"
docker compose up -d kafka minio minio-init >/dev/null
for _ in $(seq 1 60); do
  if kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 \
  || fail "kafka never became ready"

for count in "${COUNTS[@]}"; do
  log "=== $count topic(s), $TOTAL_RECORDS records spread across them ==="

  # A FRESH ENGINE PER COUNT. Its committed offsets and its ledger both carry
  # over otherwise, so a later count would start with work the earlier one
  # left and the cycle profile would describe a mixture.
  docker compose rm -sf vtop-engine >/dev/null 2>&1 || true

  # Topics are recreated rather than reused: leftovers from a higher count
  # would still be polled, which is the very cost being measured.
  existing="$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | grep -E '^sweep-' || true)"
  for t in $existing; do
    kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1 || true
  done

  for i in $(seq 1 "$count"); do
    kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "sweep-$i" --partitions 1 --replication-factor 1 >/dev/null
  done

  per_topic=$(( TOTAL_RECORDS / count ))
  [ "$per_topic" -lt 1 ] && per_topic=1
  log "producing $per_topic record(s) into each of $count topic(s)"
  for i in $(seq 1 "$count"); do
    seq 1 "$per_topic" \
      | sed 's/.*/{"ts":"2026-01-01T00:00:00Z","msg":"topic-sweep record &"}/' \
      | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server localhost:9092 --topic "sweep-$i" >/dev/null 2>&1
  done

  log "observing the engine for ${OBSERVE_SECONDS}s"
  docker compose up -d vtop-engine >/dev/null
  sleep "$OBSERVE_SECONDS"

  # The engine's OWN account of the read loop, not an inference from wall
  # clock: read_cycle_profile is emitted per cycle with the records it read
  # and the share of the read phase spent waiting on sources that had nothing.
  logs="$OUT_DIR/engine-${count}.log"
  docker compose logs --no-color vtop-engine > "$logs" 2>&1 || true
  docker compose stop vtop-engine >/dev/null 2>&1 || true

  python3 - "$logs" "$count" "$TOTAL_RECORDS" "$OBSERVE_SECONDS" "$CSV" <<'PY'
import re, sys
log, count, total, observe, csv_path = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5]
text = open(log, errors="replace").read()
# Both the JSON and the pretty tracing formats are accepted: the lab's log
# format is configurable, and a parser that only knows one silently reports
# zero cycles for the other.
cycles = re.findall(r'read_cycle_profile', text)
reads = [int(m) for m in re.findall(r'records_read[=":\s]+(\d+)', text)]
waits = [float(m) for m in re.findall(r'empty_wait_pct[=":\s]+"?([\d.]+)', text)]
phase = [float(m) for m in re.findall(r'read_phase_ms[=":\s]+(\d+)', text)]
archived = len(re.findall(r'"?verified"?', text))
n = len(cycles)
row = [count, total, observe, n, sum(reads),
       round(sum(phase)/len(phase), 1) if phase else 0,
       round(sum(waits)/len(waits), 1) if waits else 0,
       archived]
with open(csv_path, "a") as fh:
    fh.write(",".join(str(v) for v in row) + "\n")
print(f"[topic-sweep] {count} topic(s): {n} cycle(s), {sum(reads)} record(s) read, "
      f"avg read phase {row[5]} ms, avg empty wait {row[6]}%")
PY
done

log "results -> $CSV"
column -s, -t "$CSV" || cat "$CSV"
log "PASS"
