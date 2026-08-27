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
# A DEDICATED ENGINE CONFIG, because the lab's default one discovers every
# non-internal topic (`topic_include_regex: ".*"`) and `kafka-init` seeds four
# of its own. Measured against that, every cell would profile count + 4 topics
# and read the seed records too — the topic-count dimension confounded by a
# constant nobody asked for (review). This restricts discovery to the sweep
# topics and is mounted over the default via VTOP_CONFIG.
# Written into ./examples because that is the only host path the engine
# container mounts (`./examples:/app/examples:ro`); VTOP_CONFIG then points at
# it inside the container. Dot-prefixed and removed on exit so it does not
# linger in the repository.
SWEEP_CONFIG="examples/.topic-sweep.yaml"
SWEEP_CONFIG_IN_CONTAINER="/app/examples/.topic-sweep.yaml"

log()  { printf '[topic-sweep] %s\n' "$*"; }
fail() { printf '[topic-sweep] FAIL: %s\n' "$*" >&2; exit 1; }

command -v docker >/dev/null || fail "docker is required"
docker compose version >/dev/null 2>&1 || fail "docker compose plugin is required"

mkdir -p "$OUT_DIR"
CSV="$OUT_DIR/topic_sweep.csv"
echo "topics,records_produced,observe_seconds,kafka_cycles,records_read,avg_read_phase_ms,avg_empty_wait_pct,objects_archived" > "$CSV"

kafka() { docker compose exec -T kafka /opt/kafka/bin/"$@"; }

cleanup() {
  log "tearing the lab down"
  rm -f "$SWEEP_CONFIG" 2>/dev/null || true
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

  # TOPIC NAMES CARRY THE COUNT, so no cell ever reuses another's topics.
  # Deleting and recreating under one name is not enough: deletion is
  # asynchronous, and a create that succeeds with `--if-not-exists` can land
  # on a topic still holding the previous cell's records (review).
  prefix="sweep${count}"

  # A FRESH ENGINE MEANS FRESH STATE, not a fresh container. Removing the
  # container leaves the ledger and the committed Kafka offsets behind, so a
  # later cell would resume the earlier one's progress and measure a mixture
  # (review). The state is a HOST BIND MOUNT (./data/state), not a named
  # volume, so it is emptied directly — `docker volume rm` would find nothing
  # and silently leave the ledger in place, which is the failure this comment
  # exists to prevent.
  docker compose rm -sf vtop-engine >/dev/null 2>&1 || true
  rm -rf ./data/state/* ./data/work/* 2>/dev/null || true

  for i in $(seq 1 "$count"); do
    kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
      --topic "${prefix}-$i" --partitions 1 --replication-factor 1 >/dev/null
  done

  # THE REMAINDER IS DISTRIBUTED, and the ACTUAL total is what gets recorded.
  # Integer division silently changed the load between cells — 4000 over 28
  # topics is 142 each, so 3976, not 4000 — which confounds topic count with
  # record count, the one thing this sweep exists to separate (review).
  base=$(( TOTAL_RECORDS / count ))
  extra=$(( TOTAL_RECORDS % count ))
  produced=0
  for i in $(seq 1 "$count"); do
    n=$base
    [ "$i" -le "$extra" ] && n=$(( n + 1 ))
    [ "$n" -lt 1 ] && n=1
    seq 1 "$n" \
      | sed 's/.*/{"ts":"2026-01-01T00:00:00Z","msg":"topic-sweep record &"}/' \
      | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server localhost:9092 --topic "${prefix}-$i" >/dev/null 2>&1
    produced=$(( produced + n ))
  done
  log "produced $produced record(s) across $count topic(s)"

  # The engine reads THIS cell's topics only.
  sed "s|^    topic_include_regex: .*|    topic_include_regex: \"^${prefix}-\"|" \
    examples/config.yaml > "$SWEEP_CONFIG"

  log "observing the engine for ${OBSERVE_SECONDS}s"
  VTOP_CONFIG="$SWEEP_CONFIG_IN_CONTAINER" docker compose up -d vtop-engine >/dev/null
  sleep "$OBSERVE_SECONDS"

  logs="$OUT_DIR/engine-${count}.log"
  docker compose logs --no-color vtop-engine > "$logs" 2>&1 || true
  docker compose stop vtop-engine >/dev/null 2>&1 || true

  python3 - "$logs" "$count" "$produced" "$OBSERVE_SECONDS" "$CSV" <<'PY_PARSE'
import re, sys
log, count, produced, observe, csv_path = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
text = open(log, errors="replace").read()

# KAFKA LINES ONLY. read_cycle_profile is emitted once per cycle PER SOURCE
# TYPE, so aggregating every line mixes the file and syslog planes into a
# Kafka measurement (review).
kafka_lines = [ln for ln in text.splitlines()
               if "read_cycle_profile" in ln and "kafka" in ln.lower()]

def nums(field):
    out = []
    for ln in kafka_lines:
        m = re.search(rf'{field}[=":\s]+"?([\d.]+)', ln)
        if m:
            out.append(float(m.group(1)))
    return out

reads = nums("records_read")
waits = nums("empty_wait_pct")
phase = nums("read_phase_ms")
# THE EVENT THE ENGINE ACTUALLY EMITS. The first version counted the word
# "verified", which a successful batch never logs — it logs
# `verification_passed` and then `source_committed` — so the column read zero
# on every healthy run (review). One `object_uploaded` per archived object.
archived = len(re.findall(r"object_uploaded", text))

n = len(kafka_lines)
row = [count, produced, observe, n, int(sum(reads)),
       round(sum(phase) / len(phase), 1) if phase else 0,
       round(sum(waits) / len(waits), 1) if waits else 0,
       archived]
with open(csv_path, "a") as fh:
    fh.write(",".join(str(v) for v in row) + "\n")

if n == 0:
    print(f"[topic-sweep] WARNING: {count} topic(s) produced NO kafka read_cycle_profile "
          f"lines — the engine may not have started, or discovery matched nothing. "
          f"This row is not a measurement.")
else:
    print(f"[topic-sweep] {count} topic(s): {n} cycle(s), {int(sum(reads))} record(s) read, "
          f"avg read phase {row[5]} ms, avg empty wait {row[6]}%, {archived} object(s) archived")
PY_PARSE
done

log "results -> $CSV"
column -s, -t "$CSV" || cat "$CSV"
log "PASS"
