#!/usr/bin/env bash
# Continuous, massive, multi-format load generator for the VTOP lab.
#
# Creates many topics across many formats and produces randomized batches to
# them forever (or for a fixed DURATION), so you can watch the engine's
# throughput / compression / latency metrics under sustained load.
#
# Environment (all optional):
#   BOOTSTRAP          Kafka bootstrap servers          (default kafka:9092)
#   FORMATS            space-separated formats          (default "cef leef json jsonl syslog logfmt apache text")
#   TOPICS_PER_FORMAT  topics created per format         (default 3)
#   MIN_BATCH/MAX_BATCH records per produce burst        (default 50 / 500)
#   SLEEP_SECONDS      pause between bursts              (default 1)
#   DURATION_SECONDS   stop after N seconds; 0 = forever (default 0)
#   PARTITIONS         partitions per topic             (default 1)
#
# The generator script lives next to this one.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SEED="$HERE/seed-events.sh"

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
FORMATS="${FORMATS:-cef leef json jsonl syslog logfmt apache text}"
TOPICS_PER_FORMAT="${TOPICS_PER_FORMAT:-3}"
MIN_BATCH="${MIN_BATCH:-50}"
MAX_BATCH="${MAX_BATCH:-500}"
SLEEP_SECONDS="${SLEEP_SECONDS:-1}"
DURATION_SECONDS="${DURATION_SECONDS:-0}"
PARTITIONS="${PARTITIONS:-1}"

# Validate numeric envs and the batch range so a misconfiguration fails fast
# instead of producing a negative/zero modulus later.
is_uint() { [[ "$1" =~ ^[0-9]+$ ]]; }
for var in TOPICS_PER_FORMAT MIN_BATCH MAX_BATCH SLEEP_SECONDS DURATION_SECONDS PARTITIONS; do
  if ! is_uint "${!var}"; then
    echo "load-generator: $var must be a non-negative integer (got '${!var}')" >&2
    exit 2
  fi
done
if [ "$MAX_BATCH" -lt "$MIN_BATCH" ]; then
  echo "load-generator: MAX_BATCH ($MAX_BATCH) must be >= MIN_BATCH ($MIN_BATCH)" >&2
  exit 2
fi
# These must be >= 1: PARTITIONS=0 fails Kafka topic creation (partitions must be
# >= 1) and TOPICS_PER_FORMAT=0 / MAX_BATCH=0 would create no topics / no records,
# silently producing nothing. is_uint() alone accepts 0, so reject it here.
for var in TOPICS_PER_FORMAT MAX_BATCH PARTITIONS; do
  if [ "${!var}" -lt 1 ]; then
    echo "load-generator: $var must be >= 1 (got '${!var}')" >&2
    exit 2
  fi
done

echo "load-generator: bootstrap=$BOOTSTRAP formats=[$FORMATS] topics/format=$TOPICS_PER_FORMAT batch=$MIN_BATCH..$MAX_BATCH sleep=${SLEEP_SECONDS}s duration=${DURATION_SECONDS}s"

# Create topics: <format>_events_<n>
TOPICS=()
for fmt in $FORMATS; do
  for n in $(seq 1 "$TOPICS_PER_FORMAT"); do
    topic="${fmt}_events_${n}"
    kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$topic" --partitions "$PARTITIONS" --replication-factor 1 >/dev/null 2>&1 || true
    TOPICS+=("${fmt}:${topic}")
  done
done
echo "load-generator: created ${#TOPICS[@]} topics; starting continuous production"

start=$(date +%s)
burst=0
while true; do
  for entry in "${TOPICS[@]}"; do
    fmt="${entry%%:*}"
    topic="${entry##*:}"
    n=$(( RANDOM % (MAX_BATCH - MIN_BATCH + 1) + MIN_BATCH ))
    bash "$SEED" "$fmt" "$n" | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$topic" >/dev/null 2>&1 || true
    burst=$(( burst + 1 ))
  done
  echo "load-generator: completed burst round (#$burst total topic-produces); sleeping ${SLEEP_SECONDS}s"
  if [ "$DURATION_SECONDS" -gt 0 ]; then
    elapsed=$(( $(date +%s) - start ))
    if [ "$elapsed" -ge "$DURATION_SECONDS" ]; then
      echo "load-generator: duration reached (${elapsed}s); exiting"
      exit 0
    fi
  fi
  sleep "$SLEEP_SECONDS"
done
