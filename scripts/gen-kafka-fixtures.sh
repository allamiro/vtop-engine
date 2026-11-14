#!/usr/bin/env bash
# Regenerate the compressed Kafka RecordBatch fixtures that the gateway's
# codec tests decode (#596).
#
#   scripts/gen-kafka-fixtures.sh [output-dir]
#
# The fixtures are what the pinned JVM producer writes, not batches built by
# the code under test, so they test the decoder against the client it has to
# serve. One single-node Kafka broker (the image the Kafka comparison pins)
# is started. For each codec, the script produces the records in
# `records.txt` with `compression.type=<codec>`, then copies the partition's
# first log segment. A v2 broker stores a producer's batch as the producer
# compressed it. It rewrites only the base offset and the leader epoch, and
# both sit outside the batch CRC. So the segment holds the client's batches,
# CRC and all.
#
# Needs Docker. The output is a pure function of `records.txt` and the image,
# except for the timestamps, producer id and compressed bytes, which the tests
# do not pin.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/crates/vtop-kafka/tests/fixtures}"
IMAGE="${KAFKA_IMAGE:-apache/kafka:4.3.2@sha256:852a0d49fc4e656b022e7f23785411802c9a088c78a7c5e39f33969de9c227a7}"
CODECS="${FIXTURE_CODECS:-gzip snappy lz4 zstd}"
RECORDS="$OUT/records.txt"
NAME="vtop-kafka-fixtures-$$"
BIN=/opt/kafka/bin
BOOT=localhost:9092
LOGS=/tmp/kraft-combined-logs

[[ -f "$RECORDS" ]] || {
  echo "gen-kafka-fixtures: $RECORDS is missing; it names the records every fixture carries" >&2
  exit 2
}
for codec in $CODECS; do
  case "$codec" in
    gzip | snappy | lz4 | zstd) ;;
    *)
      echo "gen-kafka-fixtures: codec $codec is not one Kafka has (gzip, snappy, lz4, zstd)" >&2
      exit 2
      ;;
  esac
done

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$NAME" "$IMAGE" >/dev/null
for _ in $(seq 1 60); do
  if docker exec "$NAME" "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOT" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$NAME" "$BIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOT" >/dev/null \
  || { echo "gen-kafka-fixtures: the broker did not come up in 60 s" >&2; exit 1; }

for codec in $CODECS; do
  topic="fixture-$codec"
  docker exec "$NAME" "$BIN/kafka-topics.sh" --bootstrap-server "$BOOT" --create \
    --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
  # `--compression-codec`, not `compression.type`: the console producer sets
  # compression.type from its own option, default none, over a
  # --producer-property. One linger window wide enough that the whole file
  # is one batch, and acks=all with idempotence (the client default) so the
  # batch carries a producer id like the benchmark's.
  docker exec -i "$NAME" "$BIN/kafka-console-producer.sh" --bootstrap-server "$BOOT" \
    --topic "$topic" --property parse.key=true --property key.separator=: \
    --compression-codec "$codec" --producer-property linger.ms=2000 \
    --producer-property batch.size=1048576 --producer-property acks=all < "$RECORDS"
  docker cp "$NAME:$LOGS/$topic-0/00000000000000000000.log" "$OUT/$codec.batch"
  echo "gen-kafka-fixtures: $OUT/$codec.batch ($(wc -c < "$OUT/$codec.batch" | tr -d ' ') bytes)"
done
