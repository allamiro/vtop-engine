#!/usr/bin/env bash
# The VTOP native-path client for the failover benchmark (#597), run by
# failover.py inside a container of the vtop-bench image on the cluster's
# network. It is what a Kafka client does for itself and the native client
# does not: find the leader, produce to it, and when the produce ends early
# (connection lost, refused, or no answer within the request timeout), look
# the leader up again and resume from the acknowledged floor.
#
# NOT the Kafka client path: a candidate node refuses the Kafka gateway, so no
# Kafka client can follow a VTOP failover (see docs/benchmarks/kafka-failover.md).
#
# Behaviour kept comparable with the JVM producer on the Kafka side:
#   - one fixed-rate schedule for the whole run (`--rate` anchored at
#     ANCHOR_US), so records the outage delayed are sent as a backlog after
#     it, as the JVM producer sends what it buffered;
#   - the leader is looked up again only when a produce ends, never on a
#     timer: the JVM client also refreshes metadata on an error or a request
#     timeout, not by watching the controller;
#   - REQUEST_TIMEOUT_MS is the JVM client's request.timeout.ms default and
#     BACKOFF_SECONDS its retry.backoff.ms default;
#   - one producer identity and epoch for the whole run, resumed with
#     `--first-sequence <acked floor>`: a batch that was written but whose
#     acknowledgement was lost is answered from the broker's deduplication
#     state rather than written twice (the JVM side is idempotent too).
#
# Separately, a lease observer polls metadata every LEASE_POLL_SECONDS and
# logs each change of holder or epoch; it feeds only the "time to new
# leader" measurement, never the producer.
#
# Writes under /out: acks.log (one line per acknowledged batch, from
# `vtop-node produce --ack-log`), acked (the floor), driver.log and lease.log
# (`<unix_micros> <event...>`), done (when TOTAL records are acknowledged).
set -u

: "${TOTAL:?}" "${RATE:?}" "${BATCH:?}" "${VALUE_BYTES:?}" "${ANCHOR_US:?}"
REQUEST_TIMEOUT_MS="${REQUEST_TIMEOUT_MS:-30000}"
BACKOFF_SECONDS="${BACKOFF_SECONDS:-0.1}"
LEASE_POLL_SECONDS="${LEASE_POLL_SECONDS:-0.1}"
TOPIC_UUID="aaaaaaaa-0000-0000-0000-0000000000b1"
RANGE_UUID="aaaaaaaa-0000-0000-0000-0000000000c1"
OUT=/out

now_us() { local t="$EPOCHREALTIME"; echo "${t/./}"; }
note() { echo "$(now_us) $*" >> "$OUT/driver.log"; }

# The admin configs, one per metadata voter, written by failover.py. A read is
# tried at each in turn, each bounded, so a dead or cut-off voter costs at
# most its bound and never wedges the loop.
#
# Parsed with bash alone: the runtime image has perl-base, without JSON::PP.
# `lease` is a flat object, so its fields are read from inside its braces.
read_lease() {
  local n json lease holder epoch
  for n in 1 2 3; do
    if json="$(timeout 2 vtopctl --json meta range-lease --config "$OUT/admin-$n.yaml" \
      --topic-uuid "$TOPIC_UUID" --range-uuid "$RANGE_UUID" 2>/dev/null)"; then
      json="${json//[$'\n\t ']/}"
      [[ "$json" =~ \"lease\":\{([^}]*)\} ]] || continue
      lease="${BASH_REMATCH[1]}"
      [[ "$lease" =~ \"holder_node_uuid\":\"([0-9a-f-]+)\" ]] || continue
      holder="${BASH_REMATCH[1]}"
      [[ "$lease" =~ \"fencing_epoch\":([0-9]+) ]] || continue
      epoch="${BASH_REMATCH[1]}"
      echo "$holder $epoch"
      return 0
    fi
  done
  return 1
}

observe_lease() {
  local last="" current
  while [[ ! -e "$OUT/done" ]]; do
    if current="$(read_lease)"; then
      if [[ "$current" != "$last" ]]; then
        echo "$(now_us) $current" >> "$OUT/lease.log"
        last="$current"
      fi
    else
      echo "$(now_us) unreadable" >> "$OUT/lease.log"
    fi
    sleep "$LEASE_POLL_SECONDS"
  done
}

client_config() { # <epoch>
  local path="$OUT/client-$1.yaml"
  cat > "$path" <<EOF
cluster_id: 11111111-2222-3333-4444-555555555555
principal_id: aaaaaaaa-0000-0000-0000-0000000000ce
producer_id: aaaaaaaa-0000-0000-0000-0000000000ce
producer_epoch: 1
fencing_epoch: $1
range: { topic: "bench", topic_epoch: 1, range_id: $RANGE_UUID, range_generation: 0 }
server_name: "localhost"
tls: { ca: /etc/vtop/tls/ca.pem, cert: /etc/vtop/tls/data-1.pem, key: /etc/vtop/tls/data-1-key.pem }
EOF
  echo "$path"
}

observe_lease &
OBSERVER=$!

acked=0
attempt=0
note "start total=$TOTAL rate=$RATE batch=$BATCH request_timeout_ms=$REQUEST_TIMEOUT_MS"
while [[ "$acked" -lt "$TOTAL" ]]; do
  attempt=$((attempt + 1))
  if ! lease="$(read_lease)"; then
    note "attempt=$attempt lease_unreadable"
    sleep "$BACKOFF_SECONDS"
    continue
  fi
  read -r holder epoch <<<"$lease"
  # The node uuids end in a1..a3 and the containers are vtop-1..3.
  node="${holder: -1}"
  config="$(client_config "$epoch")"
  note "attempt=$attempt produce holder=$holder epoch=$epoch addr=vtop-$node:9400 first=$acked"
  vtop-node produce --client-config "$config" --addr "vtop-$node:9400" \
    --records "$((TOTAL - acked))" --first-sequence "$acked" --batch "$BATCH" \
    --value-bytes "$VALUE_BYTES" --durability quorum --acked-file "$OUT/acked" \
    --rate "$RATE" --rate-anchor-unix-micros "$ANCHOR_US" --ack-log "$OUT/acks.log" \
    --request-timeout-ms "$REQUEST_TIMEOUT_MS" > "$OUT/produce.out" 2> "$OUT/produce.err"
  code=$?
  acked="$(cat "$OUT/acked" 2>/dev/null || echo "$acked")"
  reason="$(tail -c 300 "$OUT/produce.err" | tr '\n' ' ')"
  note "attempt=$attempt exit=$code acked=$acked reason=$reason"
  if [[ "$code" -ne 0 ]]; then
    sleep "$BACKOFF_SECONDS"
  fi
done
note "done acked=$acked attempts=$attempt"
touch "$OUT/done"
wait "$OBSERVER"
