#!/usr/bin/env bash
# The entrypoint of one VTOP container in the Kafka comparison (#596): it
# starts every data-node process this container hosts, one per range, from
# the configs topology.py generated (mounted at $VTOP_CONFIG_DIR).
#
#   run-node.sh          start them and supervise (the container's PID 1)
#   run-node.sh health   exit 0 only when every process here answers /readyz
#
# Followers start first, because a leader's quorum needs them listening.
# A leader starts only once its followers in the OTHER containers answer
# /readyz, which is what compose's depends_on did for the one-range shape.
# If any process exits, the container stops: a range that lost a replica
# is not the cell being measured, and the harness must see it fail rather
# than time a degraded cluster.
set -euo pipefail
shopt -s nullglob

CONFIG_DIR="${VTOP_CONFIG_DIR:-/etc/vtop}"
WAIT_SECONDS="${VTOP_START_WAIT_SECONDS:-180}"

metrics_port() { # <config> — the observability port it listens on
  sed -n 's/^observability: { listen: "[^:]*:\([0-9]*\)" }$/\1/p' "$1"
}

partition_of() { # <config> — from its name, p<N>-<role>.yaml
  local base="${1##*/}"
  base="${base#p}"
  echo "${base%%-*}"
}

ready() { # <host> <port>
  curl -sf "http://$1:$2/readyz" > /dev/null 2>&1
}

await_ready() { # <host> <port> <what>
  local deadline=$((SECONDS + WAIT_SECONDS))
  until ready "$1" "$2"; do
    if (( SECONDS >= deadline )); then
      echo "run-node: $3 at $1:$2 was not ready within ${WAIT_SECONDS}s; stopping" >&2
      return 1
    fi
    sleep 0.5
  done
}

followers=("$CONFIG_DIR"/p*-follower.yaml)
leaders=("$CONFIG_DIR"/p*-leader.yaml)
configs=("${followers[@]}" "${leaders[@]}")
if (( ${#configs[@]} == 0 )); then
  echo "run-node: no p<N>-<role>.yaml config in $CONFIG_DIR; topology.py writes them" >&2
  exit 2
fi

if [[ "${1:-run}" == "health" ]]; then
  for config in "${configs[@]}"; do
    ready 127.0.0.1 "$(metrics_port "$config")" || exit 1
  done
  exit 0
fi

pids=()
stop_all() {
  local pid
  for pid in "${pids[@]}"; do
    kill "$pid" 2> /dev/null || true
  done
  wait || true
}
trap 'stop_all; exit 143' TERM INT

start() { # <config>
  vtop-node data --config "$1" &
  pids+=("$!")
  echo "run-node: started $(basename "$1") as pid $!"
}

for config in "${followers[@]}"; do
  start "$config"
done
for config in "${followers[@]}"; do
  await_ready 127.0.0.1 "$(metrics_port "$config")" "follower $(basename "$config")" || { stop_all; exit 1; }
done
for config in "${leaders[@]}"; do
  p="$(partition_of "$config")"
  # Each follower's host from the leader's `followers:` list; its
  # observability port is the partition's, as on every container.
  while read -r host; do
    await_ready "$host" "$(metrics_port "$config")" "partition $p's follower" || { stop_all; exit 1; }
  done < <(sed -n 's/.*addr: "\([^:"]*\):[0-9]*".*/\1/p' "$config")
  start "$config"
done

# The first process to exit ends the container, with its status.
set +e
wait -n
status=$?
set -e
echo "run-node: a data-node process exited with status $status; stopping the others" >&2
stop_all
exit "$status"
