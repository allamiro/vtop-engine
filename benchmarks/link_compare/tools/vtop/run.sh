#!/bin/sh
# Inside the VTOP container (link_compare/run.py): start the engine, wait until
# EXPECTED batches have committed (one batch_metrics line each), print the
# epoch time of that moment, then stop the engine. Exit 3 if the engine dies
# first, with its last log lines on stderr.
EXPECTED="$1"
vtopctl run --config /cfg/config.yaml > /state/engine.log 2>&1 &
pid=$!
while :; do
  got=$(grep -c batch_metrics /state/engine.log 2>/dev/null || true)
  if [ "${got:-0}" -ge "$EXPECTED" ]; then
    echo "done_at=$(date +%s.%N) batches=$got"
    kill -TERM "$pid"; wait "$pid"
    cat /state/engine.log > /dev/stderr
    exit 0
  fi
  if ! kill -0 "$pid" 2>/dev/null; then
    tail -n 20 /state/engine.log >&2
    exit 3
  fi
  sleep 0.2
done
