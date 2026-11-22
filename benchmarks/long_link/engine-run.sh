#!/usr/bin/env bash
# Inside the engine container (run.sh): start the engine, wait until every
# record of the topic is in a committed batch (summed from batch_metrics),
# then stop it and record the wall clock and the engine's CPU ticks.
TOTAL="$1"
t0=$(date +%s.%N)
vtopctl run --config /r/config.yaml > /r/engine.log 2>&1 &
pid=$!
got=0
while sleep 1; do
  got=$(sed 's/\x1b\[[0-9;]*m//g' /r/engine.log | grep batch_metrics \
    | sed -n 's/.* records=\([0-9]*\).*/\1/p' | awk '{s+=$1} END{print s+0}')
  [ "$got" -ge "$TOTAL" ] && break
  kill -0 "$pid" 2>/dev/null || break
done
t1=$(date +%s.%N)
ticks=$(awk '{print $14+$15}' "/proc/$pid/stat")
kill -TERM "$pid"
wait "$pid"
echo "t0=$t0 t1=$t1 ticks=$ticks hz=$(getconf CLK_TCK) got=$got total=$TOTAL" > /r/result.txt
chown -R "$2" /r
