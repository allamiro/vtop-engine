#!/usr/bin/env bash
# The topic-count dimension of the benchmark matrix (#130).
#
# WHY THIS IS NOT A SWEEP CELL. Every other dimension — format, compression,
# size, batch — is a property of the DATA, so `run_matrix.py --sweep` can vary
# it by writing a scenario and pointing the file harness at it. Topic count is
# a property of the SOURCE SET, and what it costs is paid in the read loop
# rather than in any batch.
#
# THE HYPOTHESIS THIS SCRIPT WAS FIRST WRITTEN FOR WAS ALREADY FALSE, and
# recording that is more useful than quietly replacing it. It assumed
# `source_poll_wait_ms` is spent PER SOURCE, serially — so N idle topics cost
# N times the window, and 100 topics at 250 ms would be 25 seconds of a cycle
# spent waiting on nothing. That was true of the loop #96 deleted. The adapter
# now assigns every topic-partition to ONE consumer and polls them together,
# so a hundred idle topics cost one window. The claim survived in three
# comments (fixed in #372) and this benchmark was built on top of it.
#
# WHAT TOPIC COUNT STILL COSTS, read off the adapter rather than assumed:
#
#   1. Per-topic metadata. `partitions_for` is called once per source per pass
#      and, on a cache miss, does a `fetch_metadata(Some(topic))` round trip
#      to the broker. N topics with a cold cache is N round trips; the cache
#      is pruned as topics come and go, so churn re-pays it.
#   2. Assignment construction. The (topic, partition) list is built and
#      reconciled every pass, and the seek path examines every partition to
#      find the diverged ones even though it seeks only those.
#   3. Demultiplexing. Every polled message is routed back to its source by
#      (topic, partition).
#
# None of those is N * poll_wait, and all of them are linear in the topic or
# partition count — so the shape to look for is a gentle slope, not a cliff,
# and the interesting question is whether the first pass (cold cache) differs
# from the steady state.
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
# A run id, so two runs of the same count cannot share topics. The count alone
# was not enough: a broker surviving a previous run keeps its topics, and
# `--if-not-exists` then reuses one still holding the earlier cell's records
# (review).
RUN_ID="${RUN_ID:-$(date -u +%H%M%S)$$}"
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

# CREATED BEFORE COMPOSE SEES THEM. These are bind-mount SOURCES; if they do
# not exist Docker creates them as root, the engine (uid 10001) cannot write
# them, it exits, and the cell produces no profile at all — which the script
# would then have reported as a row of zeros (review). Creating them here as
# the invoking user is what makes the run possible rather than merely honest.
mkdir -p "$OUT_DIR" ./data/state ./data/work ./data/input ./data/spool

# CREATING THEM IS NOT THE SAME AS THE ENGINE BEING ABLE TO WRITE THEM. A
# previous lab run that started without this script leaves ./data owned by
# root, and `mkdir -p` over an existing directory succeeds while changing
# nothing — so the entrypoint's own check (docker/entrypoint.sh: work and
# state must be writable) still exits the container, every cell still profiles
# nothing, and the sweep still prints a grid of zeros and PASS (review).
#
# uid 10001 is neither the owner nor in the group of anything this user
# creates, so the only permission bit that helps it is other-write. Try to
# grant it; refuse to run if we could not, and say what to remove.
#
# OTHER-WRITE, and nothing weaker. An earlier version accepted "owned by
# 10001" as sufficient, which passes for a directory owned by the engine at
# mode 0500 that the engine still cannot write — and passes for one at 0700
# that the CALLER cannot write, while this script empties ./data/state between
# every cell (review). Two different users have to write here, so the only
# mode that satisfies both is the one that says so.
for d in ./data/state ./data/work; do
  chmod a+rwx "$d" 2>/dev/null || true
  mode=$(stat -c '%a' "$d")
  # WRITE AND EXECUTE. On a directory, write permits creating entries and
  # EXECUTE permits reaching them; 0622 grants the first without the second,
  # so every path inside is unreachable and the cell still profiles nothing
  # (review). Both bits, or the guard passes a directory nobody can use.
  if [ "$(( 0$mode & 0003 ))" -ne 3 ]; then
    fail "$d is mode $mode owned by $(stat -c '%U' "$d"); the engine (uid 10001) and this script both write it, so it needs other-write AND other-execute and the chmod could not grant them. Remove ./data (it is lab scratch: sudo rm -rf ./data) and rerun."
  fi
done
FAILED_CELLS=""
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
  prefix="sweep${RUN_ID}c${count}"

  # A FRESH ENGINE MEANS FRESH STATE, not a fresh container. Removing the
  # container leaves the ledger and the committed Kafka offsets behind, so a
  # later cell would resume the earlier one's progress and measure a mixture
  # (review). The state is a HOST BIND MOUNT (./data/state), not a named
  # volume, so it is emptied directly — `docker volume rm` would find nothing
  # and silently leave the ledger in place, which is the failure this comment
  # exists to prevent.
  docker compose rm -sf vtop-engine >/dev/null 2>&1 || true
  rm -rf ./data/state/* ./data/work/* 2>/dev/null || true

  # A count above the record total cannot hold the total CONSTANT — every
  # topic would be forced to one record and the cell would carry more than the
  # others, which is the confound this design exists to avoid. Refused rather
  # than fudged (review).
  #
  # BEFORE the topics are created, not after. The refusal was originally
  # placed with the arithmetic it protects, which meant `TOTAL_RECORDS=10
  # topic_sweep.sh 100000` created a hundred thousand Kafka topics and then
  # announced that the cell was invalid (review).
  if [ "$count" -gt "$TOTAL_RECORDS" ]; then
    fail "topic count $count exceeds TOTAL_RECORDS=$TOTAL_RECORDS: the constant \
record total cannot be spread one-per-topic without changing it. Raise \
TOTAL_RECORDS or lower the count."
  fi

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
    seq 1 "$n" \
      | sed 's/.*/{"ts":"2026-01-01T00:00:00Z","msg":"topic-sweep record &"}/' \
      | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
          --bootstrap-server localhost:9092 --topic "${prefix}-$i" >/dev/null 2>&1
    produced=$(( produced + n ))
  done
  log "produced $produced record(s) across $count topic(s)"

  # DISCOVERY NARROWED ON ALL THREE PLANES, not just Kafka. The lab's config
  # also enables the file and syslog-spool sources over host-mounted
  # directories, and anything sitting in them is archived during a Kafka cell
  # — inflating objects_archived with work this sweep did not ask for
  # (review). They are pointed at an empty directory rather than disabled: the
  # adapters still run, so their share of the read cycle stays visible in the
  # profile, which is part of what is being measured.
  #
  # The rule rewrites every host path bullet, not the file source's two. The
  # first version of this narrowed `- /data/input/...` and then set a
  # `spool_dir:` key that examples/config.yaml does not have — so the syslog
  # plane kept reading `- /data/spool/*.log`, and whatever a previous lab run
  # left in ./data/spool was archived inside every cell (review). The comment
  # above said "all three planes" while the code narrowed one and a half.
  # AND THE BATCHES MUST SEAL INSIDE THE WINDOW, or objects_archived measures
  # the clock rather than the work. The lab's batch thresholds are 10,000
  # records and 100 MiB; a cell spreads a few thousand records across every
  # topic, so no threshold is ever reached and the only thing that seals a
  # batch is `max_batch_age_seconds`, which the shipped config sets to 60 —
  # exactly the default observation window. The first seal therefore raced the
  # log collection, and objects_archived came out zero or not, depending on
  # scheduling, while every record had in fact been read (review).
  #
  # A quarter of the window instead, asserted below rather than assumed, so
  # several seals land inside every cell and the column is comparable between
  # them.
  seal_age=$(( OBSERVE_SECONDS / 4 ))
  [ "$seal_age" -ge 1 ] || fail "OBSERVE_SECONDS=$OBSERVE_SECONDS is too short to \
seal a batch inside a cell; objects_archived would measure scheduling. Use 4s or more."
  mkdir -p ./data/sweep-empty
  sed -e "s|^    topic_include_regex: .*|    topic_include_regex: \"^${prefix}-\"|" \
      -e "s|^\( *\)- /data/.*|\1- /data/sweep-empty/*|" \
      -e "s|^\( *\)max_batch_age_seconds: .*|\1max_batch_age_seconds: ${seal_age}|" \
    examples/config.yaml > "$SWEEP_CONFIG"

  # The seal age is the one substitution whose ABSENCE is silent: the sweep
  # would still run, still read every record, and still report a column of
  # zeros that looked like a finding.
  grep -q "max_batch_age_seconds: ${seal_age}\b" "$SWEEP_CONFIG" \
    || fail "the sweep config kept the shipped max_batch_age_seconds; batches would \
seal at or after the observation window and objects_archived would be meaningless"

  # ASSERTED, NOT ASSUMED — that is the whole lesson of the bug above. A
  # config that grows a host path this rule does not cover fails the sweep
  # instead of quietly folding someone else's backlog into the measurement.
  #
  # Not a pipeline. `grep ... | grep -qv ...` looks equivalent and is not:
  # `-q` exits at the first match, the upstream grep takes SIGPIPE, and under
  # `pipefail` the whole condition reports failure — so the `if` is false and
  # the sweep sails past the very thing it just detected (review).
  stray="$(grep -nE '^[[:space:]]*- /data/' "$SWEEP_CONFIG" | grep -v 'sweep-empty' || true)"
  if [ -n "$stray" ]; then
    fail "the sweep config still reads a host path outside /data/sweep-empty: $(printf '%s' "$stray" | tr '\n' ' ')"
  fi

  log "observing the engine for ${OBSERVE_SECONDS}s"
  VTOP_CONFIG="$SWEEP_CONFIG_IN_CONTAINER" docker compose up -d vtop-engine >/dev/null
  sleep "$OBSERVE_SECONDS"

  logs="$OUT_DIR/engine-${count}.log"
  docker compose logs --no-color vtop-engine > "$logs" 2>&1 || true
  docker compose stop vtop-engine >/dev/null 2>&1 || true

  # A FAILED CELL IS RECORDED AND THE SWEEP CONTINUES. Aborting here would
  # throw away the cells that did measure something, and a grid with a hole
  # in it is more useful than no grid — as long as the hole is reported,
  # which is what the refusal at the end is for.
  cell_ok=0
  python3 - "$logs" "$count" "$produced" "$OBSERVE_SECONDS" "$CSV" <<'PY_PARSE' || cell_ok=$?
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
if n == 0:
    # NOT WRITTEN AT ALL. A row of zeros is indistinguishable from a measured
    # zero once anyone loads the CSV, and an engine that failed to start is
    # exactly how a cell reaches here (review).
    # AND A NON-ZERO EXIT. Not writing a fabricated row stopped the CSV
    # lying, but the sweep still printed PASS over a grid with cells missing
    # from it (review) — an incomplete benchmark reported as a successful one,
    # which is the same failure one level up.
    print(f"[topic-sweep] WARNING: {count} topic(s) produced NO kafka read_cycle_profile "
          f"lines — the engine may not have started, or discovery matched nothing. "
          f"NO ROW WRITTEN; this cell is not a measurement.")
    sys.exit(3)
else:
    row = [count, produced, observe, n, int(sum(reads)),
           round(sum(phase) / len(phase), 1) if phase else 0,
           round(sum(waits) / len(waits), 1) if waits else 0,
           archived]
    with open(csv_path, "a") as fh:
        fh.write(",".join(str(v) for v in row) + "\n")
    print(f"[topic-sweep] {count} topic(s): {n} cycle(s), {int(sum(reads))} record(s) read, "
          f"avg read phase {row[5]} ms, avg empty wait {row[6]}%, {archived} object(s) archived")
PY_PARSE
  if [ "$cell_ok" -ne 0 ]; then
    FAILED_CELLS="${FAILED_CELLS}${FAILED_CELLS:+ }${count}"
  fi
done

log "results -> $CSV"
column -s, -t "$CSV" || cat "$CSV"
if [ -n "$FAILED_CELLS" ]; then
  fail "topic count(s) [$FAILED_CELLS] produced no measurement, so this grid is \
incomplete. Their engine logs are in $OUT_DIR/engine-<count>.log. A sweep that \
reported PASS over a missing cell is how a comparison gets drawn between a \
number and an absence."
fi
log "PASS"
