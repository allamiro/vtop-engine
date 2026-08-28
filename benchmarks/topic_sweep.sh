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

# Run kafka-init to completion before any cell, so its seed topics exist when
# the first purge happens rather than arriving after it.
seed_kafka_init() {
  docker compose up -d kafka-init >/dev/null 2>&1 \
    || fail "could not start kafka-init; its seed topics would appear after the first \
purge and sit in every cell's metadata fetch"
  # AN EMPTY STATUS IS NOT A FINISHED ONE. Treating a failed or unanswered
  # query as completion is how the seeds arrive after the purge instead of
  # before it — the exact failure this function exists to prevent (review).
  local state=""
  for _ in $(seq 1 60); do
    state="$(docker compose ps -a --format '{{.State}}' kafka-init 2>/dev/null | head -1)"
    [ "$state" = "exited" ] && break
    sleep 1
  done
  [ "$state" = "exited" ] || fail "kafka-init did not finish (last state: ${state:-unknown}); \
its seed topics would arrive after the purge and contaminate every cell"
}

# EVERY TOPIC THAT IS NOT THIS CELL'S IS A CONFOUND. `discover_sources` fetches
# metadata for the whole broker BEFORE topic_include_regex is applied, so the
# four topics `kafka-init` seeds — which compose starts through the engine's
# own depends_on, after the reset above — are in every cell's metadata cost
# (review). Deleted once, and then asserted per cell, because an invariant that
# is only established at startup is one that drifts.
purge_foreign_topics() { # $1 = the prefix this cell owns, empty for "none"
  local own="${1:-}"
  local listing
  listing="$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)" \
    || fail "the broker did not answer a topic listing; the empty-broker invariant is unknown"
  local topic
  while IFS= read -r topic; do
    [ -n "$topic" ] || continue
    case "$topic" in
      __*) continue ;;                       # Kafka's own internal topics
    esac
    if [ -n "$own" ] && case "$topic" in "${own}-"*) true ;; *) false ;; esac; then
      continue
    fi
    kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete \
      --topic "$topic" >/dev/null 2>&1 || true
  done <<< "$listing"
}

cleanup() {
  log "tearing the lab down"
  rm -f "$SWEEP_CONFIG" 2>/dev/null || true
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# FROM NOTHING, EVERY TIME. `up` over a lab that is already running reuses
# whatever topics are in the broker — a previous sweep interrupted before its
# cleanup, or someone's compose lab — and `discover_sources` fetches metadata
# for the WHOLE broker before the include regex is applied, so those topics
# are in every cell's measurement (review). The teardown at the end cannot
# help a run that starts dirty.
log "tearing down any existing lab so the broker starts empty"
# NOT `|| true`. The empty broker is an INVARIANT this sweep depends on, not a
# convenience: a teardown that half-failed leaves a surviving broker whose
# topics land in every cell's metadata fetch (review). Failing here costs a
# rerun; proceeding costs the whole grid, silently.
docker compose down -v --remove-orphans >/dev/null 2>&1 \
  || fail "could not tear down the existing lab, so the broker cannot be trusted to be empty; \
run 'docker compose down -v --remove-orphans' by hand and rerun"

log "bringing up kafka + minio + engine"
# kafka-init IS BROUGHT UP HERE, ahead of the cells. It is a dependency of the
# engine, so leaving it implicit meant compose started it on the first `up -d
# vtop-engine` — after the per-cell purge had already run — and its four seed
# topics were back in the broker for the whole of that cell (review). Started
# and awaited once, up front, it can be purged like any other foreign topic and
# never returns.
docker compose up -d kafka minio minio-init >/dev/null
for _ in $(seq 1 60); do
  if kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 \
  || fail "kafka never became ready"

# Now, while nothing is measuring: let kafka-init finish seeding so its topics
# are present for the first purge rather than arriving after it.
seed_kafka_init

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

  # kafka-init's seed topics come back with every `up`, so they are removed
  # here rather than once at startup, and what is left is asserted.
  purge_foreign_topics "$prefix"
  stray="$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null \
    | grep -v '^__' | grep -cv "^${prefix}-" || true)"
  [ "${stray:-0}" -eq 0 ] || fail "$stray topic(s) outside this cell survive in the broker; \
every one of them is in this cell's metadata fetch and the topic-count dimension would be a fiction"

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
  # EMPTIED, not merely created. `mkdir -p` over a directory that already
  # holds a file leaves it there, and both the file and syslog adapters are
  # pointed here — so that one file is re-archived after every cell's state
  # reset and counted in objects_archived, which is the confound this
  # redirection exists to remove (review).
  mkdir -p ./data/sweep-empty
  rm -rf ./data/sweep-empty/* ./data/sweep-empty/.[!.]* 2>/dev/null || true
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
  # JSON, so the failure guards have one shape to match rather than two
  # (review). The parser reads levels out of the log, and a text-formatted run
  # would hide the very lines it is looking for.
  VTOP_CONFIG="$SWEEP_CONFIG_IN_CONTAINER" VTOP_LOG_FORMAT=json \
    docker compose up -d vtop-engine >/dev/null
  sleep "$OBSERVE_SECONDS"

  # STILL RUNNING? A cell that emitted a profile and then died leaves a
  # non-empty log and a plausible row, and the sweep would report it (review).
  # Asked before the container is stopped, so "not running" means it stopped
  # itself.
  engine_state="$(docker compose ps --format '{{.State}}' vtop-engine 2>/dev/null | head -1)"

  # TWO SNAPSHOTS, because the drain is not part of the measurement. The
  # window's log is taken at the declared deadline and is what the read-cycle
  # numbers come from; the drain that follows exists only so the last batch can
  # seal, and folding its extra cycles and records into the profile would make
  # the sweep report more work than it declared it observed (review).
  logs="$OUT_DIR/engine-${count}.log"
  docker compose logs --no-color vtop-engine > "$logs" 2>&1 || true

  # THE DRAIN IS A SHUTDOWN, NOT A SLEEP. A grace period with the engine still
  # running keeps READING — so the uploads it produces during it are counted
  # against read-cycle metrics captured at the deadline, and the two columns
  # describe different intervals (review). SIGINT is the engine's own
  # shutdown: it forces a final cycle that SEALS every open buffer
  # (vtop-cli/src/engine.rs, "shutdown signal received; flushing and exiting"),
  # which is exactly the drain wanted, and it stops the reader at the same
  # instant the window closes. `docker compose stop` would send SIGTERM, which
  # this binary does not handle.
  log "closing the window: SIGINT flushes and seals without reading further"
  docker compose kill -s SIGINT vtop-engine >/dev/null 2>&1 \
    || fail "could not signal the engine to flush; the cell's uploads would be incomplete"
  drained_ok=""
  for _ in $(seq 1 60); do
    if [ "$(docker compose ps --format '{{.State}}' vtop-engine 2>/dev/null | head -1)" != "running" ]; then
      drained_ok=yes
      break
    fi
    sleep 1
  done
  # A WAIT THAT EXPIRES IS NOT A DRAIN THAT FINISHED (review). Falling through
  # here would snapshot a flush still in progress and record its partial
  # archive count as a completed cell — the row would be a measurement of when
  # we happened to look.
  [ -n "$drained_ok" ] || fail "the engine was still running 60s after SIGINT in the \
${count}-topic cell; its flush did not finish, so anything read from its log now is a \
snapshot of a drain in progress rather than a measurement"


  drained="$OUT_DIR/engine-${count}.drained.log"
  docker compose logs --no-color vtop-engine > "$drained" 2>&1 || true
  docker compose stop vtop-engine >/dev/null 2>&1 || true

  # THE TOPICS GO WITH THE CELL, or the sweep stops measuring what it says.
  # Unique names per cell keep the RECORDS apart, which is what they were for,
  # and do nothing about the metadata: `KafkaSource::discover_sources` fetches
  # metadata for the whole broker BEFORE applying topic_include_regex, so the
  # cells accumulate — the 100-topic cell asks about 1 + 4 + 16 + 64 + 100
  # topics, and the topic-count dimension is confounded with sweep position,
  # which is the one thing this design exists to separate (review).
  #
  # Deletion is asynchronous, so it is WAITED FOR rather than fired and
  # forgotten; a create in the next cell that landed on a half-deleted topic
  # is the failure the unique names were guarding against in the first place.
  # EXACT NAMES. `--topic "${prefix}-.*"` is a literal topic name to Kafka
  # here, not a pattern, so it deleted nothing and the wait below would have
  # aborted the sweep after its first cell (review).
  for i in $(seq 1 "$count"); do
    kafka kafka-topics.sh --bootstrap-server localhost:9092 --delete \
      --topic "${prefix}-$i" >/dev/null 2>&1 || true
  done
  # A FAILED LIST IS UNKNOWN, NOT ZERO. `grep -c` over an empty stream counts
  # nothing whether the topics are gone or the query never answered, and
  # treating the second as success is how the next cell silently inherits them
  # (review).
  remaining=""
  for _ in $(seq 1 60); do
    if listing="$(kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)"; then
      remaining="$(printf '%s\n' "$listing" | grep -c "^${prefix}-" || true)"
      [ "$remaining" -eq 0 ] && break
    else
      remaining=""
    fi
    sleep 1
  done
  if [ -z "$remaining" ]; then
    fail "the broker never answered a topic listing after the ${count}-topic cell, so \
whether its topics are gone is unknown — and an unknown here means the next cell may be \
measuring them."
  fi
  if [ "$remaining" -ne 0 ]; then
    fail "$remaining topic(s) from the ${count}-topic cell survived deletion; the next \
cell would measure them too, and the topic-count dimension would be a fiction."
  fi

  # A FAILED CELL IS RECORDED AND THE SWEEP CONTINUES. Aborting here would
  # throw away the cells that did measure something, and a grid with a hole
  # in it is more useful than no grid — as long as the hole is reported,
  # which is what the refusal at the end is for.
  cell_ok=0
  if [ "$engine_state" != "running" ]; then
    log "WARNING: the engine was '${engine_state:-absent}' at collection, not running — \
this cell did not survive its own observation window"
    cell_ok=4
  fi
  python3 - "$logs" "$count" "$produced" "$OBSERVE_SECONDS" "$CSV" "$drained" <<'PY_PARSE' || cell_ok=$?
import re, sys
log, count, produced, observe, csv_path = (
    sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), sys.argv[5])
# TWO SNAPSHOTS (review). `text` is the declared observation window and is
# where every read-cycle number comes from; `drained` also covers the grace
# period afterwards, and is used ONLY to judge whether the pipeline completed —
# uploads, commits and errors — since the last batch of the window seals in it.
text = open(log, errors="replace").read()
drained = open(sys.argv[6], errors="replace").read() if len(sys.argv) > 6 else text

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
# FROM THE WINDOW, not the drain. The engine's shutdown runs a full cycle —
# it READS before it force-flushes — so counting uploads from the drained log
# put post-deadline work in the same row as a records_read that covers only
# the window, and the two columns stopped describing the same thing (review).
# Every CSV column now comes from the declared window; the drained log is used
# below only to judge whether the pipeline COMPLETED.
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
    # THE ROW IS WRITTEN LAST, after every guard below has passed (review).
    # Appending first and then exiting non-zero left the CSV holding a
    # measurement the sweep had already decided not to trust, and a CSV is
    # exactly the artefact nobody re-reads the log beside.
    # READS WITHOUT ARCHIVES MEANS THE PIPELINE BROKE BEHIND THE READER
    # (review). Before the seal age was narrowed this was ambiguous — a batch
    # simply might not have aged out inside the window — but the config now
    # seals several times per cell, so records read and nothing archived is an
    # upload, verification or commit that failed, and the row it produced
    # describes half a pipeline.
    # A CYCLE THAT ERRORED IS A FAILED CELL EVEN IF OTHERS SUCCEEDED
    # (review). `Engine::run` logs a cycle error and keeps going, so one batch
    # can upload while another fails verification or its commit — and the row
    # above would describe the half that worked.
    # BOTH LOG SHAPES, and the adapter's own WARNINGS. The engine logs JSON
    # when VTOP_LOG_FORMAT=json and plain text otherwise, and a partial Kafka
    # read is a WARN — `adapter read pass failed`, `source read failed` — not
    # an ERROR, so matching only ERROR let a cell that read some of its topics
    # pass as if it had read all of them (review).
    # NAMED FAILURES, NOT A LOG LEVEL. Matching WARN caught every healthy run:
    # the lab points the engine at a plaintext MinIO with verify_tls=false, and
    # `S3NativeBackend::new` warns about BOTH on every startup (review). A guard
    # that fails every cell is not a strict guard, it is a broken one — and it
    # would have failed them for the one reason the lab is guaranteed to have.
    #
    # ERROR is still matched wholesale, because nothing in a healthy lab run
    # logs at ERROR. Below that, only the phrases that name a failure of the
    # pipeline this sweep measures — the partial-read cases are WARN, which is
    # why the level alone was reached for in the first place.
    trouble = re.findall(
        r'"level"\s*:\s*"ERROR"'
        r'|adapter read pass failed|source read failed|process cycle error'
        r'|batch failed|upload_failed|verification_failed|cycle_error',
        drained)
    # And the profile's own count of sources it could not read. A cell that
    # read four of its five topics is not a measurement of five.
    failed_sources = [int(m) for m in re.findall(r'failed_sources[=":\s]+"?(\d+)', drained)]
    if trouble or any(failed_sources):
        print(f"[topic-sweep] WARNING: {count} topic(s) logged {len(trouble)} engine "
              f"error/warning line(s) and {sum(failed_sources)} failed source(s); some "
              f"batch or topic did not complete its pipeline and this row describes only "
              f"the part that did.")
        sys.exit(6)

    # EVERY BATCH MUST HAVE COMMITTED. `object_uploaded` says an object landed;
    # `source_committed` is the engine saying the batch is done end to end, and
    # a cell with more uploads than commits is a pipeline that stopped halfway
    # through at least one of them (review).
    # Like compared with like: BOTH counted over the drained log, because a
    # batch that sealed at the deadline commits during the grace period and
    # comparing a window count against a drained one would always pass.
    uploaded_total = len(re.findall(r"object_uploaded", drained))
    committed = len(re.findall(r"source_committed", drained))
    if committed < uploaded_total:
        print(f"[topic-sweep] WARNING: {count} topic(s) uploaded {uploaded_total} object(s) "
              f"but only committed {committed}; at least one batch did not finish.")
        sys.exit(7)

    if sum(reads) > 0 and archived == 0:
        print(f"[topic-sweep] WARNING: {count} topic(s) read {int(sum(reads))} record(s) and "
              f"archived NOTHING, with a seal age well inside the window — the upload, "
              f"verification or commit stage failed. The row above is not a measurement of "
              f"a working pipeline.")
        sys.exit(5)

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
