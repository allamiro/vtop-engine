#!/usr/bin/env bash
# Shared plumbing for the disaster-recovery drills (#735). Sourced by each
# drill, never run on its own.
#
# A drill sets up a lab out of real parts: the `vtopctl` binary under test, a
# MinIO store, a PostgreSQL or SQLite ledger and, where the source matters, a
# Kafka broker, each in a container this file starts and removes. It injects
# one disaster, follows the recovery steps docs/operate/disaster-recovery.md
# gives, and checks the outcome against that page's stated pass criteria and
# bounds.
#
# What each drill counts, it counts from the objects the store holds, not from
# what the engine reports it wrote: every record is numbered, and the drill
# reads the archive back and says which numbers are missing and which are
# there twice.
#
# Environment (all optional):
#   DR_DRILL_PG_IMAGE   the PostgreSQL image (default postgres:18)
#   DR_DRILL_MINIO_IMAGE the MinIO image (default: the lab's, built from
#                       docker/minio when it is not present)
#   DR_DRILL_KAFKA_IMAGE the Kafka image (default apache/kafka:4.3.1)
#   DR_DRILL_NETWORK    a docker network mode for every helper container. Set it
#                       to container:<name> when the drill itself runs in a
#                       container, so that the helpers share its network
#                       namespace and are reached on 127.0.0.1. Unset, each
#                       helper publishes its port on 127.0.0.1.
#   DR_DRILL_PREFIX     the helper containers' name prefix (default vtop-dr)
#   DR_DRILL_WORKDIR    where the drill works (default: a new temporary
#                       directory); it must be empty
#   DR_DRILL_RESULT     where the JSON result goes (default <workdir>/result.json)
#   DR_DRILL_KEEP       set to keep the workdir and containers on success
#   DR_DRILL_CPUS       the CPU cap of each helper container (default 1)
#
# Modes (the drill's arguments are `<vtopctl> [--handover | --check | --teardown] [--leg NAME]`):
#   (none)      set the lab up, cause the disaster, follow the page, check, tear down.
#   --handover  set the lab up and cause the disaster, then stop and leave the
#               lab running for a second person: it prints the page section to
#               follow, the lab's endpoints, credentials and environment, and how
#               to tear it down, and runs none of the recovery steps (#735).
#   --check     after the person has finished (and stopped the engine), run only
#               the pass criteria against the lab and report RPO, RTO and
#               pass or fail. It can be run again; it changes nothing.
#   --teardown  remove the lab's containers, volumes, mounts, processes and
#               workdir.
# A drill with legs runs every leg unless --leg names one; --handover runs the
# first leg when none is named. DR_DRILL_WORKDIR names the lab's directory; for
# the three handover modes it defaults to $TMPDIR/vtop-dr-handover-<drill>.
#
# Needs bash 5 (EPOCHREALTIME), docker, python3 and curl.
# shellcheck disable=SC2034  # variables set here are read by the drill scripts

set -euo pipefail
umask 077

DR_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DR_REPO="$(cd "$DR_LIB_DIR/../.." && pwd)"
DR_DOC="$DR_REPO/docs/operate/disaster-recovery.md"
DR_CHECKS="$DR_LIB_DIR/dr_checks.py"

DR_PG_IMAGE="${DR_DRILL_PG_IMAGE:-postgres:18}"
DR_MINIO_IMAGE="${DR_DRILL_MINIO_IMAGE:-vtop-minio:RELEASE.2025-09-07T16-13-09Z}"
DR_KAFKA_IMAGE="${DR_DRILL_KAFKA_IMAGE:-apache/kafka:4.3.1}"
DR_PREFIX="${DR_DRILL_PREFIX:-vtop-dr}"
DR_CPUS="${DR_DRILL_CPUS:-1}"
DR_S3_USER=minioadmin
DR_S3_PASSWORD=minioadmin
export AWS_ACCESS_KEY_ID="$DR_S3_USER" AWS_SECRET_ACCESS_KEY="$DR_S3_PASSWORD" AWS_REGION=us-east-1

declare -A DR_CONT=()  # label -> container name
declare -A DR_PORT=()  # label -> port the service listens on, on 127.0.0.1
DR_CONTAINERS=()
DR_VOLUMES=()
DR_PORTS_USED=" "
DR_ENGINE_PID=""
DR_ENGINE_RUN=0
DR_FAILURES=()
DR_MOUNTS=()

# ---- logging, failing, cleanup -----------------------------------------------
log() { echo "[dr-drill/$DR_DRILL] $*"; }
fail() {
  echo "[dr-drill/$DR_DRILL] FAIL: $*" >&2
  local f
  for f in "$DR_LOGS"/*.log; do
    [[ -f "$f" ]] || continue
    echo "--- tail of $f" >&2
    tail -n 12 "$f" | cut -c1-300 >&2 || true
  done
  exit 1
}

# shellcheck disable=SC2329  # invoked via `trap`
dr_cleanup() {
  local status=$?
  # A lab handed over, and a lab being checked, stay up: the person may still be
  # working in it, and a failed check is run again after a fix.
  if [[ ($DR_MODE == handover && $status -eq 0) || $DR_MODE == check ]]; then return; fi
  dr_teardown_lab "$status"
}
# dr_teardown_lab <status>: stop everything the lab started, and remove it (a
# failed run keeps its workdir and logs).
dr_teardown_lab() {
  local status="${1:-0}"
  if [[ $DR_MODE != teardown ]] || dr_is_engine_pid "$DR_ENGINE_PID"; then
    [[ -z "$DR_ENGINE_PID" ]] || kill -9 "$DR_ENGINE_PID" 2> /dev/null || true
  fi
  dr_stop_extra
  local m
  for m in ${DR_MOUNTS[@]+"${DR_MOUNTS[@]}"}; do dr_as_root umount "$m" 2> /dev/null || true; done
  if [[ -n "${DR_DRILL_KEEP:-}" && $status -eq 0 && $DR_MODE == run ]]; then
    log "kept $DR_WORKDIR and containers ${DR_CONTAINERS[*]:-none}"
    return
  fi
  local c v
  for c in ${DR_CONTAINERS[@]+"${DR_CONTAINERS[@]}"}; do docker rm -fv "$c" > /dev/null 2>&1 || true; done
  for v in ${DR_VOLUMES[@]+"${DR_VOLUMES[@]}"}; do docker volume rm -f "$v" > /dev/null 2>&1 || true; done
  # A failure keeps its logs. A clean run removes the directory unless the
  # caller named it (CI reads the result and the logs from there); a teardown
  # of a handed-over lab removes it, its work being done.
  if [[ $status -eq 0 && ( -z "${DR_DRILL_WORKDIR:-}" || $DR_MODE == teardown ) ]]; then rm -rf "${DR_WORKDIR:?}"; fi
}
# A drill that starts a process of its own (a fault proxy) defines this.
dr_stop_extra() { :; }
# dr_is_engine_pid <pid>: the pid is a running engine, not a number a handover
# left behind that the system has since given to something else.
dr_is_engine_pid() {
  [[ -n "${1:-}" ]] && [[ "$(ps -o command= -p "$1" 2> /dev/null)" == *" run --config "* ]]
}

dr_as_root() {
  if ((EUID == 0)); then "$@"; else sudo -n "$@"; fi
}

# dr_init <drill name> <arguments of the drill...>
dr_init() {
  DR_DRILL="$1"
  shift
  DR_MODE=run
  DR_LEG=""
  DR_BIN=""
  DR_HANDED_OVER=0
  DR_HANDOVER_US=""
  local usage
  usage="usage: $(basename "${BASH_SOURCE[1]}") <vtopctl> [--handover | --check | --teardown] [--leg NAME]"
  while (($# > 0)); do
    case "$1" in
      --handover) DR_MODE=handover ;;
      --check) DR_MODE=check ;;
      --teardown) DR_MODE=teardown ;;
      --leg)
        [[ -n "${2:-}" ]] || { echo "FAIL: --leg needs a name; $usage" >&2; exit 2; }
        DR_LEG="$2"
        shift
        ;;
      -*) echo "FAIL: unknown option $1; $usage" >&2; exit 2 ;;
      *)
        [[ -z "$DR_BIN" ]] || { echo "FAIL: more than one vtopctl given; $usage" >&2; exit 2; }
        DR_BIN="$1"
        ;;
    esac
    shift
  done
  [[ -n "$DR_BIN" ]] || { echo "FAIL: $usage" >&2; exit 2; }
  [[ -x "$DR_BIN" ]] || { echo "FAIL: $DR_BIN is not executable" >&2; exit 2; }
  DR_BIN="$(cd "$(dirname "$DR_BIN")" && pwd)/$(basename "$DR_BIN")"
  ((BASH_VERSINFO[0] >= 5)) || { echo "FAIL: bash 5 or later is needed (EPOCHREALTIME)" >&2; exit 2; }
  local tool
  for tool in docker python3 curl; do
    command -v "$tool" > /dev/null || { echo "FAIL: $tool is needed" >&2; exit 2; }
  done
  if [[ $DR_MODE == run ]]; then
    DR_WORKDIR="${DR_DRILL_WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/vtop-dr-drill.XXXXXX")}"
  else
    DR_WORKDIR="${DR_DRILL_WORKDIR:-${TMPDIR:-/tmp}/vtop-dr-handover-$DR_DRILL}"
  fi
  if [[ $DR_MODE == check || $DR_MODE == teardown ]]; then
    [[ -d "$DR_WORKDIR" ]] || { echo "[dr-drill/$DR_DRILL] FAIL: $DR_WORKDIR does not exist: no lab was handed over there (set DR_DRILL_WORKDIR to its directory)" >&2; exit 2; }
  fi
  mkdir -p "$DR_WORKDIR"
  DR_WORKDIR="$(cd "$DR_WORKDIR" && pwd)"
  DR_STATE_FILE="$DR_WORKDIR/handover.state"
  if [[ $DR_MODE == run || $DR_MODE == handover ]]; then
    if [[ -n "$(ls -A "$DR_WORKDIR")" ]]; then
      local hint="name a new directory in DR_DRILL_WORKDIR"
      [[ $DR_MODE != handover ]] || hint="a lab is already there: run --teardown on it, or name a new directory in DR_DRILL_WORKDIR"
      echo "[dr-drill/$DR_DRILL] FAIL: $DR_WORKDIR is not empty; $hint" >&2
      exit 2
    fi
  else
    [[ -f "$DR_STATE_FILE" ]] \
      || { echo "[dr-drill/$DR_DRILL] FAIL: no handed-over lab in $DR_WORKDIR (no handover.state); run --handover first, or set DR_DRILL_WORKDIR to its directory" >&2; exit 2; }
  fi
  DR_RESULT_FILE="${DR_DRILL_RESULT:-$DR_WORKDIR/result.json}"
  DR_LOGS="$DR_WORKDIR/logs"
  mkdir -p "$DR_LOGS"
  DR_RUN_ID="$$"
  trap dr_cleanup EXIT
}

# ---- the page's numbers are the script's --------------------------------------
# dr_check_doc NAME=VALUE...: docs/operate/disaster-recovery.md has a row
# "| `NAME` | VALUE |" for each; the drill refuses to run when it does not,
# so the bounds the page promises and the bounds the drill enforces are one.
dr_check_doc() {
  [[ -f "$DR_DOC" ]] || fail "$DR_DOC not found"
  local pair name value
  for pair in "$@"; do
    name="${pair%%=*}"
    value="${pair#*=}"
    grep -qF "| \`$name\` | $value |" "$DR_DOC" \
      || fail "the page $DR_DOC has no row '| \`$name\` | $value |': the bounds there and in this drill must match"
  done
}

# ---- time ---------------------------------------------------------------------
now_us() { local t="$EPOCHREALTIME"; echo "${t//[.,]/}"; }
ms_since() { echo $((($(now_us) - $1) / 1000)); }
secs() { printf '%d.%03d' $(($1 / 1000)) $(($1 % 1000)); }

# wait_until <timeout seconds> <what> <predicate...>
wait_until() {
  local deadline=$((SECONDS + $1)) what="$2"
  shift 2
  until "$@"; do
    if [[ -n "$DR_ENGINE_PID" ]] && ! kill -0 "$DR_ENGINE_PID" 2> /dev/null; then
      DR_ENGINE_PID=""
      fail "the engine exited while waiting for $what"
    fi
    ((SECONDS < deadline)) || fail "timed out waiting for $what"
    sleep 0.3
  done
}

# ---- a lab handed to a second person -------------------------------------------
# State that must survive from --handover to --check and --teardown: the
# containers and ports, the workdir's run id, and whatever the drill names in
# DR_HANDOVER_VARS. Written as `declare -g` statements, read back by sourcing.
DR_STATE_VARS=(DR_RUN_ID DR_CONT DR_PORT DR_CONTAINERS DR_VOLUMES DR_MOUNTS DR_PORTS_USED DR_PG DR_DB
  DR_KAFKA DR_KAFKA_BOOTSTRAP DR_ENGINE_PID DR_ENGINE_RUN DR_HANDOVER_US DR_HANDOVER_ISO DR_LEG VTOP_STATE_STORE)
DR_HANDOVER_VARS=()      # the drill's own, set before dr_run_drill
DR_HANDOVER_ENV=()       # NAME=value lines for lab.env, appended by the drill's lab phase
DR_LEGS=()               # the drill's legs, in order; empty for a drill without legs
DR_PAGE_SECTION=""       # the page's heading the second person follows

dr_state_save() {
  local v out="$DR_STATE_FILE.tmp"
  : > "$out"
  for v in "${DR_STATE_VARS[@]}" ${DR_HANDOVER_VARS[@]+"${DR_HANDOVER_VARS[@]}"}; do
    # A variable that was never set is not saved.
    declare -p "$v" > /dev/null 2>&1 || continue
    declare -p "$v" | sed 's/^declare /declare -g /' >> "$out"
  done
  mv "$out" "$DR_STATE_FILE"
}
dr_state_load() {
  # shellcheck disable=SC1090  # a file this library wrote
  source "$DR_STATE_FILE"
  # This process's own results, not the handover's.
  rm -f "$DR_WORKDIR/result.tsv"
}

# dr_write_lab_env: what a person sources to talk to the lab as the engine does.
dr_write_lab_env() {
  local f="$DR_WORKDIR/lab.env" kv
  {
    echo "# The lab's environment for the drill $DR_DRILL. Source it: . $f"
    printf 'export AWS_ACCESS_KEY_ID=%q AWS_SECRET_ACCESS_KEY=%q AWS_REGION=%q\n' "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" "$AWS_REGION"
    [[ -z "${VTOP_STATE_STORE:-}" ]] || printf 'export VTOP_STATE_STORE=%q\n' "$VTOP_STATE_STORE"
    for kv in ${DR_HANDOVER_ENV[@]+"${DR_HANDOVER_ENV[@]}"}; do printf 'export %s=%q\n' "${kv%%=*}" "${kv#*=}"; done
  } > "$f"
  chmod 0600 "$f"
}

# dr_lab_endpoints: one line for each service the lab started.
dr_lab_endpoints() {
  local label
  for label in $(printf '%s\n' "${!DR_CONT[@]}" | sort); do
    case "$label" in
      minio-*) echo "  object store ${label#minio-}: http://127.0.0.1:${DR_PORT[$label]}  user $DR_S3_USER, password $DR_S3_PASSWORD (its client: docker exec -e HOME=/tmp ${DR_CONT[$label]} mc ...; its container is ${DR_CONT[$label]})" ;;
      pg-*) echo "  ledger server ${label#pg-}: postgres://vtop:vtop@127.0.0.1:${DR_PORT[$label]}/<database> (psql: docker exec -it ${DR_CONT[$label]} psql -U vtop -d <database>)" ;;
      kafka-*) echo "  Kafka ${label#kafka-}: bootstrap 127.0.0.1:${DR_PORT[$label]} (tools: docker exec -i ${DR_CONT[$label]} /opt/kafka/bin/<tool>.sh --bootstrap-server 127.0.0.1:${DR_PORT[$label]})" ;;
      *) echo "  $label: ${DR_CONT[$label]} on 127.0.0.1:${DR_PORT[$label]}" ;;
    esac
  done
  if [[ -n "${DR_DRILL_NETWORK:-}" ]]; then
    echo "  (the containers were started with DR_DRILL_NETWORK=$DR_DRILL_NETWORK: they are reached where that network puts them)"
  fi
}

# dr_handover <leg>: the lab is ready and the disaster has happened. Print what
# the second person needs, and leave. The drill supplies `handover_text <leg>`,
# which prints what is lost and what is in the lab (see the drills).
dr_handover() {
  local leg="${1:-}" me script
  me="$(basename "${BASH_SOURCE[2]:-$0}")"
  script="scripts/dr-drill/$me"
  ((${#DR_FAILURES[@]} == 0))     || fail "the lab is not in the state the drill expects, so it is not handed over: ${DR_FAILURES[*]}"
  DR_HANDOVER_US="$(now_us)"
  DR_HANDOVER_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  dr_write_lab_env
  dr_state_save
  local legarg="" envarg="DR_DRILL_WORKDIR=$DR_WORKDIR"
  [[ -z "$leg" ]] || legarg=" --leg $leg"
  [[ -z "${DR_DRILL_NETWORK:-}" ]] || envarg+=" DR_DRILL_NETWORK=$DR_DRILL_NETWORK"
  {
    echo "=== HANDOVER: $DR_DRILL${leg:+ (leg $leg)}: the disaster has happened; nothing has been recovered ==="
    echo "Handed over at $DR_HANDOVER_ISO. The clock for the run starts at your first step."
    echo
    echo "Follow only: docs/operate/disaster-recovery.md, section \"$DR_PAGE_SECTION\""
    echo "Note every step that needed a guess, and time the run from the first step to the last check."
    echo
    echo "What happened and what you are given:"
    handover_text "$leg"
    echo
    echo "The lab:"
    echo "  vtopctl: $DR_BIN"
    echo "  workdir: $DR_WORKDIR (logs in logs/, configs in it)"
    echo "  environment: . $DR_WORKDIR/lab.env  (the store's credentials and the ledger's connection string)"
    dr_lab_endpoints
    if [[ -n "$DR_ENGINE_PID" ]]; then
      echo "  an engine is running, pid $DR_ENGINE_PID, log $(dr_engine_log); a step that stops it stops it"
    fi
    echo
    echo "When you have finished the page's steps, stop the engine (SIGTERM) and run:"
    echo "  $envarg $script $DR_BIN --check$legarg"
    echo "It runs only the pass criteria, reports the RPO, the RTO and pass or fail, and can be run again."
    echo
    echo "To tear the lab down (containers, volumes, mounts, processes, workdir):"
    echo "  $envarg $script $DR_BIN --teardown"
  } | tee "$DR_WORKDIR/HANDOVER.txt"
  DR_HANDED_OVER=1
  exit 0
}

# dr_run_drill: the drill's lifecycle. A drill defines
#   lab_shared            (optional) what every leg shares: containers, a proxy
#   lab <leg>             the lab and the disaster, for one leg
#   recover <leg>         the page's steps, run by the drill (never by --handover)
#   check <leg>           the outcome's pass criteria; sets RTO_MS (and CAUGHT_MS)
#                         itself in a run, and is given the handover-to-check time in --check
#   handover_text <leg>   what is lost and what the lab holds, for --handover
#   leg_done <leg>        (optional) what a run tears down between legs
# and DR_LEGS (empty for a drill without legs), DR_PAGE_SECTION, DR_HANDOVER_VARS.
dr_run_drill() {
  local leg legs=()
  if [[ -n "$DR_LEG" ]]; then
    local known=0
    for leg in ${DR_LEGS[@]+"${DR_LEGS[@]}"}; do [[ "$leg" != "$DR_LEG" ]] || known=1; done
    ((known)) || { echo "[dr-drill/$DR_DRILL] FAIL: no leg $DR_LEG; the legs are: ${DR_LEGS[*]:-(none)}" >&2; exit 2; }
    legs=("$DR_LEG")
  elif ((${#DR_LEGS[@]} > 0)); then
    legs=("${DR_LEGS[@]}")
  else
    legs=("")
  fi
  case "$DR_MODE" in
    run)
      declare -F lab_shared > /dev/null && lab_shared
      for leg in "${legs[@]}"; do
        lab "$leg"
        recover "$leg"
        check "$leg"
        ! declare -F leg_done > /dev/null || leg_done "$leg"
      done
      dr_finish
      ;;
    handover)
      # One leg: the first, unless --leg names one.
      leg="${legs[0]}"
      DR_LEG="$leg"
      declare -F lab_shared > /dev/null && lab_shared
      lab "$leg"
      dr_handover "$leg"
      ;;
    check)
      local cli_leg="$DR_LEG"
      dr_state_load
      [[ -z "$cli_leg" || "$cli_leg" == "$DR_LEG" ]] \
        || fail "the lab was handed over for leg ${DR_LEG:-(none)}, not $cli_leg"
      leg="$DR_LEG"
      # An engine the lab started is stopped, as a run's own recovery stops it.
      dr_engine_stop_detached
      RTO_MS="$(ms_since "$DR_HANDOVER_US")"
      CAUGHT_MS="$RTO_MS"
      check "$leg"
      log "the lab is still up: run --check again after a fix, or --teardown to remove it"
      dr_finish
      ;;
    teardown)
      dr_state_load
      log "tearing the lab down"
      exit 0
      ;;
  esac
}

# ---- results and criteria -----------------------------------------------------
# dr_set <key> <value>: a number or string for the JSON result (the last
# value set for a key wins).
dr_set() { printf '%s\t%s\n' "$1" "$2" >> "$DR_WORKDIR/result.tsv"; }
# crit <ok|bad> <what> [detail]: one pass criterion. A bad one is recorded and
# the drill goes on, so that one run reports every criterion; it exits 1 at
# the end.
crit() {
  local verdict="$1" what="$2" detail="${3:-}"
  if [[ "$verdict" == ok ]]; then
    log "PASS  $what${detail:+ ($detail)}"
  else
    log "FAIL  $what${detail:+ ($detail)}"
    DR_FAILURES+=("$what${detail:+ ($detail)}")
  fi
}
# crit_test <what> <detail when bad> <command...>: ok when the command succeeds.
crit_test() {
  local what="$1" detail="$2"
  shift 2
  if "$@"; then crit ok "$what"; else crit bad "$what" "$detail"; fi
}
# crit_eq <what> <got> <want>
crit_eq() {
  if [[ "$2" == "$3" ]]; then crit ok "$1" "$2"; else crit bad "$1" "got $2, want $3"; fi
}
# crit_ge <what> <got> <bound>
crit_ge() {
  if (($2 >= $3)); then crit ok "$1" "$2 >= $3${4:-}"; else crit bad "$1" "$2 is below $3${4:-}"; fi
}
# crit_rto <what> <got ms> <bound seconds>: a recovery time against the page's
# bound. A run of the drill enforces it. After a handover the clock is the
# person's (handover to the start of --check, so it includes reading and
# typing), which the automated bound was never set for: it is reported, with
# whether it fell inside the bound, and not a criterion.
crit_rto() {
  local what="$1" ms="$2" bound="$3"
  if [[ $DR_MODE == check ]]; then
    local within=true
    (($((ms / 1000)) <= bound)) || within=false
    log "INFO  $what: $(secs "$ms") s from the handover to the check (the automated drill's bound is $bound s; not enforced for a person)"
    dr_set rto_within_automated_bound "$within"
    dr_set rto_bound_s "$bound"
  else
    crit_le "$what" "$((ms / 1000))" "$bound" " s"
  fi
}
# dr_check_summary <records lost>: what --check reports as the RPO and RTO.
dr_check_summary() {
  [[ $DR_MODE == check ]] || return 0
  dr_set mode check
  dr_set handover_at "$DR_HANDOVER_ISO"
  dr_set rpo_records_lost "$1"
  dr_set rto_ms "$RTO_MS"
  log "RPO: $1 record(s) lost. RTO: $(secs "$RTO_MS") s from the handover to this check."
}
# crit_le <what> <got> <bound> [unit]
crit_le() {
  if (($2 <= $3)); then crit ok "$1" "$2 <= $3${4:-}"; else crit bad "$1" "$2 is past the bound of $3${4:-}"; fi
}

# dr_finish: write the JSON, print the summary, exit 1 if any criterion failed.
dr_finish() {
  local pass=true
  ((${#DR_FAILURES[@]} == 0)) || pass=false
  dr_set pass "$pass"
  python3 "$DR_CHECKS" result "$DR_WORKDIR/result.tsv" "$DR_RESULT_FILE" "$DR_DRILL"
  cat "$DR_RESULT_FILE"
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### disaster-recovery drill: $DR_DRILL"
      echo
      echo '```json'
      cat "$DR_RESULT_FILE"
      echo '```'
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  if [[ "$pass" == true ]]; then
    log "OK"
  else
    echo "[dr-drill/$DR_DRILL] FAIL: ${#DR_FAILURES[@]} criterion(s) failed:" >&2
    printf '  - %s\n' "${DR_FAILURES[@]}" >&2
    exit 1
  fi
}

# ---- containers ---------------------------------------------------------------
dr_free_port() {
  local p
  while :; do
    p="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
    [[ "$DR_PORTS_USED" == *" $p "* ]] || break
  done
  DR_PORTS_USED+="$p "
  echo "$p"
}

# dr_run <label> <port> <docker run args...> -- <image> <args...>: start a
# detached helper container that listens on <port> and is reached on
# 127.0.0.1:<port>.
dr_run() {
  local label="$1" port="$2"
  shift 2
  local name="$DR_PREFIX-$DR_RUN_ID-$label"
  local -a net
  if [[ -n "${DR_DRILL_NETWORK:-}" ]]; then net=(--network "$DR_DRILL_NETWORK"); else net=(-p "127.0.0.1:$port:$port"); fi
  DR_CONT["$label"]="$name"
  DR_PORT["$label"]="$port"
  DR_CONTAINERS+=("$name")
  docker run -d --name "$name" --cpus="$DR_CPUS" "${net[@]}" "$@" > /dev/null \
    || fail "could not start the container $name"
}
dr_volume() { # <name>
  DR_VOLUMES+=("$1")
  docker volume create "$1" > /dev/null
}
# dr_kill <label>: the container is lost, with its volumes (a lost host or region).
dr_kill() {
  docker kill "${DR_CONT[$1]}" > /dev/null 2>&1 || true
  docker rm -fv "${DR_CONT[$1]}" > /dev/null
}

# ---- MinIO --------------------------------------------------------------------
# dr_minio_start <label> [extra docker args]: a store on its own port. A label
# is a region (primary, dr), or just "store".
dr_minio_start() {
  local label="minio-$1" port
  shift
  # A replacement store comes up on the address the lost one had: DR_MINIO_PORT.
  port="${DR_MINIO_PORT:-$(dr_free_port)}"
  DR_MINIO_PORT=""
  if ! docker image inspect "$DR_MINIO_IMAGE" > /dev/null 2>&1; then
    log "building the lab's MinIO image $DR_MINIO_IMAGE"
    docker build -q -t "$DR_MINIO_IMAGE" "$DR_REPO/docker/minio" > /dev/null || fail "could not build $DR_MINIO_IMAGE"
  fi
  dr_run "$label" "$port" --memory=1g -e "MINIO_ROOT_USER=$DR_S3_USER" -e "MINIO_ROOT_PASSWORD=$DR_S3_PASSWORD" \
    -e HOME=/tmp "$@" "$DR_MINIO_IMAGE" server /data --address ":$port" --console-address ":$(dr_free_port)"
  dr_wait_minio "$label"
}
dr_wait_minio() { # <label of minio-*>
  local label="minio-${1#minio-}" port
  port="${DR_PORT[$label]}"
  wait_until 60 "MinIO $label to be live" curl -fs -o /dev/null "http://127.0.0.1:$port/minio/health/live"
  dr_mc "${label#minio-}" alias set local "http://127.0.0.1:$port" "$DR_S3_USER" "$DR_S3_PASSWORD" > /dev/null
}
# dr_mc <region> <mc args...>: the store's own client, in its container.
dr_mc() {
  local label="minio-$1"
  shift
  docker exec -e HOME=/tmp "${DR_CONT[$label]}" mc "$@"
}
dr_s3_endpoint() { echo "http://127.0.0.1:${DR_PORT[minio-$1]}"; }

# dr_bucket_make <region> <bucket> [versioning]
dr_bucket_make() {
  dr_mc "$1" mb --ignore-existing "local/$2" > /dev/null
  [[ "${3:-}" != versioning ]] || dr_mc "$1" version enable "local/$2" > /dev/null
}

# dr_readback <region> <bucket> <prefix> <dest>: copy every object under the
# prefix out of the store, as a tar on stdout, so that nothing is mounted and
# it works from a runner and from a container driving the host's docker alike.
dr_readback() {
  local region="$1" bucket="$2" prefix="$3" dest="$4"
  rm -rf "$dest"
  mkdir -p "$dest"
  docker exec -e HOME=/tmp "${DR_CONT[minio-$region]}" sh -c \
    "rm -rf /tmp/rb && mkdir -p /tmp/rb && mc mirror --quiet 'local/$bucket/$prefix' /tmp/rb > /dev/null && tar -C /tmp/rb -cf - ." \
    2> "$dest.mc.log" | tar -C "$dest" -xf - || fail "could not copy s3://$bucket/$prefix out of the $region store: $(tail -n 5 "$dest.mc.log")"
}

# dr_count <readback dir> <record prefix> <expected records>: from the objects
# alone, which of the expected records the archive holds and which twice. Sets
# DR_FOUND, DR_UNIQUE, DR_MISSING (a count), DR_MISSING_FIRST (the first few),
# DR_DUPLICATES (extra copies).
dr_count() {
  local json
  json="$(python3 "$DR_CHECKS" records "$1" "$2" "$3")" || fail "could not count the records in $1"
  DR_FOUND="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["found"])' "$json")"
  DR_UNIQUE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["unique"])' "$json")"
  DR_MISSING="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["missing"])' "$json")"
  DR_MISSING_FIRST="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["missing_first"])' "$json")"
  DR_DUPLICATES="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["duplicates"])' "$json")"
  DR_OBJECTS="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["data_objects"])' "$json")"
}

# dr_mc_pipe <region> <target>: the object at <target> becomes stdin.
dr_mc_pipe() {
  docker exec -i -e HOME=/tmp "${DR_CONT[minio-$1]}" mc pipe "$2" > /dev/null
}
# dr_count_committed <readback dir> <prefix> <keys file> <record prefix> <expected>:
# dr_count over the objects the keys file names (one full key per line) alone.
dr_count_committed() {
  local json
  json="$(python3 "$DR_CHECKS" records-of "$1" "$4" "$5" "$3" "$2/")" || fail "could not count the records in $1"
  DR_FOUND="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["found"])' "$json")"
  DR_UNIQUE="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["unique"])' "$json")"
  DR_MISSING="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["missing"])' "$json")"
  DR_DUPLICATES="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["duplicates"])' "$json")"
}

# ---- PostgreSQL ---------------------------------------------------------------
# dr_pg_start <label> [extra docker args]: a ledger server. Sets DR_PG (the
# label in use) and the VTOP_STATE_STORE the engine and vtopctl read.
dr_pg_start() {
  local label="pg-$1" port
  shift
  port="$(dr_free_port)"
  dr_run "$label" "$port" -e POSTGRES_USER=vtop -e POSTGRES_PASSWORD=vtop -e POSTGRES_DB=vtop \
    -e "PGPORT=$port" -e PGDATA=/pgdata "$@" "$DR_PG_IMAGE" -c "port=$port"
  dr_pg_use "${label#pg-}"
}
dr_pg_ready() {
  [[ "$(docker inspect -f '{{.State.Running}}' "${DR_CONT[pg-$DR_PG]}" 2> /dev/null || echo false)" == true ]] \
    || fail "the PostgreSQL server in ${DR_CONT[pg-$DR_PG]} exited"
  [[ "$(docker exec -e PGPASSWORD=vtop "${DR_CONT[pg-$DR_PG]}" psql --no-psqlrc -h 127.0.0.1 -U vtop -d vtop -tA \
    -c 'SELECT pg_is_in_recovery()' 2> /dev/null || true)" == f ]]
}
dr_pg_use() { # <label>
  DR_PG="${1#pg-}"
  wait_until 120 "PostgreSQL $DR_PG to take connections" dr_pg_ready
  DR_DB=vtop
  dr_pg_db vtop
}
# dr_pg_db <name>: the database the engine and dr_sql use from now on.
dr_pg_db() {
  DR_DB="$1"
  VTOP_STATE_STORE="postgres://vtop:vtop@127.0.0.1:${DR_PORT[pg-$DR_PG]}/$1?sslmode=disable"
  export VTOP_STATE_STORE
}
# dr_pg_admin <sql>: a statement against the server's maintenance database.
dr_pg_admin() {
  docker exec "${DR_CONT[pg-$DR_PG]}" psql --no-psqlrc -U vtop -d postgres -v ON_ERROR_STOP=1 -c "$1" > /dev/null
}
# dr_sql <sql>: the current ledger, tuples only. Fails when the query does.
dr_sql() {
  docker exec "${DR_CONT[pg-$DR_PG]}" psql --no-psqlrc -U vtop -d "${DR_DB:-vtop}" -tA -v ON_ERROR_STOP=1 -c "$1"
}
dr_sql_num() { local n; n="$(dr_sql "$1" 2> /dev/null | head -n 1 || true)"; echo "${n:-0}"; }

# ---- SQLite -------------------------------------------------------------------
# dr_sqlite <db> <sql>
dr_sqlite() { sqlite3 -cmd '.timeout 5000' "$1" "$2"; }
# dr_sqlite_num <db> <sql>: a number, 0 when the ledger or the table is not there yet.
dr_sqlite_num() { local n; n="$(dr_sqlite "$1" "$2" 2> /dev/null | head -n 1 || true)"; echo "${n:-0}"; }

# ---- Kafka --------------------------------------------------------------------
dr_kafka_start() {
  local label="kafka-$1" port ctl
  port="$(dr_free_port)"
  ctl="$(dr_free_port)"
  dr_run "$label" "$port" --memory=1500m \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
    -e "KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:$port,CONTROLLER://127.0.0.1:$ctl" \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$port" \
    -e "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:$ctl" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    "$DR_KAFKA_IMAGE"
  DR_KAFKA="$1"
  DR_KAFKA_BOOTSTRAP="127.0.0.1:$port"
  wait_until 120 "Kafka $1 to list topics" dr_kafka_up
  # A fresh broker creates the group coordinator on the first group operation;
  # until then a consumer gets NotCoordinator. Consume once to force it.
  dr_kafka_tool kafka-topics.sh --create --if-not-exists --topic dr_warmup --partitions 1 --replication-factor 1 > /dev/null
  echo warmup | docker exec -i "${DR_CONT[kafka-$1]}" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server "$DR_KAFKA_BOOTSTRAP" --topic dr_warmup > /dev/null
  timeout 90 docker exec "${DR_CONT[kafka-$1]}" /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$DR_KAFKA_BOOTSTRAP" --topic dr_warmup --group dr-warmup --from-beginning \
    --max-messages 1 --timeout-ms 60000 > /dev/null 2>&1 || fail "the Kafka group coordinator did not come up"
}
dr_kafka_up() {
  timeout 20 docker exec "${DR_CONT[kafka-$DR_KAFKA]}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$DR_KAFKA_BOOTSTRAP" --list > /dev/null 2>&1
}
dr_kafka_tool() { # <tool> <args...>
  local tool="$1"
  shift
  timeout 120 docker exec -i "${DR_CONT[kafka-$DR_KAFKA]}" "/opt/kafka/bin/$tool" --bootstrap-server "$DR_KAFKA_BOOTSTRAP" "$@"
}
# dr_kafka_produce <topic> <first> <count> <record prefix> <tag>: records
# numbered first .. first+count-1, "<prefix>NNNNNN payload <tag>".
dr_kafka_produce() {
  local topic="$1" first="$2" count="$3" prefix="$4" tag="$5" i
  for ((i = first; i < first + count; i++)); do
    printf '%s%06d payload for the disaster-recovery drill %s\n' "$prefix" "$i" "$tag"
  done | dr_kafka_tool kafka-console-producer.sh --topic "$topic" > /dev/null
}
# dr_kafka_trim <topic> <offset>: the broker no longer holds records below the
# offset, as when retention has expired them.
dr_kafka_trim() {
  printf '{"partitions":[{"topic":"%s","partition":0,"offset":%s}],"version":1}' "$1" "$2" \
    | docker exec -i "${DR_CONT[kafka-$DR_KAFKA]}" sh -c 'cat > /tmp/trim.json'
  dr_kafka_tool kafka-delete-records.sh --offset-json-file /tmp/trim.json > "$DR_LOGS/kafka-trim.log" 2>&1 \
    || fail "could not trim $1: $(tail -n 3 "$DR_LOGS/kafka-trim.log")"
}
dr_kafka_topic() { # <topic>
  dr_kafka_tool kafka-topics.sh --create --if-not-exists --topic "$1" --partitions 1 --replication-factor 1 > /dev/null
}
# dr_kafka_group_offset <group> <topic>: the committed offset, or - when none.
dr_kafka_group_offset() {
  local out
  out="$(dr_kafka_tool kafka-consumer-groups.sh --describe --group "$1" 2>&1)" || return 1
  awk -v t="$2" '$1 == $1 && $2 == t && $3 == 0 { print ($4 == "-" ? "-" : $4); found = 1 } END { if (!found) print "-" }' <<< "$out" | head -n 1
}

# ---- the engine ---------------------------------------------------------------
# dr_config <path> key=value...: the engine's config. Keys (defaults):
#   state       sqlite:<path> or env (default: env, VTOP_STATE_STORE)
#   work        the work directory
#   digests     engine.ledger_digest_dir (default: not set)
#   source      kafka | file                 topic= group= bootstrap= | input=
#   endpoint    the S3 endpoint     bucket=  prefix=
#   checksum    sha256 (default) | blake3
#   batch       records per batch (default 20)
#   versioning  true: upload.require_object_versioning
#   manifest    extra YAML placed under `manifest:` (indented by the caller)
#   timeout     upload.request_timeout_seconds (default: the engine's)
#   top         extra YAML lines at the top level (for example manifest_mac_key_env)
#   tenant      engine.tenant (default drill)
dr_config() {
  local path="$1"
  shift
  local -A c=([state]=env [source]=file [checksum]=sha256 [batch]=20 [versioning]=false [digests]="" [manifest]="" [timeout]="" [tenant]=drill [top]="")
  local kv
  for kv in "$@"; do c["${kv%%=*}"]="${kv#*=}"; done
  local state_line source_block digests_line="" versioning_line="" manifest_block="" timeout_line=""
  if [[ "${c[state]}" == env ]]; then state_line='{env: VTOP_STATE_STORE}'; else state_line="\"${c[state]}\""; fi
  [[ -z "${c[digests]}" ]] || digests_line="  ledger_digest_dir: \"${c[digests]}\""
  [[ "${c[versioning]}" != true ]] || versioning_line="  require_object_versioning: true"
  [[ -z "${c[timeout]}" ]] || timeout_line="  request_timeout_seconds: ${c[timeout]}"
  [[ -z "${c[manifest]}" ]] || manifest_block="manifest:
${c[manifest]}"
  if [[ "${c[source]}" == kafka ]]; then
    source_block="  kafka:
    enabled: true
    bootstrap_servers: [\"${c[bootstrap]}\"]
    consumer_group: \"${c[group]}\"
    topic_include_regex: \"^${c[topic]}\$\"
    auto_offset_reset: earliest
    enable_auto_commit: false"
  else
    source_block="  file:
    enabled: true
    paths: [\"${c[input]}\"]"
  fi
  cat > "$path" << YAML
engine:
  name: dr-drill
  tenant: ${c[tenant]}
  state_store: $state_line
  work_dir: "${c[work]}"
${digests_line}
  log_level: info
batching:
  max_records: ${c[batch]}
  max_bytes: 104857600
  max_batch_age_seconds: 2
  source_poll_wait_ms: 250
  idle_poll_interval_ms: 300
  max_concurrent_batches: 1
compression:
  type: gzip
  level: 1
checksum:
  algorithm: ${c[checksum]}
sources:
$source_block
upload:
  backend: s3_native
  bucket: "${c[bucket]}"
  prefix: "${c[prefix]}"
  endpoint_url: "${c[endpoint]}"
  region: us-east-1
  force_path_style: true
  verify_tls: false
  create_bucket: false
  require_strong_verification: true
${versioning_line}
${timeout_line}
${manifest_block}
${c[top]}
YAML
}

dr_engine_start() { # <config> [extra env as NAME=value...]
  local config="$1"
  shift
  DR_ENGINE_RUN=$((DR_ENGINE_RUN + 1))
  # In a lab that will be handed over, the engine must outlive this script.
  if [[ $DR_MODE == handover ]]; then
    nohup env "$@" "$DR_BIN" run --config "$config" > "$DR_LOGS/engine-$DR_ENGINE_RUN.log" 2>&1 &
  else
    env "$@" "$DR_BIN" run --config "$config" > "$DR_LOGS/engine-$DR_ENGINE_RUN.log" 2>&1 &
  fi
  DR_ENGINE_PID=$!
}
dr_engine_log() { echo "$DR_LOGS/engine-$DR_ENGINE_RUN.log"; }
dr_engine_kill() { # a crash, not a shutdown
  kill -9 "$DR_ENGINE_PID" 2> /dev/null || true
  wait "$DR_ENGINE_PID" 2> /dev/null || true
  DR_ENGINE_PID=""
}
dr_engine_stop() { # SIGTERM: stop after the pass and flush; exit 0
  local pid="$DR_ENGINE_PID" rc=0
  DR_ENGINE_PID=""
  kill -TERM "$pid" 2> /dev/null || true
  wait "$pid" || rc=$?
  [[ $rc -eq 0 ]] || fail "the engine exited $rc on SIGTERM; see $DR_LOGS/engine-$DR_ENGINE_RUN.log"
}
# dr_engine_stop_any: SIGTERM, and whatever the engine exits with is DR_RC. A
# stop while the store is down exits non-zero ("shutdown flush incomplete":
# the unflushed batches are read again on the next start).
dr_engine_stop_any() {
  local pid="$DR_ENGINE_PID"
  DR_ENGINE_PID=""
  DR_RC=0
  kill -TERM "$pid" 2> /dev/null || true
  wait "$pid" || DR_RC=$?
}
# dr_bg <log> <command...>: a background process of the lab, its output in <log>;
# DR_BG_PID is its pid. In a lab that will be handed over it outlives this script.
dr_bg() {
  local out="$1"
  shift
  if [[ $DR_MODE == handover ]]; then
    nohup "$@" > "$out" 2>&1 &
  else
    "$@" > "$out" 2>&1 &
  fi
  DR_BG_PID=$!
}
# dr_engine_stop_detached: SIGTERM an engine this shell did not start (the lab
# was handed over with it running), and wait for it to exit.
dr_engine_stop_detached() {
  local pid="$DR_ENGINE_PID" i
  DR_ENGINE_PID=""
  dr_is_engine_pid "$pid" || return 0
  kill -TERM "$pid" 2> /dev/null || return 0
  for ((i = 0; i < 100; i++)); do
    kill -0 "$pid" 2> /dev/null || return 0
    sleep 0.3
  done
  kill -9 "$pid" 2> /dev/null || true
}
dr_engine_alive() { [[ -n "$DR_ENGINE_PID" ]] && kill -0 "$DR_ENGINE_PID" 2> /dev/null; }

# dr_ctl <label> <args...>: a vtopctl command whose output is kept; fails the
# drill if it exits non-zero.
dr_ctl() {
  local label="$1"
  shift
  "$DR_BIN" "$@" > "$DR_LOGS/$label.log" 2>&1 || fail "vtopctl $* failed (exit $?); see $DR_LOGS/$label.log"
}
# dr_ctl_rc <label> <args...>: the same, and the exit code is DR_RC.
dr_ctl_rc() {
  local label="$1"
  shift
  DR_RC=0
  "$DR_BIN" "$@" > "$DR_LOGS/$label.log" 2>&1 || DR_RC=$?
}

# dr_checks <label> <config> <digest-dir|-> [extra scrub args]: the three commands
# docs/operate/backup-and-recovery.md says to run after a restore. Sets
# DR_AUDIT_RC, DR_SCRUB_RC and DR_VERIFY_RC; the logs are $DR_LOGS/<label>-*.log.
dr_checks() {
  local label="$1" config="$2" digests="$3"
  shift 3
  local -a d=()
  [[ "$digests" == - ]] || d=(--digest-dir "$digests")
  dr_ctl_rc "$label-audit-coverage" audit-coverage --config "$config" ${d[@]+"${d[@]}"} --check-storage
  DR_AUDIT_RC=$DR_RC
  dr_ctl_rc "$label-scrub" scrub --config "$config" ${d[@]+"${d[@]}"} "$@"
  DR_SCRUB_RC=$DR_RC
  dr_ctl_rc "$label-audit-verify" audit verify --config "$config"
  DR_VERIFY_RC=$DR_RC
}
