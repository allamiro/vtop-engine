#!/usr/bin/env bash
# The metadata cell (#743): a metadata voter's directory (the Raft log, the
# snapshot, the applied, purged and hard-state files) between a released
# vtop-node and this build.
#
#   upgrade  the RELEASE writes the directory (scripts/compat/meta-scenario.sh,
#            raised to the highest feature level it accepts) and THIS BUILD's
#            vtop-node opens it: it must report what the release reported (the
#            applied index, the level, the voter), answer a compare-and-swap
#            on a record only the replayed state can satisfy, raise the level
#            to this build's, and come back after a SIGKILL;
#   forward  THIS BUILD writes the directory, twice (the level left at its
#            baseline, which every release must open, as a rollback before
#            any raise is the way back; and raised to this build's highest),
#            and the RELEASE's vtop-node opens each. Expectation `pass`: it
#            opens, reports the applied index, accepts a write and comes back
#            after a SIGKILL. A release below the raised level reports the
#            lower level (it applies a raise above what it supports as a
#            rejection), which the log says. Expectation `refuse`: it exits
#            non-zero with <message> (a newer format, not corruption) and the
#            directory is byte for byte what it was.
#
# One voter on plaintext loopback; a directory written with 5100 entries holds
# a snapshot, which the shorter ones do not (COMPAT_META_ENTRIES to shorten a
# local run). The released binaries are the published ones fetch-release.sh
# verified; this build's directory needs vtop-node beside vtopctl.
#
# Usage: meta.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]
set -euo pipefail
[[ $# -ge 6 ]] || { echo "usage: meta.sh <tag> <direction> <expect> <released-bin-dir> <head-bin-dir> <workdir> [message]" >&2; exit 2; }
TAG="$1" DIRECTION="$2" EXPECT="$3" OLD="$4" NEW="$5" WORKDIR="$6" MESSAGE="${7:-}"
CELL=meta
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENTRIES="${COMPAT_META_ENTRIES:-5100}"
PORT="${COMPAT_META_PORT:-9100}"
# The newest level this build writes; a raise beyond it is the operator's.
HEAD_LEVEL="${COMPAT_META_HEAD_LEVEL:-}"

log() { echo "[compat/$CELL/$DIRECTION/$TAG] $*" >&2; }
fail() { echo "[compat/$CELL/$DIRECTION/$TAG] FAIL: $*" >&2; exit 1; }

for bin in "$OLD/vtop-node" "$OLD/vtopctl" "$NEW/vtop-node" "$NEW/vtopctl"; do
  [[ -x "$bin" ]] || fail "$bin is not executable"
done
case "$EXPECT" in
  pass) ;;
  refuse) [[ "$DIRECTION" == forward && -n "$MESSAGE" ]] || fail "a refuse expectation is for the forward direction and needs the message to find" ;;
  *) fail "the metadata cell expects pass or refuse, not $EXPECT" ;;
esac

rm -rf "${WORKDIR:?}"
mkdir -p "$WORKDIR"
NODE_PID=
NODE_EXIT=
trap '[[ -z "$NODE_PID" ]] || kill -9 "$NODE_PID" 2> /dev/null || true' EXIT

expect_value() { awk -v k="$2" '$1 == k { print $2 }' "$1/expect.txt"; }

# digest <dir>: a hash of every file in a directory, names included.
digest() {
  (cd "$1" && find . -type f | LC_ALL=C sort | while read -r f; do
    printf '%s ' "$f"
    shasum -a 256 "$f" | awk '{ print $1 }'
  done) | shasum -a 256 | awk '{ print $1 }'
}

# json <file> <python expression over d>
json() { python3 -I -c 'import json,sys; d=json.load(sys.stdin); print('"$2"')' < "$1"; }

# start <vtop-node> <scenario-dir> <log-name>: run the voter on the directory.
# Returns 0 once it is ready, 1 if it exited first (the log says why), with
# its exit status in NODE_EXIT: a refusal is a non-zero exit, and a node that
# printed the refusal's message and exited 0 must not pass for one.
start() {
  local node="$1" dir="$2" name="$3"
  "$node" meta --config "$dir/meta.yaml" > "$dir/logs/$name.log" 2>&1 &
  NODE_PID=$!
  local _
  for _ in $(seq 1 150); do
    grep -q '^meta_node_ready' "$dir/logs/$name.log" 2> /dev/null && return 0
    kill -0 "$NODE_PID" 2> /dev/null || {
      NODE_EXIT=0
      wait "$NODE_PID" 2> /dev/null || NODE_EXIT=$?
      NODE_PID=
      return 1
    }
    sleep 0.1
  done
  fail "$name: the node neither became ready nor exited"
}
stop() {
  kill -9 "$NODE_PID" 2> /dev/null || true
  wait "$NODE_PID" 2> /dev/null || true
  NODE_PID=
}

# ctl <vtopctl> <scenario-dir> <name> <args...>: `vtopctl meta`, output kept.
ctl() {
  local bin="$1" dir="$2" name="$3"
  shift 3
  "$bin" --json meta "$@" --config "$dir/admin.yaml" > "$dir/logs/$name.out" 2> "$dir/logs/$name.err"
}
must_ctl() {
  ctl "$@" || { cat "$2/logs/$3.err" >&2; fail "vtopctl meta ${*:4} failed"; }
}

# probe <vtop-node> <vtopctl> <scenario-dir> <label>: open the directory with
# this node, check what it reports against what the writer wrote, compare and
# swap a record, and leave the voter running. Sets READ_LEVEL.
probe() {
  local node="$1" cli="$2" dir="$3" label="$4" want_index want_generation
  want_index="$(expect_value "$dir" last_applied_index)"
  want_generation="$(expect_value "$dir" node_generation)"
  start "$node" "$dir" "$label" \
    || { sed 's/^/    /' "$dir/logs/$label.log" >&2; fail "$label: the voter did not start on the directory"; }
  must_ctl "$cli" "$dir" "$label-status" status
  [[ "$(json "$dir/logs/$label-status.out" 'd["last_applied"]["index"]')" -ge "$want_index" ]] \
    || fail "$label: the voter applied less than the $want_index entries the writer reported"
  [[ "$(json "$dir/logs/$label-status.out" 'd["membership"]["voters"]')" == "[1]" ]] \
    || fail "$label: the voter set is not [1]"
  must_ctl "$cli" "$dir" "$label-versions" node-versions
  READ_LEVEL="$(json "$dir/logs/$label-versions.out" 'd["feature_level"]')"
  # The generation is a compare-and-swap token: only a state that replayed the
  # log (and the snapshot below it) to the writer's last record can accept it.
  must_ctl "$cli" "$dir" "$label-cas" register-node \
    --node-uuid "$(expect_value "$dir" node_uuid)" --addr "$(expect_value "$dir" node_addr)" \
    --expected-generation "$want_generation" --request-id "$(printf 'eeeeeeee-0000-0000-0000-%012x' "$RANDOM")"
  grep -q "Ack { generation: $((want_generation + 1)) }" "$dir/logs/$label-cas.out" \
    || { cat "$dir/logs/$label-cas.out" >&2; fail "$label: the node record is not at generation $want_generation"; }
}

# restarted <vtop-node> <vtopctl> <scenario-dir> <label>: SIGKILL, open again,
# and see the write that was acknowledged.
restarted() {
  local node="$1" cli="$2" dir="$3" label="$4" want_generation
  want_generation="$(expect_value "$dir" node_generation)"
  stop
  start "$node" "$dir" "$label" \
    || { sed 's/^/    /' "$dir/logs/$label.log" >&2; fail "$label: the voter did not restart on its own directory"; }
  must_ctl "$cli" "$dir" "$label-cas" register-node \
    --node-uuid "$(expect_value "$dir" node_uuid)" --addr "$(expect_value "$dir" node_addr)" \
    --expected-generation "$((want_generation + 1))" --request-id "$(printf 'eeeeeeee-0000-0000-0001-%012x' "$RANDOM")"
  grep -q "Ack { generation: $((want_generation + 2)) }" "$dir/logs/$label-cas.out" \
    || { cat "$dir/logs/$label-cas.out" >&2; fail "$label: an acknowledged write did not survive a SIGKILL"; }
  stop
}

write() { # <bin-dir> <name> <level>: the scenario, into $WORKDIR/<name>
  META_PORT_BASE="$PORT" LEVEL="$3" ENTRIES="$ENTRIES" \
    "$here/../meta-scenario.sh" "$1/vtop-node" "$1/vtopctl" "$WORKDIR/$2" \
    || fail "$1 could not write the directory ($2)"
}

case "$DIRECTION" in
  upgrade)
    write "$OLD" released max
    dir="$WORKDIR/released"
    written_level="$(expect_value "$dir" feature_level)"
    [[ "$ENTRIES" -lt 5000 || -n "$(find "$dir/data" -name 'snapshot-*.vmsnap' -print -quit)" ]] \
      || fail "the release left no snapshot in a directory of $ENTRIES entries"
    log "the release wrote $(expect_value "$dir" last_applied_index) entries at level $written_level"
    probe "$NEW/vtop-node" "$NEW/vtopctl" "$dir" head
    [[ "$READ_LEVEL" == "$written_level" ]] \
      || fail "this build reads feature level $READ_LEVEL from a directory written at $written_level"
    # The operator's step: raise to this build's level on the migrated voter.
    if [[ -z "$HEAD_LEVEL" ]]; then
      # vtopctl refuses a level above the ones it knows, and says which.
      HEAD_LEVEL="$("$NEW/vtopctl" meta enable-features --level 999 --config "$dir/admin.yaml" 2>&1 \
        | sed -n 's/.*feature levels up to \([0-9][0-9]*\).*/\1/p' | head -1 || true)"
      [[ -n "$HEAD_LEVEL" ]] || fail "this build's vtopctl did not say which feature levels it knows"
    fi
    if [[ "$HEAD_LEVEL" -gt "$written_level" ]]; then
      must_ctl "$NEW/vtopctl" "$dir" head-raise enable-features --level "$HEAD_LEVEL"
      must_ctl "$NEW/vtopctl" "$dir" head-raised node-versions
      [[ "$(json "$dir/logs/head-raised.out" 'd["feature_level"]')" == "$HEAD_LEVEL" ]] \
        || fail "this build could not raise a migrated directory to level $HEAD_LEVEL"
    fi
    restarted "$NEW/vtop-node" "$NEW/vtopctl" "$dir" head-restart
    log "ok: opened, read level $READ_LEVEL, swapped a record, raised, restarted"
    ;;
  forward)
    for variant in baseline raised; do
      level=0
      [[ "$variant" == baseline ]] || level=max
      write "$NEW" "head-$variant" "$level"
      dir="$WORKDIR/head-$variant"
      written_level="$(expect_value "$dir" feature_level)"
      if [[ "$EXPECT" == refuse && "$variant" == raised ]]; then
        before="$(digest "$dir/data")"
        if start "$OLD/vtop-node" "$dir" released-refuses; then
          stop
          fail "the release opened the raised directory; it was expected to refuse"
        fi
        [[ "$NODE_EXIT" -ne 0 ]] \
          || { sed 's/^/    /' "$dir/logs/released-refuses.log" >&2; fail "the release exited 0 on the raised directory without becoming ready; a refusal exits non-zero"; }
        grep -q -- "$MESSAGE" "$dir/logs/released-refuses.log" \
          || { sed 's/^/    /' "$dir/logs/released-refuses.log" >&2; fail "the release's refusal does not say: $MESSAGE"; }
        [[ "$(digest "$dir/data")" == "$before" ]] || fail "the refusal changed the directory"
        log "ok: the release refused the raised directory (level $written_level) and changed nothing"
        continue
      fi
      probe "$OLD/vtop-node" "$OLD/vtopctl" "$dir" "released-$variant"
      # Never above what was written; below it only for a release that does
      # not support the level, which applies the raise as a rejection.
      [[ "$READ_LEVEL" -le "$written_level" ]] \
        || fail "the release reads feature level $READ_LEVEL from a directory written at $written_level"
      restarted "$OLD/vtop-node" "$OLD/vtopctl" "$dir" "released-$variant-restart"
      log "ok: $variant directory (level $written_level) opened by the release, which reads level $READ_LEVEL"
    done
    ;;
  *) fail "no direction $DIRECTION" ;;
esac
