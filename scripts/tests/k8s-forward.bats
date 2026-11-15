#!/usr/bin/env bats
# Tests for scripts/k8s-forward.sh, the port-forward helpers of
# scripts/k8s-smoke.sh (#786).
#
# The helm job used to fail with curl exit 7 when `kubectl port-forward` was
# not listening yet after a fixed 4 s sleep. These tests drive the helpers
# against a fake kubectl that listens late, exits once before listening, or
# never listens, and check that a forward is only returned once its port
# accepts, that one which exited is started again, and that one which never
# listens fails within the bound with the pod, the port and kubectl's own
# output in the message.

setup() {
    WORK="$(mktemp -d)"
    FAKE_STATE="$WORK/fake"
    mkdir -p "$WORK/bin" "$FAKE_STATE"
    export FAKE_STATE
    # A fake kubectl: `kubectl -n <ns> port-forward pod/<pod> <local>:<remote>`.
    # FAKE_MODE says how it behaves; every start is counted.
    cat >"$WORK/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
# Close bats' fd 3, or bats waits for this process to exit.
exec 3>&-
echo start >>"$FAKE_STATE/starts"
local_port="${5%%:*}"
case "${FAKE_MODE:-late}" in
  late)
    sleep "${FAKE_DELAY:-0}"
    echo "Forwarding from 127.0.0.1:${local_port} -> ${5##*:}"
    exec nc -lk -p "$local_port" -e true
    ;;
  die-once)
    if [ "$(wc -l <"$FAKE_STATE/starts")" -eq 1 ]; then
      echo "error: unable to forward port because pod is not running. Current status=Pending"
      exit 1
    fi
    exec nc -lk -p "$local_port" -e true
    ;;
  never)
    echo "error: unable to listen on port ${local_port}: address already in use"
    exec sleep 600
    ;;
esac
EOF
    chmod +x "$WORK/bin/kubectl"
    PATH="$WORK/bin:$PATH"
    FORWARDS=""
    PORT=$((20000 + RANDOM % 20000))
    # shellcheck source=scripts/k8s-forward.sh
    . "${BATS_TEST_DIRNAME}/../k8s-forward.sh"
}

teardown() {
    for pid in ${FORWARDS:-}; do
        kill "$pid" 2>/dev/null || true
    done
    rm -rf "$WORK"
}

# Runs a helper in THIS shell, not bats' `run` subshell, so the pids it adds
# to FORWARDS reach teardown. Sets status and output as `run` does.
call() {
    status=0
    "$@" 2>"$WORK/stderr" || status=$?
    output="$(cat "$WORK/stderr")"
}

starts() {
    wc -l <"$FAKE_STATE/starts" | tr -d ' '
}

@test "a forward that starts listening after the old fixed 4 s is waited for" {
    export FAKE_MODE=late FAKE_DELAY=6
    FORWARD_WAIT_SECONDS=30
    begin=$SECONDS
    call forward_open vtop-smoke vtop-0 "$PORT" 9500
    [ "$status" -eq 0 ]
    # It returned only once the port accepted, and no earlier than the
    # forward began listening.
    forward_port_open "$PORT"
    [ $((SECONDS - begin)) -ge 6 ]
}

@test "a forward whose kubectl exited is started again on the same port" {
    export FAKE_MODE=die-once
    FORWARD_WAIT_SECONDS=30
    call forward_open vtop-smoke vtop-0 "$PORT" 9500
    [ "$status" -eq 0 ]
    [ "$(starts)" -eq 2 ]
    forward_port_open "$PORT"
}

@test "a forward that never listens fails within the bound, naming the pod, the port and kubectl's output" {
    export FAKE_MODE=never
    FORWARD_WAIT_SECONDS=3
    begin=$SECONDS
    call forward_open vtop-smoke vtop-1 "$PORT" 9500
    [ "$status" -eq 1 ]
    [ $((SECONDS - begin)) -lt 10 ]
    [[ "$output" == *"pod/vtop-1 port 9500 in namespace vtop-smoke"* ]]
    [[ "$output" == *"localhost:$PORT"* ]]
    [[ "$output" == *"address already in use"* ]]
    [[ "$output" == *"was stopped"* ]]
}

@test "a supervised forward that never listens warns and lets the caller's retries decide" {
    export FAKE_MODE=never
    FORWARD_WAIT_SECONDS=2
    call forward_supervised_open vtop-smoke-replicated vtop-meta-0 "$PORT" 9200
    [ "$status" -eq 0 ]
    [[ "$output" == *"pod/vtop-meta-0 port 9200"* ]]
    [[ "$output" == *"continuing"* ]]
}

@test "a supervised forward that listens late is waited for" {
    export FAKE_MODE=late FAKE_DELAY=5
    FORWARD_WAIT_SECONDS=30
    call forward_supervised_open vtop-smoke-replicated vtop-meta-0 "$PORT" 9200
    [ "$status" -eq 0 ]
    forward_port_open "$PORT"
}

# Review of #789: the replicated `meta init` loop counted attempts, and each
# attempt used to cost a fixed 4 s settle. With the settle replaced by the
# readiness wait, an init that fails at once burned all its attempts in well
# under a minute instead of the six the loop's comment budgets for a cold
# kind install. `retry_for` bounds the loop by elapsed time instead.

# Fails until it has been called $1 times, counting calls in $WORK/tries.
fail_until() {
    echo try >>"$WORK/tries"
    [ "$(wc -l <"$WORK/tries")" -ge "$1" ]
}

tries() {
    wc -l <"$WORK/tries" | tr -d ' '
}

@test "an attempt that fails at once is retried for the whole window, not a count" {
    begin=$SECONDS
    call retry_for 4 1 false
    [ "$status" -eq 1 ]
    [ $((SECONDS - begin)) -ge 4 ]
    [ $((SECONDS - begin)) -lt 10 ]
}

@test "retry_for pauses after a failed attempt and stops at the first success" {
    begin=$SECONDS
    call retry_for 30 1 fail_until 3
    [ "$status" -eq 0 ]
    [ "$(tries)" -eq 3 ]
    # Two pauses of one second: not a busy loop, and no wait after success.
    [ $((SECONDS - begin)) -ge 2 ]
    [ $((SECONDS - begin)) -lt 10 ]
}
