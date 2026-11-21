#!/usr/bin/env bats
# Tests for the leader waits in scripts/live-chaos/lib.sh (review of #758).
#
# Scenario 22 restarts an old voter that leads, and has to wait for an
# upgraded node to win the election first. After the leader is killed, its
# followers keep naming it until their election timeout runs out, and
# wait_meta_leader took the first leader any node named: the dead one, at
# once. These tests drive the waits against a fake `meta status` whose
# answers per node are scripted, one line per call (the last line repeats).

setup() {
    WORK="$(mktemp -d)"
    export WORK
}

teardown() {
    rm -rf "$WORK"
}

# answers <node> <answer>... — what node <node> reports as current_leader,
# one call at a time; "down" makes the call fail, as for a killed node.
answers() {
    local node="$1"
    shift
    printf '%s\n' "$@" >"$WORK/node$node"
}

# Run <function and args> with lib.sh sourced and meta_status_field faked.
run_wait() {
    run bash -c '
        SCENARIO=leader-test
        export CHAOS_ELECTION_TIMEOUT_SECONDS=3
        source "$1/scripts/live-chaos/lib.sh"
        trap - EXIT
        meta_status_field() {
            local file="$WORK/node$1" answer
            [[ -f "$file" ]] || return 1
            answer="$(head -n 1 "$file")"
            if [[ "$(wc -l <"$file")" -gt 1 ]]; then
                tail -n +2 "$file" >"$file.next" && mv "$file.next" "$file"
            fi
            [[ "$answer" != down ]] || return 1
            echo "$answer"
        }
        sleep() { :; }
        shift
        "$@"
    ' _ "$BATS_TEST_DIRNAME/../.." "$@"
}

@test "wait_meta_leader names the killed leader while a follower still does" {
    answers 3 down
    answers 1 3 3 3 2
    answers 2 null null 2
    run_wait wait_meta_leader 1 2 3
    [ "$status" -eq 0 ]
    [ "$output" = "3" ]
}

@test "wait_meta_leader_except waits for another node to win and name itself" {
    answers 3 down
    # Node 1 names the dead leader, then node 2 before node 2 has won;
    # node 2 names itself only from its third answer.
    answers 1 3 3 2 2 2 2
    answers 2 null null null 2
    run_wait wait_meta_leader_except 3 1 2
    [ "$status" -eq 0 ]
    [ "$output" = "2" ]
}

@test "wait_meta_leader_except never returns the excluded node" {
    answers 3 down
    answers 1 3
    answers 2 3
    run_wait wait_meta_leader_except 3 1 2
    [ "$status" -ne 0 ]
    [[ "$output" == *"no meta leader other than node 3 among nodes 1 2"* ]]
    [[ "$output" == *"last reported: 3"* ]]
}

@test "wait_meta_leader_except does not take a leader outside the nodes it asks" {
    answers 1 4
    answers 2 4
    answers 4 4
    run_wait wait_meta_leader_except 3 1 2
    [ "$status" -ne 0 ]
}

@test "wait_meta_leader_except does not take a leader that does not name itself" {
    answers 3 down
    answers 1 2
    answers 2 null
    run_wait wait_meta_leader_except 3 1 2
    [ "$status" -ne 0 ]
    [[ "$output" == *"last reported: 2"* ]]
}
