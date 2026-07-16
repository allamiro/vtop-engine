#!/usr/bin/env bats
# Tests for docker/seed-events.sh.
#
# These target the classes of bug that have already shipped from this script:
#   * `pick()` indexing off the wrong array length (returned empty values);
#   * a port off-by-one (`RANDOM % 65535` can never yield 65535).
# Plus the basic contract: the right number of lines, in the requested format.

setup() {
    SEED="${BATS_TEST_DIRNAME}/../seed-events.sh"
}

@test "emits the requested number of lines" {
    run bash "$SEED" cef 7
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 7 ]
}

@test "defaults to 50 lines when count is omitted" {
    run bash "$SEED" json
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 50 ]
}

@test "cef output is CEF-shaped" {
    run bash "$SEED" cef 5
    [ "$status" -eq 0 ]
    for l in "${lines[@]}"; do
        [[ "$l" == CEF:0\|* ]]
    done
}

@test "leef output is LEEF-shaped and syslog-framed" {
    # LEEF is deliberately emitted syslog-framed (<pri> ts host LEEF:1.0|...),
    # which is what real collectors send and what the engine's syslog-wrapped
    # LEEF detection expects. Assert both halves of that contract.
    run bash "$SEED" leef 5
    [ "$status" -eq 0 ]
    for l in "${lines[@]}"; do
        [[ "$l" =~ ^\<[0-9]+\> ]]
        [[ "$l" == *"LEEF:1.0|"* ]]
    done
}

@test "json output is one self-contained object per line" {
    # No python/jq in the bats image, so assert structurally: each line is a
    # complete object with the expected keys and no stray newline splitting.
    run bash "$SEED" json 5
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 5 ]
    for l in "${lines[@]}"; do
        [[ "$l" == \{*\} ]]
        [[ "$l" == *'"ts":'* ]]
        [[ "$l" == *'"event":'* ]]
    done
}

@test "syslog output starts with a priority value" {
    run bash "$SEED" syslog 5
    [ "$status" -eq 0 ]
    for l in "${lines[@]}"; do
        [[ "$l" =~ ^\<[0-9]+\> ]]
    done
}

# Regression: pick() previously indexed with the wrong length and could emit
# empty fields. Nothing should ever render as an empty or literal-unset value.
#
# NOTE: a bare `! grep ...` does NOT fail a Bats test (the negation suppresses
# set -e), which would make this assertion a no-op. Use an explicit `return 1`.
@test "no empty or unset substitutions leak into output" {
    run bash "$SEED" cef 40
    [ "$status" -eq 0 ]
    if grep -qE '(=[[:space:]]|=$|=\|)' <<< "$output"; then
        echo "empty substitution leaked into output:" >&2
        echo "$output" >&2
        return 1
    fi
    if grep -q 'unbound variable' <<< "$output"; then
        echo "unbound variable leaked into output" >&2
        return 1
    fi
}

# Regression: ports were drawn with `RANDOM % 65535`, which can never produce
# 65535 and is a classic off-by-one. Assert every port is in the valid range.
@test "generated ports are within 0..65535" {
    run bash "$SEED" cef 60
    [ "$status" -eq 0 ]
    while IFS= read -r port; do
        [ "$port" -ge 0 ]
        [ "$port" -le 65535 ]
    done < <(grep -oE '\b(spt|dpt)=[0-9]+' <<< "$output" | cut -d= -f2)
}

@test "mixed format yields more than one distinct shape" {
    run bash "$SEED" mixed 80
    [ "$status" -eq 0 ]
    shapes=0
    grep -q '^CEF:0|' <<< "$output" && shapes=$((shapes + 1))
    grep -q '^LEEF:' <<< "$output" && shapes=$((shapes + 1))
    grep -q '^{' <<< "$output" && shapes=$((shapes + 1))
    grep -qE '^<[0-9]+>' <<< "$output" && shapes=$((shapes + 1))
    [ "$shapes" -ge 2 ]
}

@test "unknown format is rejected rather than emitting garbage" {
    run bash "$SEED" definitely-not-a-format 3
    [ "$status" -ne 0 ]
}
