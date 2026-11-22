#!/usr/bin/env bats
# Tests for scripts/s3-lab.sh (#722): the parts every S3 compatibility leg
# relies on that need neither Docker nor a server. The script finds the
# repository from its own path, so each test runs a copy in a scratch tree.

setup() {
    ROOT="$(mktemp -d)"
    mkdir -p "$ROOT/scripts" "$ROOT/docker/s3-servers/demo" "$ROOT/tests/s3-compat/servers"
    cp "${BATS_TEST_DIRNAME}/../s3-lab.sh" "$ROOT/scripts/"
    LAB="$ROOT/scripts/s3-lab.sh"
    printf 'services: {}\n' > "$ROOT/docker/s3-servers/demo/compose.yml"
    printf 'SERVER_VERSION=1.2.3\nVTOP_TEST_S3_ENDPOINT=http://127.0.0.1:9\nAWS_ACCESS_KEY_ID=k\nAWS_SECRET_ACCESS_KEY=s\n' \
        > "$ROOT/docker/s3-servers/demo/server.env"
    printf 'id: demo\ngating: false\ncapabilities: {}\n' > "$ROOT/tests/s3-compat/servers/demo.yaml"
}

teardown() {
    rm -rf "$ROOT"
}

@test "a server id that could escape the directory is refused" {
    run bash "$LAB" up ../etc
    [ "$status" -eq 2 ]
    [[ "$output" == *"is not a server id"* ]]
}

@test "a server with no compose file is refused, naming it" {
    run bash "$LAB" up nosuch
    [ "$status" -eq 2 ]
    [[ "$output" == *"docker/s3-servers/nosuch/compose.yml"* ]]
}

@test "gating is read from the capability file" {
    run bash "$LAB" gating demo
    [ "$status" -eq 0 ]
    [ "$output" = "false" ]
    sed -i.bak 's/^gating: false/gating: true/' "$ROOT/tests/s3-compat/servers/demo.yaml"
    run bash "$LAB" gating demo
    [ "$output" = "true" ]
}

@test "a capability file without a gating line is refused, not read as false" {
    printf 'id: demo\ncapabilities: {}\n' > "$ROOT/tests/s3-compat/servers/demo.yaml"
    run bash "$LAB" gating demo
    [ "$status" -eq 2 ]
    [[ "$output" == *"no 'gating: true|false' line"* ]]
}

@test "env names the server, its endpoint and credentials, and where the files are" {
    run bash "$LAB" env demo
    [ "$status" -eq 0 ]
    [[ "$output" == *"VTOP_TEST_S3_ENDPOINT=http://127.0.0.1:9"* ]]
    [[ "$output" == *"AWS_ACCESS_KEY_ID=k"* ]]
    [[ "$output" == *"VTOP_TEST_S3_SERVER=demo"* ]]
    [[ "$output" == *"VTOP_TEST_S3_SERVERS_DIR=$ROOT/tests/s3-compat/servers"* ]]
    [[ "$output" != *"SERVER_VERSION=1.2.3"* ]]
}

@test "a test run with no binaries fails rather than passing having run nothing" {
    mkdir "$ROOT/empty"
    run bash "$LAB" test demo "$ROOT/empty" "$ROOT/results.jsonl"
    [ "$status" -eq 2 ]
    [[ "$output" == *"no test binary"* ]]
}

@test "a test run fails when one binary fails, after running every one" {
    mkdir "$ROOT/bins"
    printf '#!/bin/sh\necho "first $VTOP_TEST_S3_SERVER $VTOP_TEST_S3_RESULTS"; exit 1\n' > "$ROOT/bins/a"
    printf '#!/bin/sh\necho "second ran"\n' > "$ROOT/bins/b"
    chmod +x "$ROOT/bins/a" "$ROOT/bins/b"
    run bash "$LAB" test demo "$ROOT/bins" "$ROOT/out/results.jsonl"
    [ "$status" -eq 1 ]
    [[ "$output" == *"first demo $ROOT/out/results.jsonl"* ]]
    [[ "$output" == *"second ran"* ]]
}

@test "a summary of no results says the tests reached no case" {
    run bash "$LAB" summary demo "$ROOT/missing.jsonl"
    [ "$status" -eq 0 ]
    [[ "$output" == *"did not reach a case"* ]]
}

@test "a test run can be limited to the binaries named" {
    mkdir "$ROOT/bins"
    printf '#!/bin/sh\necho "a ran"\n' > "$ROOT/bins/a"
    printf '#!/bin/sh\necho "b ran"\n' > "$ROOT/bins/b"
    chmod +x "$ROOT/bins/a" "$ROOT/bins/b"
    run bash "$LAB" test demo "$ROOT/bins" "$ROOT/results.jsonl" b
    [ "$status" -eq 0 ]
    [[ "$output" == *"b ran"* ]]
    [[ "$output" != *"a ran"* ]]
}

@test "naming only binaries that do not exist fails rather than running nothing" {
    mkdir "$ROOT/bins"
    printf '#!/bin/sh\nexit 0\n' > "$ROOT/bins/a"
    chmod +x "$ROOT/bins/a"
    run bash "$LAB" test demo "$ROOT/bins" "$ROOT/results.jsonl" nosuch
    [ "$status" -eq 2 ]
}
