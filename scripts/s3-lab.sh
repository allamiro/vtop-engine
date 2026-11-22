#!/usr/bin/env bash
# One server of the S3 compatibility matrix (#722), on a laptop or in a CI
# leg: the same compose file, bootstrap and test run either way.
#
#   scripts/s3-lab.sh build <id>          build the server's image, if it is built
#                                         rather than pulled (the lab's MinIO)
#   scripts/s3-lab.sh up <id>             start the server and create its buckets
#   scripts/s3-lab.sh down <id>           stop it and delete its volumes
#   scripts/s3-lab.sh logs <id>           its logs
#   scripts/s3-lab.sh env <id>            the variables the live tests need for it
#   scripts/s3-lab.sh label <id>          the variables that label its results:
#                                         release, image digest (or local image id)
#   scripts/s3-lab.sh gating <id>         true or false, from its capability file
#   scripts/s3-lab.sh test <id> <dir> <results> [name...]
#                                         run every live test binary in <dir>
#                                         (or only those named) against it,
#                                         appending to <results>
#   scripts/s3-lab.sh summary <id> <results>
#                                         the results as a Markdown table, then each
#                                         unexpected outcome
#   scripts/s3-lab.sh pins                every pulled image is pinned by digest and
#                                         the digest resolves (the weekly s3-pins job)
#
# <id> names docker/s3-servers/<id>/ (compose.yml, server.env, and an
# optional setup.sh run before the buckets are made) and
# tests/s3-compat/servers/<id>.yaml.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

die() {
    echo "s3-lab: $*" >&2
    exit 2
}

usage() {
    sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

# Every pulled image, one per line, from every server's compose file. A
# service with `build:` is built from a pinned source (docker/minio/Dockerfile
# pins MinIO by commit) and never pulled.
pulled_images() {
    local file
    for file in "$root"/docker/s3-servers/*/compose.yml; do
        docker compose -f "$file" config --format json |
            jq -r --arg f "${file#"$root"/}" \
                '.services[] | select(has("build") | not) | select(has("image")) | "\($f) \(.image)"'
    done
}

pins() {
    local failed=0 file image
    while read -r file image; do
        if [[ "$image" != *@sha256:* ]]; then
            echo "::error file=$file::$image is not pinned by digest"
            failed=1
        elif docker manifest inspect "$image" >/dev/null 2>&1; then
            echo "ok $image"
        else
            echo "::error file=$file::$image no longer resolves: the registry dropped it; pick a pin that exists (docker manifest inspect) before blaming the code"
            failed=1
        fi
    done < <(pulled_images)
    return "$failed"
}

[ $# -ge 1 ] || usage
action=$1
shift
if [ "$action" = pins ]; then
    pins
    exit
fi
[ $# -ge 1 ] || usage
id=$1
shift
case "$id" in
*[!a-z0-9-]* | "") die "'$id' is not a server id (lower-case letters, digits and '-')" ;;
esac
dir="$root/docker/s3-servers/$id"
capabilities="$root/tests/s3-compat/servers/$id.yaml"
[ -f "$dir/compose.yml" ] || die "no $dir/compose.yml"
[ -f "$dir/server.env" ] || die "no $dir/server.env"
[ -f "$capabilities" ] || die "no $capabilities"
compose=(docker compose -p "vtop-s3-$id" -f "$dir/compose.yml")

case "$action" in
build)
    if "${compose[@]}" config --format json | jq -e '.services.server | has("build")' >/dev/null; then
        "${compose[@]}" build server
    else
        echo "s3-lab: $id is pulled, not built"
    fi
    ;;
up)
    "${compose[@]}" up -d server
    # A server that needs more than buckets (Garage: a cluster layout and
    # an imported key) does it in its own setup.sh, before the bootstrap.
    if [ -x "$dir/setup.sh" ]; then
        "$dir/setup.sh" "vtop-s3-$id" "$dir/compose.yml"
    fi
    # The init container waits for the server itself (bootstrap.sh retries
    # list-buckets), so a server with no shell for a healthcheck still works.
    if ! "${compose[@]}" run --rm init; then
        "${compose[@]}" logs --tail=200 server >&2 || true
        die "the buckets for $id could not be created"
    fi
    ;;
down) "${compose[@]}" down -v ;;
logs) "${compose[@]}" logs --tail=200 ;;
env)
    grep -E '^(VTOP_TEST_S3_|AWS_)' "$dir/server.env"
    echo "VTOP_TEST_S3_SERVER=$id"
    echo "VTOP_TEST_S3_SERVERS_DIR=$root/tests/s3-compat/servers"
    ;;
label)
    version=$(sed -n 's/^SERVER_VERSION=//p' "$dir/server.env")
    image=$("${compose[@]}" config --format json | jq -r '.services.server.image')
    if [[ "$image" == *@sha256:* ]]; then
        digest=${image##*@}
    else
        # Built locally: no registry digest, so the image id names it.
        digest=$(docker image inspect --format '{{.Id}}' "$image")
    fi
    echo "VTOP_TEST_S3_SERVER_VERSION=$version"
    echo "VTOP_TEST_S3_IMAGE_DIGEST=$digest"
    ;;
gating)
    gating=$(sed -n 's/^gating: *//p' "$capabilities")
    case "$gating" in
    true | false) echo "$gating" ;;
    *) die "$capabilities has no 'gating: true|false' line" ;;
    esac
    ;;
test)
    [ $# -ge 2 ] || usage
    bins=$1
    results=$2
    shift 2
    only=("$@")
    [ -d "$bins" ] || die "no binary directory $bins"
    mkdir -p "$(dirname "$results")"
    set -a
    # shellcheck disable=SC1090,SC1091 # the server's own settings
    . "$dir/server.env"
    set +a
    export VTOP_TEST_S3_SERVER="$id" VTOP_TEST_S3_RESULTS="$results"
    export VTOP_TEST_S3_SERVERS_DIR="$root/tests/s3-compat/servers"
    status=0
    ran=0
    for bin in "$bins"/*; do
        [ -x "$bin" ] || continue
        if [ ${#only[@]} -gt 0 ] && [[ " ${only[*]} " != *" $(basename "$bin") "* ]]; then
            continue
        fi
        ran=$((ran + 1))
        echo "::group::$(basename "$bin")"
        # One thread: the tests share the store, and the multipart ones count
        # the requests a forwarder in front of it sees.
        "$bin" --ignored --test-threads=1 || status=1
        echo "::endgroup::"
    done
    [ "$ran" -gt 0 ] || die "no test binary in $bins"
    exit "$status"
    ;;
summary)
    [ $# -eq 1 ] || usage
    results=$1
    echo "### S3 compatibility: $id"
    echo
    if [ ! -s "$results" ]; then
        echo "No results were written: the tests did not reach a case."
        exit 0
    fi
    echo "| case | outcome | expectation | method |"
    echo "|---|---|---|---|"
    jq -r '"| \(.case) | \(.outcome) | \(.expectation) | \(.method // "-") |"' "$results"
    echo
    jq -r 'select(.expected == false)
      | "\(if .outcome == "unexpected_pass" then "UNEXPECTED PASS" else "UNEXPECTED FAIL" end) \(.case) (\(.deciding_capability // "every capability"), expected \(.expectation)): edit tests/s3-compat/servers/\(.server).yaml or fix the cause"' "$results"
    ;;
*) usage ;;
esac
