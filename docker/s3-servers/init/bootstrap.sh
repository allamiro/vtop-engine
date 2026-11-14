#!/bin/sh
# Create the S3 compatibility matrix's buckets on one server (#722, D8).
#
# Run by the pinned amazon/aws-cli one-shot container for EVERY server, so
# bucket creation is one set of commands over the same wire API the engine's
# SDK uses, never each server's own CLI. Readiness is this script retrying
# `list-buckets`, not a container healthcheck (some images, Garage's, have no
# shell or curl to run one).
#
#   S3_ENDPOINT     the server's URL as this container reaches it
#   S3_REGION       default us-east-1
#   PLAIN_BUCKET    default telemetry-raw: no Object Lock
#   LOCKED_BUCKET   default telemetry-locked: Object Lock, so versioning on
#   CREATE_LOCKED   true | false: false for a server whose capability file
#                   says object_lock is unsupported; its bucket is not made
#   READY_SECONDS   default 120: how long to wait for the server to answer
#
# Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY. Exits
# non-zero, naming the step, if any bucket cannot be made.
set -eu

: "${S3_ENDPOINT:?S3_ENDPOINT must name the server}"
region=${S3_REGION:-us-east-1}
plain=${PLAIN_BUCKET:-telemetry-raw}
locked=${LOCKED_BUCKET:-telemetry-locked}
create_locked=${CREATE_LOCKED:-true}
ready=${READY_SECONDS:-120}

s3api() {
    aws --endpoint-url "$S3_ENDPOINT" --region "$region" s3api "$@"
}

waited=0
until s3api list-buckets >/dev/null 2>&1; do
    if [ "$waited" -ge "$ready" ]; then
        echo "bootstrap: $S3_ENDPOINT did not answer list-buckets within ${ready}s; last error:" >&2
        s3api list-buckets >&2 || true
        exit 1
    fi
    sleep 2
    waited=$((waited + 2))
done
echo "bootstrap: $S3_ENDPOINT answered list-buckets after ${waited}s"

exists() {
    s3api head-bucket --bucket "$1" >/dev/null 2>&1
}

# us-east-1 takes no LocationConstraint; every other region needs one.
location() {
    if [ "$region" != "us-east-1" ]; then
        printf '%s' "--create-bucket-configuration LocationConstraint=$region"
    fi
}

if exists "$plain"; then
    echo "bootstrap: $plain exists"
else
    # shellcheck disable=SC2046 # the location flag is two words or none
    s3api create-bucket --bucket "$plain" $(location) >/dev/null
    echo "bootstrap: created $plain"
fi

case "$create_locked" in
true)
    if exists "$locked"; then
        echo "bootstrap: $locked exists"
    else
        # shellcheck disable=SC2046
        s3api create-bucket --bucket "$locked" --object-lock-enabled-for-bucket $(location) >/dev/null
        echo "bootstrap: created $locked with Object Lock"
    fi
    # A store may need versioning turned on explicitly even with the lock
    # (SeaweedFS documents both steps); S3 turns it on with the lock, and
    # enabling it again is a no-op.
    s3api put-bucket-versioning --bucket "$locked" \
        --versioning-configuration Status=Enabled
    status=$(s3api get-bucket-versioning --bucket "$locked" --query Status --output text)
    if [ "$status" != "Enabled" ]; then
        echo "bootstrap: $locked has versioning '$status' after enabling it" >&2
        exit 1
    fi
    ;;
false)
    echo "bootstrap: not creating $locked (CREATE_LOCKED=false: the server's capability file says it has no Object Lock)"
    ;;
*)
    echo "bootstrap: CREATE_LOCKED=$create_locked is not true or false" >&2
    exit 2
    ;;
esac
