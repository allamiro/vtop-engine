#!/usr/bin/env bats
# Tests for docker/load-generator.sh env validation.
#
# The generator produces forever and shells out to Kafka CLI tools, so these
# tests only exercise the validation phase: every case here must fail fast
# BEFORE any topic creation or production is attempted.
#
# Regression covered: `is_uint()` accepts "0", so PARTITIONS=0 passed validation
# and then failed Kafka topic creation silently (`|| true`), producing nothing.

setup() {
    GEN="${BATS_TEST_DIRNAME}/../load-generator.sh"
    # Point at an unroutable broker: validation must fail before this matters,
    # and DURATION_SECONDS=1 bounds anything that slips through.
    export BOOTSTRAP="127.0.0.1:1"
    export DURATION_SECONDS=1
}

# --- rejects non-integers ---------------------------------------------------

@test "rejects non-integer TOPICS_PER_FORMAT" {
    TOPICS_PER_FORMAT=abc run bash "$GEN"
    [ "$status" -eq 2 ]
    [[ "$output" == *"TOPICS_PER_FORMAT"* ]]
}

@test "rejects negative MIN_BATCH" {
    MIN_BATCH=-5 run bash "$GEN"
    [ "$status" -eq 2 ]
}

@test "rejects non-integer PARTITIONS" {
    PARTITIONS=x run bash "$GEN"
    [ "$status" -eq 2 ]
}

# --- rejects zero where Kafka/logic requires >= 1 ---------------------------

# The exact defect the review bot caught: 0 is a valid uint but an invalid
# partition count, and Kafka topic creation would fail silently.
@test "rejects PARTITIONS=0 (Kafka requires >= 1)" {
    PARTITIONS=0 run bash "$GEN"
    [ "$status" -eq 2 ]
    [[ "$output" == *"PARTITIONS"* ]]
}

@test "rejects TOPICS_PER_FORMAT=0 (would create no topics)" {
    TOPICS_PER_FORMAT=0 run bash "$GEN"
    [ "$status" -eq 2 ]
}

@test "rejects MAX_BATCH=0 (would produce no records)" {
    MIN_BATCH=0 MAX_BATCH=0 run bash "$GEN"
    [ "$status" -eq 2 ]
}

# --- range sanity -----------------------------------------------------------

@test "rejects MAX_BATCH < MIN_BATCH" {
    MIN_BATCH=100 MAX_BATCH=10 run bash "$GEN"
    [ "$status" -eq 2 ]
    [[ "$output" == *"MAX_BATCH"* ]]
}

@test "accepts MAX_BATCH == MIN_BATCH" {
    # Valid config: must pass validation (it then fails reaching the fake broker,
    # which is fine - we only assert it got past the validation gate).
    MIN_BATCH=5 MAX_BATCH=5 TOPICS_PER_FORMAT=1 PARTITIONS=1 FORMATS=cef \
        run timeout 20 bash "$GEN"
    [ "$status" -ne 2 ]
    [[ "$output" == *"load-generator: bootstrap="* ]]
}

@test "valid config passes validation and reports its settings" {
    TOPICS_PER_FORMAT=1 PARTITIONS=1 MIN_BATCH=1 MAX_BATCH=2 FORMATS=cef \
        run timeout 20 bash "$GEN"
    [ "$status" -ne 2 ]
    [[ "$output" == *"formats=[cef]"* ]]
}
