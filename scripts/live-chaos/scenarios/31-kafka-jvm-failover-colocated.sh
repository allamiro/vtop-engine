#!/usr/bin/env bash
# 31 — scenario 28 in the co-located shape (#751): the JVM Kafka client, under
# both `metadata.recovery.strategy` values, follows a failover of three
# `vtop-node node` processes, each a metadata voter AND a candidate.
#
# Who answers the client's metadata refresh after the kill is what the shape
# changes (a survivor that may also be electing a new metadata leader), and
# the `none` strategy only ever refreshes from brokers it has been told about,
# so this re-checks that it still finds the new leader. See 30 for the fault
# and 28 for the assertions; this file sets the shape and runs 28, so there is
# one copy of the logic.
#
# Like 28 it SKIPS (exit 77) without CHAOS_KAFKA_HOME or CHAOS_KAFKA_IMAGE.
set -euo pipefail
export CHAOS_KAFKA_SHAPE=colocated
# shellcheck source=28-kafka-jvm-client-failover.sh
source "$(dirname "${BASH_SOURCE[0]}")/28-kafka-jvm-client-failover.sh"
