#!/usr/bin/env bash
# 30 — scenario 27 in the co-located shape (#751): a librdkafka client follows
# a failover of three `vtop-node node` processes, each a metadata voter AND a
# candidate serving the Kafka gateway.
#
# That is the shape the Helm chart renders for a replicated topology and the
# failover benchmark runs (benchmarks/kafka_compare/vtop/ha-node-*.yaml), and
# it is a different fault from 27's: the SIGKILL of the range's leader takes a
# Raft voter with it, and when that voter is the metadata leader (the result
# line says so, `killed_meta_leader=yes`), the survivors must elect a new
# metadata leader before the lease can move. The survivors' lease agents ask
# their own process's voter first and follow `admin_peers` to the leader; the
# harness reads the lease from the survivors only.
#
# Every assertion is 27's, unchanged: this file sets the shape and runs 27, so
# there is one copy of the logic. 27's header lists them.
set -euo pipefail
export CHAOS_KAFKA_SHAPE=colocated
# shellcheck source=27-kafka-client-follows-failover.sh
source "$(dirname "${BASH_SOURCE[0]}")/27-kafka-client-follows-failover.sh"
