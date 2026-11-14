# Several engines sharing one ledger

!!! warning "Not available yet"

    Running several engines against one PostgreSQL ledger is not supported.
    Run one engine per ledger.

This page exists so the option has an honest answer. The design is Tier 2 in
[Production HA §9](../PRODUCTION_HA.md#9-deployment-topologies): several
engines in one Kafka consumer group, sharing one PostgreSQL-compatible ledger.
It depends on deterministic object keys (Phase 4) and Kafka consumer-group
fleet mode (Phase 5), and neither has started
([Production HA §1](../PRODUCTION_HA.md#1-where-things-stand)).

## Prerequisites

Not applicable until fleet mode ships.

## What happens if you try

- On the same host, a second engine with the same ledger or work directory is
  refused at startup: `another VTOP engine already holds ... multi-instance
  operation is UNSUPPORTED`.
- On other hosts, a file lock cannot see it. On PostgreSQL every engine logs
  at startup that it is single-instance per state store and that you should
  not run replicas against the database.

The ledger already fences claimed batches with a generation per row, so a
stalled engine cannot overwrite a batch another engine has since claimed. That
protects the ledger; it does not make several engines a supported
configuration.

## What to do instead

- For a ledger that survives the host: [Single engine with PostgreSQL](postgresql.md).
- For upload throughput, raise `batching.max_concurrent_batches`: it sets how
  many batches are uploaded and verified at once, and cannot weaken the
  commit rule.
- For a replicated log with failover: the [cluster plane](cluster-plane.md),
  which is a separate system from the archive engine.

## Verify it works

Not applicable.

## What to monitor

Not applicable.

## Upgrade notes

Watch the [roadmap](../ROADMAP.md) and [Production HA](../PRODUCTION_HA.md)
for fleet mode.
