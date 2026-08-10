# VTOP Engine — Production HA Design

> Status: **Design & reference document** for taking the VTOP *engine* (the
> telemetry-object transfer pipeline: `vtop-core`, `vtop-state`,
> `vtop-adapters`, `vtop-upload`, `vtopctl engine`) from a single-process
> deployment to a highly-available fleet, without weakening the core guarantee.
>
> This document holds the **architecture, invariants, and operational
> reference**. The phased implementation plan, per-phase status, risk register,
> readiness checklist, and open decisions live in the companion
> [`PRODUCTION_HA_ROADMAP.md`](PRODUCTION_HA_ROADMAP.md) — the two documents do
> not repeat each other.
>
> Claims that depend on unbuilt work are marked **[PROPOSED]**; claims about
> shipped behavior are marked **[IMPLEMENTED]**. Wording is intentionally
> qualified ("safe under these assumptions", "requires validation").
>
> Scope note: this document is about the **engine pipeline**. The distributed
> VTOP *cluster* (`vtop-meta` Raft metadata, `vtop-node`, `vtop-broker`,
> `vtop-log` sealed-segment replication) is a separate plane with its own
> narrative roadmap in [`ROADMAP.md`](ROADMAP.md).

---

## Table of contents
1. Scope, goals, assumptions
2. System model
3. Definition of VERIFIED
4. What production-grade HA needs
5. The `StateStore` abstraction
6. Object storage, idempotency & Object Lock / WORM
7. Kafka HA: choreography, rebalance, autoscaling
8. Deployment topologies
9. Docker Compose vs. real hardware
10. Hardware sizing
11. Database choice matrix
12. Configuration & environment reference
13. Observability
14. Failure modes & recovery
15. Backup, restore & disaster recovery
16. Security hardening
17. Operator runbook & rollback procedures
18. Known limitations
19. TL;DR

---

## 1. Scope, goals, assumptions

### 1.1 What we are building toward
Take the VTOP engine from a **single-process deployment** (one engine, its own
ledger) to a **horizontally scalable, highly-available** telemetry-object
transfer engine, **without weakening the core guarantee**:

> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

### 1.2 Non-negotiable invariants
- **Verify-before-commit** — source progress (Kafka offset / file byte cursor /
  spool position) advances only after the object **and** manifest are uploaded
  and verified.
- **Replay safety** — a crash at any stage is recoverable with **no data loss**.
- **Delivery semantics (accurate):** today the system is **at-least-once with
  possible duplicate objects** (§1.5, §6). *Idempotent at the archive layer*
  requires **deterministic / content-addressed object keys [PROPOSED]**.

### 1.3 Non-goals
- Not a stream-processing/analytics engine (only framing/format detection).
- Not a datastore for the telemetry itself — object storage is the archive.

### 1.4 Assumptions
This plan is "safe under these assumptions"; where one fails, the relevant
section calls out the consequence.
- **Kafka is the primary production ingress** (file/syslog are secondary; their
  HA is optional and built only if required).
- Source systems **retain data long enough for replay** (Kafka retention; files
  retained + fingerprinted; syslog durably spooled before ingest).
- **Object keys and manifests should be deterministic** for safe retry — **not**
  true today (§1.5); a **[PROPOSED]** change.
- Object storage provides **read-after-write consistency** on the verification
  path (true for S3 and MinIO today).
- **Secrets are injected externally** via a secret manager (never in `config.yaml`).
- Production uses **TLS everywhere** (Kafka, DB, object store, metrics).
- **File/syslog HA requires extra coordination** (leases) and is built only if
  required.

### 1.5 Current vs. target behavior (read this before trusting any HA claim)
| Property | Current code | Target for production HA |
|---|---|---|
| Object key | `vtop-<UTCstamp>-<source>-<range>-<uuid8>` — **non-deterministic** (`Utc::now()` + `Uuid::new_v4()`) | **Deterministic or content-addressed** so retries are idempotent **[PROPOSED]** |
| Replay outcome | Re-processing writes a **new object** (new key) → **duplicate object, no overwrite, no loss** | Retry resolves to the **same** object/version; duplicates impossible **[PROPOSED]** |
| File/syslog cursor | Lives **only in the state store** (rebuilt from `SOURCE_COMMITTED` rows) | Same, but **migrate/drain on backend switch** (§5.4) |
| Kafka offset | Committed to the **broker** after VERIFIED (resumes broker-side) | Same; consumer-group mode for multi-instance **[PROPOSED]** |
| Concurrency | Single process, **single-instance lock enforced at startup** (#66) | N replicas via Kafka consumer groups **[PROPOSED]** |
| State backend | **SQLite and PostgreSQL** (feature-gated) behind the `StateStore` trait **[IMPLEMENTED]** | Same; Postgres-compatible store is the production path |
| Metrics | Prometheus endpoint, opt-in via `VTOP_METRICS_ADDR` **[IMPLEMENTED]** | Same, plus traces and alerting (§13) |

---

## 2. System model

VTOP has a clean **two-plane** design; understanding it keeps the HA design small.

| Plane | Holds | Component | Scale/HA story |
|---|---|---|---|
| **Data plane** | telemetry bytes | Object storage (S3 / MinIO) | Durable + HA-capable; add Object Lock (WORM) |
| **Control plane** | batch lifecycle ledger | **State store** (SQLite dev / Postgres prod) | The single thing blocking horizontal scale |

```
  sources                        VTOP engine                               archive
 ┌─────────┐  read   ┌─────────────────────────────────────────────┐  put   ┌──────────┐
 │ Kafka   │────────►│ discover → batch → seal → compress →        │───────►│ S3 /     │
 │ files   │         │ checksum → upload object → upload manifest  │        │ MinIO    │
 │ syslog  │         │ → VERIFY → COMMIT source progress           │        │ (WORM)   │
 └─────────┘         └──────────────────────┬──────────────────────┘        └──────────┘
                                            │ ~9 small state writes/batch
                                            ▼
                             ┌──────────────────────────────┐
                             │         STATE STORE          │
                             │ replay ledger; enforces the  │
                             │ verify-before-commit rule    │
                             └──────────────────────────────┘
                               SQLite (dev) · Postgres-compatible (prod)
```

**Codebase facts that shape the plan:**
- ~**9 tiny state writes/batch** (`save → sealed → compressed → checksummed →
  object_uploaded → manifest_uploaded → verified → source_committed`) + a recovery
  scan (`list_incomplete`) at startup.
- The run loop is **single-process**, and the engine takes an exclusive
  instance lock at startup (§18).
- The **bottleneck is S3 upload**, not the state store.
- **Object keys are non-deterministic** (§1.5) — central to §6.

---

## 3. Definition of VERIFIED (precise)

The whole invariant hinges on `VERIFIED`, so it must be unambiguous.

**A batch is VERIFIED only when all of the following hold:**
1. the **object exists** in object storage;
2. the **manifest exists** in object storage;
3. the **object size matches** the manifest's recorded size;
4. the object **checksum** (SHA-256 or BLAKE3), derived from stored bytes or
   computed by the storage service, **matches the manifest checksum**;
5. the **state store has persisted** `object_key`, `manifest_key`, checksum,
   checksum algorithm, compression type, source range, and `batch_id` **before**
   the VERIFIED transition;
6. **[PROPOSED]** the S3 `version_id` (and/or checksum header) is recorded when
   Object Lock / versioning is enabled;
7. **verification failure prevents source commit** — the batch never advances to
   SOURCE_COMMITTED.

**Verification strength (current code):** the engine supports **strong**
(stored-content/service-computed checksum) and **backend-limited** (size /
existence only) verification. Strong verification defaults on.
`upload.require_strong_verification: false` is an explicit compatibility/lab
opt-out that allows a backend-limited result to commit.

**ETag caveat:** S3 multipart ETags are **not** reliable MD5 checksums. The
authoritative integrity value is VTOP's own **SHA-256/BLAKE3 manifest checksum**,
never the ETag.

---

## 4. What production-grade HA actually needs

The honest, minimal set — **one** durable store, not a zoo.

| Need | Component | Required? | Notes |
|---|---|---|---|
| Durable shared ledger | **ONE** Postgres-compatible DB | **Yes** | PostgreSQL, **or** YugabyteDB/CockroachDB for a self-HA store. Pick one (§11). Backend **[IMPLEMENTED]**. |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | **Required for HA** | Single-node Kafka **reading exists today**; **fleet consumer-group mode is [PROPOSED]**. Kafka is the coordinator — no extra coordination DB for the Kafka path. |
| Durable data plane | **S3 / MinIO** | **Yes (have it)** | Distributed MinIO (erasure-coded) or S3; Object Lock for WORM. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | **Yes for HA** | Restarts, rolling upgrades, lag-based autoscale. An engine chart is **[PROPOSED]** — the existing `helm/vtop` chart deploys the *cluster*, not the engine (§8). |
| Observability | **Prometheus + Grafana (+ traces)** | **Yes** | Metrics endpoint **[IMPLEMENTED]**; dashboards in `observability/`; traces **[PROPOSED]** (§13). |
| Secrets | **Vault / external-secrets / k8s Secrets** | Recommended | Creds already injected via env. |
| File/syslog HA ownership | **etcd / Consul** (leases) | **Only if** file/syslog must be HA-distributed | The only thing Kafka groups don't solve. |

### 4.1 Deliberately NOT added
- **Redis** — not the durable store (durability is the point); nothing to cache.
- **Two databases at once** — Postgres *and* Yugabyte/Cockroach are alternatives.
- **etcd** — skip unless distributed file/syslog ingestion is required.

> If Kafka is the primary ingress, enterprise HA is essentially **one new database
> + the `StateStore` abstraction + k8s/Prometheus** — and the database and
> abstraction are already built.

---

## 5. The `StateStore` abstraction **[IMPLEMENTED]**

Shipped in `crates/vtop-state`: the trait, a SQLite backend, a PostgreSQL
backend behind `--features postgres`, and a shared backend-agnostic test
battery.

### 5.1 Backend selection = a config or secret reference
The engine reads `engine.state_store`; a factory dispatches on the resolved
scheme. SQLite paths may be inline. PostgreSQL URLs must come from an env/file
secret reference so credentials never enter serializable config.

| Deployment | `engine.state_store` |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `{ env: VTOP_STATE_STORE }` → `postgres://…?sslmode=verify-full` |
| Production (Yugabyte/Cockroach) | `{ file: /run/secrets/vtop-state-store }` → `postgres://…?sslmode=verify-full` |

### 5.2 The trait (one source of truth for the invariant)
- Abstracts: `save_batch_state`, `update_batch_state`, `mark_verified`,
  `mark_source_committed`, `mark_failed`, `get_batch`, `list_incomplete_batches`,
  `list_failed_batches`, `list_batches`.
- Engine holds `Box<dyn StateStore>`.
- The **verify-before-commit guard stays as pure logic in `vtop-core`**, called by
  every backend — never re-implemented per backend.
- **Defense in depth:** the database **also** enforces the invariant via
  constraints (§5.5). Do not rely on application logic alone.

### 5.3 Backend differences
| Concern | SQLite | Postgres / Yugabyte / Cockroach |
|---|---|---|
| Driver | `sqlx` `SqlitePool` | `sqlx` `PgPool` (pure-Rust, no libpq) |
| Placeholders | `?` | `$1, $2, …` |
| Insert | plain `INSERT` | plain `INSERT` |
| Migrations | SQLite initializes locally | `vtopctl migrate` with a separate privileged identity; runtime executes no DDL |
| **Conflict retry** | none | **retry on SQLSTATE `40001`** (distributed serialization) **[IMPLEMENTED]** |
| Build | default | behind Cargo `--features postgres` |

### 5.4 Backend-switching policy (NOT "no migration ever")
The state store is a replay ledger, **but the file/syslog byte cursor lives only in
it** (rebuilt from `SOURCE_COMMITTED` rows by `seed_committed_offsets`). Kafka
offsets live in the broker. Therefore:

```text
Backend switching policy:
- Dev/test:                a fresh state store is acceptable.
- Production, Kafka-only:   may be safe AFTER engine DRAIN + offset verification
                           (offsets are broker-side, so resume is safe).
- Production, file/syslog:  REQUIRES cursor migration OR a controlled drain,
                           else files reprocess from byte 0 (duplicates) and
                           spool position is lost.
- Any production switch:    MUST include drain → checkpoint → validate → rollback.
```

**Safe switch procedure:** drain (stop sources; let in-flight batches reach
`SOURCE_COMMITTED`) → confirm zero `incomplete` rows → export file/syslog cursors →
import into the new store (or accept Kafka reprocessing for Kafka-only) → validate →
keep the old store until validation passes (rollback path).

**Why no row migration for the *Kafka* path:** resume position is broker-side
committed offsets, so a fresh store does not lose Kafka progress (it
re-discovers). This is the *only* path where "no migration" holds.

### 5.5 Database schema & constraints (defense in depth) **[IMPLEMENTED]**
Applied by `vtopctl migrate` under a deployment identity; the engine role gets
only schema `USAGE` plus `SELECT, INSERT, UPDATE` on `batches`, and the live
battery proves DDL, `DELETE`, and `TRUNCATE` remain denied. The constraints are
**state-aware**:

```sql
CREATE TABLE batches (
  batch_id            TEXT  PRIMARY KEY,            -- UNIQUE
  tenant              TEXT,
  source_type         TEXT,                          -- kafka | file | syslog_spool
  source_name         TEXT,
  -- Kafka identity
  topic               TEXT,
  partition           INT,
  start_offset        BIGINT,
  end_offset          BIGINT,                         -- last record offset (see §7.1)
  -- File identity
  file_path           TEXT,
  byte_start          BIGINT,
  byte_end            BIGINT,
  file_fingerprint    TEXT,                           -- inode/size/mtime hash
  -- Object identity
  object_key          TEXT,
  manifest_key        TEXT,
  checksum            TEXT,
  checksum_algorithm  TEXT,                           -- sha256 | blake3 | none
  compression         TEXT,                           -- gzip | zstd | none
  version_id          TEXT,                           -- [PROPOSED] Object Lock / versioning
  -- Lifecycle
  state               TEXT NOT NULL,
  retry_count         INT  NOT NULL DEFAULT 0,
  last_error          TEXT,
  created_at          TIMESTAMPTZ NOT NULL,
  updated_at          TIMESTAMPTZ NOT NULL,
  verified_at         TIMESTAMPTZ,
  source_committed_at TIMESTAMPTZ,
  -- File/syslog HA (optional phase)
  lease_owner         TEXT,
  lease_until         TIMESTAMPTZ,

  -- Constrained lifecycle set
  CONSTRAINT state_enum CHECK (state IN (
    'DISCOVERED','BATCHING','SEALED','COMPRESSED','CHECKSUMMED',
    'OBJECT_UPLOADED','MANIFEST_UPLOADED','VERIFIED','SOURCE_COMMITTED',
    'FAILED','REPLAY_REQUIRED')),

  -- THE INVARIANT (two equivalent guards, keep both):
  CONSTRAINT commit_needs_verify_state CHECK (
    state <> 'SOURCE_COMMITTED' OR verified_at IS NOT NULL),
  CONSTRAINT commit_needs_verify_ts CHECK (
    source_committed_at IS NULL OR verified_at IS NOT NULL),

  -- Object identity must exist by the time we claim VERIFIED:
  CONSTRAINT verified_needs_object CHECK (
    state NOT IN ('VERIFIED','SOURCE_COMMITTED')
    OR (object_key IS NOT NULL AND manifest_key IS NOT NULL))
);

-- Idempotency / dedup (one batch per source range):
CREATE UNIQUE INDEX uq_kafka_range
  ON batches (source_name, topic, partition, start_offset, end_offset)
  WHERE source_type = 'kafka';
CREATE UNIQUE INDEX uq_file_range
  ON batches (source_name, file_path, byte_start, byte_end, file_fingerprint)
  WHERE source_type = 'file';

-- Recovery / ops:
CREATE INDEX ix_state        ON batches (state);
CREATE INDEX ix_source_time  ON batches (source_type, source_name, updated_at);
CREATE INDEX ix_incomplete   ON batches (state)
  WHERE state NOT IN ('SOURCE_COMMITTED','FAILED');   -- hot incomplete set
```

**Multi-writer recovery [PROPOSED]:** claim incomplete rows with
`SELECT … FOR UPDATE SKIP LOCKED`, or via `lease_owner`/`lease_until`, so two
instances never recover the same batch. Required before fleet mode.

### 5.6 Ledger retention / pruning **[IMPLEMENTED — incremental delete, #128]**
```text
- keep ACTIVE, FAILED, REPLAY_REQUIRED, and recent SOURCE_COMMITTED rows hot;
- delete old SOURCE_COMMITTED rows incrementally, always retaining the
  per-path row with the highest committed end_byte so recovery cursor
  seeding is unchanged;
- retain enough for audit, replay, and compliance (age window is
  configurable; archival to cold tables / object storage remains a
  possible future extension).
```
Configured with `engine.ledger_retention_days` (disabled by default) plus
`engine.ledger_prune_batch`; the engine prunes only on idle cycles, at most
once a minute, one bounded batch per pass. Engine-side pruning is SQLite
only: the PostgreSQL runtime identity is deliberately denied DELETE, so the
engine refuses the setting there and pruning runs as a scheduled `vtopctl
prune-ledger --older-than-days N` under a maintenance identity (see
[`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md)).
Rows in any non-committed state are never touched.

---

## 6. Object storage, idempotency & Object Lock / WORM

This section supersedes any earlier "rewrites the same object" wording.

### 6.1 Current reality
Object keys are **non-deterministic** (`Utc::now()` + `Uuid::new_v4()`), so a
replayed batch writes a **new** object. Result: **no data loss, but duplicate
objects** can accumulate on crash/replay. The **state ledger + manifests** are the
dedup authority today — not key collision.

### 6.2 Object Lock / WORM safe-retry rules
With S3 Object Lock, protected object versions **cannot be overwritten or deleted**.
Retry behavior must therefore be explicit:

```text
Object Lock DISABLED:
  • deterministic keys MAY be overwritten safely on retry. [PROPOSED key scheme]

Object Lock ENABLED:
  • a retry MUST NOT depend on overwriting a protected object. It MUST do one of:
     1. detect an existing VERIFIED object+manifest and treat it as success
        (no re-upload);                                          [PROPOSED]
     2. write a NEW object version and record version_id in the
        manifest + state store;                                  [PROPOSED]
     3. use CONTENT-ADDRESSED keys so duplicates are inherently harmless. [PROPOSED]
```

### 6.3 Recommendation
Adopt **(1) deterministic keys + "existing verified object = success"** (optionally
with **(2)** version_id recording). Only after this change is replay **idempotent at
the archive layer** and the delivery guarantee may be described that way.

### 6.4 ETag caveat (repeat)
Never treat an S3 ETag as the integrity checksum (multipart ETags aren't MD5). The
manifest SHA-256/BLAKE3 is the source of truth.

---

## 7. Kafka HA: choreography, rebalance, autoscaling **[PROPOSED multi-instance]**

### 7.1 Commit choreography (exact order)
```text
1. Poll records.
2. Build batch with topic/partition/start_offset/end_offset.
3. Persist DISCOVERED/BATCHING in the state store.
4. Upload object.
5. Upload manifest.
6. Verify object + manifest (see §3).
7. Mark VERIFIED in the state store.
8. Commit Kafka offset MANUALLY (enable.auto.commit = false).
9. Mark SOURCE_COMMITTED.
```

**Offset semantics (avoid off-by-one):** Kafka commits the **next offset to
consume**, i.e. `last_verified_record_offset + 1`, **not** the last processed
offset itself. The current code already does this (`commit_at = end_offset + 1`).
Define `end_offset` as the **last record's offset**; the committed offset is
`end_offset + 1`.

**Crash edge case (must be documented as safe):**
```text
If the engine marks VERIFIED but crashes before the Kafka offset commit, the
batch is replayed after rebalance. This is safe ONLY once object keys are
deterministic (§6.3): the existing verified object+manifest is treated as
success. Until then, replay produces a duplicate object (no loss).
```

### 7.2 Single-node vs fleet consumer mode (design decision)
- The current code uses per-read **`assign()`** (fixed the single-node stall:
  re-`subscribe()` per read caused rebalance + reseek-to-earliest).
- **For a fleet, use long-lived `subscribe()` + manual commit-after-verify** so
  Kafka distributes partitions across replicas. Expose as a config toggle
  (`VTOP_KAFKA_GROUP_MODE = assign | subscribe`). Subscribe-once also avoids the
  original stall because committed offsets exist after the first verified batch.

### 7.3 Rebalance requirements (fleet-mode correctness)
```text
- disable auto-commit; manual commit only after VERIFIED;
- handle partition REVOCATION cleanly:
    * stop accepting new batches for revoked partitions,
    * complete or safely abandon in-flight batches (abandon = no commit → replay);
- tune max.poll.interval.ms, session.timeout.ms, heartbeat.interval.ms so long
  uploads don't trigger spurious rebalances;
- prefer cooperative (incremental) rebalancing if the client supports it.
```

### 7.4 Autoscaling caveat (KEDA)
```text
Useful engine replicas for a topic are bounded by its ACTIVE PARTITION COUNT.
More replicas than partitions does NOT add throughput. KEDA should scale on
consumer lag, but min/max replicas must align with partition count AND downstream
upload/store capacity.
```

---

## 8. Deployment topologies

### Tier 0 — Single node, Docker Compose (dev / demo / small)
One engine, **SQLite**, single MinIO, single Kafka (KRaft). The current lab.
**Fully testable on one machine.** No HA.

### Tier 1 — Single engine + external Postgres (small prod, durable store)
One engine, **Postgres** ledger (backable, survives engine host restart), S3/MinIO.
One engine (no horizontal scale) but a proper durable store. **Testable on one
machine** (add a `postgres` Compose service). **Available today** — the Postgres
backend is implemented (§5); see
[`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md).

### Tier 2 — HA fleet, Kafka-primary (recommended enterprise baseline) **[PROPOSED]**
```
         ┌── engine-1 ─┐
 Kafka ──┼── engine-2 ─┼──► S3 / MinIO (WORM)
 (group) └── engine-3 ─┘
              │
              ▼
   Postgres / Yugabyte / Cockroach (one logical store, HA)

   metrics ─► Prometheus ─► Grafana ; traces ─► OTel ; autoscale ◄─ KEDA (lag)
```
N replicas on Kubernetes; Kafka consumer-group mode (§7.2); one Postgres-compatible
store; distributed MinIO/S3 + Object Lock; KEDA autoscale on lag (§7.4). File/syslog
**not HA** here (disabled or pinned to one replica). Needs real multi-node infra;
rehearsable on single-node k8s, true HA needs ≥3 nodes.

> **Chart caveat:** the repository's `helm/vtop` chart deploys the co-located
> VTOP *cluster* (`vtop-node` metadata + data planes), **not** the engine
> pipeline. An engine chart (Deployment/probes/KEDA) is future work tracked in
> the roadmap.

### Tier 3 — Full HA incl. file/syslog (only if required) **[PROPOSED]**
Tier 2 **+ etcd/Consul leases** so each file/spool is owned by one replica, with
takeover resuming from the durable state-store cursor.
```text
Replayability differs by source — design accordingly:
  Kafka  is replayable (broker retention).
  Files  are replayable only while retained AND fingerprinted.
  Syslog is replayable only after it is durably SPOOLED.
  UDP syslog is NOT lossless by design.
  => For syslog HA, ingest from a durable spool, never from volatile UDP buffers.
```

### 8.1 Topology selection guide
| If your reality is… | Use |
|---|---|
| Laptop / demo | Tier 0 (Compose, SQLite) |
| Small prod, one engine, want backups | Tier 1 (Postgres) — available today |
| Enterprise, Kafka is primary | **Tier 2** (k8s fleet) — pending fleet mode |
| Enterprise + distributed file/syslog | Tier 3 (+ etcd) — pending |

---

## 9. Docker Compose vs. real hardware

| Capability | Compose (1 machine) | Needs multi-node |
|---|---|---|
| Full pipeline correctness (all sources → verified → committed) | ✅ | |
| SQLite ↔ Postgres backend switch (config-string) | ✅ | |
| Postgres backend + retry-on-`40001` | ✅ (single Postgres) | |
| Yugabyte/Cockroach **wire** compatibility | ✅ (single container) | |
| Distributed-store **HA** (survive DB node loss) | ⚠️ partial | ✅ (≥3 DB nodes) |
| Multi-engine Kafka consumer-group distribution | ❌ **not yet** — single instance per state store, enforced at startup (#66/#93) | ✅ (after fleet mode, on k8s) |
| Engine failover / rebalance under node loss | ⚠️ (kill a container) | ✅ (k8s, ≥3 nodes) |
| KEDA lag autoscaling | | ✅ (k8s) |
| etcd file-lease ownership + takeover | ⚠️ (rehearsal) | ✅ (k8s, ≥3 nodes) |
| Object Lock / WORM | ✅ (MinIO) | ✅ (S3 in prod) |

**Correctness and backend portability are fully Compose-testable on one machine;
only true HA behavior needs multi-node Kubernetes.**

---

## 10. Hardware sizing (starting points — validate with `benchmarks/`)

| Tier | Engine | State store | Object store | Kafka |
|---|---|---|---|---|
| 0 | 2 vCPU / 2–4 GB | SQLite (in engine) | MinIO 1 node | 1 broker (KRaft) |
| 1 | 2–4 vCPU / 4 GB | Postgres 2 vCPU / 4 GB / SSD | MinIO 1–4 nodes | 1–3 brokers |
| 2 | 3+ replicas × (2–4 vCPU / 2–4 GB) | Postgres HA (3) **or** Yugabyte/CRDB (3 × 4 vCPU / 8–16 GB / NVMe) | MinIO ≥4 erasure-coded, or S3 | ≥3 brokers, RF=3 |
| 3 | as Tier 2 | as Tier 2 | as Tier 2 | + etcd (3 small nodes) |

**Notes.** Engine is **CPU/mem-light** (~8 MiB idle); scale by **replica count vs
Kafka partitions**, not big boxes. Memory spikes only in **whole-file mode** on
large files. State-store sizing is dominated by **write IOPS + fsync latency**; use
fast SSD/NVMe. Distributed SQL needs **3 nodes minimum** + NVMe.

---

## 11. Database choice matrix

No option is universally best — compare against your priorities.

| Option | Strength | Weakness | Best fit |
|---|---|---|---|
| **PostgreSQL + Patroni** | Familiar, mature tooling, **lowest write latency**, excellent PITR/backup | HA requires failover management (Patroni/etcd) | Teams with Postgres experience |
| **YugabyteDB** | PostgreSQL-compatible **distributed SQL**, self-healing HA | More operational complexity; higher write latency; plan for `40001` retries | Self-healing DB HA preferred over latency |
| **CockroachDB** | Strong distributed-SQL story, self-healing HA, k8s-native | Some SQL-compatibility differences; latency trade-offs; plan for `40001` | Cloud-native distributed-DB teams |

**Default recommendation:** start with **PostgreSQL + Patroni** unless the team has a
clear requirement for distributed SQL **and** the operational skill to run it.
Because all three speak the Postgres wire protocol, the `StateStore` Postgres backend
works against any of them — the choice is a connection-string + ops decision, made
once, and **requires validation** under your write rate. The retry-on-`40001`
path needed by the distributed options is already implemented (§5.3).

---

## 12. Configuration & environment reference

### 12.1 Current config (`config.yaml`) — implemented
| Key | Meaning |
|---|---|
| `engine.name` / `engine.tenant` | identity; default tenant |
| `engine.state_store` | backend selector: inline `sqlite://…`, or `{ env: … }` / `{ file: … }` secret reference for PostgreSQL |
| `engine.work_dir` / `log_level` | scratch dir; verbosity |
| `engine.ledger_retention_days` / `ledger_prune_batch` | committed-row retention (§5.6; SQLite engine-side only) |
| `batching.max_records` / `max_bytes` / `max_batch_age_seconds` | seal thresholds |
| `compression.type` / `level` | `gzip` \| `zstd` \| `none` |
| `checksum.algorithm` | `sha256` \| `blake3` \| disabled |
| `manifest_mac_key_env` | optional env-var name for the 32-byte hex manifest MAC key; the secret is not serialized |
| `sources.kafka.*` | brokers, group, include/exclude, `enable_auto_commit:false` |
| `sources.file.*` | paths, `delete_after_commit`, `whole_file` |
| `sources.syslog_spool.*` | spool paths |
| `upload.backend` | `s3_native` \| `s3cmd` \| `awscli` \| `minio` \| `localfs` \| `mock` |
| `upload.bucket` | bucket (supports `telemetry-{format}`) |
| `upload.endpoint_url` / `region` / `force_path_style` / `verify_tls` | S3 endpoint |
| `upload.command_binary` / `command_timeout_seconds` / `command_max_output_bytes` | hardened external-tool path and invocation bounds (compatibility backends only) |
| `upload.command_env_allowlist` | exact runtime environment names copied into an otherwise empty command environment |
| `upload.create_bucket` | auto-create per-format buckets |
| `upload.require_strong_verification` | defaults true — false explicitly permits size-only commit |
| `partitioning.template` | object key layout |

### 12.2 Current environment variables — implemented
| Variable | Purpose |
|---|---|
| `VTOP_CONFIG` | path to `config.yaml` |
| `RUST_LOG` | log filter |
| `VTOP_STATE_STORE` | PostgreSQL URL when named by `engine.state_store.env`; remote URLs require `sslmode=verify-full` |
| `VTOP_METRICS_ADDR` | opt-in Prometheus metrics + readiness endpoint (e.g. `0.0.0.0:9090`); nothing listens unless set |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials |
| `AWS_REGION` | S3 region |
| `VTOP_S3_ENDPOINT_URL` | S3/MinIO endpoint |
| `VTOP_S3_FORCE_PATH_STYLE` | path-style addressing (MinIO) |
| `VTOP_S3_VERIFY_TLS` | TLS verification toggle (off = lab only) |
| *(Kafka SASL)* | password read from the **env var named** in `sasl_password_env` |

### 12.3 Proposed environment variables (future HA work) **[PROPOSED]**
| Variable | Purpose |
|---|---|
| `VTOP_PG_MAX_CONNECTIONS` | Postgres pool size per replica |
| `VTOP_PG_STATEMENT_TIMEOUT_MS` | guard stuck statements |
| `VTOP_OBJECT_KEY_MODE` | `legacy` \| `deterministic` \| `content-addressed` |
| `VTOP_KAFKA_GROUP_MODE` | `assign` (single-node) \| `subscribe` (fleet) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OpenTelemetry collector |
| `VTOP_INSTANCE_ID` | stable replica identity (group member / lease owner) |
| `VTOP_ETCD_ENDPOINTS` | etcd/Consul endpoints for leases |
| `VTOP_LEASE_TTL_SECONDS` | source-ownership lease TTL |

> Secrets (DB password, S3 keys, Kafka SASL) **must** come from a secret manager,
> never `config.yaml`.

---

## 13. Observability

**Implemented today:**
- **Prometheus metrics endpoint** — opt-in via `VTOP_METRICS_ADDR`
  (`crates/vtop-observe`, served by `vtopctl`), including a readiness probe.
- **Structured per-stage events** — one logical span per batch with child
  events per pipeline stage.
- **Dashboard tooling** — the `observability/` directory carries a
  Grafana/Loki/Mimir/Tempo/Alloy compose stack
  (`docker-compose.observability.yml`) and generated dashboards for the
  pipeline, Kafka, and flow views.

**Still needed for production [PROPOSED]:**
- **Alerting rules** (Alertmanager): verification failures > 0; replay-rate
  spike; lag growth; store write-latency SLO breach; no committed batches in N
  minutes.
- **OpenTelemetry trace export** from the engine (the Tempo backend is already
  in the stack; the engine does not yet emit OTLP).
- Metric coverage review: **verification-failure rate**, **replay /
  REPLAY_REQUIRED rate**, Kafka **consumer lag**, state-store write latency,
  **duplicate-object rate** (needed until deterministic keys land).

---

## 14. Failure modes & recovery semantics

```text
Recovery scan at startup, per incomplete row:

  DISCOVERED … MANIFEST_UPLOADED   → replay from last committed source position
  VERIFIED (not yet committed)     → retry the source commit only
  SOURCE_COMMITTED                 → nothing to do

Invariant preserved: source progress is NEVER advanced for unverified data.
```

| Failure | Behavior | Why it's safe |
|---|---|---|
| Engine crash mid-batch | `recover()`: pre-VERIFIED → replay from last committed source position; VERIFIED-but-not-committed → retry commit | Progress never advanced for unverified data |
| Replay after crash | **Today:** new object key → duplicate object (no loss). **After deterministic keys:** same key, existing verified = success | At-least-once now; idempotent later |
| State store unavailable | Cannot transition → stops committing (fails safe) | No commit-before-verify possible |
| Kafka rebalance (fleet) | Revoked partitions: finish/abandon in-flight, no commit; reassigned replica resumes from committed offset | Offsets commit only post-verify |
| File-owner replica dies (Tier 3) | Lease expires; another replica resumes from stored byte cursor | Cursor in shared durable store |
| Object Lock blocks overwrite | Retry uses deterministic key / new version, never overwrite | §6.2 rule |

---

## 15. Backup, restore & disaster recovery **[PROPOSED]**
```text
- State store: Postgres PITR (WAL archiving) or distributed-SQL backup policy.
- Object store: bucket replication or MinIO/S3 backup policy; Object Lock retention.
- Restore test cadence: scheduled DR drills (e.g., quarterly).
- Targets: define explicit RPO and RTO.
- DR startup sequence: restore state store → restore/verify object store →
  start engine (recovery scan re-reconciles incomplete batches).
- Post-restore validation: reconcile state-store rows vs object-store contents
  (orphan objects / missing objects / stale in-flight rows).
```

---

## 16. Security hardening

**Already in place:**
- Least-privilege database identities: the runtime role cannot run DDL,
  `DELETE`, or `TRUNCATE`; migrations and pruning use separate identities (§5.5–5.6).
- Remote PostgreSQL URLs must use `sslmode=verify-full`.
- Secrets referenced by env/file, never serialized into config (§5.1, §12).
- Hardened external-upload-tool invocation: explicit binary path, timeout,
  output bounds, and an environment allowlist (§12.1).
- Release pipeline publishes **SBOM, cosign signatures, and `SHA256SUMS`**;
  `cargo-deny` runs in CI.

**Production baseline still to apply [PROPOSED]:**
```text
- TLS for Kafka, DB, object store, and the metrics endpoint.
- IAM/bucket policy scoped to required prefixes only (least privilege).
- Separate credentials per environment and per tenant.
- Secrets via Vault / Kubernetes Secrets / external-secrets.
- Server-side encryption / KMS where available; encryption in transit everywhere.
- Audit logs for object-store writes and state-store changes.
- Kubernetes NetworkPolicies isolating engine ↔ DB ↔ Kafka ↔ object store.
- Restricted admin access; least-privilege RBAC.
- Vulnerability scanning (image + dependency) each release cycle.
```

---

## 17. Operator runbook & rollback procedures

Short procedures now; expand into a full ops runbook before go-live.

- **Engine restart:** `docker compose restart vtop-engine` (Tier 0) or
  `kubectl rollout restart deploy/vtop-engine` (Tier 2). On start, the recovery scan
  reconciles incomplete batches; no manual step needed.
- **Replay a failed batch:** locate the row (`state IN ('FAILED','REPLAY_REQUIRED')`),
  confirm source data is still retained, set it to `REPLAY_REQUIRED`; the engine
  re-reads from the last committed source position on the next cycle.
- **Force-mark a poison batch FAILED:** if a batch cannot progress (bad data),
  set `state='FAILED'`, populate `last_error`; it is then excluded from the hot path
  and surfaced on the failed-batches dashboard for investigation.
- **Drain before a backend switch (§5.4):** stop sources → wait until no `incomplete`
  rows remain → export file/syslog cursors → switch `engine.state_store` → import
  cursors → validate → keep the old store until validation passes.
- **Prune the PostgreSQL ledger:** run `vtopctl prune-ledger --older-than-days N`
  on a schedule under the maintenance identity (§5.6).
- **Restore from DB backup:** restore Postgres PITR → verify object store →
  start the engine (recovery scan re-reconciles) → run the divergence checker.
- **Reconcile state store vs object storage:** list `SOURCE_COMMITTED` rows and
  confirm each object+manifest exists; flag orphan objects (no row) and missing
  objects (row but no object) for remediation.
- **Rotate credentials (S3 / Kafka / DB):** update the secret in the secret manager
  → rolling-restart engines → confirm new connections succeed → revoke old creds.
- **Scale engine replicas [after fleet mode]:**
  `kubectl scale deploy/vtop-engine --replicas=N`, with `N ≤ active partition
  count` for the topic (§7.4); KEDA can automate within bounds.
- **Respond to a verification-failure alert:** check the failing backend (object
  store reachability, checksum mismatch), confirm `require_strong_verification`, and
  hold commits (the engine already refuses to commit unverified batches).

---

## 18. Known limitations (current code)
- **SINGLE-INSTANCE ONLY — enforced at startup (#66).** The engine takes an
  exclusive OS lock on its work directory and refuses to start beside another
  engine on the same host. There is no claim/lease/fencing in the state store
  yet (#93), so two engines over the same store would both recover the
  same incomplete batches and both commit source progress — duplicate ingestion
  at best, double-commit at worst. The work-dir lock CANNOT see an engine on a
  different host pointed at the same Postgres; that configuration is
  unsupported and warned about at startup. Do not scale replicas.
- **Non-deterministic object keys** — replay can create **duplicate objects** (no
  loss). Fixed by deterministic/content-addressed keys (roadmap Phase 4).
- **Engine loop is single-process** — no horizontal scale until Kafka
  consumer-group fleet mode lands (roadmap Phase 5).
- **File/syslog HA is not solved without leases** — single-owner only.
- **UDP syslog cannot be made lossless** without durable spooling before VTOP.
- **Whole-file mode can cause memory spikes** — it loads the whole file into memory;
  size accordingly or keep large inputs line-oriented.
- **No engine Helm chart** — `helm/vtop` deploys the cluster plane, not the
  engine pipeline (§8).

---

## 19. TL;DR
- **One** durable Postgres-compatible store; **no Redis**; etcd only for distributed
  file/syslog.
- The **`StateStore` abstraction is built and shipped**: SQLite ↔ Postgres ↔
  Yugabyte/Cockroach is now a **config-string** choice (§5, §11), with the
  invariant enforced in core logic **and** database constraints, and
  retry-on-`40001` in place. Tier 1 (single engine + Postgres) is deployable
  today.
- **Two current-behavior caveats the design must still fix for fleet HA:**
  (a) object keys are **non-deterministic** → replay makes **duplicates**; fix with
  **deterministic keys** for idempotency + Object Lock safety.
  (b) the engine is **single-instance by design** until Kafka consumer-group
  fleet mode and multi-writer recovery land.
- **VERIFIED is defined precisely (§3); strong content-derived verification is
  the default and production must not opt out.**
- File/syslog cursors live **only in the state store** → **migrate/drain** on
  backend switch (Kafka is broker-side and safe).
- **Correctness and backend portability are fully Docker-Compose-testable on one
  machine;** only true HA behavior needs multi-node Kubernetes.
- For phase-by-phase status, risks, and the readiness checklist, see
  [`PRODUCTION_HA_ROADMAP.md`](PRODUCTION_HA_ROADMAP.md).
