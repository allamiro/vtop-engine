# VTOP Engine — Production-Grade HA Plan

> Status: **Proposal / design doc** (no code changes implied by this document).
> Audience: engineers, operators, and decision-makers taking VTOP from a single-node
> prototype to an enterprise, highly-available archive engine.
>
> This document distinguishes **current behavior** (what the code does today) from
> **proposed behavior** (what production HA requires). Claims that depend on unbuilt
> work are marked **[PROPOSED]**. Wording is intentionally qualified ("safe under
> these assumptions", "requires validation").
>
> A companion document, [`PRODUCTION_HA_ROADMAP.md`](PRODUCTION_HA_ROADMAP.md),
> contains an alternate, roadmap-first presentation of the same material.

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
11. Phased implementation plan
12. **Production Roadmap (table)**
13. **Database choice matrix**
14. Configuration & environment reference
15. Observability
16. Failure modes & recovery
17. Backup, restore & disaster recovery
18. Security hardening
19. **Risk register**
20. **Operator runbook & rollback procedures**
21. **Known limitations**
22. **Production-readiness checklist**
23. Open decisions
24. TL;DR

---

## 1. Scope, goals, assumptions

### 1.1 What we are building toward
Take VTOP from a **single-process prototype** (SQLite ledger, one engine) to a
**horizontally scalable, highly-available** telemetry-object transfer engine,
**without weakening the core guarantee**:

> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

### 1.2 Non-negotiable invariants
- **Verify-before-commit** — source progress (Kafka offset / file byte cursor /
  spool position) advances only after the object **and** manifest are uploaded and
  verified.
- **Replay safety** — a crash at any stage is recoverable with **no data loss**.
- **Delivery semantics (accurate):** today the system is **at-least-once with
  possible duplicate objects** (§1.5, §6). *Idempotent at the archive layer*
  requires **deterministic / content-addressed object keys [PROPOSED]**.

### 1.3 Non-goals
- Not a stream-processing/analytics engine (only framing/format detection).
- Not a datastore for the telemetry itself — object storage is the archive.

### 1.4 Assumptions
This plan is "safe under these assumptions"; where one fails, the relevant section
calls out the consequence.
- **Kafka is the primary production ingress** (file/syslog are secondary; their HA
  is optional, Phase 7).
- Source systems **retain data long enough for replay** (Kafka retention; files
  retained + fingerprinted; syslog durably spooled before ingest).
- **Object keys and manifests should be deterministic** for safe retry — **not**
  true today (§1.5); a **[PROPOSED]** change (Phase 4).
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
| Concurrency | Single process, **sequential** loop | N replicas via Kafka consumer groups **[PROPOSED]** |
| State backend | **SQLite only** | Pluggable; Postgres-compatible for HA **[PROPOSED]** |

---

## 2. System model

VTOP has a clean **two-plane** design; understanding it keeps the HA design small.

| Plane | Holds | Component | Scale/HA story |
|---|---|---|---|
| **Data plane** | telemetry bytes | Object storage (S3 / MinIO) | Durable + HA-capable; add Object Lock (WORM) |
| **Control plane** | batch lifecycle ledger | **State store** (SQLite today) | The single thing blocking horizontal scale |

```
                         ┌──────────────────────────────────────────┐
  sources               │                VTOP engine                 │      archive
 ┌─────────┐  read      │  discover → batch → seal → compress →       │  put ┌──────────┐
 │ Kafka   │──────────► │  checksum → upload object → upload manifest │─────►│  S3 /    │
 │ files   │            │  → VERIFY → COMMIT source progress          │      │  MinIO   │
 │ syslog  │            └───────────────┬────────────────────────────┘      │ (WORM)   │
 └─────────┘                            │ ~9 small state writes/batch        └──────────┘
                                        ▼
                              ┌────────────────────┐
                              │   STATE STORE       │  ← SQLite (dev) / Postgres-compat (prod)
                              │ replay ledger,      │
                              │ enforces invariant  │
                              └────────────────────┘
```

**Codebase facts that shape the plan:**
- ~**9 tiny state writes/batch** (`save → sealed → compressed → checksummed →
  object_uploaded → manifest_uploaded → verified → source_committed`) + a recovery
  scan (`list_incomplete`) at startup.
- The run loop is **single-process and sequential** today.
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
| Durable shared ledger | **ONE** Postgres-compatible DB | **Yes** | PostgreSQL, **or** YugabyteDB/CockroachDB for a self-HA store. Pick one (§13). |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | **Required for HA** | Single-node Kafka **reading exists today**; **fleet consumer-group mode is [PROPOSED]** (Phase 5). Kafka is the coordinator — no extra coordination DB for the Kafka path. |
| Durable data plane | **S3 / MinIO** | **Yes (have it)** | Distributed MinIO (erasure-coded) or S3; Object Lock for WORM. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | **Yes for HA** | Restarts, rolling upgrades, lag-based autoscale. |
| Observability | **Prometheus + Grafana + OpenTelemetry** | **Yes** | Metrics, dashboards, traces, alerts. |
| Secrets | **Vault / external-secrets / k8s Secrets** | Recommended | Creds already injected via env. |
| File/syslog HA ownership | **etcd / Consul** (leases) | **Only if** file/syslog must be HA-distributed | The only thing Kafka groups don't solve. |

### 4.1 Deliberately NOT added
- **Redis** — not the durable store (durability is the point); nothing to cache.
- **Two databases at once** — Postgres *and* Yugabyte/Cockroach are alternatives.
- **etcd** — skip unless distributed file/syslog ingestion is required.

> If Kafka is the primary ingress, enterprise HA is essentially **one new database
> + the `StateStore` abstraction + k8s/Prometheus**.

---

## 5. The `StateStore` abstraction (the one piece of real work)

### 5.1 Backend selection = a config or secret reference
The engine reads `engine.state_store`; a factory dispatches on the resolved
scheme. SQLite paths may be inline. PostgreSQL URLs must come from an env/file
secret reference so credentials never enter serializable config.

| Deployment | `engine.state_store` |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `{ env: VTOP_STATE_STORE }` → `postgres://…?sslmode=verify-full` |
| Production (Yugabyte) | `{ file: /run/secrets/vtop-state-store }` → `postgres://…?sslmode=verify-full` |


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
| **Conflict retry** | none | **retry on SQLSTATE `40001`** (distributed serialization) |
| Build | default | behind Cargo `--features postgres` |

### 5.4 Backend-switching policy (corrected — NOT "no migration ever")
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

### 5.5 Database schema & constraints (defense in depth) **[PROPOSED]**
Even though `vtop-core` enforces the invariant, the database must too. Note the
constraints are **state-aware** so they are directly implementable:

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
  -- File/syslog HA (Phase 7)
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

**Multi-writer recovery:** claim incomplete rows with
`SELECT … FOR UPDATE SKIP LOCKED`, or via `lease_owner`/`lease_until`, so two
instances never recover the same batch.

### 5.6 Ledger retention / pruning **[PROPOSED]**
```text
- keep ACTIVE, FAILED, REPLAY_REQUIRED, and recent SOURCE_COMMITTED rows hot;
- archive old SOURCE_COMMITTED rows to cold tables / object storage;
- retain enough for audit, replay, and compliance;
- prune by tenant / source / date / status.
```

### 5.7 Why no row migration for the *Kafka* path
For Kafka, resume position is broker-side committed offsets, so a fresh store does
not lose Kafka progress (it re-discovers). This is the *only* path where "no
migration" holds — see §5.4 for the file/syslog exception.

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

### 7.3 Rebalance requirements (Phase 5 correctness)
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
machine** (add a `postgres` Compose service).

### Tier 2 — HA fleet, Kafka-primary (recommended enterprise baseline)
```
        ┌── engine-1 ─┐
 Kafka ─┼── engine-2 ─┼──► S3 / MinIO (WORM)
 (group)└── engine-3 ─┘          │
            │  │  │              │
            └──┴──┴──► Postgres / Yugabyte / Cockroach (one logical store, HA)
   metrics ─► Prometheus ─► Grafana ;  traces ─► OTel ;  autoscale ◄─ KEDA (lag)
```
N replicas on Kubernetes; Kafka consumer-group mode (§7.2); one Postgres-compatible
store; distributed MinIO/S3 + Object Lock; KEDA autoscale on lag (§7.4). File/syslog
**not HA** here (disabled or pinned to one replica). Needs real multi-node infra;
rehearsable on single-node k8s, true HA needs ≥3 nodes.

### Tier 3 — Full HA incl. file/syslog (only if required)
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
| Small prod, one engine, want backups | Tier 1 (Postgres) |
| Enterprise, Kafka is primary | **Tier 2** (k8s fleet) |
| Enterprise + distributed file/syslog | Tier 3 (+ etcd) |

---

## 9. Docker Compose vs. real hardware

| Capability | Compose (1 machine) | Needs multi-node |
|---|---|---|
| Full pipeline correctness (all sources → verified → committed) | ✅ | |
| SQLite ↔ Postgres backend switch (config-string) | ✅ | |
| Postgres backend + retry-on-`40001` | ✅ (single Postgres) | |
| Yugabyte/Cockroach **wire** compatibility | ✅ (single container) | |
| Distributed-store **HA** (survive DB node loss) | ⚠️ partial | ✅ (≥3 DB nodes) |
| Multi-engine Kafka consumer-group distribution | ❌ **not until Phase 5** — single instance per state store, enforced at startup (#66/#93) | ✅ (after Phase 5, on k8s) |
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

## 11. Phased implementation plan

Each phase ships independently. Phases 1–2 are zero-behavior-change groundwork. Full
roadmap detail (dependencies, test plans, rollback) is in §12.

### Phase 0 — Baseline hardening of the current prototype
- **Objective:** make the current SQLite + Kafka + MinIO Compose deployment
  **measurable and reproducible** before any architectural change.
- **Tasks:** document current state-machine behavior; add crash/replay tests; add
  **duplicate-object measurement**; add benchmark datasets; record baseline
  throughput, upload latency, replay rate, verification latency; confirm
  `require_strong_verification: true` behavior.
- **Exit:** current behavior reproducible; crash **before** VERIFIED → replay; crash
  **after** VERIFIED but before source commit → safe; duplicate-object behavior
  measured and documented.

### Phase 1 — Extract the `StateStore` trait  ✅ DONE
- Trait + SQLite impl; engine holds `Box<dyn StateStore>`; scheme factory (SQLite).
- **Exit:** all existing tests pass; behavior identical; `sqlite://` works.

### Phase 2 — Centralize invariant + shared test battery  ✅ DONE
- Move verify-before-commit guard into `vtop-core`; backend-agnostic test battery.
- **Exit:** one invariant implementation; battery green on SQLite.

### Phase 3 — Postgres backend + DB constraints (`--features postgres`)  ✅ DONE
- `PgStateStore` (PgPool, `$N`, Postgres DDL); **schema constraints from §5.5**;
  **retry-on-`40001`**; run the battery against Postgres.
- PostgreSQL DDL is an explicit `vtopctl migrate` deployment step. The engine
  role receives only schema `USAGE` plus `SELECT, INSERT, UPDATE` on `batches`;
  the live battery proves DDL, `DELETE`, and `TRUNCATE` remain denied.
- **Exit:** identical behavior SQLite/Postgres; `postgres://` selectable; DB
  enforces the invariant too.

### Phase 4 — Deterministic keys + idempotent retry
- Deterministic / content-addressed object keys; "existing verified object =
  success"; record `version_id`. (Before Kafka fleet mode so replays don't
  duplicate.)
- **Exit:** replaying a batch produces **no duplicate object**; Object Lock safe.

### Phase 5 — Kafka consumer-group (multi-instance) mode
- Long-lived `subscribe` + manual commit-after-verify; revocation handling (§7.3);
  config toggle `assign|subscribe`.
- **Exit:** two replicas split partitions; killing one rebalances; no double-commit.

### Phase 6 — Operability (k8s + observability)
- Helm; liveness/readiness; Prometheus endpoint; OTel traces; Grafana; Alertmanager;
  KEDA on lag (§7.4 bounds).
- **Exit:** rolling upgrade w/o loss; autoscale on lag; alerts fire.

### Phase 7 — File/syslog HA (optional)
- etcd/Consul leases; takeover from state-store cursor; durable-spool requirement
  for syslog; chaos tests.
- **Exit:** owner death transfers ownership with no gap/duplication.

### Phase 8 — Hardening / DR
- Object Lock/WORM profile; Vault; multipart + concurrent uploads; multi-writer
  recovery (`SELECT … FOR UPDATE SKIP LOCKED` / lease column); retention/pruning
  (§5.6); backup/DR (§17); security baseline (§18).

---

## 12. Production Roadmap

Dependency-ordered, with priority, test plan, exit criteria, and rollback per phase.
Priority: **P0** = prerequisite for any HA; **P1** = required for the Tier-2
baseline; **P2** = situational/optional.

| Phase | Objective | Pri | Depends on | Main work | Test plan | Exit criteria | Rollback |
|---|---|---|---|---|---|---|---|
| 0 | Baseline hardening | P0 | current code | crash/replay tests, benchmarks, structured logs, dup-object measurement | Compose end-to-end + fault injection | reproducible baseline; crash semantics confirmed | stay on current SQLite build |
| 1 | `StateStore` trait | P0 | 0 | trait + SQLite impl + scheme factory | existing test suite | no behavior change; `sqlite://` works | revert trait extraction (pure refactor) |
| 2 | Shared invariant tests | P0 | 1 | move guard to core; backend-agnostic battery | invalid-transition + replay tests | one invariant impl; battery green | keep SQLite path |
| 3 | Postgres backend | P1 | 2 | `PgStateStore`, migrations, §5.5 constraints, retry-`40001` | SQLite/Postgres parity; constraint-violation tests | `postgres://` works; DB enforces invariant | switch back to `sqlite://` (drain first) |
| 4 | Deterministic keys | P1 | 3 | idempotent object naming; existing-verified = success; record version_id | replay same batch; Object Lock test | no duplicate objects; Object Lock safe | `VTOP_OBJECT_KEY_MODE=legacy` |
| 5 | Kafka fleet mode | P1 | 4 | `subscribe` + manual commit + rebalance handling | two replicas + kill test | safe rebalance; no double-commit | `VTOP_KAFKA_GROUP_MODE=assign`, 1 replica |
| 6 | Kubernetes ops + observability | P1 | 3,4,5 | Helm, probes, metrics, traces, alerts, KEDA | rolling upgrade under load; pod-kill | no loss on rollout; autoscale within partition bound | Helm rollback; disable metrics endpoint |
| 7 | File/syslog HA | P2 | 3,6 | etcd leases + cursor takeover | owner-death test | no gaps/duplication on takeover | pin source to one replica |
| 8 | Hardening / DR | P1 | 6 | backup, restore, WORM, retention, security | DR drill; divergence checker | documented RPO/RTO; security baseline met | restore previous release |

```
 Phase dependency graph
   0 ─► 1 ─► 2 ─► 3 ─► 4 ─► 5 ─► 6 ─► (7 optional, 8) 
   base  trait  inv   PG   keys  kafka  k8s     fileHA / hardening+DR
```

---

## 13. Database choice matrix

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
once, and **requires validation** under your write rate.

---

## 14. Configuration & environment reference

### 14.1 Current config (`config.yaml`) — implemented
| Key | Meaning |
|---|---|
| `engine.name` / `engine.tenant` | identity; default tenant |
| `engine.state_store` | backend selector: inline `sqlite://…`, or `{ env: … }` / `{ file: … }` secret reference for PostgreSQL |
| `engine.work_dir` / `log_level` | scratch dir; verbosity |
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
| `upload.create_bucket` | auto-create per-format buckets |
| `upload.require_strong_verification` | defaults true — false explicitly permits size-only commit |
| `partitioning.template` | object key layout |

### 14.2 Current environment variables — implemented
| Variable | Purpose |
|---|---|
| `VTOP_CONFIG` | path to `config.yaml` |
| `RUST_LOG` | log filter |
| `VTOP_STATE_STORE` | PostgreSQL URL when named by `engine.state_store.env`; remote URLs require `sslmode=verify-full` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials |
| `AWS_REGION` | S3 region |
| `VTOP_S3_ENDPOINT_URL` | S3/MinIO endpoint |
| `VTOP_S3_FORCE_PATH_STYLE` | path-style addressing (MinIO) |
| `VTOP_S3_VERIFY_TLS` | TLS verification toggle (off = lab only) |
| *(Kafka SASL)* | password read from the **env var named** in `sasl_password_env` |

### 14.3 Proposed environment variables (HA phases) **[PROPOSED]**
| Variable | Phase | Purpose |
|---|---|---|
| `VTOP_PG_MAX_CONNECTIONS` | 3 | Postgres pool size per replica |
| `VTOP_PG_STATEMENT_TIMEOUT_MS` | 3 | guard stuck statements |
| `VTOP_STATE_RETRY_MAX` | 3 | max retries on SQLSTATE `40001` |
| `VTOP_OBJECT_KEY_MODE` | 4 | `legacy` \| `deterministic` \| `content-addressed` |
| `VTOP_KAFKA_GROUP_MODE` | 5 | `assign` (single-node) \| `subscribe` (fleet) |
| `VTOP_METRICS_ADDR` | 6 | Prometheus endpoint (e.g. `0.0.0.0:9090`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 6 | OpenTelemetry collector |
| `VTOP_INSTANCE_ID` | 5/7 | stable replica identity (group member / lease owner) |
| `VTOP_ETCD_ENDPOINTS` | 7 | etcd/Consul endpoints for leases |
| `VTOP_LEASE_TTL_SECONDS` | 7 | source-ownership lease TTL |

> Secrets (DB password, S3 keys, Kafka SASL) **must** come from a secret manager,
> never `config.yaml`.

---

## 15. Observability
- **Metrics (Prometheus):** batches/sec; records/sec; bytes in/out; compression
  ratio; per-stage latency; upload latency; **verification-failure rate**; **replay
  / REPLAY_REQUIRED rate**; Kafka **consumer lag**; state-store write latency;
  in-flight batches; failed batches; **duplicate-object rate** (until Phase 4).
- **Traces (OpenTelemetry):** one span per batch, child spans per stage (already
  emitted as structured events).
- **Dashboards (Grafana) + Alerts (Alertmanager):** verification failures > 0;
  replay-rate spike; lag growth; store write-latency SLO breach; no committed
  batches in N minutes.

---

## 16. Failure modes & recovery semantics
| Failure | Behavior | Why it's safe |
|---|---|---|
| Engine crash mid-batch | `recover()`: pre-VERIFIED → replay from last committed source position; VERIFIED-but-not-committed → retry commit | Progress never advanced for unverified data |
| Replay after crash | **Today:** new object key → duplicate object (no loss). **Phase 4:** same key, existing verified = success | At-least-once now; idempotent after Phase 4 |
| State store unavailable | Cannot transition → stops committing (fails safe) | No commit-before-verify possible |
| Kafka rebalance (fleet) | Revoked partitions: finish/abandon in-flight, no commit; reassigned replica resumes from committed offset | Offsets commit only post-verify |
| File-owner replica dies (Tier 3) | Lease expires; another replica resumes from stored byte cursor | Cursor in shared durable store |
| Object Lock blocks overwrite | Retry uses deterministic key / new version, never overwrite | §6.2 rule |

---

## 17. Backup, restore & disaster recovery **[PROPOSED]**
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

## 18. Security hardening **[PROPOSED baseline]**
```text
- TLS for Kafka, DB, object store, and the metrics endpoint.
- IAM/bucket policy scoped to required prefixes only (least privilege).
- Separate credentials per environment and per tenant.
- Secrets via Vault / Kubernetes Secrets / external-secrets — never config.yaml.
- Server-side encryption / KMS where available; encryption in transit everywhere.
- Audit logs for object-store writes and state-store changes.
- Kubernetes NetworkPolicies isolating engine ↔ DB ↔ Kafka ↔ object store.
- Restricted admin access; least-privilege RBAC.
- Signed container images (e.g. cosign) if applicable.
- Vulnerability scanning (image + dependency) in CI; SBOM per release.
```

---

## 19. Risk register
| Risk | Impact | Mitigation | Phase | Owner |
|---|---|---|---|---|
| Object Lock prevents overwrite-based retry | Retries fail / stuck batches | Deterministic keys + "existing verified = success" / version_id (§6.2) | 4 | Engineering (storage) |
| Kafka rebalance during in-flight batch | Duplicate work / spurious revocation | Revocation handler; tune poll/session timeouts; cooperative rebalancing (§7.3) | 5 | Engineering (ingest) |
| State database unavailable | Engine cannot commit | Fail safe (stop committing); HA store; alerts | 3,6 | Platform |
| Object uploaded but manifest upload failed | Batch stuck pre-VERIFIED | Retry manifest; verify existing object; bounded `retry_count` → FAILED | 0,3 | Engineering (pipeline) |
| Manifest uploaded but source-commit failed | Possible replay | Recovery retries commit; deterministic keys make replay idempotent | 4 | Engineering (engine) |
| File cursor lost during backend switch | File reprocessing / duplicates | Drain + cursor migration + validation + rollback (§5.4) | 3 | Platform / Ops |
| Syslog UDP packet loss | Silent data loss | Ingest only from durable spool; document UDP limits (§8) | 7 | Engineering / Ops |
| KEDA scales beyond useful partition count | Wasted replicas, no throughput gain | Bound max replicas to partition count (§7.4) | 6 | Platform |
| State-store growth without retention | Ledger bloat, slow scans | Retention/pruning policy; archive cold rows (§5.6) | 8 | Ops |
| Distributed-SQL serialization retries (`40001`) | Latency spikes under contention | Retry-on-`40001`; capacity test; store choice (§13) | 3 | Engineering (state) |

---

## 20. Operator runbook & rollback procedures

Short procedures now; expand into a full ops runbook before go-live (Phase 8/10).

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
- **Restore from DB backup:** restore Postgres PITR → verify object store →
  start the engine (recovery scan re-reconciles) → run the divergence checker.
- **Reconcile state store vs object storage:** list `SOURCE_COMMITTED` rows and
  confirm each object+manifest exists; flag orphan objects (no row) and missing
  objects (row but no object) for remediation.
- **Rotate credentials (S3 / Kafka / DB):** update the secret in the secret manager
  → rolling-restart engines → confirm new connections succeed → revoke old creds.
- **Scale engine replicas:** `kubectl scale deploy/vtop-engine --replicas=N`, with
  `N ≤ active partition count` for the topic (§7.4); KEDA can automate within bounds.
- **Respond to a verification-failure alert:** check the failing backend (object
  store reachability, checksum mismatch), confirm `require_strong_verification`, and
  hold commits (the engine already refuses to commit unverified batches).

---

## 21. Known limitations (current code)
- **SINGLE-INSTANCE ONLY — enforced at startup (#66).** The engine takes an
  exclusive OS lock on its work directory and refuses to start beside another
  engine on the same host. There is no claim/lease/fencing in the state store
  yet (#93, Phase 5), so two engines over the same store would both recover the
  same incomplete batches and both commit source progress — duplicate ingestion
  at best, double-commit at worst. The work-dir lock CANNOT see an engine on a
  different host pointed at the same Postgres; that configuration is
  unsupported and warned about at startup. Do not scale replicas.
- **Non-deterministic object keys** — replay can create **duplicate objects** (no
  loss). Fixed by deterministic/content-addressed keys (Phase 4).
- **Engine loop is single-process / sequential** — no horizontal scale until
  Phase 5 (Kafka consumer-group mode).
- **File/syslog HA is not solved without leases** — single-owner only until Phase 7.
- **UDP syslog cannot be made lossless** without durable spooling before VTOP.
- **Whole-file mode can cause memory spikes** — it loads the whole file into memory;
  size accordingly or keep large inputs line-oriented.

---

## 22. Production-readiness checklist
```text
Correctness & invariant
  [ ] require_strong_verification remains true
  [ ] deterministic or content-addressed object keys enabled
  [ ] Object Lock behavior tested (retry never overwrites a locked object)
  [ ] crash before VERIFIED → replay; crash after VERIFIED/before COMMIT → safe
  [ ] DB constraints enforce no commit before verify (state-aware CHECKs)

State store
  [ ] Postgres-compatible state store deployed
  [ ] StateStore trait + shared test battery green on SQLite AND Postgres
  [ ] retry-on-40001 implemented and load-tested
  [ ] schema constraints + indexes (UNIQUE source range; state index)
  [ ] multi-writer recovery (FOR UPDATE SKIP LOCKED or leases)
  [ ] retention/pruning policy defined

Kafka / scaling
  [ ] Kafka auto-commit disabled; commit only after VERIFIED
  [ ] rebalance/revocation handling tested
  [ ] KEDA min/max replicas bounded by partition count

Object storage
  [ ] distributed MinIO / S3; Object Lock policy; retention configured

Operability
  [ ] Prometheus metrics enabled; Grafana dashboards; alerts configured
  [ ] OpenTelemetry traces; health/readiness endpoints; SLOs documented
  [ ] k8s: probes, resource limits, PDB, NetworkPolicy, rolling-upgrade runbook

Security
  [ ] TLS for Kafka, DB, object store, and metrics
  [ ] secrets from Vault / k8s Secrets / external-secrets (none in config.yaml)
  [ ] least-privilege IAM; per-env/per-tenant credentials; encryption at rest
  [ ] image signing; vuln scan; SBOM

Resilience / DR
  [ ] crash/replay tests passed
  [ ] backup/restore tested; object-store ↔ state-store reconciliation tested
  [ ] DR runbook approved; RPO/RTO documented
  [ ] risk register reviewed; each risk has an owner
```

---

## 23. Open decisions (need a human call)
1. **Primary ingress:** Kafka-only (stop at Tier 2) **or** file/syslog at scale
   (Tier 3 + etcd, Phase 7)?
2. **Store choice:** PostgreSQL + Patroni vs Yugabyte/Cockroach (§13) — familiar ops
   + low latency vs self-healing DB HA.
3. **Object-key scheme:** adopt deterministic / content-addressed keys (Phase 4)?
   Required for idempotency + Object Lock safety.
4. **Compliance:** is Object Lock / WORM mandatory (audit/regulatory)? If yes it
   becomes baseline, not optional.
5. **State-write latency target & RPO/RTO:** drives store choice, retry tuning, and
   backup design.
6. **Scope/timeline:** which target tier (1, 2, or 3) is in scope for the first
   production release?

---

## 24. TL;DR
- **One** durable Postgres-compatible store; **no Redis**; etcd only for distributed
  file/syslog.
- The **`StateStore` trait (Phase 1)** is the single real prerequisite; after it,
  SQLite ↔ Postgres ↔ Yugabyte/Cockroach is a **config-string** choice (§13).
- **Two current-behavior caveats the design must fix for production:**
  (a) object keys are **non-deterministic** → replay makes **duplicates**; fix with
  **deterministic keys (Phase 4)** for idempotency + Object Lock safety.
  (b) file/syslog cursors live **only in the state store** → **migrate/drain** on
  backend switch (Kafka is broker-side and safe).
- **VERIFIED is defined precisely (§3); strong content-derived verification is
  the default and production must not opt out.**
- **Kafka consumer groups + k8s + Prometheus** deliver HA for the Kafka path with
  minimal new infrastructure (fleet mode is **[PROPOSED]**, Phase 5).
- **Correctness and backend portability are fully Docker-Compose-testable on one
  machine;** only true HA behavior needs multi-node Kubernetes.
- See **§12 Production Roadmap**, **§19 Risk register**, **§20 Operator runbook**,
  and **§22 Readiness checklist** for execution.
