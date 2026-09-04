# VTOP Engine — Production HA: Design, Status & Roadmap

> **Status:** living design + execution document. **Audience:** engineers,
> operators, and decision-makers taking the VTOP *engine* (the telemetry-object
> transfer pipeline: `vtop-core`, `vtop-state`, `vtop-adapters`, `vtop-upload`,
> `vtopctl engine`) from a single-process deployment to a highly-available
> fleet, without weakening the core guarantee.
>
> This is the **single source of truth** for engine HA: Part I says where
> things stand, Part II holds the architecture and operational reference,
> Part III holds the phased roadmap, risks, and readiness gates. It replaces
> the former `PRODUCTION_HA_PLAN.md` / `PRODUCTION_HA_ROADMAP.md` pair.
>
> Normative behavior (states, commit rule, verification classes, naming) is
> defined by [`VTOP_PROTOCOL_DRAFT.md`](VTOP_PROTOCOL_DRAFT.md); this document
> cites it as "protocol §N" and describes how to *deploy* a conformant engine,
> not what conformance means. Claims that depend on unbuilt work are marked
> **[PROPOSED]**; shipped behavior is marked **[IMPLEMENTED]**.
>
> Scope note: this document is about the **engine pipeline**. The distributed
> VTOP *cluster* (`vtop-meta` Raft metadata, `vtop-node`, `vtop-broker`,
> `vtop-log` sealed-segment replication) is a separate plane with its own
> narrative roadmap in [`ROADMAP.md`](ROADMAP.md); §25 explains how the two
> relate.

---

## Table of contents

**Part I — Status**
1. Where things stand

**Part II — Design & reference**
2. Scope, goals, assumptions
3. System model
4. Definition of VERIFIED
5. What production-grade HA needs
6. The `StateStore` abstraction
7. Object storage, idempotency & Object Lock / WORM
8. Kafka HA: choreography, rebalance, autoscaling
9. Deployment topologies
10. Docker Compose vs. real hardware
11. Hardware sizing
12. Database choice matrix
13. Configuration & environment reference
14. Observability
15. Failure modes & recovery
16. Backup, restore & disaster recovery
17. Security hardening
18. Operator runbook & rollback procedures
19. Known limitations

**Part III — Roadmap & governance**
20. Roadmap: phase model and details
21. Roadmap table
22. Risk register
23. Production-readiness checklist
24. Open decisions requiring human approval
25. Relationship to the cluster roadmap
26. TL;DR

---

# Part I — Status

## 1. Where things stand

As of **August 2026** (repo at v0.3.0):

**Done**
- `StateStore` trait extracted; engine holds `Box<dyn StateStore>`; scheme
  factory on `engine.state_store` (Phase 1).
- Verify-before-commit guard centralized in `vtop-core`; backend-agnostic
  shared test battery (Phase 2).
- PostgreSQL backend behind `--features postgres`: `PgStateStore`, state-aware
  DB constraints, retry-on-SQLSTATE-`40001`, `vtopctl migrate` under a
  separate privileged identity, runtime role denied DDL/`DELETE`/`TRUNCATE`
  (Phase 3). **Tier 1 (single engine + Postgres) is deployable today from a
  source build with `--features postgres`** — release binaries and the Docker
  image do not yet enable the feature.
- Ledger retention/pruning: `engine.ledger_retention_days` incremental delete
  on SQLite; `vtopctl prune-ledger` under a maintenance identity on
  PostgreSQL (#128, part of Phase 8).
- Prometheus metrics + readiness endpoint, opt-in via `VTOP_METRICS_ADDR`
  (`vtop-observe`); structured per-stage events; Grafana/Loki/Mimir/Tempo/Alloy
  compose stack and generated dashboards in `observability/` (part of Phase 6).
- Single-instance safety: exclusive work-dir lock at startup (#66) so the
  unsupported multi-writer configuration fails loudly instead of corrupting
  progress.
- Release hygiene: SBOM, cosign signatures, `SHA256SUMS`, `cargo-deny` in CI
  (part of Phase 8's security baseline).

**Not started — the critical path to a fleet**
- Phase 4: deterministic / content-addressed object keys + idempotent retry.
  Batch ids still embed `Utc::now()` + `Uuid::new_v4()`; replay still writes a
  duplicate object. This is also the implementation's one known conformance
  gap against protocol §15.1 (deterministic naming is a MUST).
- Phase 5: Kafka consumer-group fleet mode. The consumer still uses per-read
  `assign()`; no `VTOP_KAFKA_GROUP_MODE` toggle; no multi-writer recovery
  (`FOR UPDATE SKIP LOCKED` / leases, #93).
- Phase 7: an engine Helm chart. `helm/vtop` deploys the *cluster* plane, not
  the engine.

**Partially done**
- Phase 6 (observability & ops): metrics, readiness, dashboards exist; OTel
  trace export from the engine and Alertmanager rules do not.
- Phase 8 (retention/backup/DR/security): retention shipped; backup/restore
  runbook, DR drills, and the divergence checker have not been exercised.

**Optional / gated**
- Phase 9 (file/syslog HA via leases) — only if distributed file/syslog
  ingestion is actually required (open decision §24.1).
- Phase 10 (production readiness review) — after the target tier's phases.

---

# Part II — Design & reference

## 2. Scope, goals, assumptions

### 2.1 What we are building toward
Take the VTOP engine from a **single-process deployment** (one engine, its own
ledger) to a **horizontally scalable, highly-available** telemetry-object
transfer engine, **without weakening the core guarantee**:

> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

### 2.2 Non-negotiable invariants
- **Verify-before-commit** — source progress (Kafka offset / file byte cursor /
  spool position) advances only after the object **and** manifest are uploaded
  and verified.
- **Replay safety** — a crash at any stage is recoverable with **no data loss**.
- **Delivery semantics (accurate):** today the system is **at-least-once with
  possible duplicate objects** (§2.5, §7). *Idempotent at the archive layer*
  requires **deterministic / content-addressed object keys [PROPOSED]** —
  which protocol §15.1 already mandates (deterministic naming is a **MUST**);
  this is the one known conformance gap in the reference implementation.

### 2.3 Non-goals
- Not a stream-processing/analytics engine (only framing/format detection).
- Not a datastore for the telemetry itself — object storage is the archive.

### 2.4 Assumptions
This plan is "safe under these assumptions"; where one fails, the relevant
section calls out the consequence.
- **Kafka is the primary production ingress** (file/syslog are secondary; their
  HA is optional and built only if required).
- Source systems **retain data long enough for replay** (Kafka retention; files
  retained + fingerprinted; syslog durably spooled before ingest).
- **Object keys and manifests should be deterministic** for safe retry — **not**
  true today (§2.5); a **[PROPOSED]** change.
- Object storage provides **read-after-write consistency** on the verification
  path (true for S3 and MinIO today).
- **Secrets are injected externally** via a secret manager (never in `config.yaml`).
- Production uses **TLS everywhere** (Kafka, DB, object store, metrics).
- **File/syslog HA requires extra coordination** (leases) and is built only if
  required.

### 2.5 Current vs. target behavior (read this before trusting any HA claim)
| Property | Current code | Target for production HA |
|---|---|---|
| Object key | `vtop-<UTCstamp>-<source>-<range>-<uuid8>` — **non-deterministic** (`Utc::now()` + `Uuid::new_v4()`) | **Deterministic or content-addressed** so retries are idempotent **[PROPOSED]** |
| Replay outcome | Re-processing writes a **new object** (new key) → **duplicate object, no overwrite, no loss** | Retry resolves to the **same** object/version; duplicates impossible **[PROPOSED]** |
| File/syslog cursor | Lives **only in the state store** (rebuilt from `SOURCE_COMMITTED` rows) | Same, but **migrate/drain on backend switch** (§6.4) |
| Kafka offset | Committed to the **broker** after VERIFIED (resumes broker-side) | Same; consumer-group mode for multi-instance **[PROPOSED]** |
| Concurrency | Single process, **single-instance lock enforced at startup** (#66) | N replicas via Kafka consumer groups **[PROPOSED]** |
| State backend | **SQLite and PostgreSQL** (feature-gated) behind the `StateStore` trait **[IMPLEMENTED]** | Same; Postgres-compatible store is the production path |
| Metrics | Prometheus endpoint, opt-in via `VTOP_METRICS_ADDR` **[IMPLEMENTED]** | Same, plus traces and alerting (§14) |

---

## 3. System model

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
  instance lock at startup (§19).
- The **bottleneck is S3 upload**, not the state store.
- **Object keys are non-deterministic** (§2.5) — central to §7.

---

## 4. Definition of VERIFIED (precise)

The whole invariant hinges on `VERIFIED`, so it must be unambiguous. This
section operationalizes the protocol's commit rule (protocol §13) and
verification semantics (protocol §17); the state names are those of the
protocol state machine (protocol §12).

**A batch is VERIFIED only when all of the following hold:**
1. the **object exists** in object storage;
2. the **manifest exists** in object storage;
3. the **object size matches** the manifest's recorded size;
4. the object **checksum** (SHA-256 or BLAKE3), derived from stored bytes or
   computed by the storage service, **matches the manifest checksum** — this
   is the **strong** class, and it is what item 4 means under the default
   configuration; see the verification-strength note below for the explicit
   opt-outs that weaken it;
5. the **state store has persisted** `object_key`, `manifest_key`, checksum,
   checksum algorithm, compression type, source range, and `batch_id` **before**
   the VERIFIED transition;
6. **[PROPOSED]** the S3 `version_id` (and/or checksum header) is recorded when
   Object Lock / versioning is enabled;
7. **verification failure prevents source commit** — the batch never advances to
   SOURCE_COMMITTED.

**Verification strength (current code):** protocol §17 names three classes —
**strong** (stored-content/service-computed checksum), **backend-limited**
(size/existence only), and **disabled checksum** (size-only by configuration,
the weakest, an explicit opt-in). The engine's verification result carries a
single strength flag, so the code REPORTS the third class through the same
flag as the second: `checksum.algorithm: none` verifies size and existence
only and surfaces as a backend-limited result. Strong verification defaults
on, and `upload.require_strong_verification: false` is the explicit
compatibility/lab opt-out that allows a non-strong result to commit — so
under the default, batches produced with checksums disabled **refuse to
commit**, and running that way requires both opt-outs deliberately: the
algorithm choice and the strength opt-out, each visible in configuration.

**ETag caveat:** S3 multipart ETags are **not** reliable MD5 checksums. The
authoritative integrity value is VTOP's own **SHA-256/BLAKE3 manifest checksum**,
never the ETag.

---

## 5. What production-grade HA actually needs

The honest, minimal set — **one** durable store, not a zoo.

| Need | Component | Required? | Notes |
|---|---|---|---|
| Durable shared ledger | **ONE** Postgres-compatible DB | **Yes** | PostgreSQL, **or** YugabyteDB/CockroachDB for a self-HA store. Pick one (§12). Backend **[IMPLEMENTED]**. |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | **Required for HA** | Single-node Kafka **reading exists today**; **fleet consumer-group mode is [PROPOSED]**. Kafka is the coordinator — no extra coordination DB for the Kafka path. |
| Durable data plane | **S3 / MinIO** | **Yes (have it)** | Distributed MinIO (erasure-coded) or S3; Object Lock for WORM. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | **Yes for HA** | Restarts, rolling upgrades, lag-based autoscale. An engine chart is **[PROPOSED]** — the existing `helm/vtop` chart deploys the *cluster*, not the engine (§9). |
| Observability | **Prometheus + Grafana (+ traces)** | **Yes** | Metrics endpoint **[IMPLEMENTED]**; dashboards in `observability/`; traces **[PROPOSED]** (§14). |
| Secrets | **Vault / external-secrets / k8s Secrets** | Recommended | Creds already injected via env. |
| File/syslog HA ownership | **etcd / Consul** (leases) | **Only if** file/syslog must be HA-distributed | The only thing Kafka groups don't solve. |

### 5.1 Deliberately NOT added
- **Redis** — not the durable store (durability is the point); nothing to cache.
- **Two databases at once** — Postgres *and* Yugabyte/Cockroach are alternatives.
- **etcd** — skip unless distributed file/syslog ingestion is required.

> If Kafka is the primary ingress, enterprise HA is essentially **one new database
> + the `StateStore` abstraction + k8s/Prometheus** — and the database and
> abstraction are already built.

---

## 6. The `StateStore` abstraction **[IMPLEMENTED]**

Shipped in `crates/vtop-state`: the trait, a SQLite backend, a PostgreSQL
backend behind `--features postgres`, and a shared backend-agnostic test
battery.

### 6.1 Backend selection = a config or secret reference
The engine reads `engine.state_store`; a factory dispatches on the resolved
scheme. SQLite paths may be inline. PostgreSQL URLs must come from an env/file
secret reference so credentials never enter serializable config.

| Deployment | `engine.state_store` |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `{ env: VTOP_STATE_STORE }` → `postgres://…?sslmode=verify-full` |
| Production (Yugabyte/Cockroach) | `{ file: /run/secrets/vtop-state-store }` → `postgres://…?sslmode=verify-full` |

### 6.2 The trait (one source of truth for the invariant)
- Abstracts: `save_batch_state`, `update_batch_state`, `mark_verified`,
  `mark_source_committed`, `mark_failed`, `get_batch`, `list_incomplete_batches`,
  `list_failed_batches`, `list_batches`.
- Engine holds `Box<dyn StateStore>`.
- The **verify-before-commit guard stays as pure logic in `vtop-core`**, called by
  every backend — never re-implemented per backend.
- **Defense in depth:** the database **also** enforces the invariant via
  constraints (§6.5). Do not rely on application logic alone.

### 6.3 Backend differences
| Concern | SQLite | Postgres / Yugabyte / Cockroach |
|---|---|---|
| Driver | `sqlx` `SqlitePool` | `sqlx` `PgPool` (pure-Rust, no libpq) |
| Placeholders | `?` | `$1, $2, …` |
| Insert | plain `INSERT` | plain `INSERT` |
| Migrations | SQLite initializes locally | `vtopctl migrate` with a separate privileged identity; runtime executes no DDL |
| **Conflict retry** | none | **retry on SQLSTATE `40001`** (distributed serialization) **[IMPLEMENTED]** |
| Build | default | behind Cargo `--features postgres` |

### 6.4 Backend-switching policy (NOT "no migration ever")
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

### 6.5 Database schema & constraints (defense in depth) **[IMPLEMENTED — with one gap]**

**What ships today**
(`crates/vtop-state/migrations/postgres/0001_state_store.sql`, applied by
`vtopctl migrate` under a deployment identity): the `batches` table with a
CHECK-constrained state enum, an **invariant trigger** that rejects
`source_committed` without verification (the commit rule at the database
layer), state / source / commit-cursor indexes, and the privilege split — the
engine role gets only schema `USAGE` plus `SELECT, INSERT, UPDATE` on
`batches`, and the live battery proves DDL, `DELETE`, and `TRUNCATE` remain
denied. Progress markers are stored as JSON columns
(`progress_start_json` / `progress_end_json`).

**The gap:** the **UNIQUE source-range dedup indexes below are [PROPOSED]**,
not shipped — they require the marker components as real columns rather than
JSON. Until they exist, duplicate ranges are prevented by the single-instance
lock and ledger logic, **not** by the database; they must land with the
Phase 5 multi-writer work.

The schema below is the **reference/target** shape (state-aware constraints):

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
  end_offset          BIGINT,                         -- last record offset (see §8.1)
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

### 6.6 Ledger retention / pruning **[IMPLEMENTED — incremental delete, #128]**
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

## 7. Object storage, idempotency & Object Lock / WORM

This section supersedes any earlier "rewrites the same object" wording.

### 7.1 Current reality
Object keys are **non-deterministic** (`Utc::now()` + `Uuid::new_v4()` inside
the `batch_id`), so a replayed batch writes a **new** object. Result: **no data
loss, but duplicate objects** can accumulate on crash/replay. The **state
ledger + manifests** are the dedup authority today — not key collision.

Protocol §15.1 requires the naming scheme to be **deterministic so that replay
produces the same object key**, and protocol §14 says re-uploads of identical
content SHOULD be idempotent. The reference implementation follows the §15.1
partition layout (`tenant=…/source=…/format=…/year=…/…/{batch_id}…`) but the
random `batch_id` component breaks the determinism MUST — this is the
conformance gap that roadmap Phase 4 closes.

### 7.2 Object Lock / WORM safe-retry rules
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

### 7.3 Recommendation
Adopt **(1) deterministic keys + "existing verified object = success"** (optionally
with **(2)** version_id recording). Only after this change is replay **idempotent at
the archive layer** and the delivery guarantee may be described that way.

### 7.4 ETag caveat (repeat)
Never treat an S3 ETag as the integrity checksum (multipart ETags aren't MD5). The
manifest SHA-256/BLAKE3 is the source of truth.

---

## 8. Kafka HA: choreography, rebalance, autoscaling **[PROPOSED multi-instance]**

### 8.1 Commit choreography (exact order)
```text
1. Poll records.
2. Build batch with topic/partition/start_offset/end_offset.
3. Persist DISCOVERED/BATCHING in the state store.
4. Upload object.
5. Upload manifest.
6. Verify object + manifest (see §4).
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
deterministic (§7.3): the existing verified object+manifest is treated as
success. Until then, replay produces a duplicate object (no loss).
```

### 8.2 Single-node vs fleet consumer mode (design decision)
- The current code uses per-read **`assign()`** (fixed the single-node stall:
  re-`subscribe()` per read caused rebalance + reseek-to-earliest).
- **For a fleet, use long-lived `subscribe()` + manual commit-after-verify** so
  Kafka distributes partitions across replicas. Expose as a config toggle
  (`VTOP_KAFKA_GROUP_MODE = assign | subscribe`). Subscribe-once also avoids the
  original stall because committed offsets exist after the first verified batch.

### 8.3 Rebalance requirements (fleet-mode correctness)
```text
- disable auto-commit; manual commit only after VERIFIED;
- handle partition REVOCATION cleanly:
    * stop accepting new batches for revoked partitions,
    * complete or safely abandon in-flight batches (abandon = no commit → replay);
- tune max.poll.interval.ms, session.timeout.ms, heartbeat.interval.ms so long
  uploads don't trigger spurious rebalances;
- prefer cooperative (incremental) rebalancing if the client supports it.
```

### 8.4 Autoscaling caveat (KEDA)
```text
Useful engine replicas for a topic are bounded by its ACTIVE PARTITION COUNT.
More replicas than partitions does NOT add throughput. KEDA should scale on
consumer lag, but min/max replicas must align with partition count AND downstream
upload/store capacity.
```

---

## 9. Deployment topologies

### Tier 0 — Single node, Docker Compose (dev / demo / small)
One engine, **SQLite**, single MinIO, single Kafka (KRaft). The current lab.
**Fully testable on one machine.** No HA.

### Tier 1 — Single engine + external Postgres (small prod, durable store)
One engine, **Postgres** ledger (backable, survives engine host restart), S3/MinIO.
One engine (no horizontal scale) but a proper durable store. **Testable on one
machine** (add a `postgres` Compose service). **Available today from source** —
the Postgres backend is implemented (§6) but feature-gated: build `vtopctl`
with `--features postgres`. The published release binaries and Docker image do
**not** yet enable the feature and reject `postgres://` state stores at
runtime. See [`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md).

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
N replicas on Kubernetes; Kafka consumer-group mode (§8.2); one Postgres-compatible
store; distributed MinIO/S3 + Object Lock; KEDA autoscale on lag (§8.4). File/syslog
**not HA** here (disabled or pinned to one replica). Needs real multi-node infra;
rehearsable on single-node k8s, true HA needs ≥3 nodes.

> **Chart caveat:** the repository's `helm/vtop` chart deploys the co-located
> VTOP *cluster* (`vtop-node` metadata + data planes), **not** the engine
> pipeline. An engine chart (Deployment/probes/KEDA) is Phase 7.

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

### 9.1 Topology selection guide
| If your reality is… | Use |
|---|---|
| Laptop / demo | Tier 0 (Compose, SQLite) |
| Small prod, one engine, want backups | Tier 1 (Postgres) — available today via a `--features postgres` source build |
| Enterprise, Kafka is primary | **Tier 2** (k8s fleet) — pending fleet mode |
| Enterprise + distributed file/syslog | Tier 3 (+ etcd) — pending |

---

## 10. Docker Compose vs. real hardware

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

## 11. Hardware sizing (starting points — validate with `benchmarks/`)

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

## 12. Database choice matrix

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
path needed by the distributed options is already implemented (§6.3).

---

## 13. Configuration & environment reference

### 13.1 Current config (`config.yaml`) — implemented
| Key | Meaning |
|---|---|
| `engine.name` / `engine.tenant` | identity; default tenant |
| `engine.state_store` | backend selector: inline `sqlite://…`, or `{ env: … }` / `{ file: … }` secret reference for PostgreSQL |
| `engine.work_dir` / `log_level` | scratch dir; verbosity |
| `engine.ledger_retention_days` / `ledger_prune_batch` | committed-row retention (§6.6; SQLite engine-side only) |
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

### 13.2 Current environment variables — implemented
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

### 13.3 Proposed environment variables (future HA work) **[PROPOSED]**
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

## 14. Observability

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

## 15. Failure modes & recovery semantics

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
| Object Lock blocks overwrite | Retry uses deterministic key / new version, never overwrite | §7.2 rule |

---

## 16. Backup, restore & disaster recovery **[PROPOSED]**
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

## 17. Security hardening

**Already in place:**
- Least-privilege database identities: the runtime role cannot run DDL,
  `DELETE`, or `TRUNCATE`; migrations and pruning use separate identities (§6.5–6.6).
- Remote PostgreSQL URLs must use `sslmode=verify-full`.
- Secrets referenced by env/file, never serialized into config (§6.1, §13).
- Hardened external-upload-tool invocation: explicit binary path, timeout,
  output bounds, and an environment allowlist (§13.1).
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

## 18. Operator runbook & rollback procedures

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
- **Drain before a backend switch (§6.4):** stop sources → wait until no `incomplete`
  rows remain → export file/syslog cursors → switch `engine.state_store` → import
  cursors → validate → keep the old store until validation passes.
- **Prune the PostgreSQL ledger:** run `vtopctl prune-ledger --older-than-days N`
  on a schedule under the maintenance identity (§6.6).
- **Restore from DB backup:** restore Postgres PITR → verify object store →
  start the engine (recovery scan re-reconciles) → run the divergence checker.
- **Reconcile state store vs object storage:** list `SOURCE_COMMITTED` rows and
  confirm each object+manifest exists; flag orphan objects (no row) and missing
  objects (row but no object) for remediation.
- **Rotate credentials (S3 / Kafka / DB):** update the secret in the secret manager
  → rolling-restart engines → confirm new connections succeed → revoke old creds.
- **Scale engine replicas [after fleet mode]:**
  `kubectl scale deploy/vtop-engine --replicas=N`, with `N ≤ active partition
  count` for the topic (§8.4); KEDA can automate within bounds.
- **Respond to a verification-failure alert:** check the failing backend (object
  store reachability, checksum mismatch), confirm `require_strong_verification`, and
  hold commits (the engine already refuses to commit unverified batches).

---

## 19. Known limitations (current code)
- **SINGLE-INSTANCE ONLY — enforced at startup (#66).** The engine takes an
  exclusive OS lock on its work directory and refuses to start beside another
  engine on the same host. There is no claim/lease/fencing in the state store
  yet (#93), so two engines over the same store would both recover the
  same incomplete batches and both commit source progress — duplicate ingestion
  at best, double-commit at worst. The work-dir lock CANNOT see an engine on a
  different host pointed at the same Postgres; that configuration is
  unsupported and warned about at startup. Do not scale replicas.
- **Non-deterministic object keys** — replay can create **duplicate objects** (no
  loss). This is also the implementation's one known gap against the protocol
  draft's deterministic-naming MUST (protocol §15.1). Fixed by
  deterministic/content-addressed keys (Phase 4).
- **Engine loop is single-process** — no horizontal scale until Kafka
  consumer-group fleet mode lands (Phase 5).
- **File/syslog HA is not solved without leases** — single-owner only.
- **UDP syslog cannot be made lossless** without durable spooling before VTOP.
- **Whole-file mode can cause memory spikes** — it loads the whole file into memory;
  size accordingly or keep large inputs line-oriented.
- **No engine Helm chart** — `helm/vtop` deploys the cluster plane, not the
  engine pipeline (§9).

---

# Part III — Roadmap & governance

## 20. Roadmap: phase model and details

Phases are dependency-ordered and independently shippable. Deterministic keys
(Phase 4) deliberately precede fleet mode (Phase 5): once several replicas can
replay each other's work, replays must stop producing duplicate objects.

```text
  Critical path:   0 ──► 1 ──► 2 ──► 3 ──► 4 ──► 5 ──► 7 ──► 10
                  base  trait  inv   PG   keys  fleet  k8s  review

  Parallel tracks: 0 ──► 6  observability & alerting (independent; most
                             valuable alongside Phase 5)
                   3 ──► 8  retention / backup / DR / security
                 3,7 ──► 9  file/syslog HA (optional, gated on §24.1)

  Status: 0–3 ✅ done · 6, 8 ◐ partial · 4, 5, 7, 9, 10 ⬜ not started
```

(True dependencies per phase are in the §21 table; the critical path shows
what blocks the Tier 2 fleet, not a claim that every phase blocks the next.)

### Phase 0 — Baseline hardening ✅ DONE
Made the single-node SQLite + Kafka + MinIO Compose deployment measurable and
reproducible: state machine documented, crash/replay tests, benchmark harness
(`benchmarks/`), duplicate-object behavior measured, strong verification
confirmed as the default.

### Phase 1 — Extract the `StateStore` trait ✅ DONE
Trait + SQLite impl in `crates/vtop-state`; engine holds `Box<dyn StateStore>`;
scheme factory. Pure refactor — no behavior change.

### Phase 2 — Centralize invariant + shared test battery ✅ DONE
Verify-before-commit guard moved into `vtop-core`; one implementation of the
invariant; backend-agnostic battery (`vtop-state/src/test_battery.rs`) green on
SQLite.

### Phase 3 — Postgres backend + DB constraints ✅ DONE
`PgStateStore` (PgPool, `$N` placeholders) behind `--features postgres`;
the shipped subset of §6.5 (state-enum CHECK + commit-rule invariant trigger +
state/source indexes — the UNIQUE range indexes remain §6.5's open gap);
bounded retry on SQLSTATE `40001`; battery green on Postgres. DDL is an explicit `vtopctl migrate` step under a
deployment identity; the live battery proves the runtime role cannot run DDL,
`DELETE`, or `TRUNCATE`. See [`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md).

### Phase 4 — Deterministic keys + idempotent retry ⬜ NOT STARTED — next up
- **Objective:** replaying a batch must resolve to the same object, so retry is
  idempotent at the archive layer and Object Lock is safe (§7). This also
  closes the implementation's one known conformance gap against the protocol
  draft, whose naming rule is a MUST
  ([`VTOP_PROTOCOL_DRAFT.md`](VTOP_PROTOCOL_DRAFT.md) §15.1: replay produces
  the same object key; §14: identical re-uploads SHOULD be idempotent).
- **Work:** deterministic / content-addressed object keys behind
  `VTOP_OBJECT_KEY_MODE=legacy|deterministic|content-addressed`; "existing
  verified object = success" on retry; record `version_id` when
  versioning/Object Lock is on.
- **Why before fleet mode:** rebalance-induced replays multiply duplicates
  across replicas otherwise.
- **Test plan:** replay the same batch; assert no duplicate object and no
  overwrite attempt against a locked bucket (MinIO Object Lock in Compose).
- **Exit criteria:** replay produces zero duplicate objects; retries never
  overwrite or delete a protected version.
- **Rollback:** `VTOP_OBJECT_KEY_MODE=legacy` (duplicates return; no loss).

### Phase 5 — Kafka consumer-group fleet mode ⬜ NOT STARTED
- **Objective:** N replicas split partitions safely (§8).
- **Work:** long-lived `subscribe()` + manual commit-after-verify behind
  `VTOP_KAFKA_GROUP_MODE=assign|subscribe`; revocation handling; poll/session/
  heartbeat tuning; **multi-writer recovery** in the state store
  (`SELECT … FOR UPDATE SKIP LOCKED` or `lease_owner`/`lease_until`, #93);
  the **UNIQUE source-range dedup indexes** (§6.5's open gap — requires
  promoting marker components out of the JSON columns); retire the
  single-instance restriction (#66) for subscribe mode; `VTOP_INSTANCE_ID`
  for stable member identity.
- **Test plan:** two replicas share a topic; kill one; assert rebalance, no
  double-commit, no commit-before-verify, and no double-recovery of the same
  incomplete batch.
- **Exit criteria:** replicas split partitions; killing one rebalances; replay
  is idempotent (needs Phase 4 for zero duplicates).
- **Rollback:** `VTOP_KAFKA_GROUP_MODE=assign` and one replica.

### Phase 6 — Observability & alerting ◐ PARTIAL
- **Done:** Prometheus metrics + readiness endpoint (`VTOP_METRICS_ADDR`,
  `vtop-observe`); structured per-stage events; `observability/` compose stack
  (Grafana, Loki, Mimir, Tempo, Alloy) with generated dashboards.
- **Remaining:** Alertmanager rules (verification failures > 0, replay-rate
  spike, lag growth, store-latency SLO breach, no-commits-in-N-minutes); OTLP
  trace export from the engine into the existing Tempo backend; metric
  coverage review (duplicate-object rate until Phase 4 lands, consumer lag,
  state-store write latency); documented SLOs.
- **Test plan:** force a verification failure and confirm the alert; load test
  and watch lag/replay panels.
- **Exit criteria:** operators see throughput, lag, replay, failures; alerts
  fire without manual dashboard-watching.
- **Rollback:** unset `VTOP_METRICS_ADDR` (no data-path impact).

### Phase 7 — Engine Kubernetes deployment ⬜ NOT STARTED
- **Objective:** deploy the engine fleet as an HA service. Note `helm/vtop`
  deploys the co-located *cluster* (vtop-node StatefulSet) — the engine needs
  its own chart (Deployment, not StatefulSet, once Phase 5 removes the
  single-instance restriction).
- **Work:** engine chart with secrets, ConfigMap, liveness/readiness (the
  readiness endpoint exists), resource limits, PDB, NetworkPolicy,
  rolling-upgrade runbook; optional KEDA ScaledObject bounded by partition
  count (§8.4).
- **Dependencies:** Phases 4–6.
- **Test plan:** rolling upgrade under load (no loss); pod-kill (auto-replace +
  rebalance); lag-based scaling within partition bounds.
- **Exit criteria:** rolling upgrade causes no data loss; failed pod
  auto-replaced; autoscale respects partition count.
- **Rollback:** Helm rollback to the previous revision.

### Phase 8 — Retention, backup/restore, DR, security baseline ◐ PARTIAL
- **Done:** ledger retention/pruning (#128; §6.6); least-privilege DB
  identities; hardened external-tool invocation; SBOM + cosign + `SHA256SUMS`
  in the release pipeline; `cargo-deny` in CI.
- **Remaining:** backup/restore runbook exercised end-to-end (Postgres PITR);
  object-store ↔ state-store divergence checker; scheduled DR drills with
  documented RPO/RTO; TLS-everywhere deployment profile; Object Lock/WORM
  retention profile (pairs with Phase 4); Vault/external-secrets integration;
  audit logging.
- **Test plan:** restore from backup into a clean environment; run the
  divergence checker; verify RPO/RTO targets.
- **Exit criteria:** ledger growth bounded by policy; restore tested; RPO/RTO
  documented; security baseline (§17) met.
- **Rollback:** disable pruning; restore from backup.

### Phase 9 — File/syslog HA (optional) ⬜ GATED on open decision §24.1
- **Objective:** distributed file/spool ownership via etcd/Consul leases with
  takeover from the durable state-store cursor (§9 Tier 3).
- **Work:** leases (`VTOP_ETCD_ENDPOINTS`, `VTOP_LEASE_TTL_SECONDS`,
  `VTOP_INSTANCE_ID`); `lease_owner`/`lease_until` takeover logic; fencing
  against split-brain; durable-spool requirement for syslog documented.
- **Test plan:** two replicas; kill the owner; assert ownership transfer with
  no gap and no uncontrolled duplication.
- **Exit criteria:** one owner per file/spool; failure transfers ownership
  safely.
- **Rollback:** pin file/syslog to a single replica (Tier 2 behavior).

### Phase 10 — Production readiness review ⬜ NOT STARTED
- **Objective:** go/no-go for the target tier. Architecture review, threat
  model, chaos + load + DR drill executed together, runbooks validated, §23
  checklist signed off, every open risk owned.
- **Mitigation for late surprises:** run this review incrementally as phases
  land, not only at the end — the cluster plane's v0.3.0 retrospective
  ("exercise the composition, not only the layers", [`ROADMAP.md`](ROADMAP.md))
  applies verbatim to the engine fleet.
- **Rollback:** a no-go keeps the system at its last validated tier.

---

## 21. Roadmap table

| Phase | Objective | Status | Pri | Depends on | Exit criteria | Rollback |
|---|---|---|---|---|---|---|
| 0 | Baseline hardening | ✅ done | P0 | — | reproducible baseline; crash semantics confirmed | stay on current build |
| 1 | `StateStore` trait | ✅ done | P0 | 0 | no behavior change; `sqlite://` works | revert refactor |
| 2 | Shared invariant tests | ✅ done | P0 | 1 | one invariant impl; battery green | keep SQLite path |
| 3 | Postgres backend | ✅ done | P1 | 2 | `postgres://` works; DB enforces invariant | switch to `sqlite://` (drain first) |
| 4 | Deterministic keys | ⬜ next | P1 | 3 | no duplicate objects; Object Lock safe | `VTOP_OBJECT_KEY_MODE=legacy` |
| 5 | Kafka fleet mode | ⬜ | P1 | 4 | safe rebalance; no double-commit; no double-recovery | `assign` mode, 1 replica |
| 6 | Observability & alerting | ◐ partial | P1 | 0 | alerts fire; traces exported; SLOs documented | disable metrics endpoint |
| 7 | Engine k8s deployment | ⬜ | P1 | 4,5,6 | rolling upgrade w/o loss; bounded autoscale | Helm rollback |
| 8 | Retention / backup / DR / security | ◐ partial | P1 | 3 | restore tested; RPO/RTO documented; baseline met | disable pruning; restore backup |
| 9 | File/syslog HA | ⬜ optional | P2 | 3,7 | lease takeover, no gaps/duplication | pin source to one replica |
| 10 | Readiness review | ⬜ | P1 | target tier | documented go/no-go; risks owned | stay at last validated tier |

---

## 22. Risk register

| Risk | Impact | Mitigation | Phase | Status |
|---|---|---|---|---|
| Object Lock prevents overwrite-based retry | Retries fail / stuck batches | Deterministic keys + "existing verified = success" / version_id (§7.2) | 4 | open |
| Kafka rebalance during in-flight batch | Duplicate work / spurious revocation | Revocation handler; tune poll/session timeouts; cooperative rebalancing (§8.3) | 5 | open |
| Two engines over one store double-recover | Duplicate ingestion / double-commit | **Same host only:** the startup instance lock (#66) refuses it. **Cross-host over shared Postgres it cannot detect** — unsupported and warned at startup (§19); open until claim/fencing lands (#93) | 5 | partially mitigated (same-host only) |
| State database unavailable | Engine cannot commit | Fails safe (stops committing); HA store; alerts | 3,6 | mitigated |
| Object uploaded but manifest upload failed | Batch stuck pre-VERIFIED | Retry manifest; verify existing object; bounded `retry_count` → FAILED | 0,3 | mitigated |
| Manifest uploaded but source-commit failed | Possible replay | Recovery retries commit; deterministic keys make replay idempotent | 4 | partially mitigated (duplicates until 4) |
| File cursor lost during backend switch | File reprocessing / duplicates | Drain + cursor migration + validation + rollback (§6.4) | — | procedural |
| Syslog UDP packet loss | Silent data loss | Ingest only from durable spool; document UDP limits (§9) | 9 | documented |
| KEDA scales beyond useful partition count | Wasted replicas, no throughput gain | Bound max replicas to partition count (§8.4) | 7 | open |
| State-store growth without retention | Ledger bloat, slow scans | Retention/pruning (#128); scheduled `vtopctl prune-ledger` on Postgres | 8 | **closed** |
| Distributed-SQL serialization retries (`40001`) | Latency spikes under contention | Bounded retry implemented; capacity-test under real write rate (§12) | 3 | mitigated (load test pending) |

---

## 23. Production-readiness checklist

Boxes are checked only for behavior that exists **and** is exercised by tests
or CI today. Deployment-time items (TLS profiles, IAM scoping) stay unchecked
until a real production environment applies them.

```text
Correctness & invariant
  [x] verify-before-commit enforced in core AND database (state-aware CHECKs)
  [x] crash before VERIFIED → replay; crash after VERIFIED/before COMMIT → safe
  [x] require_strong_verification defaults true
  [ ] deterministic or content-addressed object keys enabled           (Phase 4)
  [ ] Object Lock behavior tested (retry never overwrites)             (Phase 4)

State store
  [x] StateStore trait + shared test battery green on SQLite AND Postgres
  [x] retry-on-40001 implemented                                       (load test pending)
  [x] state-enum CHECK + commit-rule invariant trigger + state/source indexes
  [ ] UNIQUE source-range dedup indexes (markers are JSON columns today) (Phase 5)
  [x] runtime role denied DDL/DELETE/TRUNCATE; migrate/prune identities split
  [x] retention/pruning policy (engine.ledger_retention_days, #128)
  [ ] multi-writer recovery (FOR UPDATE SKIP LOCKED or leases)         (Phase 5)

Kafka / scaling
  [x] Kafka auto-commit disabled; commit only after VERIFIED
  [ ] consumer-group (subscribe) mode with revocation handling         (Phase 5)
  [ ] KEDA min/max replicas bounded by partition count                 (Phase 7)

Object storage
  [ ] distributed MinIO / S3; Object Lock policy; retention configured (deploy-time)

Operability
  [x] Prometheus metrics endpoint + readiness probe (VTOP_METRICS_ADDR)
  [x] dashboards defined (observability/ stack)
  [ ] Alertmanager rules wired                                         (Phase 6)
  [ ] OpenTelemetry traces exported                                    (Phase 6)
  [ ] engine k8s chart: probes, limits, PDB, NetworkPolicy, runbook    (Phase 7)

Security
  [x] secrets referenced via env/file, never serialized in config
  [x] remote Postgres requires sslmode=verify-full
  [x] SBOM + cosign signatures + SHA256SUMS in releases; cargo-deny in CI
  [ ] TLS for Kafka, DB, object store, metrics in the deployed profile (deploy-time)
  [ ] least-privilege IAM; per-env/per-tenant credentials              (deploy-time)

Resilience / DR
  [x] crash/replay tests passing in CI
  [ ] backup/restore exercised; reconciliation/divergence checker      (Phase 8)
  [ ] DR runbook approved; RPO/RTO documented                          (Phase 8)
  [ ] risk register reviewed; each open risk has an owner              (Phase 10)
```

---

## 24. Open decisions requiring human approval

1. **Primary ingress:** Kafka-only (stop at Tier 2) **or** file/syslog at scale
   (Tier 3 + etcd, Phase 9)? Gates whether Phase 9 exists at all.
2. **Store choice:** PostgreSQL + Patroni vs Yugabyte/Cockroach (§12) —
   familiar ops + low latency vs self-healing DB HA. The backend works against
   any of them; this is now purely an operational decision.
3. **Object-key scheme:** deterministic vs content-addressed (Phase 4)?
   Required either way for idempotency + Object Lock safety; content-addressed
   changes the object layout downstream consumers see.
4. **Compliance:** is Object Lock / WORM mandatory (audit/regulatory)? If yes it
   becomes baseline, not optional, and Phase 4 + the WORM profile move earlier.
5. **State-write latency target & RPO/RTO:** drives store choice, retry tuning,
   and backup design.
6. **Scope/timeline:** which target tier (1, 2, or 3) is in scope for the first
   production engine release? Tier 1 is available today; Tier 2 needs
   Phases 4–7.

---

## 25. Relationship to the cluster roadmap

The repository carries two HA efforts with different shapes:

- **The engine pipeline (this document):** HA by *fleet + shared ledger* —
  stateless-ish replicas coordinated by Kafka consumer groups over one
  Postgres-compatible store.
- **The VTOP cluster** ([`ROADMAP.md`](ROADMAP.md)): HA by *replication* —
  Raft metadata, lease-based range leadership, verified sealed-segment
  transfer and replica replacement (v0.1.0–v0.3.0, releases and limitations
  documented there).

They share infrastructure conventions (the observability stack, release
signing, live-chaos scenario style) but not phases; progress on one does not
tick boxes on the other. The one lesson that transfers directly is recorded in
the cluster's v0.3.0 retrospective and adopted in Phase 10 here: **exercise
the composition, not only the layers** — every fleet-mode milestone must be
proven by a scenario that drives real processes, not only by unit coverage.

---

## 26. TL;DR

- **One** durable Postgres-compatible store; **no Redis**; etcd only for distributed
  file/syslog.
- The **`StateStore` abstraction is built and shipped**: SQLite ↔ Postgres ↔
  Yugabyte/Cockroach is now a **config-string** choice (§6, §12), with the
  invariant enforced in core logic **and** database constraints, and
  retry-on-`40001` in place. **Tier 1 (single engine + Postgres) is deployable
  today from a `--features postgres` source build** (release artifacts don't
  yet enable the feature).
- **Two current-behavior caveats the design must still fix for fleet HA:**
  (a) object keys are **non-deterministic** → replay makes **duplicates**; fix with
  **deterministic keys** (Phase 4) for idempotency + Object Lock safety —
  also the one conformance gap against protocol §15.1.
  (b) the engine is **single-instance by design** until Kafka consumer-group
  fleet mode and multi-writer recovery land (Phase 5).
- **VERIFIED is defined precisely (§4); strong content-derived verification is
  the default and production must not opt out.**
- File/syslog cursors live **only in the state store** → **migrate/drain** on
  backend switch (Kafka is broker-side and safe).
- **Correctness and backend portability are fully Docker-Compose-testable on one
  machine;** only true HA behavior needs multi-node Kubernetes.
- Execution state lives in Part III: phases (§20–21), risks (§22), the
  readiness checklist (§23), and the decisions that need a human call (§24).
