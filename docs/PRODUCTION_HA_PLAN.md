# VTOP Engine — Production-Grade HA Plan

> Status: **Proposal / design doc** (no code changes implied by this document).
> Audience: operators and engineers taking VTOP from a single-node prototype to an
> enterprise, highly-available archive engine.
>
> This document distinguishes **current behavior** (what the code does today) from
> **proposed behavior** (what production HA requires). Where a claim depends on a
> change that has not been implemented, it is marked **[PROPOSED]**.

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
  possible duplicate objects** (see §1.5 and §6). True *effectively-exactly-once*
  at the archive layer requires **deterministic / content-addressed object keys
  [PROPOSED]**.

### 1.3 Non-goals
- Not a stream-processing/analytics engine (only framing/format detection).
- Not a datastore for the telemetry itself — object storage is the archive.

### 1.4 Assumptions
This plan assumes:
- **Kafka is the primary production ingress** (file/syslog are secondary).
- Source systems **retain data long enough for replay** (Kafka retention; files
  retained + fingerprinted; syslog durably spooled before ingest).
- Object storage provides **read-after-write consistency** on the verification
  path (true for S3 and MinIO today).
- **Secrets are injected externally** (never in `config.yaml`).
- Production uses **TLS everywhere** (Kafka, DB, object store, metrics).

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
- **Object keys are non-deterministic** (see §1.5) — central to §6.

---

## 3. Definition of VERIFIED (precise)

The whole invariant hinges on `VERIFIED`, so it must be unambiguous.

**A batch is VERIFIED only when all of the following hold:**
1. the **object exists** in object storage;
2. the **manifest exists** in object storage;
3. the **object size matches** the manifest's recorded size;
4. **strong check (when available / required):** the object **checksum**
   (SHA-256 or BLAKE3) recomputed/served matches the manifest checksum;
5. the **state store has persisted** `object_key`, `manifest_key`, checksum,
   checksum algorithm, compression type, source range, and `batch_id` **before**
   the VERIFIED transition;
6. **[PROPOSED]** the S3 `version_id` (and/or checksum header) is recorded when
   Object Lock / versioning is enabled.

**Verification strength (current code):**
- The engine supports **strong** (checksum) and **backend-limited** (size /
  existence only) verification.
- `upload.require_strong_verification: true` **rejects** a backend-limited result
  instead of committing. **Production must set this true.**

**ETag caveat:** S3 multipart ETags are **not** reliable MD5 checksums. The
authoritative integrity value is VTOP's own **SHA-256/BLAKE3 manifest checksum**,
never the ETag.

---

## 4. What production-grade HA actually needs

The honest, minimal set — **one** durable store, not a zoo.

| Need | Component | Required? | Notes |
|---|---|---|---|
| Durable shared ledger | **ONE** Postgres-compatible DB | **Yes** | PostgreSQL, **or** YugabyteDB/CockroachDB for a self-HA store. Pick one. |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | **Yes (have it)** | Kafka is the coordinator; no extra coordination DB. |
| Durable data plane | **S3 / MinIO** | **Yes (have it)** | Distributed MinIO (erasure-coded) or S3; Object Lock for WORM. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | **Yes for HA** | Restarts, rolling upgrades, lag-based autoscale. |
| Observability | **Prometheus + Grafana + OpenTelemetry** | **Yes** | Metrics, dashboards, traces, alerts. |
| Secrets | **Vault / external-secrets** | Recommended | Creds already injected via env. |
| File/syslog HA ownership | **etcd / Consul** (leases) | **Only if** file/syslog must be HA-distributed | The only thing Kafka groups don't solve. |

### 4.1 Deliberately NOT added
- **Redis** — not the durable store (durability is the point); nothing to cache.
- **Two databases at once** — Postgres *and* Yugabyte/Cockroach are alternatives.
- **etcd** — skip unless distributed file/syslog ingestion is required.

> If Kafka is the primary ingress, enterprise HA is essentially **one new database
> + the `StateStore` abstraction + k8s/Prometheus**.

---

## 5. The `StateStore` abstraction (the one piece of real work)

### 5.1 Backend selection = a config string
The engine reads `engine.state_store`; a factory dispatches on the scheme. Same
binary; switching is one config line.

| Deployment | `engine.state_store` |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `postgres://vtop@pg-host:5432/vtop` |
| Production (Yugabyte) | `postgres://vtop@yb-host:5433/vtop` (same driver) |
| Production (Cockroach) | `postgres://vtop@crdb-host:26257/vtop` (same driver) |

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
| Migrations | SQLite DDL | Postgres DDL (separate file) |
| **Conflict retry** | none | **retry on SQLSTATE `40001`** (distributed serialization) |
| Build | default | behind Cargo `--features postgres` |

### 5.4 Backend-switching policy (corrected — NOT "no migration ever")
The state store is a replay ledger, **but the file/syslog byte cursor lives only
in it** (rebuilt from `SOURCE_COMMITTED` rows by `seed_committed_offsets`). Kafka
offsets live in the broker. Therefore:

```text
Backend switching policy:
- Dev/test:                a fresh state store is acceptable.
- Production, Kafka-only:   allowed after engine DRAIN + offset verification
                           (offsets are broker-side, so resume is safe).
- Production, file/syslog:  REQUIRES cursor migration OR a controlled drain,
                           else files reprocess from byte 0 (duplicates) and
                           spool position is lost.
```

**Safe switch procedure:** drain (stop sources, let in-flight batches reach
`SOURCE_COMMITTED`), confirm no `incomplete` rows, export file/syslog cursors,
import into the new store (or accept reprocessing for Kafka-only).

### 5.5 Database schema & constraints (defense in depth) **[PROPOSED]**
Even though `vtop-core` enforces the invariant, the DB must too:

```text
batches:
  batch_id            PK / UNIQUE
  tenant, source_type, source_name, format, compression
  state               CHECK (state IN (<allowed lifecycle states>))
  object_key, manifest_key
  checksum, checksum_algorithm
  source_progress     (offset/byte range)
  version_id          (nullable; Object Lock / versioning)   [PROPOSED]
  retry_count, last_error
  lease_owner, lease_until                                    [Tier 3]
  created_at, updated_at, verified_at, source_committed_at

Constraints:
  - UNIQUE(batch_id)
  - Kafka: UNIQUE(source_name, partition, start_offset, end_offset)
  - File : UNIQUE(source_name, file_path, byte_start, byte_end, file_fingerprint)
  - CHECK: source_committed_at IS NULL OR verified_at IS NOT NULL   ← the invariant
  - CHECK: object_key IS NOT NULL AND manifest_key IS NOT NULL before VERIFIED
Indexes:
  - (state) for the recovery scan; (tenant, source_name, created_at) for ops
```

### 5.6 Ledger retention / pruning **[PROPOSED]**
Rows are tiny but unbounded over time:
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

## 6. Object storage, idempotency & Object Lock

This section supersedes any earlier "rewrites the same object" wording.

### 6.1 Current reality
- Object keys are **non-deterministic** (`Utc::now()` + `uuid`), so a replayed
  batch writes a **new** object. Result: **no data loss, but duplicate objects**
  can accumulate on crash/replay. The **state ledger + manifests** are the dedup
  authority, not key collision.

### 6.2 Object Lock / WORM rule (important)
With S3 Object Lock, protected object versions **cannot be overwritten or
deleted**. So a retry strategy **must not depend on overwriting**:

```text
If Object Lock is enabled, a retry MUST NOT rely on overwriting an existing
object version. A retry must do ONE of:
  1. use a deterministic key and treat an existing verified object+manifest
     as success (no re-upload);                                  [PROPOSED]
  2. write a new version and record the S3 version_id in the
     manifest + state store;                                     [PROPOSED]
  3. use content-addressed keys so duplicates are harmless.      [PROPOSED]
```

### 6.3 Recommendation
Adopt **(1) deterministic keys + "existing verified object = success"** (optionally
with **(2)** version_id recording). This makes replay **idempotent**, makes Object
Lock safe, and upgrades the delivery guarantee to *effectively exactly-once at the
archive layer* — the claim is only valid **after** this change.

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
8. Commit Kafka offsets MANUALLY (enable.auto.commit = false).
9. Mark SOURCE_COMMITTED.
```

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
  Kafka distributes partitions across replicas. Expose this as a config toggle
  (`VTOP_KAFKA_GROUP_MODE = assign | subscribe`). Subscribe-once also avoids the
  original stall because committed offsets exist after the first verified batch.

### 7.3 Rebalance requirements (Phase 4 correctness)
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
consumer lag, but min/max replicas must be aligned with partition count AND
downstream upload/store capacity.
```

---

## 8. Deployment topologies

### Tier 0 — Single node, Docker Compose (dev / demo / small)
- One engine, **SQLite**, single MinIO, single Kafka (KRaft). This is the current
  lab. **Fully testable on one machine.** No HA.

### Tier 1 — Single engine + external Postgres (small prod, durable store)
- One engine, **Postgres** ledger (backable, survives engine host restart), S3/MinIO.
- One engine (no horizontal scale) but a proper durable store.
- **Testable on one machine** (add a `postgres` Compose service).

### Tier 2 — HA fleet, Kafka-primary (recommended enterprise baseline)
- **N engine replicas** on Kubernetes; **Kafka consumer-group mode** (§7.2).
- **One** Postgres-compatible store (Postgres+Patroni, or Yugabyte/Cockroach).
- Distributed MinIO or S3 + Object Lock.
- **KEDA** autoscale on Kafka lag (§7.4).
- File/syslog **not HA** here (disabled or pinned to one replica).
- **Needs real multi-node infra**; rehearsable on single-node k8s, true HA ≥3 nodes.

### Tier 3 — Full HA incl. file/syslog (only if required)
- Tier 2 **+ etcd/Consul leases** so each file/spool is owned by one replica, with
  takeover resuming from the **state-store cursor** (which must be migrated/durable).
- **Replayability differs by source — design accordingly:**
```text
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
| Multi-engine Kafka consumer-group distribution | ✅ (scale replicas) | recommended on k8s |
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

**Notes**
- Engine is **CPU/mem-light** (~8 MiB idle); scale by **replica count vs Kafka
  partitions**, not big boxes.
- Memory spikes only in **whole-file mode** on large files (loads file into
  memory). Size to your largest whole-file object, or keep large inputs
  line-oriented.
- State-store sizing is dominated by **write IOPS + fsync latency**, not capacity.
  Use fast SSD/NVMe.
- Distributed SQL needs **3 nodes minimum** + NVMe; trades per-write latency for
  self-healing HA.

---

## 11. Phased implementation plan

Each phase ships independently. Phases 1–2 are zero-behavior-change groundwork.

### Phase 1 — Extract the `StateStore` trait
- Trait + SQLite impl; engine holds `Box<dyn StateStore>`; scheme factory (SQLite).
- **Exit:** all existing tests pass; behavior identical.

### Phase 2 — Centralize invariant + shared test battery
- Move verify-before-commit guard into `vtop-core`; backend-agnostic test battery.
- **Exit:** one invariant implementation; battery green on SQLite.

### Phase 3 — Postgres backend + DB constraints (`--features postgres`)
- `PgStateStore` (PgPool, `$N`, Postgres DDL); **schema constraints from §5.5**;
  **retry-on-`40001`**; run the battery against Postgres.
- **Exit:** identical behavior SQLite/Postgres; `postgres://` selectable; DB
  enforces the invariant too.

### Phase 4 — Deterministic keys + idempotent retry (enables exactly-once-archive)
- Deterministic / content-addressed object keys; "existing verified object =
  success"; record `version_id`.
- **Exit:** replaying a batch produces **no duplicate object**; Object Lock safe.

### Phase 5 — Kafka consumer-group (multi-instance) mode
- Long-lived `subscribe` + manual commit-after-verify; revocation handling (§7.3);
  config toggle `assign|subscribe`.
- **Exit:** two replicas split partitions; killing one rebalances; no double-commit.

### Phase 6 — Operability (k8s + observability)
- Helm; liveness/readiness; Prometheus endpoint; OTel traces; Grafana; Alertmanager;
  KEDA on lag (with §7.4 bounds).
- **Exit:** rolling upgrade w/o loss; autoscale on lag; alerts fire.

### Phase 7 — File/syslog HA (optional)
- etcd/Consul leases; takeover from state-store cursor; durable-spool requirement
  for syslog; chaos tests.
- **Exit:** owner death transfers ownership with no gap/duplication.

### Phase 8 — Hardening
- Object Lock/WORM profile; Vault; multipart + concurrent uploads; multi-writer
  recovery (`SELECT … FOR UPDATE` / lease column); retention/pruning (§5.6);
  backup/DR (§15); security baseline (§16).

---

## 12. Configuration & environment reference

### 12.1 Current config (`config.yaml`) — implemented
| Key | Meaning |
|---|---|
| `engine.name` / `engine.tenant` | identity; default tenant |
| `engine.state_store` | **backend selector** (`sqlite://…`; `postgres://…` after Phase 3) |
| `engine.work_dir` / `log_level` | scratch dir; verbosity |
| `batching.max_records` / `max_bytes` / `max_batch_age_seconds` | seal thresholds |
| `compression.type` / `level` | `gzip` \| `zstd` \| `none` |
| `checksum.algorithm` | `sha256` \| `blake3` \| disabled |
| `sources.kafka.*` | brokers, group, include/exclude, `enable_auto_commit:false` |
| `sources.file.*` | paths, `delete_after_commit`, `whole_file` |
| `sources.syslog_spool.*` | spool paths |
| `upload.backend` | `s3_native` \| `s3cmd` \| `awscli` \| `minio` \| `localfs` \| `mock` |
| `upload.bucket` | bucket (supports `telemetry-{format}`) |
| `upload.endpoint_url` / `region` / `force_path_style` / `verify_tls` | S3 endpoint |
| `upload.create_bucket` | auto-create per-format buckets |
| `upload.require_strong_verification` | **set true in prod** — refuse size-only commit |
| `partitioning.template` | object key layout |

### 12.2 Current environment variables — implemented
| Variable | Purpose |
|---|---|
| `VTOP_CONFIG` | path to `config.yaml` |
| `RUST_LOG` | log filter |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials |
| `AWS_REGION` | S3 region |
| `VTOP_S3_ENDPOINT_URL` | S3/MinIO endpoint |
| `VTOP_S3_FORCE_PATH_STYLE` | path-style addressing (MinIO) |
| `VTOP_S3_VERIFY_TLS` | TLS verification toggle (off = lab only) |
| *(Kafka SASL)* | password read from the **env var named** in `sasl_password_env` |

### 12.3 Proposed environment variables (HA phases — not yet implemented) **[PROPOSED]**
| Variable | Phase | Purpose |
|---|---|---|
| `VTOP_STATE_STORE` | 1 | override `engine.state_store` from a secret/env |
| `VTOP_PG_MAX_CONNECTIONS` | 3 | Postgres pool size per replica |
| `VTOP_PG_STATEMENT_TIMEOUT_MS` | 3 | guard stuck statements |
| `VTOP_STATE_RETRY_MAX` | 3 | max retries on SQLSTATE `40001` |
| `PGPASSWORD` / conn-string secret | 3 | DB password via secret manager |
| `VTOP_OBJECT_KEY_MODE` | 4 | `legacy` \| `deterministic` \| `content-addressed` |
| `VTOP_INSTANCE_ID` | 5/7 | stable replica identity (group member / lease owner) |
| `VTOP_KAFKA_GROUP_MODE` | 5 | `assign` (single-node) \| `subscribe` (fleet) |
| `VTOP_METRICS_ADDR` | 6 | Prometheus endpoint (e.g. `0.0.0.0:9090`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 6 | OpenTelemetry collector |
| `VTOP_ETCD_ENDPOINTS` | 7 | etcd/Consul endpoints for leases |
| `VTOP_LEASE_TTL_SECONDS` | 7 | source-ownership lease TTL |

> Secrets (DB password, S3 keys, Kafka SASL) **must** come from a secret manager,
> never `config.yaml`.

---

## 13. Observability
- **Metrics (Prometheus):** batches/sec, records/sec, bytes in/out, compression
  ratio, per-stage latency, **verification-failure rate**, **replay /
  REPLAY_REQUIRED rate**, Kafka **consumer lag**, state-store write latency,
  in-flight batches, **duplicate-object rate** (until Phase 4).
- **Traces (OpenTelemetry):** one span per batch, child spans per stage (already
  emitted as structured events).
- **Dashboards (Grafana) + Alerts (Alertmanager):** verification failures > 0,
  replay-rate spike, lag growth, store write-latency SLO breach, no committed
  batches in N minutes.

---

## 14. Failure modes & recovery semantics
| Failure | Behavior | Why it's safe |
|---|---|---|
| Engine crash mid-batch | `recover()`: pre-VERIFIED → replay from last committed source position; VERIFIED-but-not-committed → retry commit | Progress never advanced for unverified data |
| Replay after crash | **Today:** new object key → duplicate object (no loss). **Phase 4:** same key, existing verified = success | At-least-once now; idempotent after Phase 4 |
| State store unavailable | Cannot transition → stops committing (fails safe) | No commit-before-verify possible |
| Kafka rebalance (fleet) | Revoked partitions: finish/abandon in-flight, no commit; reassigned replica resumes from committed offset | Offsets commit only post-verify |
| File-owner replica dies (Tier 3) | Lease expires; another replica resumes from stored byte cursor | Cursor in shared durable store |
| Object Lock blocks overwrite | Retry uses deterministic key / new version, never overwrite | §6.2 rule |

---

## 15. Backup, restore & disaster recovery **[PROPOSED]**
```text
- State store: Postgres PITR (WAL archiving) or distributed-SQL backup policy.
- Object store: bucket replication or MinIO/S3 backup policy; Object Lock retention.
- Restore test cadence (scheduled DR drills).
- Defined RPO / RTO targets.
- Runbook: restart VTOP after a state-store restore (recovery scan re-reconciles).
- Divergence detection: reconcile state-store rows vs object-store contents after a
  restore (orphan objects / missing objects / stale in-flight rows).
```

---

## 16. Security hardening **[PROPOSED baseline]**
```text
- TLS for Kafka, DB, object store, and the metrics endpoint.
- IAM/bucket policy scoped to required prefixes only (least privilege).
- Server-side encryption / KMS where available; encryption in transit everywhere.
- Separate credentials per environment and per tenant.
- No secrets in config.yaml (env / Vault / k8s Secrets only).
- Audit logs for object-store writes and state-store changes.
- Kubernetes NetworkPolicies isolating engine ↔ DB ↔ Kafka ↔ object store.
```

---

## 17. Risks & mitigations
| Risk | Mitigation |
|---|---|
| Object uploaded but manifest upload failed | Retry manifest upload; verify existing object; batch stays pre-VERIFIED |
| Manifest uploaded but source-commit failed | Replay safely; **(Phase 4)** existing verified batch = success |
| Replay creates duplicate objects (current) | **Phase 4** deterministic/content-addressed keys |
| State DB unavailable | Stop committing; fail safe |
| Kafka rebalance during a batch | Revocation handler + manual-commit discipline (§7.3) |
| Object Lock prevents overwrite | Version-aware or existing-object-success behavior (§6.2) |
| File cursor lost on DB switch | Migrate cursor or drain before switch (§5.4) |
| Syslog loss (UDP) | Ingest from durable spool only (§8 Tier 3) |
| Ledger grows unbounded | Retention/pruning (§5.6) |
| More replicas than partitions | Bound autoscaling by partition count (§7.4) |

---

## 18. Open decisions (need a human call)
1. **Primary ingress:** Kafka-only (stop at Tier 2) **or** file/syslog at scale
   (Tier 3 + etcd)?
2. **Store choice:** PostgreSQL + Patroni vs Yugabyte/Cockroach (self-HA, higher
   per-write latency)?
3. **Object-key scheme:** adopt deterministic / content-addressed keys (Phase 4)?
   Required for true idempotency + Object Lock.
4. **Compliance:** is Object Lock / WORM mandatory (audit/regulatory)? If yes it
   becomes baseline, not optional.
5. **State-write latency target:** drives store choice and whether write-batching
   is needed.

---

## 19. TL;DR
- **One** durable Postgres-compatible store; **no Redis**; etcd only for
  distributed file/syslog.
- The **`StateStore` trait (Phase 1)** is the single real prerequisite; after it,
  SQLite ↔ Postgres ↔ Yugabyte/Cockroach is a **config-string** choice.
- **Two current-behavior caveats the design must fix for production:**
  (a) object keys are **non-deterministic** → replay makes **duplicates**; fix with
  **deterministic keys (Phase 4)** for idempotency + Object Lock safety.
  (b) file/syslog cursors live **only in the state store** → **migrate/drain** on
  backend switch (Kafka is broker-side and safe).
- **VERIFIED is defined precisely (§3); set `require_strong_verification: true` in
  prod.**
- **Kafka consumer groups + k8s + Prometheus** deliver HA for the Kafka path with
  minimal new infrastructure.
- **Correctness and backend portability are fully Docker-Compose-testable on one
  machine;** only true HA behavior needs multi-node Kubernetes.
