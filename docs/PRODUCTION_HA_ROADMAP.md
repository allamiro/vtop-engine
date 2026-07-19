# VTOP Engine — Production-Grade HA Plan & Implementation Roadmap

> Status: **Engineering design + roadmap (proposal).** No code change is implied by
> this document.
> Companion to [`PRODUCTION_HA_PLAN.md`](PRODUCTION_HA_PLAN.md) — this version adds a
> full **phased implementation roadmap**, a **database decision matrix**, a **risk
> register**, and an **executive summary / readiness checklist** at the end.
>
> Language convention: claims that depend on unbuilt work are marked **[PROPOSED]**.
> Wording is intentionally qualified ("safe under these assumptions", "requires
> validation") rather than absolute.

---

## Table of contents
1. Scope & goals
2. Assumptions
3. Core invariant & definition of VERIFIED
4. System model (current behavior)
5. What production-grade HA needs
6. Object storage, idempotency & Object Lock / WORM
7. The `StateStore` abstraction
8. State-store schema & constraints
9. Kafka HA: commit choreography, rebalance, autoscaling
10. Deployment topologies
11. Docker Compose vs. real hardware
12. Hardware sizing
13. Database decision matrix
14. Observability
15. Failure modes & recovery
16. Backup, restore & disaster recovery
17. Security hardening
18. **Production Roadmap (Phases 0–10)**
19. Risk register
20. Configuration & environment reference
21. **Executive summary**
22. **Phased roadmap table**
23. **Production-readiness checklist**
24. **Open decisions requiring human approval**

---

## 1. Scope & goals

Take VTOP from a **single-process prototype** (SQLite ledger, one engine) to a
**horizontally scalable, highly-available** telemetry-object transfer engine,
without weakening the core guarantee:

> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

Goals of this document:
1. Identify gaps and weak claims in the current HA design and correct them.
2. Provide a **dependency-ordered implementation roadmap** with per-phase test
   plans, exit criteria, and rollback.
3. Be readable by **engineers, operators, and decision-makers**.
4. Keep the invariant central in every phase.

**Non-goals:** VTOP is not a stream-processing/analytics engine and not a datastore
for the telemetry itself — object storage is the archive.

---

## 2. Assumptions

This plan is "safe under these assumptions." Where an assumption fails, the
relevant section calls out the consequence.

- **Kafka is the primary production ingress.** File/syslog are secondary and their
  HA is optional (Phase 9).
- **Source systems retain data long enough for replay** (Kafka retention; files
  retained and fingerprinted; syslog durably spooled before ingest).
- **Object keys and manifests should be deterministic** for safe retries. This is
  **not** true in the current code (keys include a timestamp + random UUID) and is
  a **[PROPOSED]** change (Phase 7 / §6).
- **Object storage supports read-after-write consistency** on the verification path
  (true for S3 and MinIO).
- **Secrets are injected through a secret manager**, never stored in config files.
- **Production uses TLS** for Kafka, database, object storage, and the metrics
  endpoint.
- **File/syslog HA requires extra coordination** (leases) and is only built if
  required.

---

## 3. Core invariant & definition of VERIFIED

### 3.1 The invariant
> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

Source progress (Kafka offset / file byte cursor / spool position) is advanced
**only after** the object and manifest are uploaded and verified. This is enforced
today in `vtop-core` logic; production should **also** enforce it in the database
(see §8) as defense in depth.

### 3.2 Precise definition of VERIFIED
A batch is **VERIFIED** only when **all** of the following hold:
1. the **object exists** in object storage;
2. the **manifest exists** in object storage;
3. the **object size matches** the size recorded in the manifest;
4. the **checksum recorded in the manifest matches** a digest derived from the
   stored object or computed over it by the storage service (SHA-256/BLAKE3);
5. **compression type, checksum algorithm, source range, `batch_id`, object key,
   and manifest key are recorded** in the state store before the VERIFIED
   transition;
6. **[PROPOSED]** optional object-store metadata (S3 `version_id`, checksum
   headers) is recorded when versioning / Object Lock is enabled;
7. **verification failure prevents source commit** (the batch never advances to
   SOURCE_COMMITTED).

**Verification strength (current code):** the engine supports **strong**
(stored-content/service-computed checksum) and **backend-limited** (size /
existence only) verification. Strong verification defaults on;
`upload.require_strong_verification: false` explicitly opts into weak
compatibility/lab behavior.

**ETag caveat:** S3 **multipart ETags are not reliable MD5 checksums** and must not
be treated as the authoritative integrity value. The manifest SHA-256/BLAKE3 is the
source of truth.

---

## 4. System model (current behavior)

VTOP has a two-plane design; understanding it keeps the HA design small.

```
                         ┌──────────────────────────────────────────┐
  sources               │                VTOP engine                 │      archive
 ┌─────────┐  read      │  discover → batch → seal → compress →       │  put ┌──────────┐
 │ Kafka   │──────────► │  checksum → upload object → upload manifest │─────►│  S3 /    │
 │ files   │            │  → VERIFY → COMMIT source progress          │      │  MinIO   │
 │ syslog  │            └───────────────┬────────────────────────────┘      │ (WORM)   │
 └─────────┘                            │ ~9 small state writes / batch      └──────────┘
                                        ▼
                              ┌────────────────────┐
                              │   STATE STORE       │  SQLite today; Postgres-compat [PROPOSED]
                              │ replay ledger,      │
                              │ enforces invariant  │
                              └────────────────────┘

  Plane split:
   • Data plane  = telemetry bytes → object storage (durable, HA-capable).
   • Control plane = batch lifecycle ledger → state store (the scale bottleneck).
```

**Current-behavior facts (verified in code), which shape every claim below:**

| Property | Current behavior | Implication |
|---|---|---|
| Object key | `vtop-<UTCstamp>-<source>-<range>-<uuid8>` — **non-deterministic** | Replay writes a **duplicate object**, not an overwrite (no loss). See §6. |
| File/syslog cursor | Stored **only in the state store** (rebuilt from `SOURCE_COMMITTED` rows) | Backend switch needs cursor migration/drain. See §7.4. |
| Kafka offset | Committed to the **broker** after VERIFIED | Resume is broker-side; safe across a state-store reset. |
| Concurrency | Single process, **sequential** loop | Multi-instance is **[PROPOSED]** (Phase 4). |
| State backend | **SQLite only** | Pluggable backend is **[PROPOSED]** (Phases 1–3). |
| Run cost | ~9 tiny state writes/batch; bottleneck is **S3 upload** | The DB is not the throughput limiter. |

---

## 5. What production-grade HA needs

The minimal, honest set — **one** durable store, not a collection of databases.

| Need | Component | Required? | Notes |
|---|---|---|---|
| Durable shared ledger | **ONE** Postgres-compatible DB | Yes | PostgreSQL, or YugabyteDB / CockroachDB for a self-HA store. Choose one (§13). |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | Yes (have it) | Kafka is the coordinator; no extra coordination DB for the Kafka path. |
| Durable data plane | **S3 / MinIO** | Yes (have it) | Distributed MinIO (erasure-coded) or S3; Object Lock for WORM. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | Yes for HA | Restarts, rolling upgrades, lag-based autoscale. |
| Observability | **Prometheus + Grafana + OpenTelemetry** | Yes | Metrics, traces, dashboards, alerts. |
| Secrets | **Vault / external-secrets / k8s Secrets** | Recommended | Credentials already injected via env. |
| File/syslog HA ownership | **etcd / Consul** (leases) | Only if file/syslog must be HA-distributed | The one thing Kafka groups do not solve. |

**Deliberately NOT added:** Redis (cannot be the durable store; nothing to cache);
two databases at once (Postgres *and* Yugabyte/Cockroach are alternatives, not a
stack); etcd (skip unless distributed file/syslog ingestion is required).

> Under the Kafka-primary assumption, enterprise HA is essentially **one new
> database + the `StateStore` abstraction + Kubernetes/Prometheus**.

---

## 6. Object storage, idempotency & Object Lock / WORM

This section supersedes any "retries rewrite the same object" wording.

### 6.1 Current reality
Object keys are **non-deterministic** (`Utc::now()` + `Uuid::new_v4()`), so a
replayed batch writes a **new** object. Result: **no data loss, but duplicate
objects can accumulate** on crash/replay. The **state ledger + manifests** are the
deduplication authority today — not key collision.

### 6.2 Object Lock / WORM safe-retry rules
With S3 Object Lock, protected object versions **cannot be overwritten or deleted**.
Retry behavior must therefore be explicit:

```text
Object Lock DISABLED:
  • deterministic keys MAY be overwritten safely on retry. [PROPOSED key scheme]

Object Lock ENABLED:
  • a retry MUST NOT depend on overwriting a protected object. It MUST do one of:
     1. detect an existing VERIFIED object+manifest and treat it as success
        (no re-upload);
     2. write a NEW object version and record the version_id in the manifest
        and the state store;
     3. use CONTENT-ADDRESSED keys so duplicates are inherently harmless.
```

### 6.3 Recommendation
Adopt **deterministic keys + "existing verified object = success"** (option 1),
optionally recording `version_id` (option 2). Only after this change is replay
**idempotent at the archive layer** and the delivery guarantee may be described as
"idempotent at the archive layer" rather than "exactly-once."

### 6.4 ETag caveat
Never treat an S3 ETag as the integrity checksum (multipart ETags are not MD5). The
manifest SHA-256/BLAKE3 remains the source of truth.

---

## 7. The `StateStore` abstraction

### 7.1 Backend selection = a config string
The engine reads `engine.state_store`; a factory dispatches on the URL scheme. Same
binary; switching backends is a one-line config change.

| Deployment | `engine.state_store` |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `postgres://vtop@pg-host:5432/vtop` |
| Production (Yugabyte) | `postgres://vtop@yb-host:5433/vtop` |
| Production (Cockroach) | `postgres://vtop@crdb-host:26257/vtop` |

### 7.2 The trait (single source of truth for the invariant)
Abstracts: `save_batch_state`, `update_batch_state`, `mark_verified`,
`mark_source_committed`, `mark_failed`, `get_batch`, `list_incomplete_batches`,
`list_failed_batches`, `list_batches`. The engine holds `Box<dyn StateStore>`. The
verify-before-commit guard stays as **pure logic in `vtop-core`**, called by every
backend; the **database also enforces it** via constraints (§8).

### 7.3 Backend differences
| Concern | SQLite | Postgres / Yugabyte / Cockroach |
|---|---|---|
| Driver | `sqlx` `SqlitePool` | `sqlx` `PgPool` (pure-Rust, no libpq) |
| Placeholders | `?` | `$1, $2, …` |
| Insert | plain `INSERT` | plain `INSERT` |
| Migrations | SQLite DDL | Postgres DDL (separate file) |
| Conflict retry | none | **retry on SQLSTATE `40001`** (distributed serialization) |
| Build | default | behind Cargo `--features postgres` |

### 7.4 Backend-switching policy (qualified — not "no migration ever")
The state store is a replay ledger, **but the file/syslog byte cursor lives only in
it** (rebuilt from `SOURCE_COMMITTED` rows). Kafka offsets live in the broker.

```text
Backend switching policy:
  • Dev/test:               a fresh state store is acceptable.
  • Kafka-only production:   may be safe AFTER draining the engine and verifying
                            committed offsets (offsets are broker-side).
  • File/syslog production:  cursor state may need MIGRATION; otherwise files
                            reprocess from byte 0 and spool position is lost.
  • Any production switch:   MUST include drain → checkpoint → validate → rollback.
```

**Safe switch procedure:** drain (stop sources; let in-flight batches reach
SOURCE_COMMITTED) → confirm zero incomplete rows → export file/syslog cursors →
import into the new store (or accept Kafka reprocessing for Kafka-only) → validate →
keep the old store until validation passes (rollback path).

---

## 8. State-store schema & constraints **[PROPOSED]**

Even though `vtop-core` enforces the invariant, the database must enforce it too
(defense in depth). Suggested shape:

```text
TABLE batches
  batch_id            TEXT  PRIMARY KEY / UNIQUE
  tenant              TEXT
  source_type         TEXT                       -- kafka | file | syslog_spool
  source_name         TEXT
  -- Kafka identity
  topic               TEXT NULL
  partition           INT  NULL
  start_offset        BIGINT NULL
  end_offset          BIGINT NULL
  -- File identity
  file_path           TEXT NULL
  byte_start          BIGINT NULL
  byte_end            BIGINT NULL
  file_fingerprint    TEXT NULL                  -- inode/size/mtime hash
  -- Object identity
  object_key          TEXT NULL
  manifest_key        TEXT NULL
  checksum            TEXT NULL
  checksum_algorithm  TEXT NULL                  -- sha256 | blake3 | none
  compression         TEXT NULL                  -- gzip | zstd | none
  version_id          TEXT NULL                  -- [PROPOSED] Object Lock / versioning
  -- Lifecycle
  state               TEXT NOT NULL              -- constrained set (below)
  retry_count         INT  NOT NULL DEFAULT 0
  last_error          TEXT NULL
  created_at          TIMESTAMPTZ NOT NULL
  updated_at          TIMESTAMPTZ NOT NULL
  verified_at         TIMESTAMPTZ NULL
  source_committed_at TIMESTAMPTZ NULL
  -- File/syslog HA (Phase 9)
  lease_owner         TEXT NULL
  lease_until         TIMESTAMPTZ NULL

Constraints:
  • CHECK (state IN (DISCOVERED, BATCHING, SEALED, COMPRESSED, CHECKSUMMED,
           OBJECT_UPLOADED, MANIFEST_UPLOADED, VERIFIED, SOURCE_COMMITTED,
           FAILED, REPLAY_REQUIRED))
  • CHECK (source_committed_at IS NULL OR verified_at IS NOT NULL)   ← the invariant
  • CHECK (state <> 'VERIFIED' OR (object_key IS NOT NULL AND manifest_key IS NOT NULL))
  • UNIQUE (source_name, topic, partition, start_offset, end_offset)        -- Kafka
  • UNIQUE (source_name, file_path, byte_start, byte_end, file_fingerprint) -- File

Indexes:
  • (state)                              -- recovery scan / list_incomplete
  • (source_type, source_name, updated_at)
  • partial index WHERE state NOT IN (SOURCE_COMMITTED, FAILED)  -- hot incomplete set

Multi-writer recovery:
  • claim incomplete rows with SELECT ... FOR UPDATE SKIP LOCKED, or via
    lease_owner/lease_until, so two instances never recover the same batch.
```

### 8.1 Ledger retention / pruning **[PROPOSED]**
```text
• keep ACTIVE, FAILED, REPLAY_REQUIRED, and recent SOURCE_COMMITTED rows hot;
• archive old SOURCE_COMMITTED rows to cold tables / object storage;
• retain enough for audit, replay, and compliance;
• prune by tenant / source / date / status.
```

---

## 9. Kafka HA: commit choreography, rebalance, autoscaling

### 9.1 Commit choreography (exact order)
```text
 1. Poll records.
 2. Build batch (topic, partition, start_offset, end_offset).
 3. Persist DISCOVERED / BATCHING in the state store.
 4. Upload object.
 5. Upload manifest.
 6. Verify object + manifest (see §3.2).
 7. Mark VERIFIED in the state store.
 8. Commit Kafka offset MANUALLY (enable.auto.commit = false).
 9. Mark SOURCE_COMMITTED.
```

```
  poll ─► build ─► persist ─► upload obj ─► upload manifest ─► VERIFY
                                                                 │
                                                                 ▼
                                              mark VERIFIED ─► commit offset ─► SOURCE_COMMITTED
                                                  (state)        (broker)          (state)
        ▲ crash here = replay (safe ONLY if keys deterministic, §6) ┘
```

### 9.2 Crash edge case (must be documented)
```text
If VTOP marks VERIFIED but crashes BEFORE committing the Kafka offset, the same
records may be replayed after rebalance. This is acceptable ONLY when deterministic
object/manifest behavior (§6.3) makes the replay idempotent. Until that change,
replay produces a duplicate object (no data loss).
```

### 9.3 Single-node vs fleet consumer mode (design decision)
- Current code uses per-read **`assign()`** (this fixed a single-node stall where
  re-`subscribe()` per read forced a rebalance and reseek-to-earliest).
- **For a fleet, use long-lived `subscribe()` + manual commit-after-verify** so
  Kafka distributes partitions across replicas. Expose as a toggle
  (`VTOP_KAFKA_GROUP_MODE = assign | subscribe`).

### 9.4 Rebalance requirements (Phase 4 correctness)
```text
• disable auto-commit; commit manually only after VERIFIED;
• handle partition REVOCATION safely:
    - stop accepting new batches for revoked partitions,
    - complete, abandon (no commit → replay), or replay in-flight batches safely;
• tune max.poll.interval.ms, session.timeout.ms, heartbeat.interval.ms so long
  uploads do not trigger spurious rebalances;
• prefer cooperative (incremental) rebalancing if the client supports it.
```

### 9.5 Autoscaling caveat (KEDA)
```text
Useful engine replicas for a topic are bounded by its ACTIVE PARTITION COUNT.
More replicas than partitions does NOT add throughput. KEDA should scale on
consumer lag, but min/max replicas must align with partition count AND downstream
upload/store capacity.
```

---

## 10. Deployment topologies

### Tier 0 — Single node, Docker Compose (dev / demo)
One engine, **SQLite**, single MinIO, single Kafka (KRaft). The current lab.
Fully testable on one machine. No HA.

### Tier 1 — Single engine + external Postgres (small prod, durable store)
One engine, **Postgres** ledger (backable, survives engine host restart), S3/MinIO.
One engine (no horizontal scale) but a proper durable store. Testable on one
machine by adding a `postgres` Compose service.

### Tier 2 — HA fleet, Kafka-primary (recommended enterprise baseline)
```
        ┌── engine-1 ─┐
 Kafka ─┼── engine-2 ─┼──► S3 / MinIO (WORM)
 (group)└── engine-3 ─┘          │
            │  │  │              │
            └──┴──┴──► Postgres / Yugabyte / Cockroach  (one logical store, HA)

   metrics ─► Prometheus ─► Grafana ;  traces ─► OTel collector
   autoscale ◄─ KEDA (Kafka lag, bounded by partitions)
```
N replicas on Kubernetes; Kafka consumer-group mode; one Postgres-compatible store;
distributed MinIO/S3 + Object Lock; KEDA autoscale on lag. File/syslog not HA here
(disabled or pinned to one replica). Needs real multi-node infra; rehearsable on a
single-node k8s, true HA needs ≥3 nodes.

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

### Topology selection guide
| If your reality is… | Use |
|---|---|
| Laptop / demo | Tier 0 (Compose, SQLite) |
| Small prod, one engine, want backups | Tier 1 (Postgres) |
| Enterprise, Kafka is primary | **Tier 2** (k8s fleet) |
| Enterprise + distributed file/syslog | Tier 3 (+ etcd) |

---

## 11. Docker Compose vs. real hardware

| Capability | Compose (1 machine) | Needs multi-node |
|---|---|---|
| Full pipeline correctness (all sources → verified → committed) | ✅ | |
| SQLite ↔ Postgres backend switch (config string) | ✅ | |
| Postgres backend + retry-on-`40001` | ✅ (single Postgres) | |
| Yugabyte/Cockroach **wire** compatibility | ✅ (single container) | |
| Distributed-store **HA** (survive DB node loss) | ⚠️ partial | ✅ (≥3 DB nodes) |
| Multi-engine Kafka consumer-group distribution | ✅ (scale replicas) | recommended on k8s |
| Engine failover / rebalance under node loss | ⚠️ (kill a container) | ✅ (k8s ≥3 nodes) |
| KEDA lag autoscaling | | ✅ (k8s) |
| etcd file-lease ownership + takeover | ⚠️ (rehearsal) | ✅ (k8s ≥3 nodes) |
| Object Lock / WORM | ✅ (MinIO) | ✅ (S3 in prod) |

**Correctness and backend portability are fully Compose-testable on one machine;
only true HA behavior requires multi-node Kubernetes.**

---

## 12. Hardware sizing (starting points — validate with `benchmarks/`)

| Tier | Engine | State store | Object store | Kafka |
|---|---|---|---|---|
| 0 | 2 vCPU / 2–4 GB | SQLite (in engine) | MinIO 1 node | 1 broker (KRaft) |
| 1 | 2–4 vCPU / 4 GB | Postgres 2 vCPU / 4 GB / SSD | MinIO 1–4 nodes | 1–3 brokers |
| 2 | 3+ replicas × (2–4 vCPU / 2–4 GB) | Postgres HA (3) **or** Yugabyte/CRDB (3 × 4 vCPU / 8–16 GB / NVMe) | MinIO ≥4 erasure-coded, or S3 | ≥3 brokers, RF=3 |
| 3 | as Tier 2 | as Tier 2 | as Tier 2 | + etcd (3 small nodes) |

**Notes.** Engine is CPU/mem-light (~8 MiB idle); scale by **replica count vs Kafka
partitions**, not big boxes. Memory spikes only in **whole-file mode** on large
files. State-store sizing is dominated by **write IOPS + fsync latency**; use fast
SSD/NVMe. Distributed SQL needs **3 nodes minimum** + NVMe.

---

## 13. Database decision matrix

No option is universally best. Compare against your priorities.

| Criterion | PostgreSQL + Patroni | YugabyteDB | CockroachDB |
|---|---|---|---|
| Operational complexity | Moderate (Patroni/etcd, failover tuning) | Lower for HA (self-healing) | Lower for HA (self-healing) |
| Write latency | **Lowest** (single primary) | Higher (distributed consensus) | Higher (distributed consensus) |
| HA behavior | Failover via Patroni (seconds, some ops care) | **Built-in**, survives node loss | **Built-in**, survives node loss |
| SQL compatibility | **Reference Postgres** | Postgres-wire (YSQL), very high | Postgres-wire, high (some differences) |
| Kubernetes friendliness | Good (operators exist) | **Strong** (cloud-native) | **Strong** (cloud-native) |
| Backup/restore maturity | **Very mature** (PITR/WAL) | Mature, distinct tooling | Mature, distinct tooling |
| Team familiarity | **Usually highest** | Lower | Lower |
| Retry handling | Rare serialization retries | Plan for `40001` retries | Plan for `40001` retries |

**Guidance (not a verdict):**
- Choose **PostgreSQL + Patroni** if the team values **familiar operations, lowest
  write latency, and mature backup/PITR**, and can accept managed failover.
- Choose **YugabyteDB / CockroachDB** only if **self-healing database HA** matters
  more than write latency and operational simplicity, and the team will plan for
  distributed-transaction retries (`40001`).
- Because all three speak the Postgres wire protocol, the **`StateStore` Postgres
  backend works against any of them** — the choice is a connection-string + ops
  decision, made once, and **requires validation** under your write rate.

---

## 14. Observability
- **Metrics (Prometheus):** batches/sec; records/sec; bytes in/out; compression
  ratio; per-stage latency; upload latency; **verification failures**; **replay /
  REPLAY_REQUIRED rate**; Kafka **consumer lag**; state-store latency; in-flight
  batches; failed batches; **duplicate-object rate** (until deterministic keys).
- **Traces (OpenTelemetry):** one span per batch, child spans per pipeline stage
  (already emitted as structured events).
- **Dashboards (Grafana)** for throughput, lag, replay, failures, store latency.
- **Alerts (Alertmanager):** verification failures > 0; replay-rate spike; lag
  growth; store write-latency SLO breach; no committed batches in N minutes.

---

## 15. Failure modes & recovery

```
                 ┌──────────────────────── recovery scan at startup ───────────────────────┐
 incomplete row  │  state?                                                                   │
 ──────────────► │   DISCOVERED/BATCHING/SEALED/.../MANIFEST_UPLOADED → REPLAY_FROM_SOURCE   │
                 │   VERIFIED (not committed)                          → RETRY_SOURCE_COMMIT  │
                 │   SOURCE_COMMITTED                                  → NONE                 │
                 └───────────────────────────────────────────────────────────────────────────┘
        invariant preserved: source progress is NEVER advanced for unverified data.
```

| Failure | Behavior | Why it's safe |
|---|---|---|
| Engine crash mid-batch | Pre-VERIFIED → replay from last committed source position; VERIFIED-but-not-committed → retry commit | Progress never advanced for unverified data |
| Replay after crash | **Today:** new key → duplicate object (no loss). **After §6.3:** same key, existing verified = success | At-least-once now; idempotent after deterministic keys |
| State store unavailable | Cannot transition → stops committing (fails safe) | No commit-before-verify possible |
| Kafka rebalance (fleet) | Revoked partitions finish/abandon (no commit); reassigned replica resumes from committed offset | Offsets commit only post-verify |
| File-owner replica dies (Tier 3) | Lease expires; another replica resumes from stored byte cursor | Cursor in shared durable store |
| Object Lock blocks overwrite | Retry uses deterministic key / new version, never overwrite | §6.2 rule |

---

## 16. Backup, restore & disaster recovery **[PROPOSED]**
```text
• State store: Postgres PITR (WAL archiving) or the distributed-SQL backup policy.
• Object store: bucket replication or MinIO/S3 backup policy; Object Lock retention.
• Restore test cadence: scheduled DR drills (e.g., quarterly).
• Targets: define explicit RPO and RTO.
• DR startup sequence: restore state store → restore/verify object store →
  start engine (recovery scan re-reconciles incomplete batches).
• Post-restore validation: reconcile state-store rows vs object-store contents
  (orphan objects / missing objects / stale in-flight rows).
```

---

## 17. Security hardening **[PROPOSED baseline]**
```text
• TLS everywhere (Kafka, database, object store, metrics endpoint).
• IAM / bucket policy scoped to required prefixes only (least privilege).
• Separate credentials per environment and per tenant.
• Secrets via Vault / Kubernetes Secrets / external-secrets — never config.yaml.
• Kubernetes NetworkPolicies isolating engine ↔ DB ↔ Kafka ↔ object store.
• Audit logging for object-store writes and state-store changes.
• Encryption at rest (object store SSE/KMS; DB volume encryption).
• Restricted admin access; least-privilege RBAC.
• Signed container images (cosign) if applicable.
• Vulnerability scanning (image + dependency) in CI.
• SBOM generation per release.
```

---

## 18. Production Roadmap

Dependency-ordered. Each phase is independently shippable. Phases 1–2 are
zero-behavior-change groundwork.

```
 Phase dependency graph
   0 ─► 1 ─► 2 ─► 3 ─► 4 ─► 5 ─► 6 ─► (7, 8) ─► (9 optional) ─► 10
   (baseline) (trait)(invariant)(PG)(kafka grp)(observe)(k8s)  (worm/retention)  (file HA)  (go/no-go)
```

### Phase 0 — Baseline hardening of the current prototype
- **Objective:** make the current single-node SQLite + MinIO + Kafka Compose setup
  stable and measurable.
- **Tasks:** confirm pipeline correctness; document the state machine; add missing
  integration tests; add structured logs; add benchmark scripts; define sample
  datasets and batch sizes; verify crash/replay behavior.
- **Dependencies:** none.
- **Config changes:** none (lab config only).
- **Test plan:** run all source types end-to-end; inject crashes before VERIFY and
  between VERIFY and COMMIT; run the benchmark harness twice and compare.
- **Exit criteria:** all source types ingest→upload→verify→commit; crash before
  VERIFY causes replay; crash after VERIFY/before COMMIT is safe; benchmark results
  reproducible.
- **Risks:** hidden non-determinism in batch IDs surfaces here (it does — §6).
- **Rollback:** n/a (no production surface).

### Phase 1 — Extract `StateStore` trait
- **Objective:** decouple engine logic from SQLite.
- **Tasks:** define `StateStore`; implement it for SQLite; add a scheme-based
  backend factory on `engine.state_store`; keep behavior unchanged; SQLite default.
- **Dependencies:** Phase 0.
- **Config changes:** none (sqlite scheme only).
- **Test plan:** existing test suite unchanged; assert factory rejects unknown
  schemes.
- **Exit criteria:** all existing tests pass; no behavior change; `sqlite://` works.
- **Risks:** accidental behavior drift during refactor.
- **Rollback:** revert the PR (pure refactor, isolated).

### Phase 2 — Centralize invariant + shared tests
- **Objective:** make verify-before-commit backend-independent.
- **Tasks:** move transition validation into shared `vtop-core` logic; add a
  backend-agnostic test battery; test invalid transitions, replay states, and the
  source-commit guard.
- **Dependencies:** Phase 1.
- **Config changes:** none.
- **Test plan:** run the battery against the SQLite backend; add property tests for
  illegal transitions.
- **Exit criteria:** exactly one implementation of the invariant; SQLite passes the
  shared battery.
- **Risks:** missed edge transitions; mitigate with exhaustive state-pair tests.
- **Rollback:** revert; SQLite logic still intact.

### Phase 3 — Add Postgres backend
- **Objective:** support a production durable shared state store.
- **Tasks:** implement `PgStateStore` (PgPool, `$N`, Postgres DDL with §8
  constraints); connection-pool settings; **retry-on-`40001`**; config/env support;
  run the shared battery against Postgres (testcontainers / CI service).
- **Dependencies:** Phases 1–2.
- **Config changes:** `engine.state_store: postgres://…`; new env (§20.3).
- **Test plan:** shared battery on Postgres; conflict-retry test; constraint-
  violation tests (invariant enforced by DB).
- **Exit criteria:** SQLite and Postgres behave identically; `postgres://` works by
  config; default build remains lean (feature-gated).
- **Risks:** SQL dialect drift; serialization retries under load.
- **Rollback:** switch `state_store` back to `sqlite://` (drain first, §7.4).

### Phase 4 — Multi-engine Kafka consumer-group mode
- **Objective:** let multiple replicas process Kafka partitions safely.
- **Tasks:** add long-lived **`subscribe`** consumer-group mode; disable
  auto-commit; commit only after VERIFIED; implement rebalance/revocation handling
  (§9.4); document partition-count scaling limits.
- **Dependencies:** Phase 3 (shared store) strongly recommended.
- **Config changes:** `VTOP_KAFKA_GROUP_MODE=subscribe`; tune
  `max.poll.interval.ms`, `session.timeout.ms`, `heartbeat.interval.ms`.
- **Test plan:** run two+ replicas; kill one and observe rebalance; assert no
  double-commit; assert no source commit before verification.
- **Exit criteria:** replicas split partitions; killing one rebalances; replay is
  idempotent (requires §6.3 for zero duplicates).
- **Risks:** long uploads trigger spurious rebalances; duplicate objects until
  deterministic keys.
- **Rollback:** set `VTOP_KAFKA_GROUP_MODE=assign` and run a single replica.

### Phase 5 — Observability & operations
- **Objective:** make the system observable and operable.
- **Tasks:** expose Prometheus metrics; add OpenTelemetry traces; Grafana
  dashboards; Alertmanager rules; health/readiness endpoints; document SLOs.
- **Dependencies:** can proceed in parallel after Phase 0; most valuable with
  Phase 4.
- **Config changes:** `VTOP_METRICS_ADDR`, `OTEL_EXPORTER_OTLP_ENDPOINT`.
- **Test plan:** scrape metrics; force a verification failure and confirm the
  alert; load test and watch lag/replay panels.
- **Exit criteria:** operators see throughput, lag, replay, failures; alerts fire
  on verification failures, lag growth, store latency.
- **Risks:** metric cardinality blow-up; mitigate with bounded labels.
- **Rollback:** disable the metrics endpoint (no data-path impact).

### Phase 6 — Kubernetes deployment
- **Objective:** deploy VTOP as an HA service.
- **Tasks:** Helm chart / manifests; secrets; ConfigMap; liveness/readiness;
  resource requests/limits; PodDisruptionBudget; NetworkPolicy; rolling-upgrade
  runbook; optional KEDA on Kafka lag (§9.5).
- **Dependencies:** Phases 3–5.
- **Config changes:** chart values; KEDA ScaledObject (min/max ≤ partition count).
- **Test plan:** rolling upgrade under load (assert no loss); kill a pod (assert
  auto-replace + rebalance); scale on lag within partition bounds.
- **Exit criteria:** rolling upgrade causes no data loss; failed pod auto-replaced;
  lag-based scaling works within partition-count limits.
- **Risks:** rebalance storms during rollout; tune `max.poll.interval.ms`.
- **Rollback:** Helm rollback to previous revision.

### Phase 7 — Object Lock / WORM hardening (with deterministic keys)
- **Objective:** compliant, tamper-resistant, idempotent archive behavior.
- **Tasks:** **adopt deterministic / content-addressed object keys**; define WORM
  mode; record `version_id`; update retry to "existing verified = success"; verify
  existing objects; document retention; test retry against locked objects.
- **Dependencies:** Phase 6 (and depends conceptually on §6.3).
- **Config changes:** `VTOP_OBJECT_KEY_MODE=deterministic|content-addressed`;
  bucket Object Lock policy.
- **Test plan:** enable Object Lock; replay a batch; assert no overwrite attempt and
  no duplicate; assert audit metadata recorded.
- **Exit criteria:** retries never delete/overwrite protected objects; state +
  manifest hold enough metadata for audit; replay is idempotent.
- **Risks:** key-scheme change affects downstream consumers of object layout.
- **Rollback:** `VTOP_OBJECT_KEY_MODE=legacy` (duplicates return, but no loss).

### Phase 8 — State retention, pruning, backup/restore
- **Objective:** make the ledger sustainable long-term.
- **Tasks:** retention policy; archive old committed rows; keep failed/replay rows
  hot; backup/restore runbook; test restore; object-store ↔ state-store divergence
  detection.
- **Dependencies:** Phase 3.
- **Config changes:** retention settings; backup schedules.
- **Test plan:** restore from backup into a clean environment; run the divergence
  checker; verify RPO/RTO targets met.
- **Exit criteria:** ledger growth bounded by policy; restore tested; RPO/RTO
  documented.
- **Risks:** pruning a row still needed for replay; mitigate by pruning only
  SOURCE_COMMITTED beyond retention.
- **Rollback:** disable pruning; restore from backup.

### Phase 9 — Optional file/syslog HA
- **Objective:** distributed file/syslog ownership (only if required).
- **Tasks:** etcd/Consul leases; store byte cursors in shared state;
  `lease_owner`/`lease_until`; takeover logic; test owner failure; document UDP
  syslog limitations and the durable-spool requirement.
- **Dependencies:** Phases 3, 6.
- **Config changes:** `VTOP_ETCD_ENDPOINTS`, `VTOP_LEASE_TTL_SECONDS`,
  `VTOP_INSTANCE_ID`.
- **Test plan:** two replicas; kill the owner; assert ownership transfer with no gap
  and no uncontrolled duplication.
- **Exit criteria:** one owner per file/spool; failure transfers ownership safely.
- **Risks:** split-brain on lease expiry; mitigate with fencing tokens.
- **Rollback:** pin file/syslog to a single replica (Tier 2 behavior).

### Phase 10 — Production readiness review
- **Objective:** decide go/no-go for enterprise deployment.
- **Tasks:** architecture review; threat model; chaos tests; performance tests;
  validate backup/restore; validate security controls; validate runbooks; review
  open risks.
- **Dependencies:** all prior phases relevant to the target tier.
- **Config changes:** finalize production config + secrets.
- **Test plan:** chaos + load + DR drill executed together; sign-off checklist.
- **Exit criteria:** documented go/no-go; risks have owners; readiness checklist
  (§23) complete.
- **Risks:** late discovery of a blocking gap; mitigate by running this review
  incrementally, not only at the end.
- **Rollback:** no-go decision keeps the system at its last validated tier.

---

## 19. Risk register

| Risk | Impact | Mitigation | Owner | Phase |
|---|---|---|---|---|
| Object Lock prevents overwrite-based retry | Retries fail / stuck batches | Deterministic keys + "existing verified = success" / version_id | Eng (storage) | 7 |
| Kafka rebalance during in-flight batch | Duplicate work / spurious revocation | Revocation handler; tune poll/session timeouts; cooperative rebalancing | Eng (ingest) | 4 |
| State database unavailable | Engine cannot commit | Fail safe (stop committing); HA store; alerts | Eng/Ops | 3,6 |
| Object uploaded but manifest upload failed | Batch stuck pre-VERIFIED | Retry manifest; verify existing object; bounded retry_count then FAILED | Eng (pipeline) | 0,3 |
| Manifest uploaded but source commit failed | Possible replay | Recovery retries commit; deterministic keys make replay idempotent | Eng (engine) | 4,7 |
| File cursor lost during backend switch | File reprocessing / duplicates | Drain + cursor migration + validation + rollback (§7.4) | Ops | 3 |
| Syslog UDP packet loss | Silent data loss | Ingest only from durable spool; document UDP limits | Eng/Ops | 9 |
| KEDA scales beyond useful partition count | Wasted replicas, no throughput gain | Bound max replicas to partition count (§9.5) | Ops | 6 |
| State-store growth without retention | Ledger bloat, slow scans | Retention/pruning policy; archive cold rows | Ops | 8 |
| Distributed-SQL serialization retries (`40001`) | Latency spikes under contention | Retry-on-`40001`; capacity test; choose store per §13 | Eng (state) | 3 |

---

## 20. Configuration & environment reference

### 20.1 Current config (`config.yaml`) — implemented
`engine.{name,tenant,state_store,work_dir,log_level}`;
`batching.{max_records,max_bytes,max_batch_age_seconds}`;
`compression.{type,level}`; `checksum.algorithm`; `manifest_mac_key_env`
(optional env-var name; the key itself is not serialized);
`sources.kafka.*` (incl. `enable_auto_commit:false`), `sources.file.*`
(`paths,delete_after_commit,whole_file`), `sources.syslog_spool.paths`;
`upload.{backend,bucket,prefix,endpoint_url,region,force_path_style,verify_tls,
create_bucket,local_path,require_strong_verification}`; `partitioning.template`.

> Production: retain the default `upload.require_strong_verification: true` (§3.2).

### 20.2 Current environment variables — implemented
`VTOP_CONFIG`; `RUST_LOG`; `AWS_ACCESS_KEY_ID`; `AWS_SECRET_ACCESS_KEY`;
`AWS_REGION`; `VTOP_S3_ENDPOINT_URL`; `VTOP_S3_FORCE_PATH_STYLE`;
`VTOP_S3_VERIFY_TLS`; Kafka SASL password via the env var **named** in
`sasl_password_env`; the manifest MAC key via the env var named in
`manifest_mac_key_env`.

### 20.3 Proposed environment variables (HA phases) **[PROPOSED]**
| Variable | Phase | Purpose |
|---|---|---|
| `VTOP_STATE_STORE` | 1 | override `engine.state_store` from a secret/env |
| `VTOP_PG_MAX_CONNECTIONS` | 3 | Postgres pool size per replica |
| `VTOP_PG_STATEMENT_TIMEOUT_MS` | 3 | guard stuck statements |
| `VTOP_STATE_RETRY_MAX` | 3 | max retries on SQLSTATE `40001` |
| `PGPASSWORD` / conn-string secret | 3 | DB password via secret manager |
| `VTOP_KAFKA_GROUP_MODE` | 4 | `assign` (single-node) \| `subscribe` (fleet) |
| `VTOP_METRICS_ADDR` | 5 | Prometheus endpoint (e.g. `0.0.0.0:9090`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 5 | OpenTelemetry collector |
| `VTOP_OBJECT_KEY_MODE` | 7 | `legacy` \| `deterministic` \| `content-addressed` |
| `VTOP_INSTANCE_ID` | 9 | stable replica identity (group member / lease owner) |
| `VTOP_ETCD_ENDPOINTS` | 9 | etcd/Consul endpoints for leases |
| `VTOP_LEASE_TTL_SECONDS` | 9 | source-ownership lease TTL |

> Secrets must come from a secret manager, never `config.yaml`.

---

## 21. Executive summary

VTOP is a **replay-safe, verify-before-commit** telemetry-object transfer engine.
Its single hard rule — **SOURCE_COMMITTED is forbidden until VERIFIED is true** —
must hold at every scale.

Taking it to enterprise HA does **not** require a stack of databases. Under the
**Kafka-primary** assumption it requires three things: a **`StateStore`
abstraction** (so SQLite stays for dev and one Postgres-compatible store serves
production), **Kafka consumer-group mode** (Kafka already distributes and rebalances
work across replicas), and **Kubernetes + Prometheus** for operability. Object
storage (S3/MinIO) is already the durable data plane; enabling Object Lock makes it
a tamper-evident archive.

Two current-behavior facts must be addressed before "production-grade": object keys
are **non-deterministic** (so replays create duplicate objects — fix with
**deterministic/content-addressed keys** to become idempotent at the archive layer
and Object-Lock-safe), and **file/syslog cursors live only in the state store** (so
a backend switch needs **drain + cursor migration**, while Kafka resumes
broker-side). Neither blocks the design; both are scheduled (Phases 7 and 3).

The recommended baseline is **Tier 2** (k8s fleet + one Postgres-compatible store +
Kafka groups). **PostgreSQL + Patroni** is the default store choice for familiar
operations and lowest write latency; **YugabyteDB/CockroachDB** only if self-healing
DB HA outweighs latency and simplicity. File/syslog HA (etcd leases, Tier 3) is
optional and built only if required. Every claim here **requires validation** under
real production load.

---

## 22. Phased roadmap table

| Phase | Title | Depends on | Key outcome | Testable on Compose? |
|---|---|---|---|---|
| 0 | Baseline hardening | — | Reproducible, crash-safe single node | ✅ |
| 1 | Extract `StateStore` trait | 0 | Backend pluggable (SQLite default) | ✅ |
| 2 | Centralize invariant + shared tests | 1 | One invariant impl; shared battery | ✅ |
| 3 | Postgres backend | 1,2 | Durable shared store + retry-on-`40001` | ✅ |
| 4 | Kafka consumer-group mode | 3 | Multi-replica partition distribution | ⚠️ partial |
| 5 | Observability & ops | 0 (parallel) | Metrics/traces/alerts | ✅ |
| 6 | Kubernetes deployment | 3,4,5 | HA service, rolling upgrades, KEDA | ❌ (needs k8s) |
| 7 | Object Lock / WORM + deterministic keys | 6 | Idempotent, tamper-resistant archive | ✅ (MinIO) |
| 8 | Retention, pruning, backup/restore | 3 | Sustainable ledger + tested restore | ✅ |
| 9 | File/syslog HA (optional) | 3,6 | Lease-based source ownership | ⚠️ rehearsal |
| 10 | Production readiness review | all | Go/no-go decision | ❌ (full env) |

---

## 23. Production-readiness checklist

```text
Correctness & invariant
  [ ] verify-before-commit enforced in core AND database (CHECK constraint)
  [ ] crash before VERIFY → replay; crash after VERIFY/before COMMIT → safe
  [ ] deterministic/content-addressed object keys (idempotent replay)
  [ ] require_strong_verification remains true in production

State store
  [ ] StateStore trait + Postgres backend; shared test battery green on both
  [ ] retry-on-40001 implemented and load-tested
  [ ] schema constraints + indexes (UNIQUE source range, state index)
  [ ] multi-writer recovery (FOR UPDATE SKIP LOCKED or leases)
  [ ] retention/pruning policy; backup + tested restore; RPO/RTO documented

Kafka / scaling
  [ ] consumer-group mode; auto-commit OFF; commit only after VERIFIED
  [ ] rebalance/revocation handled; poll/session timeouts tuned
  [ ] KEDA min/max replicas bounded by partition count

Object storage
  [ ] distributed MinIO / S3; Object Lock policy; retention configured
  [ ] retry never overwrites locked objects; version_id recorded

Operability
  [ ] Prometheus metrics + Grafana dashboards + Alertmanager rules
  [ ] OpenTelemetry traces; health/readiness endpoints; SLOs documented
  [ ] k8s: probes, resource limits, PDB, NetworkPolicy, rolling-upgrade runbook

Security
  [ ] TLS everywhere; least-privilege IAM; per-env/per-tenant credentials
  [ ] secrets via Vault/k8s/external-secrets; encryption at rest
  [ ] audit logging; image signing; vuln scan; SBOM

Resilience
  [ ] chaos tests (pod/DB/broker loss) pass
  [ ] DR drill executed; divergence checker validated
  [ ] risk register reviewed; each risk has an owner
```

---

## 24. Open decisions requiring human approval

1. **Primary ingress:** Kafka-only (stop at Tier 2) **or** file/syslog at scale
   (Tier 3 + etcd, Phase 9)?
2. **State store choice:** PostgreSQL + Patroni vs YugabyteDB/CockroachDB (§13) —
   trade familiar ops + low latency against self-healing DB HA.
3. **Object-key scheme:** approve deterministic / content-addressed keys (Phase 7)?
   Required for idempotent replay and Object Lock safety.
4. **Compliance:** is Object Lock / WORM mandatory (audit/regulatory)? If yes, it
   becomes baseline, not optional.
5. **State-write latency target & RPO/RTO:** drives store choice, retry tuning, and
   backup design.
6. **Scope/timeline:** which target tier (1, 2, or 3) is in scope for the first
   production release?
