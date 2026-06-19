# VTOP Engine — Production-Grade HA Plan

> Status: **Proposal / design doc** (no code changes implied by this document).
> Audience: operators and engineers deciding how to deploy VTOP from a single-node
> prototype to an enterprise, highly-available archive engine.
>
> This document explains **what the system is**, **what production requires**, the
> **`StateStore` abstraction** that unlocks it, a **phased plan**, **deployment
> topologies** (from a laptop with Docker Compose to a multi-node Kubernetes
> fleet), **hardware sizing**, and a **full environment/config reference**.

---

## 1. Scope and goals

### 1.1 What we are building toward
Take VTOP from a **single-process prototype** (SQLite ledger, one engine) to a
**horizontally scalable, highly-available** telemetry-object transfer engine
suitable for enterprise production, **without weakening the core guarantee**:

> **SOURCE_COMMITTED is forbidden until VERIFIED is true.**

### 1.2 Non-negotiable invariants (must survive every change)
- **Verify-before-commit** — source progress (Kafka offsets / file byte cursor /
  spool position) is advanced only after the object **and** manifest are uploaded
  and verified.
- **Replay safety** — a crash at any stage must be recoverable without data loss
  or silent gaps.
- **Idempotency** — deterministic object keys + manifests mean a retried batch
  rewrites the *same* object, so at-least-once processing is effectively
  exactly-once at the archive layer.

### 1.3 Explicit non-goals
- Not a stream-processing/analytics engine (no transforms beyond framing/format
  detection).
- Not a database for the telemetry itself — object storage is the archive.

---

## 2. System model (recap)

VTOP has a clean **two-plane** design. Understanding this is what keeps the HA
design small.

| Plane | Holds | Component | Scale/HA story |
|---|---|---|---|
| **Data plane** | the telemetry bytes | Object storage (S3 / MinIO) | Already durable + HA-capable; add Object Lock (WORM) |
| **Control plane** | batch lifecycle ledger | **State store** (SQLite today) | The single thing that blocks horizontal scale today |

```
                         ┌──────────────────────────────────────────┐
  sources               │                VTOP engine                 │      archive
 ┌─────────┐  read      │  discover → batch → seal → compress →       │  put ┌──────────┐
 │ Kafka   │──────────► │  checksum → upload object → upload manifest │─────►│  S3 /    │
 │ files   │            │  → VERIFY → COMMIT source progress          │      │  MinIO   │
 │ syslog  │            └───────────────┬────────────────────────────┘      │ (WORM)   │
 └─────────┘                            │ state transitions                  └──────────┘
                                        ▼
                              ┌────────────────────┐
                              │   STATE STORE       │  ← SQLite (dev) / Postgres-compat (prod)
                              │ (replay ledger,     │
                              │  enforces invariant)│
                              └────────────────────┘
```

**Key facts that shape the plan (from the codebase):**
- Per batch the engine performs ~**9 tiny state writes** (`save` → `sealed` →
  `compressed` → `checksummed` → `object_uploaded` → `manifest_uploaded` →
  `verified` → `source_committed`), plus a recovery scan (`list_incomplete`) at
  startup.
- The run loop is **single-process and sequential** today (no parallel writers).
- The **bottleneck is the S3 upload**, not the state store.

---

## 3. What production-grade HA actually needs

The honest, minimal set — **one** durable store, not a zoo of databases.

| Need | Component | Required? | Notes |
|---|---|---|---|
| Durable shared ledger | **ONE** Postgres-compatible DB | **Yes** | Plain PostgreSQL *or* a distributed-SQL (YugabyteDB / CockroachDB) if you want the store itself to be HA with no failover tooling. Pick one. |
| Work distribution + failover (Kafka) | **Kafka consumer groups** | **Yes (already have)** | Kafka *is* the coordinator: it assigns/rebalances partitions across engine instances. No extra coordination DB. |
| Durable data plane | **S3 / MinIO** | **Yes (already have)** | Distributed MinIO (erasure-coded) or real S3; enable Object Lock. |
| Orchestration / heal / scale | **Kubernetes (+ KEDA)** | **Yes for HA** | Restarts, rolling upgrades, autoscale on Kafka lag. |
| Observability | **Prometheus + Grafana + OpenTelemetry** | **Yes** | Metrics, dashboards, traces, alerts. |
| Secrets | **Vault / external-secrets** | Recommended | Credentials already injected via env — good foundation. |
| File/syslog HA ownership | **etcd / Consul** (leases) | **Only if** file/syslog must be HA-distributed | The *only* thing Kafka groups don't solve. Skip if Kafka-primary. |

### 3.1 What to deliberately NOT add
- **Redis** — not needed. It can't be the durable store (durability is the point),
  and there's nothing to cache (state writes are tiny and not the bottleneck).
- **Two databases at once** — Postgres *and* Yugabyte/Cockroach are alternatives,
  never a stack. Choose one.
- **etcd** — skip unless distributed file/syslog ingestion is a hard requirement.

> **Bottom line:** if Kafka is the primary production ingress, enterprise HA is
> essentially **one new database + the `StateStore` abstraction + k8s/Prometheus**.

---

## 4. The `StateStore` abstraction (the one piece of real work)

This is the unlock. Everything else is configuration and ops.

### 4.1 Backend selection = a config string
The engine already reads `engine.state_store`. A factory dispatches on the URL
scheme — **switching backends is a one-line config change, same binary**:

| Deployment | `engine.state_store` value |
|---|---|
| Dev / single appliance | `sqlite:///data/state/vtop-state.db` |
| Production (Postgres) | `postgres://vtop@pg-host:5432/vtop` |
| Production (Yugabyte) | `postgres://vtop@yb-host:5433/vtop` (same driver) |
| Production (Cockroach) | `postgres://vtop@crdb-host:26257/vtop` (same driver) |

### 4.2 The trait (single source of truth for the invariant)
- A `StateStore` trait abstracts: `save_batch_state`, `update_batch_state`,
  `mark_verified`, `mark_source_committed`, `mark_failed`, `get_batch`,
  `list_incomplete_batches`, `list_failed_batches`, `list_batches`.
- The engine holds `Box<dyn StateStore>`.
- **The verify-before-commit guard stays as pure logic in `vtop-core`
  (`state_machine`)**, called by every backend — never re-implemented in SQL per
  backend (that is how backends drift and an invariant gets violated).

### 4.3 Backend differences (the actual implementation delta)
| Concern | SQLite backend | Postgres / Yugabyte / Cockroach backend |
|---|---|---|
| Driver | `sqlx` `SqlitePool` | `sqlx` `PgPool` (pure-Rust, no libpq) |
| Placeholders | `?` | `$1, $2, …` |
| Insert | plain `INSERT` | plain `INSERT` |
| Migrations | SQLite DDL | Postgres DDL (separate file) |
| **Conflict retry** | none | **retry on SQLSTATE `40001`** (distributed serialization) |
| Build | default | behind Cargo `--features postgres` (keeps single-node build lean) |

### 4.4 Why no data migration when switching
The state store is a **replay ledger, not the data**. On first start against a
fresh Postgres, in-flight work is simply re-discovered from sources (Kafka
offsets / file cursors). **You never migrate rows when switching backends.**

---

## 5. Deployment topologies

Four tiers, from a laptop to a fleet. Each tier is independently usable.

### Tier 0 — Single node, Docker Compose (dev / demo / small prod)
- One engine, **SQLite** ledger, single MinIO, single Kafka (KRaft).
- This is the **current lab** (`docker-compose.yml`).
- **Fully testable on one machine.** No HA.

```
[ docker compose ]  kafka(KRaft) + kafka-ui + minio + minio-init + vtop-engine
                    state: sqlite file on a volume
```

### Tier 1 — Single engine + external Postgres (small prod, simplest durable)
- One engine, **Postgres** ledger (so state survives engine host loss / restarts
  cleanly and can be backed up centrally), MinIO/S3.
- Still **one** engine (no horizontal scale) but a proper durable, backable store.
- **Testable on one machine with Docker Compose** (add a `postgres` service).

### Tier 2 — HA fleet, Kafka-primary (recommended enterprise baseline)
- **N engine replicas** on Kubernetes, **Kafka consumer-group mode** (subscribe +
  commit-after-verify) so Kafka distributes partitions and rebalances on failure.
- **One** Postgres-compatible store (Postgres+Patroni, or Yugabyte/Cockroach for
  self-HA).
- Distributed MinIO or S3 with Object Lock.
- **KEDA** autoscales replicas on Kafka consumer lag.
- File/syslog: **not HA** in this tier (either disabled, or pinned to one replica).
- **Needs real multi-node infra** (k8s). Can be *functionally* rehearsed on a
  single k8s (kind/minikube) box; true HA needs ≥3 nodes.

```
        ┌── engine-1 ─┐
 Kafka ─┼── engine-2 ─┼──► S3/MinIO (WORM)
 (group)└── engine-3 ─┘        │
            │  │  │            │
            └──┴──┴──► Postgres / Yugabyte (one logical store, HA)
   metrics ► Prometheus ► Grafana ;  traces ► OTel collector
   autoscale ◄ KEDA (Kafka lag)
```

### Tier 3 — Full HA incl. file/syslog (only if required)
- Tier 2 **plus** an **etcd/Consul lease layer** so each file/spool is owned by
  exactly one replica, with takeover-on-failure resuming from the byte cursor in
  the shared store.
- This is the only tier that adds infrastructure beyond "one database".
- **Needs real multi-node infra**; the lease/takeover path must be chaos-tested.

### 5.1 Topology selection guide
| If your reality is… | Use |
|---|---|
| Laptop / demo / proof | Tier 0 (Compose, SQLite) |
| Small prod, one engine, want backups/HA store | Tier 1 (Compose/VM, Postgres) |
| Real enterprise, Kafka is primary ingress | **Tier 2** (k8s fleet) |
| Enterprise + distributed file/syslog ingestion | Tier 3 (Tier 2 + etcd) |

---

## 6. What can be tested with Docker Compose vs. needs hardware

| Capability | Docker Compose (1 machine) | Needs real multi-node |
|---|---|---|
| Full pipeline correctness (all source types → verified → committed) | ✅ | |
| SQLite ↔ Postgres backend switch (config-string) | ✅ (add `postgres` service) | |
| Postgres backend + retry-on-`40001` logic | ✅ (against single Postgres) | |
| Yugabyte/Cockroach **wire** compatibility | ✅ (single-node container) | |
| Distributed-store **HA** (survive a DB node loss) | ⚠️ partial (multi-container) | ✅ (≥3 DB nodes) |
| Multi-engine Kafka consumer-group distribution | ✅ (scale `engine` to N replicas) | recommended on k8s |
| Engine failover / rebalance under node loss | ⚠️ (kill a container) | ✅ (k8s, ≥3 nodes) |
| KEDA lag-based autoscaling | | ✅ (k8s) |
| etcd file-lease ownership + takeover | ⚠️ (multi-container rehearsal) | ✅ (k8s, ≥3 nodes) |
| Object Lock / WORM immutability | ✅ (MinIO supports it) | ✅ (S3 in prod) |

**Summary:** *correctness and backend portability are fully Compose-testable on one
machine.* Only true **HA behavior** (surviving node loss, autoscaling, lease
takeover) requires multi-node Kubernetes.

---

## 7. Hardware sizing (starting points — tune with the benchmark suite)

> These are conservative baselines. The repo's `benchmarks/` harness should drive
> real numbers for your data shapes.

| Tier | Engine | State store | Object store | Kafka |
|---|---|---|---|---|
| 0 (Compose) | 2 vCPU / 2–4 GB | (SQLite, in engine) | MinIO 1 node, disk for data | 1 broker (KRaft) |
| 1 (single + PG) | 2–4 vCPU / 4 GB | Postgres 2 vCPU / 4 GB / fast SSD | MinIO 1–4 nodes | 1–3 brokers |
| 2 (HA fleet) | 3+ replicas × (2–4 vCPU / 2–4 GB) | Postgres HA (3 nodes) **or** Yugabyte/CRDB (3 nodes × 4 vCPU / 8–16 GB / NVMe) | MinIO ≥4 nodes erasure-coded, or S3 | ≥3 brokers, RF=3 |
| 3 (+ file HA) | as Tier 2 | as Tier 2 | as Tier 2 | as Tier 2 + etcd (3 nodes / small) |

**Notes**
- Engine is **CPU-light, memory-light** (≈8 MiB observed at idle); it scales by
  *replica count* against Kafka partitions, not by big boxes.
- Memory spikes only with **whole-file mode** on large files (loads the file into
  memory — see known limitations). Size engine memory to your largest whole-file
  object, or keep large inputs line-oriented.
- State store sizing is dominated by **write IOPS + fsync latency**, not capacity
  (rows are tiny and prunable). Put it on fast SSD/NVMe.
- Distributed SQL (Yugabyte/CRDB) wants **3 nodes minimum** for quorum and **NVMe**;
  it trades per-write latency for self-healing HA.

---

## 8. Phased implementation plan

Each phase ships independently and is safe to stop at. Phases 1–2 are pure
groundwork with **no behavior change**.

### Phase 1 — Extract the `StateStore` trait (the unlock)
- Define `StateStore`; make the existing SQLite store implement it.
- Engine holds `Box<dyn StateStore>`; add the scheme-based factory (SQLite only).
- **Exit criteria:** all existing tests pass unchanged; behavior identical;
  `sqlite://` still the only supported scheme.

### Phase 2 — Centralize the invariant + shared test battery
- Move the verify-before-commit guard to `vtop-core` logic shared by all backends.
- Add a **backend-agnostic test battery** (same tests run against any
  `StateStore`).
- **Exit criteria:** invariant has exactly one implementation; test battery green
  against SQLite.

### Phase 3 — Postgres backend (`--features postgres`)
- Implement `PgStateStore` (PgPool, `$N` placeholders, Postgres DDL migrations).
- Add **retry-on-`40001`** wrapper for distributed serialization conflicts.
- Run the shared test battery against Postgres (testcontainers / CI service).
- **Exit criteria:** identical behavior on SQLite and Postgres; `postgres://`
  selectable by config; default build unchanged.

### Phase 4 — Kafka consumer-group (multi-instance) mode
- Add a long-lived **subscribe + manual-commit-after-verify** mode (config toggle)
  so N replicas share a consumer group. (Single-node keeps the current behavior.)
- **Exit criteria:** two engine replicas split partitions; killing one rebalances
  to the other; no double-commit; offsets advance only post-verify.

### Phase 5 — Operability (k8s + observability)
- Helm chart; liveness/readiness; Prometheus metrics endpoint; OTel traces;
  Grafana dashboards; Alertmanager rules; KEDA on Kafka lag.
- **Exit criteria:** rolling upgrade with no data loss; autoscale on lag; alerts
  fire on verification-failure/replay-rate/lag.

### Phase 6 — File/syslog HA (optional, only if required)
- etcd/Consul leases for source ownership; takeover resumes from shared-store
  cursor; chaos tests for takeover.
- **Exit criteria:** killing the owner of a file/spool transfers ownership with no
  gap or duplication.

### Phase 7 — Hardening
- Object Lock/WORM profile; Vault secrets; multipart + concurrent uploads;
  multi-writer recovery (`SELECT … FOR UPDATE` / lease column); backup/restore
  runbook; DR drill.

---

## 9. Configuration & environment reference

### 9.1 Current config (`config.yaml`) — already implemented
| Key | Meaning |
|---|---|
| `engine.name` | engine identity |
| `engine.tenant` | default tenant for partitioning/multi-tenancy |
| `engine.state_store` | **backend selector** (`sqlite://…`; `postgres://…` after Phase 3) |
| `engine.work_dir` | scratch dir for staging objects |
| `engine.log_level` | log verbosity |
| `batching.max_records` / `max_bytes` / `max_batch_age_seconds` | seal thresholds |
| `compression.type` / `level` | `gzip` \| `zstd` \| `none` |
| `checksum.algorithm` | `sha256` \| `blake3` \| disabled |
| `sources.kafka.*` | brokers, group, topic include/exclude, `enable_auto_commit:false` |
| `sources.file.*` | paths, `delete_after_commit`, `whole_file` |
| `sources.syslog_spool.*` | spool paths |
| `upload.backend` | `s3_native` \| `s3cmd` \| `awscli` \| `minio` \| `localfs` \| `mock` |
| `upload.bucket` | bucket (supports `telemetry-{format}` templating) |
| `upload.endpoint_url` / `region` / `force_path_style` / `verify_tls` | S3 endpoint |
| `upload.create_bucket` | auto-create per-format buckets |
| `upload.require_strong_verification` | refuse commit on size-only verification |
| `partitioning.template` | object key layout |

### 9.2 Current environment variables — already implemented
| Variable | Purpose |
|---|---|
| `VTOP_CONFIG` | path to `config.yaml` |
| `RUST_LOG` | log filter (e.g. `info`, `info,vtop_adapters=debug`) |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials (never in config) |
| `AWS_REGION` | S3 region |
| `VTOP_S3_ENDPOINT_URL` | S3/MinIO endpoint |
| `VTOP_S3_FORCE_PATH_STYLE` | path-style addressing (MinIO) |
| `VTOP_S3_VERIFY_TLS` | TLS verification toggle (lab only when off) |
| *(Kafka SASL)* | password read from the **env var named** in `sasl_password_env` (never stored in config) |

### 9.3 Proposed environment variables (for the HA phases — not yet implemented)
| Variable | Phase | Purpose |
|---|---|---|
| `VTOP_STATE_STORE` | 1 | optional override of `engine.state_store` (so the conn string can come from a secret/env, not the file) |
| `VTOP_PG_MAX_CONNECTIONS` | 3 | Postgres pool size per engine replica |
| `VTOP_PG_STATEMENT_TIMEOUT_MS` | 3 | guard against stuck statements |
| `VTOP_STATE_RETRY_MAX` | 3 | max retries on SQLSTATE `40001` |
| `PGPASSWORD` / conn-string secret | 3 | DB password via secret manager, not config |
| `VTOP_INSTANCE_ID` | 4 | stable replica identity (consumer-group member / lease owner) |
| `VTOP_KAFKA_GROUP_MODE` | 4 | `assign` (single-node) \| `subscribe` (fleet/consumer-group) |
| `VTOP_METRICS_ADDR` | 5 | Prometheus scrape endpoint (e.g. `0.0.0.0:9090`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 5 | OpenTelemetry collector endpoint |
| `VTOP_ETCD_ENDPOINTS` | 6 | etcd/Consul endpoints for file/syslog leases |
| `VTOP_LEASE_TTL_SECONDS` | 6 | source-ownership lease TTL |

> Secrets (DB password, S3 keys, Kafka SASL) **must** come from a secret manager
> (Vault / k8s Secrets / external-secrets), never from `config.yaml`.

---

## 10. Observability (what to measure and alert on)
- **Metrics (Prometheus):** batches/sec, records/sec, bytes in/out, compression
  ratio, per-stage latency (seal/compress/upload/verify), **verification-failure
  rate**, **replay/REPLAY_REQUIRED rate**, Kafka **consumer lag**, state-store
  write latency, in-flight batches.
- **Traces (OpenTelemetry):** one span per batch with child spans per pipeline
  stage — you already emit these as structured events.
- **Dashboards (Grafana):** throughput, lag, error/replay rates, store latency.
- **Alerts (Alertmanager):** verification failures > 0, replay rate spike, lag
  growth, store write-latency SLO breach, no committed batches in N minutes.

---

## 11. Failure modes & recovery semantics
| Failure | Behavior | Why it's safe |
|---|---|---|
| Engine crash mid-batch | On restart, `recover()` scans incomplete batches; pre-VERIFIED → replay from last committed source position; VERIFIED-but-not-committed → retry commit | Source progress never advanced for unverified data |
| Duplicate processing after crash | Same deterministic object key is rewritten | Idempotent at the archive; Object Lock preserves first verified copy |
| State store unavailable | Engine cannot transition state → stops committing (fails safe) | No commit-before-verify possible |
| Kafka rebalance (fleet) | Partition reassigned to another replica; resumes from committed offset | Offsets committed only post-verify |
| File-owner replica dies (Tier 3) | Lease expires; another replica takes over from the stored byte cursor | Cursor is in the shared durable store |

---

## 12. Open decisions (need a human call)
1. **Primary ingress:** Kafka-only (→ HA is mostly config, stop at Tier 2) **or**
   file/syslog at scale too (→ Tier 3 + etcd, larger project)?
2. **Store choice:** plain **PostgreSQL + Patroni** (familiar) vs **Yugabyte/
   Cockroach** (store self-HA, higher per-write latency)?
3. **Consistency vs latency:** acceptable state-write latency target (drives the
   store choice and whether write-batching is needed).
4. **Compliance:** is **Object Lock / WORM** required (audit/regulatory)? If yes,
   it becomes part of the baseline, not optional.

---

## 13. TL;DR
- **One** durable store (Postgres-compatible), not a stack of databases. No Redis.
  etcd only for distributed file/syslog.
- The **`StateStore` trait (Phase 1)** is the single real prerequisite; after it,
  SQLite ↔ Postgres ↔ Yugabyte/Cockroach is a **config-string choice**.
- **Kafka consumer groups + k8s + Prometheus** deliver enterprise HA for the
  Kafka path with minimal new infrastructure.
- **Correctness and backend portability are fully Docker-Compose-testable on one
  machine;** only true HA behavior (node-loss survival, autoscaling, lease
  takeover) needs multi-node Kubernetes.
