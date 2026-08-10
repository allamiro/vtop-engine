# VTOP Engine — Production HA Roadmap & Status

> Status: **Execution roadmap** for the engine-pipeline HA work designed in
> [`PRODUCTION_HA_PLAN.md`](PRODUCTION_HA_PLAN.md). That document holds the
> architecture, invariants, schema, topology, sizing, and operational
> reference; this one holds **what is done, what is in flight, what remains**,
> in dependency order, with per-phase test plans, exit criteria, and rollback.
>
> Section references of the form "PLAN §N" point into the design document.
>
> Scope note: this roadmap covers the **engine pipeline** (`vtop-core`,
> `vtop-state`, `vtop-adapters`, `vtop-upload`, `vtopctl engine`). The VTOP
> *cluster* plane (`vtop-meta`/`vtop-node`/`vtop-broker`/`vtop-log`) has its
> own release-by-release roadmap in [`ROADMAP.md`](ROADMAP.md); see §8 for how
> the two relate.

---

## Table of contents
1. Where things stand
2. Phase model & dependency graph
3. Phase details
4. Roadmap table
5. Risk register
6. Production-readiness checklist
7. Open decisions requiring human approval
8. Relationship to the cluster roadmap

---

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
  (Phase 3). Tier 1 (single engine + Postgres) is deployable today.
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
  duplicate object.
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
  ingestion is actually required (open decision §7).
- Phase 10 (production readiness review) — after the target tier's phases.

---

## 2. Phase model & dependency graph

Phases are dependency-ordered and independently shippable. Deterministic keys
(Phase 4) deliberately precede fleet mode (Phase 5): once several replicas can
replay each other's work, replays must stop producing duplicate objects.

> Earlier revisions of this document numbered the phases differently
> (Kafka fleet mode as 4, keys as 7). The numbering below is now the single
> canonical one, shared with PLAN's cross-references.

```text
  0 ──► 1 ──► 2 ──► 3 ──► 4 ──► 5 ──► 6 ──► 7 ──► 8 ──► (9 optional) ──► 10
  base  trait  inv   PG   keys  fleet  obs   k8s  DR/sec    file HA     review
  ✅     ✅    ✅    ✅    ⬜     ⬜     ◐     ⬜    ◐          ⬜           ⬜

  ✅ done   ◐ partial   ⬜ not started
```

---

## 3. Phase details

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
schema constraints from PLAN §5.5; bounded retry on SQLSTATE `40001`; battery
green on Postgres. DDL is an explicit `vtopctl migrate` step under a
deployment identity; the live battery proves the runtime role cannot run DDL,
`DELETE`, or `TRUNCATE`. See [`POSTGRES_DEPLOYMENT.md`](POSTGRES_DEPLOYMENT.md).

### Phase 4 — Deterministic keys + idempotent retry ⬜ NOT STARTED — next up
- **Objective:** replaying a batch must resolve to the same object, so retry is
  idempotent at the archive layer and Object Lock is safe (PLAN §6).
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
- **Objective:** N replicas split partitions safely (PLAN §7).
- **Work:** long-lived `subscribe()` + manual commit-after-verify behind
  `VTOP_KAFKA_GROUP_MODE=assign|subscribe`; revocation handling; poll/session/
  heartbeat tuning; **multi-writer recovery** in the state store
  (`SELECT … FOR UPDATE SKIP LOCKED` or `lease_owner`/`lease_until`, #93);
  retire the single-instance restriction (#66) for subscribe mode;
  `VTOP_INSTANCE_ID` for stable member identity.
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
  count (PLAN §7.4).
- **Dependencies:** Phases 4–6.
- **Test plan:** rolling upgrade under load (no loss); pod-kill (auto-replace +
  rebalance); lag-based scaling within partition bounds.
- **Exit criteria:** rolling upgrade causes no data loss; failed pod
  auto-replaced; autoscale respects partition count.
- **Rollback:** Helm rollback to the previous revision.

### Phase 8 — Retention, backup/restore, DR, security baseline ◐ PARTIAL
- **Done:** ledger retention/pruning (#128; PLAN §5.6); least-privilege DB
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
  documented; security baseline (PLAN §16) met.
- **Rollback:** disable pruning; restore from backup.

### Phase 9 — File/syslog HA (optional) ⬜ GATED on open decision §7.1
- **Objective:** distributed file/spool ownership via etcd/Consul leases with
  takeover from the durable state-store cursor (PLAN §8 Tier 3).
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
  model, chaos + load + DR drill executed together, runbooks validated, §6
  checklist signed off, every open risk owned.
- **Mitigation for late surprises:** run this review incrementally as phases
  land, not only at the end — the cluster plane's v0.3.0 retrospective
  ("exercise the composition, not only the layers", [`ROADMAP.md`](ROADMAP.md))
  applies verbatim to the engine fleet.
- **Rollback:** a no-go keeps the system at its last validated tier.

---

## 4. Roadmap table

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

## 5. Risk register

| Risk | Impact | Mitigation | Phase | Status |
|---|---|---|---|---|
| Object Lock prevents overwrite-based retry | Retries fail / stuck batches | Deterministic keys + "existing verified = success" / version_id (PLAN §6.2) | 4 | open |
| Kafka rebalance during in-flight batch | Duplicate work / spurious revocation | Revocation handler; tune poll/session timeouts; cooperative rebalancing (PLAN §7.3) | 5 | open |
| Two engines over one store double-recover | Duplicate ingestion / double-commit | Today: startup instance lock (#66) refuses it; fleet mode adds claim/fencing (#93) | 5 | mitigated (blocked, not solved) |
| State database unavailable | Engine cannot commit | Fails safe (stops committing); HA store; alerts | 3,6 | mitigated |
| Object uploaded but manifest upload failed | Batch stuck pre-VERIFIED | Retry manifest; verify existing object; bounded `retry_count` → FAILED | 0,3 | mitigated |
| Manifest uploaded but source-commit failed | Possible replay | Recovery retries commit; deterministic keys make replay idempotent | 4 | partially mitigated (duplicates until 4) |
| File cursor lost during backend switch | File reprocessing / duplicates | Drain + cursor migration + validation + rollback (PLAN §5.4) | — | procedural |
| Syslog UDP packet loss | Silent data loss | Ingest only from durable spool; document UDP limits (PLAN §8) | 9 | documented |
| KEDA scales beyond useful partition count | Wasted replicas, no throughput gain | Bound max replicas to partition count (PLAN §7.4) | 7 | open |
| State-store growth without retention | Ledger bloat, slow scans | Retention/pruning (#128); scheduled `vtopctl prune-ledger` on Postgres | 8 | **closed** |
| Distributed-SQL serialization retries (`40001`) | Latency spikes under contention | Bounded retry implemented; capacity-test under real write rate (PLAN §11) | 3 | mitigated (load test pending) |

---

## 6. Production-readiness checklist

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
  [x] schema constraints + indexes (UNIQUE source range; state index)
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

## 7. Open decisions requiring human approval

1. **Primary ingress:** Kafka-only (stop at Tier 2) **or** file/syslog at scale
   (Tier 3 + etcd, Phase 9)? Gates whether Phase 9 exists at all.
2. **Store choice:** PostgreSQL + Patroni vs Yugabyte/Cockroach (PLAN §11) —
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

## 8. Relationship to the cluster roadmap

The repository now carries two HA efforts with different shapes:

- **The engine pipeline (this roadmap):** HA by *fleet + shared ledger* —
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
