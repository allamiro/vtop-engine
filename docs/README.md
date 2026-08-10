# VTOP Engine — Documentation

Documentation set for the **VTOP Engine** (Verified Telemetry Object Protocol
Engine), a prototype / reference implementation of a replay-safe,
manifest-driven telemetry object transfer engine — plus the distributed VTOP
*cluster* plane that is growing alongside it.

> **Status:** prototype / candidate-invention disclosure support package. Not
> patented or patent-pending.

Start with the [project README](../README.md) for setup, the CLI, and the
Docker lab.

## The one rule everything serves

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A source progress marker (Kafka offset, file byte offset, syslog spool offset)
is never committed until the telemetry object and its manifest have been
durably written **and verified** in object storage. The normative statement is
the commit rule, [VTOP_PROTOCOL_DRAFT.md §13](VTOP_PROTOCOL_DRAFT.md#13-commit-rule);
its enforcement is described in
[ARCHITECTURE.md](ARCHITECTURE.md#engine-runtime-flow) and the
[README](../README.md#core-rule).

## Two planes, one repository

| Plane | What it is | Where to read |
|---|---|---|
| **Engine (archive path)** | The verify-before-commit pipeline: sources → batches → compressed objects + bound manifests → object storage. | [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md), [ARCHITECTURE.md](ARCHITECTURE.md), [PRODUCTION_HA.md](PRODUCTION_HA.md) |
| **Cluster (native log)** | Raft metadata, lease-based range leadership, replicated sealed segments, verified transfer/repair. | [NATIVE_BROKER_ARCHITECTURE.md](NATIVE_BROKER_ARCHITECTURE.md), [ROADMAP.md](ROADMAP.md) |

The planes share terminology carefully: the archive batch state `VERIFIED`
(protocol §12) and the cluster's segment `VERIFIED` are **different states of
different subjects** — each doc says which it means.

## Reading order

- **New to the project?** Project [README](../README.md) → [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md) → [ARCHITECTURE.md](ARCHITECTURE.md).
- **Deploying it?** [PRODUCTION_HA.md](PRODUCTION_HA.md) (status first — §1) → [POSTGRES_DEPLOYMENT.md](POSTGRES_DEPLOYMENT.md) → [SECURITY_MODEL.md](SECURITY_MODEL.md) → [RELEASE_VERIFICATION.md](RELEASE_VERIFICATION.md).
- **Evaluating or depending on a version?** [ROADMAP.md](ROADMAP.md) (per-release limitations) → [COMPARISON.md](COMPARISON.md).

## Contents

### Protocol & architecture

| Document | What it covers | Read it if you want to… |
|----------|----------------|--------------------------|
| [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md) | **Normative** protocol draft (`MUST`/`SHOULD`/`MAY`), state machine, commit & replay rules, object naming, conformance profiles (`VTOP-Core`, `-Kafka`, `-File`, `-Syslog-Spool`, `-S3`, `-LocalFS`). | Understand the rules a conformant implementation must follow. |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Crate layout, the engine runtime flow, crash recovery / replay, partitioning, data-flow diagram, and implementation status vs the protocol. | Understand how the reference implementation is built. |
| [NATIVE_BROKER_ARCHITECTURE.md](NATIVE_BROKER_ARCHITECTURE.md) | Governing direction for the VTOP-owned broker/control plane, its Kafka boundary, storage kernel, and implementation order. | Understand how VTOP grows into a native log system rather than depending on Kafka for cluster correctness. |
| [COMPARISON.md](COMPARISON.md) | Honest positioning against Kafka and LinkedIn Northguard/Xinfra: where VTOP is ahead, behind, and which roadmap item closes each gap. | Position VTOP against existing systems. |

### Operations & deployment

| Document | What it covers | Read it if you want to… |
|----------|----------------|--------------------------|
| [PRODUCTION_HA.md](PRODUCTION_HA.md) | **The** engine HA document: current status (Part I), design & operational reference (Part II), phased roadmap 0–10 with risks, readiness checklist, and open decisions (Part III). | Design, operate, or plan a highly-available engine deployment — or see what's done and what remains. |
| [POSTGRES_DEPLOYMENT.md](POSTGRES_DEPLOYMENT.md) | PostgreSQL state-store rollout: `vtopctl migrate`, identity/privilege split, TLS requirements, scheduled pruning. | Deploy the engine against a Postgres ledger. |
| [SECURITY_MODEL.md](SECURITY_MODEL.md) | Threat model, transport security, credential handling, manifest confidentiality/authentication, integrity verification, immutability, hardening, supply chain, normative-rules summary. | Understand the threat model and operational security rules. |
| [NODE_OBSERVABILITY.md](NODE_OBSERVABILITY.md) | The `/metrics`, `/healthz`, `/readyz` contract shared by the engine and the cluster nodes, the full metric catalogue, and what readiness means per role (#224). | Scrape, alert on, or health-gate a VTOP process. |
| [RELEASE_VERIFICATION.md](RELEASE_VERIFICATION.md) | Verifying release artifacts (checksums, cosign signatures, SBOM). | Validate a downloaded release. |
| [ROADMAP.md](ROADMAP.md) | Release-by-release narrative for the VTOP *cluster* plane (v0.1.0+), including known limitations per release. | Decide whether to depend on a cluster version. |

### Research & validation

| Document | What it covers | Read it if you want to… |
|----------|----------------|--------------------------|
| [LIVE_CHAOS_VALIDATION.md](LIVE_CHAOS_VALIDATION.md) | Repeatable real-process metadata membership and native data-plane chaos scenarios (#215), including exact scope boundaries. | Run or audit the live-cluster validation harness and understand what it does not yet prove. |
| [WRITE_AMP_PROOF_OVERHEAD.md](WRITE_AMP_PROOF_OVERHEAD.md) | Methodology and runner for native segment write amplification + proof-carrying overhead (#189). | Measure single-copy body path and v1 vs v2 proof cost. |
| [FETCH_IO_RESEARCH.md](FETCH_IO_RESEARCH.md) | Methodology, runner, and recommendation gate for native fetch I/O (#190): buffered vs sendfile/splice vs experimental O_DIRECT. | Decide whether buffered fetch is enough before pursuing Direct I/O / io_uring. |
| [METADATA_SATURATION_RESEARCH.md](METADATA_SATURATION_RESEARCH.md) | Methodology, runner, and sharding-trigger gate for the single three-node metadata Raft group (#192). | Measure when one metadata group saturates before implementing multi-group sharding. |

### Legal / disclosure support

| Document | What it covers | Read it if you want to… |
|----------|----------------|--------------------------|
| [INVENTION_DISCLOSURE_DRAFT.md](INVENTION_DISCLOSURE_DRAFT.md) | Problem, invention, technical advantages, main claim candidate, and potential claim families. | Review the candidate invention for technical/legal evaluation. |
| [PRIOR_ART_SEARCH_PLAN.md](PRIOR_ART_SEARCH_PLAN.md) | Comparison of related tools/systems and a prior-art search strategy. | Plan a prior-art investigation. |

## Documentation conventions

- **"protocol §N"** always cites [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md),
  the single normative source.
- **`[IMPLEMENTED]` / `[PROPOSED]`** mark whether a described behavior is
  shipped or planned; status claims should be verifiable against the code or
  CI.
- **Achieved vs planned** lives in exactly two places: engine work in
  [PRODUCTION_HA.md](PRODUCTION_HA.md) Part I/III, cluster work in
  [ROADMAP.md](ROADMAP.md). Other documents link there instead of restating
  status.

## Legal note

The invention-disclosure and prior-art documents are internal drafts for
technical and legal review. They are **not** a patent application, do not
establish patent rights, and should be reviewed by qualified patent counsel
before any filing or public disclosure.
