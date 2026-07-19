# VTOP Engine — Documentation

Documentation set for the **VTOP Engine** (Verified Telemetry Object Protocol Engine), a prototype / reference implementation of a replay-safe, manifest-driven telemetry object transfer engine.

> **Status:** prototype / candidate-invention disclosure support package. Not patented or patent-pending.

Start with the [project README](../README.md) for setup, the CLI, and the Docker lab.

## Contents

| Document | What it covers | Read it if you want to… |
|----------|----------------|--------------------------|
| [VTOP_PROTOCOL_DRAFT.md](VTOP_PROTOCOL_DRAFT.md) | Normative protocol draft (`MUST`/`SHOULD`/`MAY`), state machine, commit & replay rules, object naming, conformance profiles (`VTOP-Core`, `-Kafka`, `-File`, `-Syslog-Spool`, `-S3`). | Understand the rules a conformant implementation must follow. |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Crate layout, the engine runtime flow, crash recovery / replay, partitioning, and a data-flow diagram. | Understand how the reference implementation is built. |
| [NATIVE_BROKER_ARCHITECTURE.md](NATIVE_BROKER_ARCHITECTURE.md) | Governing direction for the VTOP-owned broker/control plane, its Kafka boundary, storage kernel, and implementation order. | Understand how VTOP grows into a native log system rather than depending on Kafka for cluster correctness. |
| [SECURITY_MODEL.md](SECURITY_MODEL.md) | Transport security, credential handling, manifest confidentiality, integrity verification, immutability, hardening, supply chain, and a normative-rules summary. | Understand the threat model and operational security rules. |
| [INVENTION_DISCLOSURE_DRAFT.md](INVENTION_DISCLOSURE_DRAFT.md) | Problem, invention, technical advantages, main claim candidate, and potential claim families. | Review the candidate invention for technical/legal evaluation. |
| [PRIOR_ART_SEARCH_PLAN.md](PRIOR_ART_SEARCH_PLAN.md) | Comparison of related tools/systems and a prior-art search strategy. | Plan a prior-art investigation. |

## The one rule everything serves

```text
SOURCE_COMMITTED is forbidden until VERIFIED is true.
```

A source progress marker (Kafka offset, file byte offset, syslog spool offset) is never committed until the telemetry object and its manifest have been durably written **and verified** in object storage. The normative statement lives in [VTOP_PROTOCOL_DRAFT.md §7](VTOP_PROTOCOL_DRAFT.md#7-commit-rule); its enforcement is described in [ARCHITECTURE.md](ARCHITECTURE.md#engine-runtime-flow) and the [README](../README.md#verification-before-commit).

## Legal note

The invention-disclosure and prior-art documents are internal drafts for technical and legal review. They are **not** a patent application, do not establish patent rights, and should be reviewed by qualified patent counsel before any filing or public disclosure.
