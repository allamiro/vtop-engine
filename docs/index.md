---
title: VTOP Engine
description: Replay-safe telemetry archiving into S3-compatible object storage. Source progress is committed only after the stored object is verified.
hide:
  - navigation
  - toc
---

# VTOP Engine

VTOP Engine moves telemetry from **Kafka topics, log files and syslog spools**
into **S3-compatible object storage**, and never loses track of what has been
archived. Each batch is compressed, checksummed and uploaded with a manifest,
and the copy in storage is checked before the source moves on.

!!! info "The one rule"

    **Source progress is committed only after the stored object is verified.**

    A Kafka offset, a file byte offset or a syslog spool offset never advances
    until the object and its manifest are in storage and have been verified.
    If anything fails before that point, the data is still in the source and
    is read again. The normative statement is
    [protocol §13, the commit rule](VTOP_PROTOCOL_DRAFT.md#13-commit-rule).

VTOP is a prototype and reference implementation. Read the
[known limitations](reference/limitations.md) before you depend on it.

<div class="grid cards" markdown>

-   :material-download: **Install**

    ---

    Signed release binaries for Linux and macOS, a multi-arch container
    image, a Helm chart, or a build from source.

    [:octicons-arrow-right-24: Install VTOP](install/index.md)

-   :material-server: **Deploy**

    ---

    One engine with SQLite, a PostgreSQL ledger, or the cluster plane on
    Kubernetes.

    [:octicons-arrow-right-24: Choose a deployment](deploy/index.md)

-   :material-chart-line: **Operate**

    ---

    Metrics and alerts, re-verifying the archive, upgrades and
    troubleshooting.

    [:octicons-arrow-right-24: Run it in production](operate/index.md)

</div>

## Start here

- New to VTOP: [Getting started](getting-started/index.md), then the
  [Docker lab](getting-started/quickstart.md).
- Writing a config: [Configure](configure/index.md) and the
  [`config.yaml` reference](configure/reference.md).
- How it works: [Architecture](ARCHITECTURE.md) and the
  [protocol](VTOP_PROTOCOL_DRAFT.md).
- How it measures up, with every condition: [Measured results](benchmarks/index.md).
- What is shipped and what is not: the [roadmap](ROADMAP.md).
