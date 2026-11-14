# When to use it

## A good fit

- You archive telemetry from **Kafka, log files or syslog spools** into
  **S3-compatible storage** and need to know that what the source has let go
  of is really in the bucket.
- You need **evidence**: a manifest per object, a ledger of every batch, and
  tools that prove coverage and re-verify stored objects.
- **One engine** keeps up with your volume. Its throughput limits, and what is
  and is not measured, are in [Throughput research](../THROUGHPUT_RESEARCH.md).
- You can run a single process with a local SQLite file, or a PostgreSQL
  database for a ledger that outlives the host.

## Not a fit yet

| If you need | Today | Instead, for now |
|---|---|---|
| Several engines sharing the load, with failover | Not supported: one engine per ledger. Fleet mode is designed but not built. | One engine, with its ledger in PostgreSQL. See [Several engines](../deploy/ha.md). |
| A Kafka-compatible broker | The cluster plane's Kafka gateway is a prototype, measured well below Apache Kafka's throughput on the same host. | Apache Kafka, with VTOP archiving from it. See [Measured results](../benchmarks/index.md). |
| Exactly one object per batch, even across replays | A replay writes a new object key, so a replayed range can be stored twice (a duplicate, never a gap). | Deduplicate downstream by the manifest's source range. |
| Lossless UDP syslog | VTOP reads syslog from spool files a collector has written. Data dropped before the spool is outside its reach. | Have rsyslog or syslog-ng spool to disk, and point VTOP at the spool. |
| A Windows build | Not built. | Linux or macOS, or the container image. |

## Two parts, one of them optional

| Part | What it is | Do you need it? |
|---|---|---|
| **Archive engine** (`vtopctl run`) | Sources to verified objects, with a state ledger. | Yes. This is VTOP. |
| **Cluster plane** (`vtop-node`) | VTOP's own replicated log: metadata Raft, replication, promotion, a Kafka gateway. | Only if you are evaluating it. The engine does not depend on it. |

The rest of these docs follow the engine first and the cluster plane after.
