# Losing the engine's host

The archive engine runs as one process per ledger. Fleet mode, several
engines sharing a ledger, is not built ([Several engines](ha.md)). So the
supported way through the loss of the engine's host is a **standby**: a
second host, or a replacement pod, that starts the same engine on the same
ledger once the first is known to be dead. This page is that procedure, what
the engine does when the standby starts, and how it is tested.

## The topology it needs

Everything the engine must find again lives off the host:

| Piece | Where | Why |
|---|---|---|
| Ledger | PostgreSQL ([Single engine with PostgreSQL](postgresql.md)) | A SQLite ledger on the lost host is lost with it, and with it the record of what was archived. |
| Source | Kafka, or files and spools on storage the standby mounts | The standby reads on from the committed point the ledger holds. A file on the lost host's own disk is out of reach. |
| Object store | S3 or another remote store | The archive itself. |
| Configuration and secrets | The same `config.yaml`, and the same secrets, on both hosts | The standby must be the same engine: the same sources, consumer group, bucket, prefix and manifest key. |
| `engine.ledger_digest_dir` | Shared storage, if you prune the ledger | The coverage audit reads the digests of pruned rows. |

The work directory (`engine.work_dir`) stays local and is lost with the host:
it holds staged batches and multipart upload sessions, all of which the
ledger lets the standby rebuild.

## The procedure

1. **Fence the old host.** Power it off, stop its VM, or, on Kubernetes, make
   sure its pod is gone (a Deployment with one replica and the `Recreate`
   strategy does this for a rollout; for a node that is unreachable, delete
   the pod forcibly only once the node is known to be down; the example
   Deployment tolerates the unreachable and not-ready taints without a time
   limit, because the default 300-second toleration would let Kubernetes
   start a replacement while the old engine might still be running). The engine
   cannot do this for you: its single-instance lock is host-local, and an
   engine on another host cannot see it. It warns about exactly that at every
   start on PostgreSQL. Two engines reading the same sources into the same
   ledger archive ranges twice and race each other's consumer-group commits.
   On a cloud VM, stop or terminate the instance with your provider's tool
   (`aws ec2 stop-instances --instance-ids <id>`, then wait for the state
   `stopped`: the call returning is not the host being down) and, for a host
   that may still be running but unreachable, also remove its access to the
   ledger and the sources (revoke its database role or its security-group
   rule) before you go on. Do not start the standby until one of these is
   true; the engine will not check it for you.
2. **Start the standby** with the same configuration and secrets and an empty
   work directory. On the standby host, as the engine's user:
   `systemctl start vtop-engine` for the
   [example unit](https://github.com/allamiro/vtop-engine/blob/main/examples/systemd/vtop-engine.service),
   or `vtopctl run --config /etc/vtop/config.yaml` directly, or let the
   orchestrator start the replacement pod. The standby needs a `vtopctl` with
   the `postgres` feature, the state-store secret (the same PostgreSQL URL),
   the key files, and the shared `engine.ledger_digest_dir`. It starts with
   recovery, as [below](#what-the-standby-does).
3. **Wait for it to catch up.** With `VTOP_METRICS_ADDR` set on the engine
   (for example `VTOP_METRICS_ADDR=127.0.0.1:9090`, see
   [Metrics and alerts](../operate/monitoring.md)), `curl -s
   127.0.0.1:9090/metrics | grep vtop_commits_total` rises and the source lag
   falls; without it, `vtopctl status --config /etc/vtop/config.yaml` shows
   the batches by state, and you are caught up when none is left in an
   unfinished state and the committed count has stopped rising while the
   source is idle. The drill's figure is 8 to 12 seconds from the kill; a
   production figure is that plus your fencing and start time, and grows with
   the backlog.
4. **Check the archive.** `vtopctl audit-coverage --config
   /etc/vtop/config.yaml --check-storage` exits zero when the committed
   ranges tile every source with no hole and every committed object and
   manifest is present (a range the standby has not archived yet is pending,
   not a hole); a non-zero exit names the range or object.

## What the standby does

At start it runs recovery against the ledger, as after any crash:

- A batch the old engine had `VERIFIED` is committed at the source (its
  object is in the store, and the commit rule holds for it).
- A batch that had not reached `VERIFIED` is sent back for replay: its range
  is read again from the source and archived. Its staged file and any
  multipart session went with the old host; the replay does not need them.
- The source reads on from the highest committed point in the ledger, not
  from where the old engine happened to be.

What it can cost is a duplicate, never a hole: the batch the old engine was
uploading when it died can be archived twice, once by its object (if the
upload had completed) and once by the replay. The same bound as for a crash
on the same host applies ([Crash recovery](../concepts/architecture.md)).

## How it is tested

The `engine crash sweep` workflow has a `postgres-kafka-host-loss` leg
(`ENGINE_SWEEP_FAULT=host_loss` in `scripts/live-chaos/engine/run-sweep.sh`),
nightly and on dispatch. At every one of the engine's 21 failpoints it:

1. runs the engine against real Kafka, MinIO and a PostgreSQL ledger until the
   failpoint kills it with `SIGKILL` (the host dies there);
2. checks the commit rule at the instant of the kill;
3. deletes the dead engine's work directory and starts a standby with a new
   one, on the same ledger, consumer group and bucket;
4. waits until every record is committed, and checks the commit rule again;
5. runs the engine's own coverage audit, and counts every record in the
   objects the store holds: none missing, and duplicates only at the points
   where the at-least-once window is open.

Losing only the process, with the host and its work directory kept (a pod
restarted on the same node, a `systemd` restart), is the `postgres-kafka`
leg of the same sweep.

**Measured.** One laptop, a debug build in a container capped at two CPUs,
Kafka, MinIO and PostgreSQL 18 beside it at well under a CPU each: all 21
points passed, with no duplicate at any of them. From the kill to every
record committed by the standby took **8 to 12 seconds**, 50 records per
point. That is the engine's part of the recovery time: add the time your
fencing and your orchestrator take to start the standby.

## Running one engine

Two examples that keep to one engine and restart it on failure:

- [`examples/systemd/vtop-engine.service`](https://github.com/allamiro/vtop-engine/blob/main/examples/systemd/vtop-engine.service):
  `Restart=on-failure`, memory and CPU limits, an orderly stop with a
  two-minute grace period, the state-store URL from an environment file.
  Install it on one host; the standby host has it installed and disabled.
- [`examples/kubernetes/vtop-engine.yaml`](https://github.com/allamiro/vtop-engine/blob/main/examples/kubernetes/vtop-engine.yaml):
  a Deployment with `replicas: 1` and `strategy: Recreate`, liveness and
  readiness probes on `/healthz` and `/readyz`, requests and limits, and the
  work directory as an `emptyDir`, so a replacement pod is the standby above.
  Its image tag is a placeholder: the engine needs a `vtopctl` built with the
  `postgres` feature, which release images up to and including v0.10.2 do
  not have ([Container image](../install/container.md)).

Neither is deployed by CI; they are documented starting points, and the
Helm chart deploys the cluster plane, not the engine.

## Not covered

- Fencing is the operator's: nothing in the engine stops a second engine on
  another host, and the drill fences by killing the process before the
  standby starts.
- A ledger or a store lost with the host. Back up the PostgreSQL ledger
  ([Backup and recovery](../operate/backup-and-recovery.md)); the store's
  durability is the store's. What to do when they are lost anyway is in
  [Disaster recovery](../operate/disaster-recovery.md).
- A PostgreSQL failover is not drilled. A store outage is (the
  `sqlite-kafka-store-outage` leg), but not at the same time as a host loss.
