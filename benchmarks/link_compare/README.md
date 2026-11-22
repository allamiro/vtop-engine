# Upload over a long, lossy and policed link: VTOP against aws-cli, rclone and s5cmd

The harness of [#747](https://github.com/allamiro/vtop-engine/issues/747) (use case (c) of
[#744](https://github.com/allamiro/vtop-engine/issues/744)). The same seeded, incompressible files, from the
same local volume, to the same lab MinIO, through the same netem middlebox (#477), by each tool in turn. The
rules of #595 (1 to 10) and #744 (11 to 21) apply; the shared code is in [`../compare_common/`](../compare_common/).

| system | profile (what "committed" means) | image |
|---|---|---|
| aws-cli 2.40.3 | `exit-code` | pinned in [`products.toml`](../compare_common/products.toml) |
| VTOP (commit under test) | `strong`: verified against the stored digest before the source moves | built from `docker/Dockerfile`, tagged by the engine tree |
| rclone 1.76.0 | `size-and-checksum`: its own post-upload check | pinned |
| s5cmd 2.3.0 | `exit-code` | pinned (last release 2024-12-16, recorded) |

None of those is trusted here: **objects committed of N is the harness's own read-back** of every object by
sha256 against the seed's manifest, never a tool's exit code.

## Where it runs

| host | what |
|---|---|
| laptop (the Mac) | tests, `--dry-run`, `--plan smoke` (unshaped `lan`), chart generation. Docker Desktop's kernel has no `ifb` (#610), so no shaped run. |
| hosted runner | [`link-compare.yml`](../../.github/workflows/link-compare.yml): `smoke` nightly and on dispatch; `hosted` on dispatch (objects committed of 48 on the policed link, the one cell where the link, not the runner's CPU, decides). |
| dedicated Linux host with `ifb` | `--plan full`: defaults, equal connections, the tuned grid, the neighbour. About 40 hours, mostly waiting on a 10 Mbit/s link, run in pieces. |

## Run it

```bash
# Once: the lab's MinIO image. run.py builds the VTOP image (vtop-compare:<engine tree>) if absent.
docker build -t vtop-minio:RELEASE.2025-09-07T16-13-09Z docker/minio

python3 benchmarks/link_compare/run.py --plan smoke                  # minutes, any host
python3 benchmarks/link_compare/run.py --plan full --dry-run         # cells, rows, projected hours

# The dedicated host (Linux, `modprobe ifb sch_netem sch_tbf sch_ingress cls_u32 act_mirred act_police`):
python3 benchmarks/link_compare/run.py --plan full --run-id link-full-1 --budget-minutes 600 --cooldown-seconds 30
python3 benchmarks/link_compare/run.py --plan full --resume link-full-1 --budget-minutes 600   # until it says 0 remaining
python3 benchmarks/link_compare/run.py --check benchmarks/link_compare/results/link-full-1    # exit 0 when complete

# The page and charts (docs/benchmarks/link-comparison.md, docs/benchmarks/link/img/), from the committed run:
python3 -m pip install --require-hashes -r benchmarks/kafka_compare/requirements-report.txt
python3 benchmarks/link_compare/report.py benchmarks/link_compare/results/link-full-1
python3 benchmarks/compare_common/regenerate.py        # what CI's drift check runs
```

Other flags: `--tools aws-cli,vtop` (a subset), `--host-class`, `--private` (a product with
`publish_ok = false`; never charted), `--rebuild` (the VTOP image), `--max-hours` (default 48),
`--keep-lab`, `--lab-prefix` (container and bucket names), `--vtop-image` (a prebuilt engine image instead of
building `docker/Dockerfile`, for a laptop smoke; recorded in `environment.json`). A run refuses to start when a tool's configuration
under `tools/` differs from HEAD: the CSV records the committed file's sha256.

## What one run does

1. **Lab** ([`compose.yml`](compose.yml)): MinIO on a store-side network, the middlebox (`netem`) DNATing
   `netem:9200` onto it with an iperf3 server, a probe container, and the four tools as one-shot services, all in
   one limit class (2 CPUs, 2 GiB, no swap). `tests/test_link_compare.py` fails when the limits differ or an image
   is not the registry's pin.
2. **Files** ([`gen.py`](gen.py)): N files of `file_mib` MiB from a seeded generator, and a sha256 manifest.
3. **Calibration before**: iperf3 alone through the link (`lib/netem.py`; on the bottleneck profiles through the
   shape's impairment-free twin, as `netem.calibrate` does).
4. **The tool**, with the profile's shape installed (`lib/netem.py`, profiles from `egress_baseline.PROFILES`):
   the middlebox's engine-facing `rx_bytes` before and after (bytes on the wire, retransmissions included), the
   container's cgroup CPU sampled every 0.5 s, and its wall time (VTOP's ends when its last batch commits,
   [`tools/vtop/run.sh`](tools/vtop/run.sh)).
5. **Calibration after**. Two calibrations more than 10% apart refuse the run, naming both (the policed profile's
   reference is the token rate and is not gated; `lan` is never gated).
6. **Read-back**, outside the timed window: every object in the tool's bucket through `mc cat | sha256sum`,
   judged against the manifest (`kind.judge_readback`).

The tool order is rotated by one per repetition (A B C D, B C D A, ...), and the CSV is written in run order.

## Cells and columns

See [`kind.py`](kind.py) for the cells, the per-cell required columns and the row rules, and
[`plans.json`](plans.json) for what each plan owes. Ranked: objects committed of N, seconds to complete,
goodput as a fraction of the link reference (end to end; VTOP's includes its own verification read-back), bytes
on the wire per payload byte, CPU-seconds per GiB, the neighbour's goodput beside the tool. Not ranked: VTOP's
upload-only goodput (`vtop_upload_goodput_fraction`, from the last data object's LastModified, VTOP rows only),
tuned results (drawn only beside the defaults, with the grid size).

Equal connections: `kind.connections_of` computes workers x parts in flight per tool, each capped by what the
workload offers (aws-cli's `max_concurrent_requests` is one pool shared by files and parts; rclone does not
split a file under `--s3-upload-cutoff`, 200 MiB by default; s5cmd splits above `--part-size`, 50 MiB by default;
VTOP's 20 MiB object has 3 parts of 8 MiB). A setting that misses the cell's target is refused.

## Known gaps and deviations

- The issue's `calibrate` cell is not a row of its own: every run carries its two bracketing calibrations.
- The policed profile's calibrations are recorded and not gated (its reference is the token rate).
- CPU is the cgroup counter's largest sample, so the last half second of a run may be missing from it.
- VTOP's objects are named by batch, not by file; the read-back matches by size and sha256, so the key does not
  matter for any tool.
- No run has been measured yet: the `defaults`, `equal-conns`, `tuned` and `neighbour` data need the dedicated
  host, and each lands in its own data PR.
