# AWS round on v0.11.1: the runs no generator draws

The report is [`docs/benchmarks/aws-v0.11.1.md`](../../../docs/benchmarks/aws-v0.11.1.md). The runs that a generator
draws are committed under `benchmarks/<kind>_compare/results/`. This directory holds the rest of the round, as it
came off the hosts, unedited:

| path | what it is | why it is here and not under `results/` |
|---|---|---|
| `v0111-596-c6i4x/` | #596 throughput, latency, consume and end-to-end run (`results.csv`, `e2e_hist.csv`, `environment.json`, `configs/`, `logs/`) | stopped by SIGINT before the 1 KiB VTOP cells (`run_state` `interrupted`), so `report.py` refuses it by design |
| `crash-1000/crash-v0111-c6i4x-30/` | #599 crash run at 1,000 records/s (`crash.csv`, `environment.json`, `stuck-leader-923.json`; logs left out) | VTOP and Kafka with fsync could not hold 1,000 records/s of 1 KiB with this client, so 90 of 120 rows are `refused` and the report needs ten ok crashes per configuration. The 200 records/s run replaces it |
| `s29/` | live-chaos scenario 29 (#923) three times on the v0.11.1 release binaries: `build.txt` (checksums), `summary.txt`, `run1.log` to `run3.log` | not a benchmark kind; the 87 MB work directories stay in the tarball |
| `nocomp/` | #932 on Linux: v0.11.0 against v0.11.1, uncompressed, 3 repetitions per cell (`summary.txt`, `results.jsonl`, the driver, both releases' `SHA256SUMS`) | not a benchmark kind; engine logs and configs stay in the tarball |
| `hosts/<run>.host.env` | what each host wrote at start: instance type and id, AMI, zone, disks, kernel, Docker, the engine ref and commit, the checkout (pristine tag or harness overlay) and the harness commit | no harness records all of this |

## Tarballs

Every run was fetched as one tarball and checked against the sha256 the host wrote beside it:

| tarball | sha256 |
|---|---|
| `596.tar.gz` | `940069d01a65149df1c82fc9d170ea3b24082b88ce93f61751d865930eeefb39` |
| `597.tar.gz` | `a42fc3b8838d10396f7b879380ec92fbd8c052272acc954d576b07a5aac86bee` |
| `599.tar.gz` | `d17a60843775946abc38e1fd27bcbc456a33600e795b7a81c88e4f3b231a3f5a` |
| `599r200.tar.gz` | `bc2256ca218bd2107437047f5799d03be42b8ecf66a0d18428901828133a1bf4` |
| `746-store-kills.tar.gz` | `f5a20e46636b9b4a9a218e49963291cd3d76d9815dd03e7d36d040a8c8e13649` |
| `746-time-kills-fault.tar.gz` | `9e74e2a9923c2f598e923a34596a6fb971554d7ce52e5059fca0e0df6910b1d5` |
| `747.tar.gz` | `d7ad974c3ab0b802e43694d9591d51bf75a337d25c9543e3b09fb57f46be74de` |
| `748.tar.gz` | `d58a845d837d500e384bbc2cceabd1a41d175ab2d705334feadd7b6e88cd1ac4` |
| `nocomp.tar.gz` | `5eadd0ade6c0d64444a0ac4ceeae9f3da78b7e94ecdcd9d1938de9cdd8e1bd31` |
| `s29.tar.gz` | `def4c5ceec363fd1a2fe5bf5184f30644b0504b9d12e2e8c514a0c97b3fac533` |

The `logs/` trees of the archive (#746) and spool (#748) runs, 0.5 to 1.3 GB each, are not committed; they are in
those tarballs.
