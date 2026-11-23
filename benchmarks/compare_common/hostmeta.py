"""What machine a harness ran on, recorded in every `environment.json` (#737, the v0.11.0 AWS round).

A result from a cloud instance is only as good as the statement of which instance it was, and
`comparable.host_cpu` alone does not say (on Linux it was the platform's processor string, often just
`x86_64`). Every harness (kafka_compare's run and failover, and the archive, link and spool comparisons
through `compare_common.environment.new_environment`) records one block, `host_metadata`:

    instance_type   the EC2 instance type from the instance metadata service (IMDSv2), or `unknown`
    cpu_model       the CPU's model name: /proc/cpuinfo on Linux, sysctl on macOS
    kernel          the host's kernel, `uname -sr` style

It sits beside `comparable`, not inside it: the block is for the reader, not for deciding whether two runs
may be compared, so it is not part of the environment fingerprint. A resume therefore never refuses
because the metadata service answered differently the second time, and a run committed before this block
existed loads and regenerates exactly as it did (a reader finds no `host_metadata` and says nothing).

Only the instance type needs the network. The lookup is one link-local request pair with a short timeout,
made on Linux only, and any failure is `unknown`: a laptop, a non-EC2 cloud, or an EC2 instance with the
metadata service switched off must not stop a run. `VTOP_BENCH_NO_IMDS=1` skips the lookup (the tests set it).
"""
from __future__ import annotations

import os
import platform
import re
import subprocess
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

UNKNOWN = "unknown"
IMDS = "http://169.254.169.254/latest"
TOKEN_TTL_S = "60"
TIMEOUT_S = 1.0
# `c6i.4xlarge`, `m7g.metal`, `u-6tb1.112xlarge`, `mac2-m2.metal`: letters, digits and dashes, a dot, a size.
_INSTANCE_TYPE = re.compile(r"^[a-z0-9][a-z0-9-]*\.[a-z0-9]+$")

Opener = Callable[..., Any]


def instance_type(opener: Opener = urllib.request.urlopen, *, system: str | None = None) -> str:
    """The EC2 instance type via IMDSv2 (a PUT for a token, then a GET with it), or `unknown`."""
    if (system or platform.system()) != "Linux" or os.environ.get("VTOP_BENCH_NO_IMDS"):
        return UNKNOWN
    try:
        token_request = urllib.request.Request(f"{IMDS}/api/token", method="PUT",
                                               headers={"X-aws-ec2-metadata-token-ttl-seconds": TOKEN_TTL_S})
        with opener(token_request, timeout=TIMEOUT_S) as response:
            token = response.read().decode("ascii").strip()
        if not token:
            return UNKNOWN
        request = urllib.request.Request(f"{IMDS}/meta-data/instance-type",
                                         headers={"X-aws-ec2-metadata-token": token})
        with opener(request, timeout=TIMEOUT_S) as response:
            found = response.read().decode("ascii").strip()
    except (OSError, urllib.error.URLError, ValueError):
        return UNKNOWN
    return found if _INSTANCE_TYPE.match(found) else UNKNOWN


def cpu_model(cpuinfo: str = "/proc/cpuinfo", sysctl: Callable[[str], str] | None = None) -> str:
    """The CPU's model name; the platform's processor string, then `unknown`, when nothing names it."""
    try:
        with open(cpuinfo, encoding="utf-8") as handle:
            for line in handle:
                if line.lower().startswith(("model name", "hardware", "cpu model")):
                    named = line.split(":", 1)[1].strip()
                    if named:
                        return named
    except OSError:
        pass
    named = (sysctl or _sysctl)("machdep.cpu.brand_string")
    return named or platform.processor() or UNKNOWN


def kernel() -> str:
    return f"{platform.system()} {platform.release()}".strip() or UNKNOWN


def collect(opener: Opener = urllib.request.urlopen) -> dict[str, str]:
    return {"instance_type": instance_type(opener), "cpu_model": cpu_model(), "kernel": kernel()}


def _sysctl(name: str) -> str:
    try:
        return subprocess.run(["sysctl", "-n", name], capture_output=True, text=True, timeout=5).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ""
