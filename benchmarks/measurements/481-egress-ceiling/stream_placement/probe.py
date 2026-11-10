"""Where does each `iperf3 --json-stream` interval line put the window's opening?

The contended competitor window is placed from its interval lines: each line's
source stamp minus where that interval ends (lib/competitor.py,
Contention.close). Two capped runs of scenario 23 were refused because those
estimates spread by 0.349 s and 0.285 s. This probe runs the competitor's own
command (CompetitorSpec.argv) through the module's own source stamper inside
the lab's netshoot image, parses the output with competitor.parse_report, and
prints every line's estimate relative to the earliest one, on four links:

  none            no shaping (container to container)
  rtt100          50 ms netem delay on each container's egress: 100 ms round trip
  rtt100-10mbit   the same, with the client's egress limited to 10 Mbit/s by
                  netem behind a 125-packet limit, so a queue builds
  ... :1          the same with a second, 4-stream bulk flow from the client
                  keeping that queue full (the stand-in for VTOP)

This is NOT the lab's middlebox (ifb + tbf on a separate container): netem here
runs on the containers' own veths, so the queue sizes differ from scenario 23's.
It isolates the one question - which lines are late, and by how much.

Usage, from benchmarks/:  python measurements/481-egress-ceiling/stream_placement/probe.py
Needs docker and nicolaka/netshoot:v0.16; creates and removes its own network
and two containers (NET_ADMIN, for tc). Their names carry a per-run suffix, and
teardown removes only what this invocation created, so two probes can run at
once without one deleting the other's containers.

results.json beside this file was produced before those three guards (unique
names, setup under the cleanup, the competing flow checked alive) were added;
its contended runs show the second flow was alive (0.96 and 0.82 Mbit/s for the
measured flow against 9.57 alone on the same link).
"""
import json
import os
import secrets
import subprocess
import sys
import time

sys.path.insert(0, os.getcwd())
from lib import competitor  # noqa: E402

IMAGE = "nicolaka/netshoot:v0.16"
# Unique per invocation: fixed names plus an unconditional teardown made a
# second probe delete the first one's containers mid-measurement.
RUN = f"{os.getpid()}-{secrets.token_hex(3)}"
NET = f"vtop-placement-probe-{RUN}"
CLI, SRV = f"{NET}-cli", f"{NET}-srv"
SECONDS = 15
PLAN = [("none", False), ("rtt100", False), ("rtt100-10mbit", False),
        ("rtt100-10mbit", True), ("rtt100-10mbit", True), ("none", False)]


def sh(container, script):
    return subprocess.run(["docker", "exec", container, "sh", "-c", script],
                          capture_output=True, text=True, check=True).stdout


# What this invocation created, so teardown removes exactly that and nothing a
# concurrent probe owns - and so a setup that fails half-way still has its
# earlier steps undone.
CREATED_CONTAINERS: list[str] = []
CREATED_NETWORKS: list[str] = []


def setup():
    subprocess.run(["docker", "network", "create", NET], check=True, capture_output=True)
    CREATED_NETWORKS.append(NET)
    for name in (SRV, CLI):
        subprocess.run(["docker", "run", "-d", "--rm", "--name", name, "--network", NET,
                        "--cap-add", "NET_ADMIN", IMAGE, "sleep", "3600"],
                       check=True, capture_output=True)
        CREATED_CONTAINERS.append(name)
    for port in (5201, 5202):
        subprocess.run(["docker", "exec", "-d", SRV, "iperf3", "-s", "-p", str(port)], check=True)
    time.sleep(1)


def teardown():
    if CREATED_CONTAINERS:
        subprocess.run(["docker", "rm", "-f", *CREATED_CONTAINERS], capture_output=True)
    for net in CREATED_NETWORKS:
        subprocess.run(["docker", "network", "rm", net], capture_output=True)


def shape(link):
    for name in (CLI, SRV):
        sh(name, "tc qdisc del dev eth0 root 2>/dev/null; true")
    if link == "none":
        return
    sh(SRV, "tc qdisc add dev eth0 root netem delay 50ms limit 10000")
    if link == "rtt100":
        sh(CLI, "tc qdisc add dev eth0 root netem delay 50ms limit 10000")
    else:
        sh(CLI, "tc qdisc add dev eth0 root netem delay 50ms rate 10mbit limit 125")


def measure(link, contended):
    shape(link)
    spec = competitor.CompetitorSpec(mode="bulk", seconds=SECONDS, omit=3, server=SRV,
                                     port=5201, container=CLI)
    rival = None
    if contended:
        rival = subprocess.Popen(["docker", "exec", CLI, "iperf3", "-c", SRV, "-p", "5202",
                                  "-t", str(SECONDS + 15), "-P", "4"],
                                 stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        time.sleep(3)
        # A rival that failed to connect would leave the "contended" run
        # measuring an empty queue under a contended name.
        if rival.poll() is not None:
            raise RuntimeError(f"the competing flow exited with {rival.returncode} before the "
                               f"measurement began: {rival.stderr.read().strip()}")
    wrapped = ["sh", "-c", 'set -o pipefail; s="$1"; shift; "$@" | python3 -c "$s"',
               "competitor", competitor.SOURCE_STAMPER, *spec.argv()]
    done = subprocess.run(["docker", "exec", CLI, *wrapped], capture_output=True, text=True,
                          check=True)
    if rival is not None:
        rival.kill()
        # The measured flow has exited, so the rival is the client's only iperf3.
        subprocess.run(["docker", "exec", CLI, "pkill", "iperf3"], capture_output=True)
        time.sleep(2)
    lines = [line for line in done.stdout.splitlines() if line.strip()]
    stamps = tuple(float(line.split(" ", 1)[0]) for line in lines)
    text = "\n".join(line.split(" ", 1)[1] for line in lines)
    sample = competitor.parse_report(text, spec, "probe", stamps)
    estimates = [slot.emitted_at - slot.end for slot in sample.intervals]
    earliest = min(estimates)
    relative = [round(estimate - earliest, 4) for estimate in estimates]
    return {
        "link": link, "second_flow_keeping_the_queue_full": contended,
        "competitor_goodput_mbps": sample.mbps, "measured_intervals": len(sample.intervals),
        "spread_every_line_seconds": max(relative),
        "spread_without_the_final_line_seconds": max(relative[:-1]),
        "final_line_late_by_seconds": relative[-1],
        "per_line_estimate_minus_earliest_seconds": relative,
    }


def main():
    try:
        setup()
        record = {
            "iperf3": sh(CLI, "iperf3 --version").splitlines()[0],
            "kernel": sh(CLI, "uname -r").strip(),
            "image": IMAGE, "argv": competitor.CompetitorSpec(
                mode="bulk", seconds=SECONDS, omit=3, server=SRV, container=CLI).argv(),
            "runs": [measure(link, contended) for link, contended in PLAN],
        }
    finally:
        teardown()
    print(json.dumps(record, indent=2))


if __name__ == "__main__":
    main()
