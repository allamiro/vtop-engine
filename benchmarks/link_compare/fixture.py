"""A synthetic, complete link run for the report tests (never committed as a result).

`write_run(parent)` writes `<parent>/link-fixture/` with an environment.json and
a link.csv holding every row the `full` plan owes, with made-up but well-formed
numbers, so the report and drift tests draw every chart without a lab. VTOP is
given the lowest goodput on the clean link on purpose: the scoreboard must
still carry its row.
"""
from __future__ import annotations

import json
import os

from compare_common import plan as plans
from compare_common import runner, schema
from link_compare import kind as link
from link_compare.kind import KIND, REQUIRED

HERE = os.path.dirname(os.path.abspath(__file__))
GOODPUT = {"aws-cli": 0.80, "vtop": 0.70, "rclone": 0.85, "s5cmd": 0.90}
COMMITTED_POLICED = {"aws-cli": 30, "vtop": 48, "rclone": 41, "s5cmd": 12}


def environment(run_id: str, plan_name: str = "full", host_class: str = "dedicated") -> dict:
    plan = plans.select(plans.load_plans(os.path.join(HERE, "plans.json")), plan_name)
    plan["host_class"] = host_class
    text = {system: f"# fixture config for {system}\n" for system in KIND.profiles}
    configs = {system: {profile: {"path": f"benchmarks/link_compare/tools/{system}",
                                  "sha256": schema.sha256_text(text[system]), "text": text[system]}
                        for profile in KIND.profiles[system]} for system in KIND.profiles}
    comparable = {"host_class": host_class, "vtop_commit": "0" * 40, "host_cpu": "fixture CPU",
                  "host_logical_cpus": "8", "host_os": "Linux 6.1", "host_disk": "fixture NVMe (SSD), ext4",
                  "docker_platform": "Docker Engine", "docker_vm_cpus": 8, "docker_vm_kernel": "6.1",
                  "tool_limits": "2 CPUs, 2g, no swap"}
    return {"schema_version": schema.SCHEMA_VERSION, "kind": KIND.name, "run_id": run_id, "run_state": "complete",
            "started_at": "2026-11-22T00:00:00+00:00", "private": False,
            "store": "MinIO (lab build, upstream archived)", "comparable": comparable, "plan": plan,
            "configs": configs, "telemetry": {}, "events": [], "deviations": []}


def metrics(item: plans.Planned, plan: dict) -> dict:
    files = int(item.params.get("files", 48))
    size = int(float(item.params.get("file_mib", 20)) * 1024 * 1024)
    profile = item.variant["link_profile"]
    committed = COMMITTED_POLICED[item.system] if profile == link.POLICED and item.cell == "defaults" else files
    fraction = GOODPUT[item.system] * (0.6 if profile == "wan100-loss1" else 1.0) * (1 + 0.01 * item.repetition)
    reference = 10.0 if profile != "lan" else 900.0
    payload = committed * size
    seconds = payload * 8 / 1e6 / (fraction * reference)
    out = {column: "" for column in REQUIRED[item.cell]}
    out.update({"files_expected": files, "objects_committed": committed, "objects_mismatched": 0, "objects_extra": 0,
                "payload_bytes": payload, "seconds_to_complete": f"{seconds:.2f}", "upload_seconds": f"{seconds:.2f}",
                "link_reference_mbps": reference, "calib_before_mbps": reference, "calib_after_mbps": reference,
                "goodput_mbps": f"{fraction * reference:.3f}", "goodput_fraction": f"{fraction:.4f}",
                "wire_bytes": int(payload * 1.05), "wire_bytes_per_payload_byte": "1.0500", "cpu_seconds": "12.0",
                "cpu_seconds_per_gib": "12.80", "cpu_share": "0.0500", "bottleneck": "link", "exit_code": 0,
                "settings": "{}"})
    if item.system == "vtop" and item.cell in ("defaults", "equal-conns", "tuned"):
        out["vtop_upload_goodput_fraction"] = f"{fraction * 1.1:.4f}"
    if item.cell == "equal-conns":
        target = int(item.variant["connections"])
        settings = link.equal_connections(item.system, target)
        out.update({"settings": json.dumps(settings, sort_keys=True),
                    "connections_actual": link.connections_of(item.system, settings, size, files)})
    if item.cell in ("tuned-grid", "tuned"):
        out["grid_size"] = link.check_grids(plan["grids"], plan["systems"])
    if item.cell == "neighbour":
        out.update({"neighbour_alone_before_mbps": "9.4", "neighbour_with_mbps": "3.1",
                    "neighbour_alone_after_mbps": "9.3", "tool_with_mbps": "6.2",
                    "jain_index": link.jain(3.1, 6.2)})
    return out


def write_run(parent: str, run_id: str = "link-fixture", **env_kw) -> str:
    env = environment(run_id, **env_kw)
    run_dir = os.path.join(parent, run_id)
    os.makedirs(run_dir)
    with open(os.path.join(run_dir, "environment.json"), "w", encoding="utf-8") as handle:
        json.dump(env, handle, indent=2, sort_keys=True)
    path = os.path.join(run_dir, f"{KIND.name}.csv")
    for item in plans.schedule(env["plan"], KIND):
        row = runner.base_row(env, item, system_version="fixture", image_digest="sha256:" + "0" * 64,
                              config_sha256=env["configs"][item.system][item.profile]["sha256"])
        row.update({"status": "ok", "note": "", "started_at": env["started_at"], "wall_seconds": "60.0"})
        row.update(metrics(item, env["plan"]))
        schema.append_row(path, KIND, row)
    return run_dir
