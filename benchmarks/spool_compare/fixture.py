"""A synthetic, complete spool run for the report tests (never committed as a result).

`write_run(parent)` writes `<parent>/spool-fixture/` with an environment.json
and a spool.csv that hold every row the `full` plan owes, with made-up but
well-formed numbers, so the report and drift tests can draw every chart
without Docker. VTOP is given the lowest throughput on purpose: the
scoreboard must still carry its row.
"""
from __future__ import annotations

import json
import os

from compare_common import plan as plans
from compare_common import runner, schema
from spool_compare.kind import KIND, REQUIRED

HERE = os.path.dirname(os.path.abspath(__file__))
RATE = {"fluent-bit": 180000.0, "vtop": 60000.0, "vector": 150000.0, "otel-collector": 90000.0}


def environment(run_id: str, plan_name: str = "full", host_class: str = "dedicated") -> dict:
    plan = plans.select(plans.load_plans(os.path.join(HERE, "plans.json")), plan_name)
    plan["host_class"] = host_class
    text = {system: f"# fixture config for {system}\n" for system in KIND.profiles}
    configs = {system: {profile: {"path": f"benchmarks/spool_compare/tools/{system}/{profile}",
                                  "sha256": schema.sha256_text(text[system]), "text": text[system]}
                        for profile in KIND.profiles[system]} for system in KIND.profiles}
    comparable = {"host_class": host_class, "vtop_commit": "0" * 40, "host_cpu": "fixture CPU",
                  "host_logical_cpus": "8", "host_os": "Linux 6.1", "host_disk": "fixture NVMe (SSD), ext4",
                  "docker_platform": "Docker Engine", "docker_vm_cpus": 8, "docker_vm_kernel": "6.1",
                  "tool_limits": "2 CPUs, 2g, no swap"}
    return {"schema_version": schema.SCHEMA_VERSION, "kind": KIND.name, "run_id": run_id, "run_state": "complete",
            "started_at": "2026-11-22T00:00:00+00:00", "private": False,
            "store": "MinIO (lab build, upstream archived)", "comparable": comparable, "plan": plan,
            "configs": configs, "telemetry": {}, "events": [],
            "durability": {"fluent-bit": {"durable": {"storage.type": "filesystem", "db.sync": "full"}}},
            "deviations": ["fixture: no deviation was measured"]}


def metrics(item: plans.Planned) -> dict:
    rate = RATE[item.system] * (1 + 0.01 * item.repetition)
    out = {column: "" for column in REQUIRED[item.cell]}
    records = int(item.params.get("records", 100000))
    out.update({"records_expected": records, "records_present": records, "gaps": 0, "duplicates": 0,
                "torn_objects": 0, "orphan_objects": 0, "unexpected": 0})
    if item.cell == "drain":
        source = records * 201
        out.update({"window_seconds": f"{records / rate:.1f}", "lines_per_sec": f"{rate:.1f}",
                    "mib_per_sec": f"{rate * 201 / 2**20:.2f}", "source_bytes": source, "cpu_seconds": "50.0",
                    "cpu_seconds_per_gib": f"{4e6 / rate:.2f}", "peak_rss_sampled_bytes": 150_000_000,
                    "put_per_gib": "33.0", "get_per_gib": "33.0" if item.system == "vtop" else "0.0",
                    "head_per_gib": "1.0", "list_per_gib": "0.1", "other_per_gib": "0.0", "objects": 130,
                    "object_size_p50_bytes": 4_000_000, "object_size_p99_bytes": 4_500_000})
    elif item.cell == "live":
        out.update({"offered_rate": "30000", "achieved_rate": "30000", "sustained": "true",
                    "visible_p50_ms": "31000", "visible_p99_ms": "62000"})
    elif item.cell.startswith("crash"):
        out.update({"kills": item.params.get("kills_per_run", 3),
                    "gaps": 40 if item.system == "fluent-bit" else 0,
                    "duplicates": 0 if item.system == "vtop" else 25})
        if item.cell == "crash-K4-rotation":
            out["rotations"] = 3
    elif item.cell == "down-while-rotating":
        out.update({"rotations": 2})
    return out


def write_run(parent: str, run_id: str = "spool-fixture", **env_kw) -> str:
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
        row.update(metrics(item))
        schema.append_row(path, KIND, row)
    return run_dir
