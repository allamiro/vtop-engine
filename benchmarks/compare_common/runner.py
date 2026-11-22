"""The resumable cell runner every comparison shares (#744 sections 4.1 and 4.4).

A plan is run one system run at a time, in `plan.schedule` order. Each finished
run is appended to the CSV and fsynced before the next starts, so the CSV is
the progress record: `--resume` re-reads it and skips every key already there,
and never repeats a finished run. A resumed run must be in the environment it
was started in: a changed `comparable` block (another host, image, config or
commit) is refused by fingerprint.

Rule 18, tool failures are reported as measured: `execute` raising
`ToolFailed` writes a `failed` row with the tool's own error and moves on; the
harness never retries a run. `CellRefused` writes a `refused` row (the run
could not start: a smoke that failed, a configuration the tool would not
take). Anything else is a harness fault: it stops the run with no row written,
so a resume tries that run again.

`--budget-minutes` stops cleanly after the run that crosses the budget;
`--cooldown-seconds` sleeps between runs (the Mac overheats, #744 section 4.4).
"""
from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any, NamedTuple

from compare_common import plan as plans
from compare_common import schema


class ToolFailed(Exception):
    """The tool ran and did not give a usable result; the message is its own error."""


class CellRefused(Exception):
    """The run could not start (smoke failed, config refused, tool exited at start); the message says why."""


class Outcome(NamedTuple):
    done: int
    skipped: int
    remaining: int
    stopped_by_budget: bool


def finished_keys(csv_path: str, kind: schema.Kind) -> set[tuple[str, ...]]:
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        return set()
    return {schema.row_key(row, kind) for row in schema.read_rows(csv_path, kind)}


def check_resume(env_recorded: dict[str, Any], comparable_now: dict[str, Any]) -> None:
    then = schema.fingerprint(env_recorded.get("comparable") or {})
    now = schema.fingerprint(comparable_now)
    if then != now:
        changed = sorted(key for key in set(env_recorded.get("comparable") or {}) | set(comparable_now)
                         if (env_recorded.get("comparable") or {}).get(key) != comparable_now.get(key))
        raise schema.RunRefused(f"cannot resume: the environment changed (fingerprint {then} then, {now} now; "
                                f"changed: {', '.join(changed) or 'unknown'}). Start a new run.")


def base_row(env: dict[str, Any], item: plans.Planned, *, system_version: str, image_digest: str,
             config_sha256: str) -> dict[str, Any]:
    comparable = env["comparable"]
    row = {
        "run_id": env["run_id"],
        "env_fingerprint": schema.fingerprint(comparable),
        "vtop_commit": comparable["vtop_commit"],
        "host_class": comparable["host_class"],
        "system": item.system,
        "profile": item.profile,
        "cell": item.cell,
        "repetition": item.repetition,
        "system_version": system_version,
        "image_digest": image_digest,
        "config_sha256": config_sha256,
        "metric_class": item.metric_class,
    }
    row.update(item.variant)
    return row


def run_plan(run_dir: str, kind: schema.Kind, env: dict[str, Any],
             describe: Callable[[plans.Planned], dict[str, str]],
             execute: Callable[[plans.Planned], dict[str, Any]], *,
             budget_minutes: float | None = None, cooldown_seconds: float = 0.0,
             clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
             log: Callable[[str], None] = print) -> Outcome:
    """Run every planned run the CSV does not hold yet.

    `describe(item)` gives the row's `system_version`, `image_digest` and
    `config_sha256`; `execute(item)` runs it and returns its metric columns
    (and may set `note`)."""
    csv_path = os.path.join(run_dir, f"{kind.name}.csv")
    schema.ensure_header(csv_path, kind)
    done_keys = finished_keys(csv_path, kind)
    items = plans.schedule(env["plan"], kind)
    start = clock()
    done = skipped = 0
    for index, item in enumerate(items):
        key = item.key(kind.variant_columns)
        if key in done_keys:
            skipped += 1
            continue
        if budget_minutes is not None and done and (clock() - start) / 60.0 >= budget_minutes:
            remaining = sum(1 for later in items[index:] if later.key(kind.variant_columns) not in done_keys)
            log(f"budget of {budget_minutes} minutes spent: stopping before {'/'.join(key)}; "
                f"{remaining} runs remain (--resume {env['run_id']})")
            return Outcome(done, skipped, remaining, True)
        if done and cooldown_seconds > 0:
            sleep(cooldown_seconds)
        row = base_row(env, item, **describe(item))
        began = clock()
        row["started_at"] = _now()
        try:
            metrics = execute(item)
            row.update(metrics)
            row.setdefault("status", "ok")
        except ToolFailed as exc:
            row["status"] = "failed"
            row["note"] = str(exc) or "the tool failed and gave no message"
        except CellRefused as exc:
            row["status"] = "refused"
            row["note"] = str(exc) or "refused, no reason given"
        row["wall_seconds"] = f"{clock() - began:.1f}"
        schema.append_row(csv_path, kind, row)
        done_keys.add(key)
        done += 1
        log(f"{'/'.join(key)}: {row['status']}" + (f" ({row.get('note')})" if row.get("note") else ""))
    return Outcome(done, skipped, 0, False)


def _now() -> str:
    from compare_common.environment import utc_now

    return utc_now()
