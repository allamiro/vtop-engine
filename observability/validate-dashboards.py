#!/usr/bin/env python3
"""Structural validation of the generated dashboard JSON.

Guards the failure mode that cost the most debugging time in this repo: a panel
that is *syntactically* fine, queries correctly, and still renders blank. Drift
detection cannot catch it — the JSON matches the generator, the generator is
just wrong. So assert the invariants directly.

Run standalone or via CI:

    python3 observability/validate-dashboards.py
"""

import glob
import json
import os
import sys

DASH_DIR = os.path.join(os.path.dirname(__file__), "grafana", "dashboards")


def check(dash, path):
    """Yield a message per violation in one dashboard."""
    name = os.path.basename(path)
    panels = dash.get("panels", [])
    if not panels:
        yield f"{name}: no panels at all"

    seen_ids = {}
    for p in panels:
        title = p.get("title", "<untitled>")
        where = f"{name} / {title}"

        # Grafana 13 keys panel identity by `id`. Panels sharing an id (or
        # missing one) collide in the unified-storage layer and render blank or
        # duplicated.
        pid = p.get("id")
        if pid is None:
            yield f"{where}: missing panel id"
        elif pid in seen_ids:
            yield f"{where}: duplicate panel id {pid} (also {seen_ids[pid]})"
        else:
            seen_ids[pid] = title

        if p.get("type") == "stat":
            # Without pluginVersion, Grafana runs the stat schema-migration
            # handler, which EMPTIES reduceOptions.calcs. The panel then shows
            # nothing until a human opens the editor and saves it — the exact
            # "I have to edit the panel before numbers appear" symptom.
            if not p.get("pluginVersion"):
                yield f"{where}: stat panel without pluginVersion (will render blank until edited)"
            calcs = p.get("options", {}).get("reduceOptions", {}).get("calcs")
            if not calcs:
                yield f"{where}: stat panel with empty reduceOptions.calcs"

        # A panel with no targets queries nothing and is always blank.
        if p.get("type") not in ("row", "text", "canvas") and not p.get("targets"):
            yield f"{where}: no targets"


def main():
    files = sorted(glob.glob(os.path.join(DASH_DIR, "*.json")))
    if not files:
        print(f"error: no dashboards found in {DASH_DIR}", file=sys.stderr)
        return 1

    problems = []
    for path in files:
        with open(path) as fh:
            problems.extend(check(json.load(fh), path))

    if problems:
        print("Dashboard validation FAILED:\n", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1

    print(f"Dashboard validation OK ({len(files)} dashboards)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
