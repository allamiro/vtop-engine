#!/usr/bin/env python3
"""Check a live-chaos control-plane history (#728, criterion A4 of #714).

`record_control.py` records who led: metadata nodes' Raft status, linearizable
reads of a range's lease, and each data node's own view of whether it is the
authorized leaseholder, each with the wall-clock instants it was asked for and
answered (`invoke_ms`, `complete_ms`). An observation that failed
(`"ok": false`) is evidence of nothing and is skipped. The checks:

  meta_leader   one Raft leader per term: no two metadata nodes report
                themselves Leader in one term, and no committed vote names a
                different leader for a term than another observation does
  lease_holder  one holder per lease epoch, across every lease read
  lease_order   lease reads are linearizable: a read that begins after
                another read ended never reports an older range fencing
                epoch. The epoch only ever grows, so this real-time order is
                exactly what a linearizable history of reads allows
  meta_index    the same for the applied index each read was served at: a
                linearizable metadata read never goes back in the log
  epoch_leader  one leader per epoch on the data plane: no two data nodes
                say they are the authorized leaseholder at one epoch, and the
                one that does is the holder metadata granted that epoch to
  unknown_grant a data node never acts on an epoch metadata has not granted:
                once a node has said it holds epoch E, no lease read that
                begins later reports a range epoch below E
  stale_leader  a deposed leader stops: a node never says it holds epoch E
                after a lease read that ended earlier reported a newer epoch,
                when E's lease had a deadline and the observation began after
                the latest deadline any read reported for E. A lease with no
                deadline (an operator's grant) is not held to this, and an
                operator's revocation of a live lease is not modelled
  stalled       (only with --stall-ms N) liveness, not safety: the history
                ends with N ms or more in which metadata granted the range
                and no data node acted as its leader. A bug that leaves no
                provable quorum (#640) breaks no rule above; it stops the
                cluster, and this is how a history shows it

  coverage      (only with --require-observations) the history holds no
                answered observation of one kind (Raft status, lease read,
                data node): a recorder that ran and saw nothing checked
                nothing, and must not pass as a clean history

Usage: check_control.py [--stall-ms N] [--require-observations] HISTORY.jsonl ...
Prints a JSON summary; exits 1 when any invariant is violated, 2 on bad input.
A file whose last line is cut short (a recorder killed mid-write) has that
line ignored and noted; a bad line anywhere else is bad input.
"""

from __future__ import annotations

import json
import sys
from bisect import bisect_left, bisect_right
from collections import defaultdict

LISTED = 20
OPS = ("meta_status", "lease_read", "node_lease")


def load(texts: list[tuple[str, str]]) -> tuple[list[dict], list[str]]:
    events: list[dict] = []
    notes: list[str] = []
    for name, text in texts:
        lines = text.split("\n")
        # A file that ends with "\n" splits into a trailing "".
        last = len(lines) - 1 if lines and lines[-1] == "" else len(lines)
        for number, line in enumerate(lines[:last], 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as err:
                if number == last and not text.endswith("\n"):
                    notes.append(f"{name}: the last line is cut short and was ignored")
                    continue
                raise ValueError(f"{name} line {number}: not JSON: {err}") from err
            if not isinstance(event, dict) or event.get("op") not in OPS:
                raise ValueError(f"{name} line {number}: not a control observation: {line[:120]}")
            for key in ("invoke_ms", "complete_ms", "ok"):
                if key not in event:
                    raise ValueError(f"{name} line {number}: no {key}")
            if event["complete_ms"] < event["invoke_ms"]:
                raise ValueError(f"{name} line {number}: answered before it was asked")
            events.append(event)
    return events, notes


def _add(violations, kind, item):
    violations[kind].append(item)


def _monotonic(reads: list[dict], field: str, kind: str, what: str, violations) -> None:
    """A read that begins after another ended never reports a lower `field`."""
    usable = [r for r in reads if r.get(field) is not None]
    by_complete = sorted(usable, key=lambda r: r["complete_ms"])
    completes = [r["complete_ms"] for r in by_complete]
    best: list[dict] = []
    for r in by_complete:
        best.append(r if not best or r[field] > best[-1][field] else best[-1])
    for later in usable:
        i = bisect_left(completes, later["invoke_ms"])  # reads that ended strictly before
        if i and best[i - 1][field] > later[field]:
            earlier = best[i - 1]
            _add(violations, kind, {
                "message": f"a read that began at {later['invoke_ms']} reports {what} {later[field]}, "
                           f"below the {earlier[field]} a read that ended at {earlier['complete_ms']} reported",
                "earlier": earlier, "later": later,
            })


def check(events: list[dict], stall_ms: int | None = None, require_observations: bool = False) -> dict:
    violations: dict[str, list] = defaultdict(list)
    ok = [e for e in events if e["ok"]]
    status = [e for e in ok if e["op"] == "meta_status"]
    reads = [e for e in ok if e["op"] == "lease_read" and e.get("found", True)]
    nodes = [e for e in ok if e["op"] == "node_lease"]

    # --- meta_leader: one Raft leader per term ------------------------------
    leaders: dict[int, dict] = defaultdict(dict)  # term -> leader id -> first evidence
    for e in status:
        if e.get("state") == "Leader" and e.get("term") is not None and e.get("node_id") is not None:
            leaders[e["term"]].setdefault(e["node_id"], e)
        if e.get("vote_committed") and e.get("vote_term") is not None and e.get("voted_for") is not None:
            leaders[e["vote_term"]].setdefault(e["voted_for"], e)
    for term, named in sorted(leaders.items()):
        if len(named) > 1:
            _add(violations, "meta_leader", {
                "message": f"term {term} has {len(named)} leaders: {sorted(named)}",
                "evidence": list(named.values()),
            })

    # --- lease_holder: one holder per lease epoch ---------------------------
    holders: dict[int, dict] = defaultdict(dict)
    expiry: dict[int, int] = {}
    for r in reads:
        if r.get("lease_epoch") is None or r.get("holder") is None:
            continue
        holders[r["lease_epoch"]].setdefault(r["holder"], r)
        if r.get("expires_at_ms") is not None:
            expiry[r["lease_epoch"]] = max(expiry.get(r["lease_epoch"], 0), r["expires_at_ms"])
    for epoch, named in sorted(holders.items()):
        if len(named) > 1:
            _add(violations, "lease_holder", {
                "message": f"lease epoch {epoch} was read with {len(named)} holders: {sorted(named)}",
                "evidence": list(named.values()),
            })

    # --- lease_order and meta_index: linearizable reads ---------------------
    _monotonic(reads, "range_epoch", "lease_order", "range fencing epoch", violations)
    _monotonic(reads, "applied_index", "meta_index", "applied index", violations)

    # --- the data plane's claims ---------------------------------------------
    claims = [n for n in nodes if n.get("active") and n.get("epoch") is not None
              and (n.get("held") is None or n["held"] == n["epoch"])]
    claimed: dict[int, dict] = defaultdict(dict)
    for c in claims:
        claimed[c["epoch"]].setdefault(c.get("node_uuid") or c["node"], c)
    for epoch, named in sorted(claimed.items()):
        if len(named) > 1:
            _add(violations, "epoch_leader", {
                "message": f"epoch {epoch} has {len(named)} data nodes acting as its leader: {sorted(named)}",
                "evidence": list(named.values()),
            })
        granted = holders.get(epoch, {})
        if len(granted) == 1:
            (holder,) = granted
            for node, c in named.items():
                if node != holder:
                    _add(violations, "epoch_leader", {
                        "message": f"{c['node']} ({node}) acted as the leader of epoch {epoch}, "
                                   f"which metadata granted to {holder}",
                        "claim": c, "grant": granted[holder],
                    })

    # unknown_grant: a later read never shows less than a claimed epoch.
    epoch_reads = [r for r in reads if r.get("range_epoch") is not None]
    by_invoke = sorted(epoch_reads, key=lambda r: r["invoke_ms"])
    invokes = [r["invoke_ms"] for r in by_invoke]
    suffix_min: list[dict] = [None] * len(by_invoke)  # type: ignore[list-item]
    for i in range(len(by_invoke) - 1, -1, -1):
        r = by_invoke[i]
        nxt = suffix_min[i + 1] if i + 1 < len(by_invoke) else None
        suffix_min[i] = r if nxt is None or r["range_epoch"] < nxt["range_epoch"] else nxt
    for c in claims:
        i = bisect_right(invokes, c["complete_ms"])  # reads that began strictly after
        if i < len(by_invoke) and suffix_min[i]["range_epoch"] < c["epoch"]:
            r = suffix_min[i]
            _add(violations, "unknown_grant", {
                "message": f"{c['node']} acted as the leader of epoch {c['epoch']} by {c['complete_ms']}, "
                           f"and a lease read that began later reports range epoch {r['range_epoch']}",
                "claim": c, "read": r,
            })

    # stale_leader: a deposed leader stops by its lease's deadline.
    by_complete = sorted(epoch_reads, key=lambda r: r["complete_ms"])
    completes = [r["complete_ms"] for r in by_complete]
    prefix_max: list[dict] = []
    for r in by_complete:
        prefix_max.append(r if not prefix_max or r["range_epoch"] > prefix_max[-1]["range_epoch"] else prefix_max[-1])
    for c in claims:
        deadline = expiry.get(c["epoch"])
        if deadline is None or c["invoke_ms"] <= deadline:
            continue
        i = bisect_left(completes, c["invoke_ms"])  # reads that ended strictly before
        if i and prefix_max[i - 1]["range_epoch"] > c["epoch"]:
            r = prefix_max[i - 1]
            _add(violations, "stale_leader", {
                "message": f"{c['node']} still acted as the leader of epoch {c['epoch']} at {c['invoke_ms']}, "
                           f"after its lease's deadline {deadline} and after a read reported epoch "
                           f"{r['range_epoch']} (ended {r['complete_ms']})",
                "claim": c, "read": r,
            })

    # stalled: the history's last stretch has grants and no acting leader.
    if stall_ms is not None and events:
        end = max(e["complete_ms"] for e in events)
        last_claim = max((c["complete_ms"] for c in claims), default=None)
        held = [r for r in reads if r.get("holder") is not None]
        since = last_claim if last_claim is not None else min((r["invoke_ms"] for r in held), default=end)
        granted_after = [r for r in held if r["invoke_ms"] >= since]
        if granted_after and end - since >= stall_ms:
            _add(violations, "stalled", {
                "message": f"no data node acted as the leader for the last {end - since} ms of the history "
                           f"(from {since} to {end}), while metadata granted the range "
                           f"{len(granted_after)} time(s) (epochs "
                           f"{sorted({r['lease_epoch'] for r in granted_after if r.get('lease_epoch') is not None})})",
            })

    counts: dict[str, dict] = defaultdict(lambda: {"ok": 0, "failed": 0})
    for e in events:
        counts[e["op"]]["ok" if e["ok"] else "failed"] += 1
    if require_observations:
        for op in OPS:
            if counts[op]["ok"] == 0:
                _add(violations, "coverage", {
                    "message": f"no answered {op} observation in the history "
                               f"({counts[op]['failed']} failed): the recorder checked nothing of that kind",
                })
    return {
        "valid": not violations,
        "observations": dict(counts),
        "terms_with_a_leader": len(leaders),
        "lease_epochs": sorted(holders),
        "epochs_led": {str(e): sorted(n) for e, n in sorted(claimed.items())},
        "violations": {
            kind: {"count": len(items), "first": items[:LISTED]}
            for kind, items in sorted(violations.items())
        },
    }


def main(argv: list[str]) -> int:
    paths = argv[1:]
    stall_ms = None
    require_observations = False
    while paths[:1] in (["--stall-ms"], ["--require-observations"]):
        if paths[0] == "--require-observations":
            require_observations, paths = True, paths[1:]
            continue
        if len(paths) < 2 or not paths[1].isdigit():
            print("check_control: --stall-ms takes a number of milliseconds", file=sys.stderr)
            return 2
        stall_ms, paths = int(paths[1]), paths[2:]
    if not paths:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        texts = []
        for path in paths:
            with open(path, encoding="utf-8") as handle:
                texts.append((path, handle.read()))
        events, notes = load(texts)
    except (OSError, ValueError) as err:
        print(f"check_control: {err}", file=sys.stderr)
        return 2
    result = check(events, stall_ms, require_observations)
    result["notes"] = notes
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
