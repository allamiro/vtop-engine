"""The store side of a run: request counts from the store's own metrics, and bucket snapshots for the checker.

Performance cells count store requests from MinIO's own Prometheus endpoint
(`minio_s3_requests_total` by `api`), read before and after the window, never
from the tap (#744 rule 17). The lab MinIO runs with
`MINIO_PROMETHEUS_AUTH_TYPE=public` so the harness can read it without a token.

`snapshot_commands` lists what the harness runs (the lab MinIO image carries
`mc`) to mirror one tool's prefix to a local directory with its listing and its
incomplete multipart uploads; `checker.snapshot_from_dir` reads that back.
"""
from __future__ import annotations

from collections.abc import Sequence

from lib import prom_text

REQUEST_METRIC = "minio_s3_requests_total"
# MinIO's `api` label values, grouped by the methods #744 counts (PUT, GET, HEAD, LIST, other).
METHOD_OF = {
    "putobject": "PUT", "putobjectpart": "PUT", "copyobject": "PUT", "copyobjectpart": "PUT",
    "newmultipartupload": "POST", "completemultipartupload": "POST", "abortmultipartupload": "DELETE",
    "getobject": "GET", "getobjectattributes": "GET", "selectobjectcontent": "GET",
    "headobject": "HEAD", "headbucket": "HEAD",
    "listobjectsv1": "LIST", "listobjectsv2": "LIST", "listobjectversions": "LIST", "listmultipartuploads": "LIST",
    "listobjectparts": "LIST", "listbuckets": "LIST",
    "deleteobject": "DELETE", "deletemultipleobjects": "DELETE",
}
METHODS = ("PUT", "GET", "HEAD", "LIST", "POST", "DELETE", "OTHER")


def request_counts(exposition_text: str) -> dict[str, float]:
    """{api: count} summed over every other label (server, bucket...)."""
    exposition = prom_text.parse(exposition_text)
    out: dict[str, float] = {}
    for series in exposition.series.values():
        if series.name != REQUEST_METRIC:
            continue
        api = dict(series.labels).get("api", "")
        out[api] = out.get(api, 0.0) + series.value
    return out


def requests_by_method(before: dict[str, float], after: dict[str, float]) -> dict[str, int]:
    """Requests made inside the window, by method. A counter that went backwards (the store restarted) refuses."""
    out = {method: 0 for method in METHODS}
    for api in sorted(set(before) | set(after)):
        delta = after.get(api, 0.0) - before.get(api, 0.0)
        if delta < 0:
            raise ValueError(f"MinIO's {REQUEST_METRIC}{{api={api!r}}} went backwards: the store restarted "
                             "inside the window")
        out[METHOD_OF.get(api.lower(), "OTHER")] += int(round(delta))
    return out


def per_gib(counts: dict[str, int], archived_bytes: int) -> dict[str, float]:
    if archived_bytes <= 0:
        raise ValueError("nothing archived: requests per GiB is undefined")
    gib = archived_bytes / 2**30
    return {method: count / gib for method, count in counts.items()}


def snapshot_commands(alias_url: str, bucket: str, prefix: str, out_dir: str) -> Sequence[str]:
    """Shell lines (run with `sh -c` in the lab MinIO image, credentials in MC_HOST_lab) that write
    `objects/`, `listing.jsonl` and `incomplete.jsonl` under `out_dir`."""
    path = f"lab/{bucket}/{prefix}".rstrip("/")
    return (
        f"mkdir -p {out_dir}/objects",
        f"mc mirror --quiet {path} {out_dir}/objects >/dev/null",
        f"mc ls --json --recursive {path}/ > {out_dir}/listing.jsonl",
        f"mc ls --json --recursive --incomplete {path}/ > {out_dir}/incomplete.jsonl || true",
    )
