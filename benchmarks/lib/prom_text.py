"""The Prometheus text exposition format, read with the standard library (#510).

The harness needs a handful of the engine's own series, sampled once a second,
and nothing else a Prometheus client offers — so this is a parser for the text
format and no more, in the same spirit as the dependency-free scenario parser
in lib/scenario.py. It reads what `vtop_core::telemetry` renders through the
`prometheus` crate's TextEncoder: `# HELP` / `# TYPE` comments, then one sample
per line as `name{label="value",...} value [timestamp]`.

It is STRICT on purpose. A body it cannot read is a failed scrape, and a failed
scrape inside a measured window refuses the run (lib/engine_run.py) — so a line
this parser does not understand raises rather than being skipped. A parser that
skipped what it could not read would turn a format drift into a counter that
silently stopped moving, which reads exactly like an engine that stopped
uploading.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

# The metric types the format declares. `untyped` is the format's own word for
# "no TYPE line"; a series of an unknown type gets no counter arithmetic,
# because a delta of something that is not monotonic is not a rate.
METRIC_TYPES = ("counter", "gauge", "histogram", "summary", "untyped")

# Suffixes a histogram or summary family renders its samples under. `_bucket`,
# `_sum` and `_count` only ever increase within one process, which is what lets
# them be differenced like counters; a summary's quantile lines do not.
_HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")
_SUMMARY_MONOTONIC_SUFFIXES = ("_sum", "_count")


class PromParseError(ValueError):
    """The body is not text exposition this parser can read, with the line."""


@dataclass(frozen=True)
class Series:
    """One sample line: the rendered metric name, its labels, its value."""

    name: str
    labels: tuple[tuple[str, str], ...]
    value: float

    @property
    def key(self) -> str:
        """The series identity: name plus sorted labels, rendered canonically so
        the same series scraped twice produces the same key whatever order the
        encoder happened to write the labels in."""
        return self.name + render_labels(self.labels)


@dataclass
class Exposition:
    """One scrape: every series by key, and the declared type of each family."""

    series: dict[str, Series] = field(default_factory=dict)
    types: dict[str, str] = field(default_factory=dict)

    def kind(self, name: str) -> str:
        """How a RENDERED sample name behaves: `counter` when it only ever
        increases within a process, `gauge` when it may move either way, and
        `untyped` when the endpoint did not say.

        A histogram's `_bucket`/`_sum`/`_count` and a summary's `_sum`/`_count`
        are monotonic, so they are counters here even though their family is
        declared as something else; that is what lets a stage-duration histogram
        be bucketed per second like any other counter.
        """
        declared = self.types.get(name)
        if declared == "counter":
            return "counter"
        if declared == "gauge":
            return "gauge"
        for suffix in _HISTOGRAM_SUFFIXES:
            if name.endswith(suffix):
                family = self.types.get(name[: -len(suffix)])
                if family == "histogram":
                    return "counter"
                if family == "summary" and suffix in _SUMMARY_MONOTONIC_SUFFIXES:
                    return "counter"
        # A summary's quantile lines render under the family name itself.
        if declared == "summary":
            return "gauge"
        return "untyped"

    def total(self, name: str) -> float | None:
        """The sum of one metric across every label set, or None when the
        endpoint exposed no series of that name at all.

        None, not zero: the engine's labelled counters come into existence on
        their first increment, so an absent family means either "never happened"
        or "this binary does not have it", and those are different answers.
        """
        values = [s.value for s in self.series.values() if s.name == name]
        return sum(values) if values else None


def render_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{k}="{_escape(v)}"' for k, v in sorted(labels))
    return "{" + body + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _is_name_start(ch: str) -> bool:
    return ch.isascii() and (ch.isalpha() or ch in "_:")


def _is_name_char(ch: str) -> bool:
    return ch.isascii() and (ch.isalnum() or ch in "_:")


def _parse_value(token: str, lineno: int, line: str) -> float:
    # The format's special values are spelled exactly; Python's float() would
    # also accept "infinity" and "nan" in any case, which is looser than the
    # format and harmless, but a bare word like "abc" must still refuse.
    special = {"+Inf": math.inf, "Inf": math.inf, "-Inf": -math.inf, "NaN": math.nan}
    if token in special:
        return special[token]
    try:
        return float(token)
    except ValueError:
        raise PromParseError(f"line {lineno}: sample value {token!r} is not a number: {line!r}") from None


def _parse_labels(text: str, start: int, lineno: int, line: str) -> tuple[tuple[tuple[str, str], ...], int]:
    """Labels from the `{` at `start`; returns them and the index past `}`."""
    labels: list[tuple[str, str]] = []
    seen: set[str] = set()
    i = start + 1
    n = len(text)
    while True:
        while i < n and text[i] in " \t":
            i += 1
        if i < n and text[i] == "}":
            return tuple(labels), i + 1
        if i >= n or not _is_name_start(text[i]):
            raise PromParseError(f"line {lineno}: expected a label name: {line!r}")
        j = i
        while j < n and _is_name_char(text[j]):
            j += 1
        label = text[i:j]
        while j < n and text[j] in " \t":
            j += 1
        if j >= n or text[j] != "=":
            raise PromParseError(f"line {lineno}: label {label!r} has no '=': {line!r}")
        j += 1
        while j < n and text[j] in " \t":
            j += 1
        if j >= n or text[j] != '"':
            raise PromParseError(f"line {lineno}: label {label!r} value is not quoted: {line!r}")
        j += 1
        buf: list[str] = []
        while True:
            if j >= n:
                raise PromParseError(f"line {lineno}: unterminated label value: {line!r}")
            ch = text[j]
            if ch == "\\":
                if j + 1 >= n:
                    raise PromParseError(f"line {lineno}: dangling escape: {line!r}")
                nxt = text[j + 1]
                # The format defines exactly three escapes; anything else is
                # kept literally, which is what the reference parser does.
                buf.append({"n": "\n", "\\": "\\", '"': '"'}.get(nxt, "\\" + nxt))
                j += 2
                continue
            if ch == '"':
                j += 1
                break
            buf.append(ch)
            j += 1
        if label in seen:
            raise PromParseError(f"line {lineno}: label {label!r} repeated: {line!r}")
        seen.add(label)
        labels.append((label, "".join(buf)))
        while j < n and text[j] in " \t":
            j += 1
        if j < n and text[j] == ",":
            j += 1
        elif j < n and text[j] == "}":
            pass
        else:
            raise PromParseError(f"line {lineno}: expected ',' or '}}' after a label: {line!r}")
        i = j


def parse(text: str) -> Exposition:
    """Parse one scrape body. Raises PromParseError on anything it cannot read,
    and on a series that appears twice — two values for one series in one
    scrape is not a reading, it is an ambiguity."""
    out = Exposition()
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        if line.startswith("#"):
            parts = line[1:].split(None, 3)
            if len(parts) >= 1 and parts[0] == "TYPE":
                if len(parts) < 3:
                    raise PromParseError(f"line {lineno}: TYPE comment names no type: {raw!r}")
                family, kind = parts[1], parts[2].strip()
                if kind not in METRIC_TYPES:
                    raise PromParseError(f"line {lineno}: unknown metric type {kind!r}: {raw!r}")
                if family in out.types:
                    raise PromParseError(f"line {lineno}: family {family!r} typed twice: {raw!r}")
                out.types[family] = kind
            # HELP and free comments carry nothing a measurement needs.
            continue
        if not _is_name_start(line[0]):
            raise PromParseError(f"line {lineno}: not a metric name: {raw!r}")
        i = 0
        while i < len(line) and _is_name_char(line[i]):
            i += 1
        name = line[:i]
        labels: tuple[tuple[str, str], ...] = ()
        if i < len(line) and line[i] == "{":
            labels, i = _parse_labels(line, i, lineno, raw)
        rest = line[i:].split()
        if len(rest) not in (1, 2):
            raise PromParseError(f"line {lineno}: expected a value and an optional timestamp: {raw!r}")
        if len(rest) == 2:
            try:
                int(rest[1])
            except ValueError:
                raise PromParseError(f"line {lineno}: timestamp {rest[1]!r} is not an integer: {raw!r}") from None
        series = Series(name=name, labels=tuple(sorted(labels)), value=_parse_value(rest[0], lineno, raw))
        if series.key in out.series:
            raise PromParseError(f"line {lineno}: series {series.key} appears twice in one scrape")
        out.series[series.key] = series
    return out
