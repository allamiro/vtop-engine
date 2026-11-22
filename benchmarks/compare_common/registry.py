"""The product registry (#744 section 2.2): who may be run, who may be charted, and with what pins.

`products.toml` is the one place a product's licence decision, evidence and pins
live. Everything that runs or draws a product asks this module first:

- `require_runnable(product, private)` refuses a product whose terms bar a run
  even in private (`private_ok = false`), and one that may not be published
  (`publish_ok = false`) unless the run is `--private`;
- `require_chartable(product)` refuses a `publish_ok = false` product in any
  chart, private run or not;
- `check(registry)` refuses an image without a digest, a `latest` tag, a
  clause whose sha256 is not the one recorded, and a pin made more than 90
  days after its licence was last read.

The harness supports Python 3.9, which has no `tomllib`, so the file is read by
`tomllib` where it exists and otherwise by `parse_subset`, a reader for the
small part of TOML the file uses (tables, strings, booleans, integers and
one-line string arrays). A test reads the committed file both ways.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
from typing import Any

HERE = os.path.dirname(os.path.abspath(__file__))
PRODUCTS = os.path.join(HERE, "products.toml")

FAMILIES = ("archive", "link", "spool")
SLOT_NAMES = ("blue", "orange", "aqua", "yellow")
MAX_LICENCE_AGE_DAYS = 90
_REQUIRED = ("name", "role", "licence", "publish_ok", "private_ok", "evidence_url", "retrieved", "clause",
             "clause_sha256", "version", "pinned", "use_cases")
_DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class RegistryError(Exception):
    """The registry, or a use of it, is refused; the message says why."""


# ---------------------------------------------------------------------- parsing


def _value(text: str, where: str) -> Any:
    text = text.strip()
    if text.startswith('"'):
        if not text.endswith('"') or len(text) < 2:
            raise RegistryError(f"{where}: unterminated string {text!r}")
        body = text[1:-1]
        if '"' in body.replace('\\"', ""):
            raise RegistryError(f"{where}: more than one string on the line: {text!r}")
        return body.replace('\\"', '"').replace("\\\\", "\\")
    if text in ("true", "false"):
        return text == "true"
    if re.fullmatch(r"-?[0-9]+", text):
        return int(text)
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return []
        return [_value(item, where) for item in inner.split(",") if item.strip()]
    raise RegistryError(f"{where}: unsupported value {text!r}")


def _strip_comment(line: str) -> str:
    out, quoted = [], False
    for char in line:
        if char == '"':
            quoted = not quoted
        if char == "#" and not quoted:
            break
        out.append(char)
    return "".join(out).strip()


def parse_subset(text: str) -> dict[str, Any]:
    """The subset of TOML products.toml uses. Anything else is refused, never guessed."""
    root: dict[str, Any] = {}
    table = root
    for number, raw in enumerate(text.splitlines(), 1):
        line = _strip_comment(raw)
        if not line:
            continue
        where = f"line {number}"
        if line.startswith("["):
            if not line.endswith("]") or line.startswith("[["):
                raise RegistryError(f"{where}: unsupported table header {line!r}")
            table = root
            for part in line[1:-1].strip().split("."):
                if not re.fullmatch(r"[A-Za-z0-9_-]+", part):
                    raise RegistryError(f"{where}: unsupported table name {part!r}")
                table = table.setdefault(part, {})
            continue
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z0-9_-]+", key):
            raise RegistryError(f"{where}: not a `key = value` line: {line!r}")
        if key in table:
            raise RegistryError(f"{where}: duplicate key {key!r}")
        table[key] = _value(value, where)
    return root


def load(path: str = PRODUCTS) -> dict[str, Any]:
    with open(path, "rb") as handle:
        data = handle.read()
    try:
        import tomllib  # type: ignore[import-not-found]
    except ImportError:  # Python < 3.11
        return parse_subset(data.decode("utf-8"))
    return tomllib.loads(data.decode("utf-8"))


# --------------------------------------------------------------------- checking


def _date(value: str, where: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise RegistryError(f"{where}: {value!r} is not a YYYY-MM-DD date") from exc


def check(registry: dict[str, Any]) -> None:
    """Refuse a registry whose pins or evidence a run could not stand on."""
    problems = []
    products = registry.get("products") or {}
    if not products:
        raise RegistryError("the registry lists no products")
    for pid, entry in sorted(products.items()):
        missing = [key for key in _REQUIRED if key not in entry]
        if missing:
            problems.append(f"{pid}: missing {', '.join(missing)}")
            continue
        image = entry.get("image")
        if image is not None:
            if not _DIGEST.search(image):
                problems.append(f"{pid}: image {image!r} has no @sha256 digest")
            tag = image.split("@")[0].rsplit("/", 1)[-1].partition(":")[2]
            if not tag or tag == "latest":
                problems.append(f"{pid}: image {image!r} has tag {tag or '(none)'!r}; name the version")
        if "archive_url" in entry and not _SHA256.match(str(entry.get("archive_sha256", ""))):
            problems.append(f"{pid}: archive {entry['archive_url']} has no sha256")
        if image is None and "built_from" not in entry and entry["private_ok"]:
            problems.append(f"{pid}: neither an image by digest nor `built_from`")
        if hashlib.sha256(entry["clause"].encode()).hexdigest() != entry["clause_sha256"]:
            problems.append(f"{pid}: clause_sha256 is not the sha256 of the quoted clause")
        try:
            age = (_date(entry["pinned"], f"{pid}.pinned") - _date(entry["retrieved"], f"{pid}.retrieved")).days
        except RegistryError as exc:
            problems.append(str(exc))
            continue
        if age > MAX_LICENCE_AGE_DAYS:
            problems.append(f"{pid}: pinned {entry['pinned']}, {age} days after its licence was read on "
                            f"{entry['retrieved']}; read it again (at most {MAX_LICENCE_AGE_DAYS} days)")
        if age < 0:
            problems.append(f"{pid}: pinned {entry['pinned']} before its licence was read on {entry['retrieved']}")
        if entry["publish_ok"] and not entry["private_ok"]:
            problems.append(f"{pid}: publish_ok without private_ok")
    families = registry.get("families") or {}
    for family in FAMILIES:
        order = (families.get(family) or {}).get("order")
        if not order:
            problems.append(f"family {family}: no order")
            continue
        if len(order) > len(SLOT_NAMES):
            problems.append(f"family {family}: {len(order)} systems, at most {len(SLOT_NAMES)} slots")
        if len(order) < 2 or order[1] != "vtop":
            problems.append(f"family {family}: VTOP must hold slot 2 (orange), order is {order}")
        unknown = [pid for pid in order if pid not in products]
        if unknown:
            problems.append(f"family {family}: unknown products {unknown}")
    if problems:
        raise RegistryError("; ".join(problems))


def product(registry: dict[str, Any], pid: str) -> dict[str, Any]:
    try:
        return registry["products"][pid]
    except KeyError:
        raise RegistryError(f"{pid!r} is not in products.toml") from None


def family_order(registry: dict[str, Any], family: str) -> list[str]:
    try:
        return list(registry["families"][family]["order"])
    except KeyError:
        raise RegistryError(f"no family {family!r} in products.toml") from None


def require_runnable(registry: dict[str, Any], pid: str, *, private: bool, use_case: str | None = None) -> None:
    """Refuse a run the product's terms or registry entry do not allow."""
    entry = product(registry, pid)
    if not entry["private_ok"]:
        raise RegistryError(f"{pid}: its terms bar running it at all, private or not "
                            f"({entry['evidence_url']}, read {entry['retrieved']})")
    if not entry["publish_ok"] and not private:
        raise RegistryError(f"{pid}: publish_ok is false ({entry['licence']}); it runs only with --private, "
                            "and its output is never charted")
    if use_case is not None and use_case not in entry["use_cases"]:
        raise RegistryError(f"{pid}: not registered for use case ({use_case})")


def require_chartable(registry: dict[str, Any], pid: str) -> None:
    entry = product(registry, pid)
    if not entry["publish_ok"]:
        raise RegistryError(f"{pid}: publish_ok is false; a chart never draws it")
