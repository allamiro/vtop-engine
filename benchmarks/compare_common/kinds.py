"""The comparison kinds, found by name: `<name>_compare.kind.KIND` (#744 section 4.1).

`environment.json`'s `kind` selects the loader and the report (#600 decision
D10). A kind is found by convention rather than listed here, so adding one is
a new `<name>_compare/` package with no edit to shared code.
"""
from __future__ import annotations

import importlib
import re
from typing import Any

_NAME = re.compile(r"^[a-z][a-z0-9]{0,31}$")


def get(name: Any) -> Any:
    from compare_common.plan import PlanRefused

    if not isinstance(name, str) or not _NAME.match(name):
        raise PlanRefused(f"kind {name!r} is not a kind name")
    try:
        module = importlib.import_module(f"{name}_compare.kind")
    except ModuleNotFoundError as exc:
        raise PlanRefused(f"no comparison kind {name!r} ({name}_compare/kind.py)") from exc
    return module.KIND
