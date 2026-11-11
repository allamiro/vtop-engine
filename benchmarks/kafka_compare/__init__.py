"""Head-to-head benchmark of VTOP (through its Kafka gateway) against Apache Kafka (#595).

Everything here is stdlib-only except `report.py`, which needs matplotlib and
imports it lazily, so the CSV validation and its refusals stay testable where
matplotlib is absent (the benchmarks CI job installs pytest, PyYAML and ruff only).
"""
