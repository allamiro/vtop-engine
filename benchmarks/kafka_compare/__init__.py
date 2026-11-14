"""Head-to-head benchmarks of VTOP against Apache Kafka (#595, #597): produce and consume
through VTOP's Kafka gateway (run.py), and failover under load through VTOP's native client
(failover.py), since a Kafka client cannot follow a VTOP failover.

Everything here is stdlib-only except `report.py`, which needs matplotlib and
imports it lazily, so the CSV validation and its refusals stay testable where
matplotlib is absent (the benchmarks CI job installs pytest, PyYAML and ruff only).
"""
