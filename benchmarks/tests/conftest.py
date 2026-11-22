"""Test-wide setup for the benchmark harness tests."""
import pytest


@pytest.fixture(autouse=True)
def no_instance_metadata_lookup(monkeypatch):
    """No test asks the instance metadata service (169.254.169.254): a test must not wait on a network, and its
    answer on a CI runner is somebody else's. The host-metadata tests that exercise the lookup unset this."""
    monkeypatch.setenv("VTOP_BENCH_NO_IMDS", "1")
