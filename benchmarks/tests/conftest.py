"""Shared fixtures and test-wide setup for the benchmark tests."""
import os

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# What site_facts.py and readme_cards.py read: the README, the compose file, CI,
# the Cargo files, and the measurements under benchmarks/ and tests/. docs/ is
# left out, so a render into the tree never writes into the checkout.
LINKED = ("README.md", "docker-compose.yml", "Cargo.toml", "Cargo.lock", ".github", "benchmarks", "tests")


def _make_real(directory: str, source: str) -> None:
    """Turn a symlinked directory into a real one whose entries link to the checkout's."""
    if os.path.islink(directory):
        os.unlink(directory)
    if not os.path.isdir(directory):
        os.mkdir(directory)
        for entry in os.listdir(source) if os.path.isdir(source) else ():
            os.symlink(os.path.join(source, entry), os.path.join(directory, entry))


@pytest.fixture
def data_tree(tmp_path):
    """A copy of the repository's inputs, made of symlinks, with chosen files edited.

    data_tree({"path/in/repo.csv": lambda text: text.replace(...)}) returns the tree's root;
    each edited file is a real copy, so the checkout is never touched."""

    def make(edits=None, name="tree"):
        root = tmp_path / name
        root.mkdir()
        for entry in LINKED:
            os.symlink(os.path.join(REPO, entry), root / entry)
        for rel, change in (edits or {}).items():
            parts = rel.split("/")
            for depth in range(1, len(parts)):
                _make_real(os.path.join(root, *parts[:depth]), os.path.join(REPO, *parts[:depth]))
            target = os.path.join(root, *parts)
            source = os.path.join(REPO, *parts)
            if os.path.islink(target):
                os.unlink(target)
            if change is None:  # the file is removed from the tree
                continue
            text = None
            if os.path.isfile(source):
                with open(source, encoding="utf-8") as handle:
                    text = handle.read()
            with open(target, "w", encoding="utf-8") as handle:
                handle.write(change(text))
        return str(root)

    return make


@pytest.fixture(autouse=True)
def no_instance_metadata_lookup(monkeypatch):
    """No test asks the instance metadata service (169.254.169.254): a test must not wait on a network, and its
    answer on a CI runner is somebody else's. The host-metadata tests that exercise the lookup unset this."""
    monkeypatch.setenv("VTOP_BENCH_NO_IMDS", "1")
