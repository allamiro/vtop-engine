"""Tests for scripts/check-version-claims.py.

Each case builds a small repository tree in a temporary directory with every
version claim the check reads, makes one of them stale, and runs the script
on it as CI does. The last case runs the script on this checkout.

Run: python3 -m unittest discover -s scripts/tests -p 'test_*.py'
"""

import pathlib
import subprocess
import sys
import tempfile
import textwrap
import unittest

REPO = pathlib.Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "check-version-claims.py"

CARGO = """\
[workspace]
members = ["crates/a"]

[workspace.package]
version = "{version}"
edition = "2021"

[workspace.dependencies]
vtop-core = {{ path = "crates/vtop-core", version = "{version}" }}
"""

README = """\
# VTOP Engine

```bibtex
@software{{suliman_vtop_engine,
  title   = {{{{VTOP Engine}}}},
  year    = {{2026}},
  version = {{{version}}},
  license = {{MIT}}
}}
```
"""

CITATION = """\
cff-version: 1.2.0
title: "VTOP Engine"
version: "{version}"
date-released: "2026-11-14"
"""

CHART = """\
apiVersion: v2
name: vtop
version: {chart}
# Must track the workspace version in Cargo.toml.
appVersion: "{version}"
"""

HELM_PAGE = """\
# Helm chart

| Chart field | Value |
|---|---|
| Chart name | `vtop` |
| Chart version | `{chart}` |
| `appVersion` (the default image tag) | `{version}` |

```bash
git clone --branch v{version} https://github.com/allamiro/vtop-engine.git
```
"""

CONTAINER_PAGE = """\
# Container image

```bash
docker pull ghcr.io/allamiro/vtop-engine:{version}
docker run --rm ghcr.io/allamiro/vtop-engine:{version} validate
```
"""

STATUS_PAGE = """\
# Production HA

## 1. Where things stand

This section describes `main`.
"""


def write_tree(root: pathlib.Path, version="0.9.0", chart="0.5.2", **overrides):
    files = {
        "Cargo.toml": CARGO.format(version=version),
        "README.md": README.format(version=version),
        "CITATION.cff": CITATION.format(version=version),
        "helm/vtop/Chart.yaml": CHART.format(version=version, chart=chart),
        "docs/install/helm.md": HELM_PAGE.format(version=version, chart=chart),
        "docs/install/container.md": CONTAINER_PAGE.format(version=version),
        "docs/PRODUCTION_HA.md": STATUS_PAGE,
    }
    files.update(overrides)
    for rel, text in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)


def run(root: pathlib.Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(root)],
        capture_output=True,
        text=True,
        check=False,
    )


class CheckVersionClaims(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def assert_fails_naming(self, result, *needles):
        self.assertEqual(
            result.returncode, 1,
            f"expected exit 1, got {result.returncode}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}",
        )
        for needle in needles:
            self.assertIn(needle, result.stdout + result.stderr)

    def test_a_tree_whose_claims_agree_passes(self):
        write_tree(self.root)
        result = run(self.root)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_stale_bibtex_version_fails_naming_the_readme(self):
        write_tree(self.root, **{"README.md": README.format(version="0.8.0")})
        self.assert_fails_naming(run(self.root), "README.md", "0.8.0", "0.9.0")

    def test_a_stale_citation_version_fails(self):
        write_tree(self.root, **{"CITATION.cff": CITATION.format(version="0.8.0")})
        self.assert_fails_naming(run(self.root), "CITATION.cff", "0.8.0")

    def test_a_stale_chart_app_version_fails(self):
        write_tree(self.root, **{
            "helm/vtop/Chart.yaml": CHART.format(version="0.8.0", chart="0.5.2"),
        })
        self.assert_fails_naming(run(self.root), "helm/vtop/Chart.yaml", "appVersion")

    def test_an_install_page_that_lags_the_chart_fails(self):
        write_tree(self.root, **{
            "docs/install/helm.md": HELM_PAGE.format(version="0.8.0", chart="0.5.1"),
        })
        self.assert_fails_naming(
            run(self.root), "docs/install/helm.md", "0.5.1", "0.5.2", "0.8.0",
        )

    def test_an_install_page_pulling_an_older_image_fails(self):
        write_tree(self.root, **{
            "docs/install/container.md": CONTAINER_PAGE.format(version="0.8.0"),
        })
        self.assert_fails_naming(run(self.root), "docs/install/container.md:4")

    def test_a_dated_repo_at_lead_in_fails(self):
        write_tree(self.root, **{
            "docs/PRODUCTION_HA.md": STATUS_PAGE + "\nAs of **August 2026** (repo at v0.3.0):\n",
        })
        self.assert_fails_naming(run(self.root), "docs/PRODUCTION_HA.md:7", "repo at")

    def test_a_missing_claim_is_reported_not_skipped(self):
        write_tree(self.root, **{"README.md": "# VTOP Engine\n"})
        self.assert_fails_naming(run(self.root), "README.md", "no BibTeX")

    def test_this_checkout_passes(self):
        result = run(REPO)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
