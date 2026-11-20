"""Tests for scripts/check-release-versions.py.

Each test starts from a minimal repository whose copied versions all match the
workspace, changes one, and asserts the check names it. A final test runs the
check on the real repository. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_release_versions.py' -v
"""

from __future__ import annotations

import importlib.util
import pathlib
import sys
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-release-versions.py"
REPO = SCRIPT.parent.parent


def load():
    spec = importlib.util.spec_from_file_location("check_release_versions", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


MOD = load()

FILES = {
    "Cargo.toml": '[workspace]\nmembers = []\n\n[workspace.package]\nversion = "1.2.3"\nedition = "2021"\n',
    "docs/install/container.md": (
        "# Container image\n\n```bash\n"
        "docker pull ghcr.io/allamiro/vtop-engine:1.2.3   # this release, pinned\n"
        "docker pull ghcr.io/allamiro/vtop-engine:latest   # the newest release, currently 1.2.3\n"
        "```\n\n| Image | `ghcr.io/allamiro/vtop-engine` |\n\n"
        "docker run --rm -p 0.0.0.0:9090:9090 ghcr.io/allamiro/vtop-engine:1.2.3 validate\n"
    ),
    "README.md": "@software{vtop,\n  title = {VTOP},\n  version = {1.2.3},\n}\n",
    "CITATION.cff": 'cff-version: 1.2.0\nversion: "1.2.3"\n',
    "helm/vtop/Chart.yaml": 'apiVersion: v2\nversion: 0.5.3\nappVersion: "1.2.3"\n',
    "docs/install/helm.md": (
        "# Helm chart\n\n| Chart field | Value |\n|---|---|\n"
        "| Chart version | `0.5.3` |\n"
        "| `appVersion` (the default image tag) | `1.2.3` |\n\n"
        "```bash\ngit clone --branch v1.2.3 https://github.com/allamiro/vtop-engine.git\n```\n"
    ),
    "docs/PRODUCTION_HA.md": "# Production HA\n\n## 1. Where things stand\n\nThis section describes `main`.\n",
}


class CheckTests(unittest.TestCase):
    def repo(self, **changes: str) -> pathlib.Path:
        tmp = pathlib.Path(tempfile.mkdtemp())
        for rel, text in {**FILES, **changes}.items():
            path = tmp / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(text)
        return tmp

    def test_matching_versions_pass(self):
        self.assertEqual(MOD.check(self.repo()), [])

    def test_a_stale_pinned_pull_fails(self):
        page = FILES["docs/install/container.md"].replace("vtop-engine:1.2.3   #", "vtop-engine:1.1.0   #")
        problems = MOD.check(self.repo(**{"docs/install/container.md": page}))
        self.assertEqual(len(problems), 1)
        self.assertIn("vtop-engine:1.1.0", problems[0])

    def test_a_page_with_only_latest_fails(self):
        page = "docker pull ghcr.io/allamiro/vtop-engine:latest\n"
        problems = MOD.check(self.repo(**{"docs/install/container.md": page}))
        self.assertTrue(any("no pinned" in p for p in problems))

    def test_a_stale_currently_note_fails(self):
        page = FILES["docs/install/container.md"].replace("currently 1.2.3", "currently 1.1.0")
        problems = MOD.check(self.repo(**{"docs/install/container.md": page}))
        self.assertEqual(len(problems), 1)
        self.assertIn("currently 1.1.0", problems[0])

    def test_a_stale_bibtex_version_fails(self):
        problems = MOD.check(self.repo(**{"README.md": "  version = {1.1.0},\n"}))
        self.assertEqual(problems, ["README.md: the BibTeX version is 1.1.0, but the workspace version is 1.2.3"])

    def test_a_stale_citation_version_fails(self):
        problems = MOD.check(self.repo(**{"CITATION.cff": 'version: "1.1.0"\n'}))
        self.assertEqual(len(problems), 1)
        self.assertIn("CITATION.cff", problems[0])

    def test_a_stale_chart_app_version_fails(self):
        chart = FILES["helm/vtop/Chart.yaml"].replace('appVersion: "1.2.3"', 'appVersion: "1.1.0"')
        problems = MOD.check(self.repo(**{"helm/vtop/Chart.yaml": chart}))
        self.assertEqual(len(problems), 1)
        self.assertIn("appVersion is 1.1.0", problems[0])

    def test_the_chart_version_is_not_the_app_version(self):
        # The chart's own `version` follows its contract, not the workspace.
        self.assertEqual(MOD.check(self.repo()), [])


    def test_a_helm_page_that_lags_the_chart_fails(self):
        page = (
            FILES["docs/install/helm.md"]
            .replace("`0.5.3`", "`0.5.2`")
            .replace("`1.2.3`", "`1.1.0`")
            .replace("--branch v1.2.3", "--branch v1.1.0")
        )
        problems = MOD.check(self.repo(**{"docs/install/helm.md": page}))
        self.assertEqual(len(problems), 3, problems)
        self.assertIn("docs/install/helm.md:5: the chart version row is 0.5.2, expected 0.5.3", problems)
        self.assertIn("docs/install/helm.md:6: the appVersion row is 1.1.0, expected 1.2.3", problems)
        self.assertIn("docs/install/helm.md:9: the --branch checkout is 1.1.0, expected 1.2.3", problems)

    def test_a_helm_page_without_its_rows_is_reported_not_skipped(self):
        problems = MOD.check(self.repo(**{"docs/install/helm.md": "# Helm chart\n"}))
        self.assertEqual(len(problems), 3, problems)
        self.assertTrue(all("not found" in p for p in problems), problems)

    def test_a_stale_image_on_another_install_page_fails(self):
        page = "docker run ghcr.io/allamiro/vtop-engine:1.1.0 validate\n"
        problems = MOD.check(self.repo(**{"docs/install/kafka.md": page}))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("docs/install/kafka.md:1", problems[0])

    def test_a_dated_repo_at_lead_in_fails(self):
        page = FILES["docs/PRODUCTION_HA.md"] + "\nAs of **August 2026** (repo at v0.3.0):\n"
        problems = MOD.check(self.repo(**{"docs/PRODUCTION_HA.md": page}))
        self.assertEqual(len(problems), 1, problems)
        self.assertIn("docs/PRODUCTION_HA.md:7: dated status lead-in", problems[0])


class RealRepositoryTests(unittest.TestCase):
    def test_the_committed_repository_passes(self):
        self.assertEqual(MOD.check(REPO), [])


if __name__ == "__main__":
    unittest.main()
