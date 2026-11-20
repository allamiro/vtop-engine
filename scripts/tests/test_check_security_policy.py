"""Tests for scripts/check-security-policy.py.

Each test starts from a minimal repository that passes, removes one required
part, and asserts the check fails naming that part. A final test runs the
check on the real repository. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_check_security_policy.py' -v
"""

from __future__ import annotations

import importlib.util
import io
import pathlib
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

SCRIPT = pathlib.Path(__file__).resolve().parent.parent / "check-security-policy.py"
REPO = SCRIPT.parent.parent


def load():
    spec = importlib.util.spec_from_file_location("check_security_policy", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


POLICY = """# Security policy

See the [security model](docs/SECURITY_MODEL.md).

## Reporting a vulnerability

Use <https://github.com/allamiro/vtop-engine/security/advisories/new>.
If that page offers no form, open an issue titled `Security contact request`.

## What happens next

| Step | Target |
|---|---|
| Acknowledgement of your report | within 3 working days |

| Severity | Fix target |
|---|---|
| Critical | 30 days |
| High | 60 days |
| Medium | 90 days |
| Low | the next scheduled release |

## Supported versions

Only the latest minor release receives security fixes.

| Version | Security fixes |
|---|---|
| 0.9.x | yes |

## Coordinated disclosure

Keep the report private until a fix is released or 90 days after the report.
"""

FILES = {
    "SECURITY.md": POLICY,
    "README.md": "Report vulnerabilities as [SECURITY.md](SECURITY.md) says.\n",
    "CONTRIBUTING.md": "## Security\n\nSee [SECURITY.md](SECURITY.md).\n",
    "docs/security-policy.md": "<!-- vtop:include SECURITY.md -->\n",
    "docs/SECURITY_MODEL.md": "# Security model\n",
    "Cargo.toml": '[workspace]\nmembers = []\n\n[workspace.package]\nversion = "0.9.1"\nedition = "2021"\n',
    ".github/ISSUE_TEMPLATE/config.yml": "blank_issues_enabled: false\n",
    ".github/ISSUE_TEMPLATE/security_contact.yml": 'name: Security contact request\ntitle: "Security contact request"\nbody: []\n',
}


class CheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load()

    def problems(self, changes: dict | None = None) -> list[str]:
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            files = {**FILES, **(changes or {})}
            for name, text in files.items():
                if text is None:
                    continue
                p = root / name
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text(text, encoding="utf-8")
            return self.mod.check(root)

    def without(self, old: str, new: str = "") -> str:
        self.assertIn(old, POLICY)
        return POLICY.replace(old, new)

    def assertOneProblem(self, problems: list[str], *words: str):
        self.assertEqual(len(problems), 1, problems)
        for w in words:
            self.assertIn(w, problems[0])

    def test_the_minimal_repository_passes(self):
        self.assertEqual(self.problems(), [])

    def test_a_missing_policy_fails(self):
        problems = self.problems({"SECURITY.md": None})
        self.assertTrue(problems)
        self.assertIn("SECURITY.md", problems[0])

    def test_no_advisory_link_fails(self):
        text = self.without("<https://github.com/allamiro/vtop-engine/security/advisories/new>", "GitHub")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "advisories/new")

    def test_no_fallback_channel_fails(self):
        text = self.without("`Security contact request`", "an issue")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "Security contact request")

    def test_no_acknowledgement_time_fails(self):
        text = self.without("| Acknowledgement of your report | within 3 working days |\n")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "acknowledgement")

    def test_an_acknowledgement_without_a_number_fails(self):
        text = self.without("within 3 working days", "soon")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "acknowledgement")

    def test_each_missing_severity_row_fails(self):
        for severity in ("Critical", "High", "Medium", "Low"):
            with self.subTest(severity=severity):
                row = next(line for line in POLICY.splitlines() if line.startswith(f"| {severity} |"))
                text = self.without(row + "\n")
                self.assertOneProblem(self.problems({"SECURITY.md": text}), severity.lower(), "fix target")

    def test_a_severity_row_with_an_empty_target_fails(self):
        text = self.without("| High | 60 days |", "| High |  |")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "high")

    def test_no_supported_versions_section_fails(self):
        start = POLICY.index("## Supported versions")
        text = POLICY[:start] + POLICY[POLICY.index("## Coordinated disclosure"):]
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "Supported versions")

    def test_no_disclosure_rule_fails(self):
        text = self.without("## Coordinated disclosure\n\nKeep the report private until a fix is released or 90 days after the report.\n")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "disclosure")

    def test_a_disclosure_rule_without_a_day_count_fails(self):
        text = self.without("or 90 days after the report", "or later")
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "disclosure", "days")

    def test_a_relative_link_to_a_missing_path_fails(self):
        text = POLICY + "\nSee [support](docs/SUPPORT.md).\n"
        self.assertOneProblem(self.problems({"SECURITY.md": text}), "docs/SUPPORT.md")

    def test_external_links_and_anchors_are_not_treated_as_paths(self):
        text = POLICY + "\nSee [model](docs/SECURITY_MODEL.md#14-x) and [top](#reporting-a-vulnerability).\n"
        self.assertEqual(self.problems({"SECURITY.md": text}), [])

    def test_a_fallback_no_issue_form_can_open_fails(self):
        # Blank issues are off, so the fallback needs its own form (review of #760).
        problems = self.problems({".github/ISSUE_TEMPLATE/security_contact.yml": None})
        self.assertOneProblem(problems, "Security contact request", "blank_issues_enabled")

    def test_a_fallback_through_blank_issues_needs_no_form(self):
        problems = self.problems({
            ".github/ISSUE_TEMPLATE/config.yml": "blank_issues_enabled: true\n",
            ".github/ISSUE_TEMPLATE/security_contact.yml": None,
        })
        self.assertEqual(problems, [])

    def test_a_supported_version_behind_the_workspace_version_fails(self):
        # After a minor bump, a policy still naming the old minor is stale (review of #760).
        cargo = FILES["Cargo.toml"].replace('"0.9.1"', '"0.10.0"')
        self.assertOneProblem(self.problems({"Cargo.toml": cargo}), "0.10.x", "Supported versions")

    def test_a_patch_release_keeps_the_supported_minor(self):
        cargo = FILES["Cargo.toml"].replace('"0.9.1"', '"0.9.7"')
        self.assertEqual(self.problems({"Cargo.toml": cargo}), [])

    def test_a_readme_without_the_link_fails(self):
        self.assertOneProblem(self.problems({"README.md": "No link here.\n"}), "README.md")

    def test_a_contributing_without_the_link_fails(self):
        self.assertOneProblem(self.problems({"CONTRIBUTING.md": "## Security\n\nEmail us.\n"}), "CONTRIBUTING.md")

    def test_a_docs_page_that_does_not_include_the_policy_fails(self):
        self.assertOneProblem(self.problems({"docs/security-policy.md": "# Security policy\n"}), "docs/security-policy.md")


class RealRepositoryTests(unittest.TestCase):
    def test_the_repository_passes(self):
        mod = load()
        out = io.StringIO()
        with redirect_stdout(out):
            code = mod.main([str(REPO)])
        self.assertEqual(code, 0, out.getvalue())


if __name__ == "__main__":
    unittest.main()
