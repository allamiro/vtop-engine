"""Checks on .github/workflows/release.yml that keep the release job honest.

Two rules, each a property of the workflow text:

* No build cache is restored into the release workflow. A cache entry can be
  written by any job that runs on the default branch, and a release restores
  from that scope, so a restored cache would carry such a job's code into a
  signed release.
* No `run:` script has a value from an earlier job, a step output, a
  dispatch input or event data interpolated into it. Those go through `env:`
  and are read as shell variables, so their text is never parsed as script.

Standard library only. Run from the repository root:

    python3 -m unittest discover -s scripts/tests -p 'test_release_workflow_hardening.py' -v
"""

from __future__ import annotations

import pathlib
import re
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent.parent
RELEASE = REPO / ".github" / "workflows" / "release.yml"

# Contexts whose text is not fixed by the workflow file itself.
UNTRUSTED = re.compile(
    r"\$\{\{\s*(needs|steps|inputs|github\.event|github\.head_ref|github\.ref_name)\b"
)
RUN_KEY = re.compile(r"^(\s*)(- )?run:\s*([|>][-+]?)?\s*(.*)$")


def run_scripts(text: str):
    """Yield (line number, line) for every line of every `run:` script."""
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        match = RUN_KEY.match(lines[i])
        if not match:
            i += 1
            continue
        key_indent = len(match.group(1)) + (2 if match.group(2) else 0)
        if match.group(3):
            j = i + 1
            while j < len(lines):
                line = lines[j]
                if line.strip() and len(line) - len(line.lstrip()) <= key_indent:
                    break
                yield j + 1, line
                j += 1
            i = j
        else:
            yield i + 1, match.group(4)
            i += 1


def interpolations(text: str):
    return [
        (number, line.strip())
        for number, line in run_scripts(text)
        if UNTRUSTED.search(line)
    ]


def cache_uses(text: str):
    return [
        (number, line.strip())
        for number, line in enumerate(text.split("\n"), start=1)
        if not line.lstrip().startswith("#")
        and re.search(r"actions/cache|cache-from:|cache-to:", line)
    ]


class ReleaseWorkflowHardening(unittest.TestCase):
    def test_the_checks_can_fail(self):
        self.assertEqual(
            interpolations(
                "    steps:\n"
                "      - run: |\n"
                '          echo "${{ needs.verify.outputs.version }}"\n'
            ),
            [(3, 'echo "${{ needs.verify.outputs.version }}"')],
        )
        self.assertEqual(
            len(interpolations('      - run: echo "${{ github.event.inputs.tag }}"\n')), 1
        )
        self.assertEqual(
            interpolations(
                "      - name: ok\n"
                "        env:\n"
                "          V: ${{ needs.verify.outputs.version }}\n"
                '        run: echo "$V ${{ matrix.target }}"\n'
            ),
            [],
        )
        self.assertEqual(
            len(cache_uses("      - uses: actions/cache@abc # v6\n")), 1
        )
        self.assertEqual(
            len(cache_uses("          cache-from: type=gha,scope=x\n")), 1
        )
        self.assertEqual(cache_uses("      # actions/cache is not used here\n"), [])

    def test_no_build_cache_is_restored_into_the_release(self):
        found = cache_uses(RELEASE.read_text())
        self.assertEqual(found, [], f"release.yml restores a build cache: {found}")

    def test_no_run_script_interpolates_text_from_outside_the_file(self):
        found = interpolations(RELEASE.read_text())
        self.assertEqual(
            found,
            [],
            "release.yml interpolates untrusted-context text into a run script; "
            f"pass it through env: instead: {found}",
        )


if __name__ == "__main__":
    unittest.main()
