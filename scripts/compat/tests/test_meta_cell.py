"""Tests for scripts/compat/cells/meta.sh (#743): the forward `refuse`
expectation must hold a release to what the cell says a refusal is, a
non-zero exit with the message and the directory unchanged.

The published releases take this path on the directory raised to level 9
(expected.yaml), but only in the Compat workflow. The cell runs here against
stand-ins, so the path is checked without them: a copy of the cell
beside a meta-scenario.sh that only lays out a directory, and a vtop-node and
vtopctl that answer the probe on the baseline directory and refuse the raised
one as the environment tells them to.

Run with: python3 -m unittest discover -s scripts/compat/tests
"""

import os
import pathlib
import shutil
import subprocess
import tempfile
import textwrap
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
CELL = ROOT / "scripts/compat/cells/meta.sh"
MESSAGE = "is newer than this release supports"

# Writes what the cell reads from a directory: the two configs, a data file
# for the digest, and the writer's expect.txt.
SCENARIO = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    set -euo pipefail
    w="$3"
    rm -rf "$w"
    mkdir -p "$w/data" "$w/logs"
    echo "data_dir: $w/data" > "$w/meta.yaml"
    echo "endpoint: 127.0.0.1:1" > "$w/admin.yaml"
    echo "level $LEVEL" > "$w/data/meta.applied"
    printf '%s\\n' "last_applied_index 10" "feature_level 1" "node_generation 4" \\
      "node_uuid cccccccc-0000-0000-0000-000000000001" "node_addr 127.0.0.1:2" > "$w/expect.txt"
    """
)

# `vtop-node meta --config <dir>/meta.yaml`. The raised directory gets the
# refusal STANDIN_SAYS with the exit status STANDIN_EXIT; any other opens.
NODE = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    case "$3" in
      */head-raised/*)
        echo "error: feature level 9 $STANDIN_SAYS (8)"
        exit "$STANDIN_EXIT"
        ;;
    esac
    echo meta_node_ready
    exec sleep 60
    """
)

# `vtopctl --json meta <subcommand> ... --config <file>`: what the probe and
# the restart ask, answered as a voter that replayed expect.txt would.
CTL = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    case "$3" in
      status) echo '{"last_applied": {"index": 10}, "membership": {"voters": [1]}}' ;;
      node-versions) echo '{"feature_level": 1}' ;;
      register-node)
        while [[ $# -gt 0 && "$1" != --expected-generation ]]; do shift; done
        echo "Ack { generation: $(($2 + 1)) }"
        ;;
      *) exit 1 ;;
    esac
    """
)


class MetaCellRefusal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = pathlib.Path(self.tmp.name)
        # The cell finds meta-scenario.sh one directory above itself.
        (root / "cells").mkdir()
        shutil.copy(CELL, root / "cells/meta.sh")
        self.executable(root / "meta-scenario.sh", SCENARIO)
        self.bins = root / "bin"
        self.bins.mkdir()
        self.executable(self.bins / "vtop-node", NODE)
        self.executable(self.bins / "vtopctl", CTL)
        self.cell = root / "cells/meta.sh"
        self.workdir = root / "work"

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def executable(path, text):
        path.write_text(text)
        path.chmod(0o755)

    def run_cell(self, says, exit_status):
        env = dict(os.environ, STANDIN_SAYS=says, STANDIN_EXIT=str(exit_status))
        return subprocess.run(
            ["bash", str(self.cell), "v0.0.0", "forward", "refuse", str(self.bins), str(self.bins), str(self.workdir), MESSAGE],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )

    def test_a_release_that_exits_non_zero_with_the_message_refuses(self):
        run = self.run_cell(MESSAGE, 1)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertIn("ok: the release refused the raised directory", run.stderr)

    def test_a_release_that_prints_the_message_and_exits_zero_fails(self):
        run = self.run_cell(MESSAGE, 0)
        self.assertNotEqual(run.returncode, 0, run.stderr)
        self.assertIn("exited 0", run.stderr)

    def test_a_refusal_without_the_message_fails(self):
        run = self.run_cell("is corrupt", 1)
        self.assertNotEqual(run.returncode, 0, run.stderr)
        self.assertIn("does not say", run.stderr)


if __name__ == "__main__":
    unittest.main()
