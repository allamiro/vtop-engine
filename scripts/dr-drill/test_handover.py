#!/usr/bin/env python3
"""Tests for the drills' `--handover`, `--check` and `--teardown` modes (#735).

They run the real lib.sh with a stand-in `docker` that only records what it is
asked, so no container is started: what is tested is the lifecycle (what runs
in which mode, what survives a handover, what a check and a teardown do), and
that each real drill is wired to it.
"""
import json
import os
import re
import shutil
import stat
import subprocess
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
PAGE = os.path.join(REPO, "docs", "operate", "disaster-recovery.md")
DRILLS = ["ledger-lost", "bucket-lost", "region-lost", "disk-full", "verification-failed", "key-rotation"]


def find_bash5():
    for candidate in (shutil.which("bash"), "/opt/homebrew/bin/bash", "/usr/local/bin/bash"):
        if not candidate or not os.path.exists(candidate):
            continue
        out = subprocess.run([candidate, "-c", "echo ${BASH_VERSINFO[0]}"], capture_output=True, text=True)
        if out.stdout.strip().isdigit() and int(out.stdout.strip()) >= 5:
            return candidate
    return None


BASH = find_bash5()

DOCKER_SHIM = """#!/usr/bin/env bash
# Records the call and succeeds; `inspect -f` answers as a running container.
echo "$*" >> "$DOCKER_LOG"
case "$1 $2" in
  "inspect -f") echo true ;;
esac
exit 0
"""

# A drill with two legs: lab makes a container and a marker, recover would repair it, check judges it.
MINI = r"""#!/usr/bin/env bash
# shellcheck source=scripts/dr-drill/lib.sh
source "$LIB"
dr_init mini "$@"
DR_LEGS=(one two)
DR_PAGE_SECTION="The mini disaster"
DR_HANDOVER_VARS=(LOST)
DR_HANDOVER_ENV=("MINI_SECRET=hunter 2")

lab_shared() { echo shared >> "$DR_WORKDIR/order"; }
lab() {
  echo "lab $1" >> "$DR_WORKDIR/order"
  dr_run "store-$1" 9000 --memory=1g -- img
  DR_CONT["minio-$1"]="${DR_CONT["store-$1"]}"
  DR_PORT["minio-$1"]=9000
  LOST=7
  echo broken > "$DR_WORKDIR/state-$1"
  [[ -z "${MINI_LAB_FAIL:-}" ]] || crit bad "the lab is wrong"
}
handover_text() { echo "  leg $1 is broken; $LOST records are lost"; }
recover() { echo "recover $1" >> "$DR_WORKDIR/order"; echo fixed > "$DR_WORKDIR/state-$1"; RTO_MS=500; }
check() {
  echo "check $1 rto=$RTO_MS" >> "$DR_WORKDIR/order"
  crit_eq "leg $1 is fixed" "$(cat "$DR_WORKDIR/state-$1")" fixed
  crit_rto "RTO" "$RTO_MS" 1
  dr_set lost "$LOST"
  dr_check_summary "$LOST"
}
dr_run_drill
"""


# A drill with no legs and no shared lab: the lifecycle's other shape.
MINI_PLAIN = r"""#!/usr/bin/env bash
source "$LIB"
dr_init plain "$@"
DR_PAGE_SECTION="The plain disaster"
lab() { echo "lab [$1]" >> "$DR_WORKDIR/order"; echo broken > "$DR_WORKDIR/state"; }
handover_text() { echo "  it is broken"; }
recover() { echo recover >> "$DR_WORKDIR/order"; echo fixed > "$DR_WORKDIR/state"; RTO_MS=100; }
check() { crit_eq "fixed" "$(cat "$DR_WORKDIR/state")" fixed; dr_check_summary 0; }
dr_run_drill
"""


@unittest.skipUnless(BASH, "bash 5 is needed")
class Lifecycle(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = self.tmp.name
        self.bin = os.path.join(root, "bin")
        os.makedirs(self.bin)
        self.docker_log = os.path.join(root, "docker.log")
        open(self.docker_log, "w").close()
        self.write(os.path.join(self.bin, "docker"), DOCKER_SHIM, 0o755)
        self.write(os.path.join(root, "vtopctl"), "#!/bin/sh\nexit 0\n", 0o755)
        self.drill = os.path.join(root, "mini.sh")
        self.write(self.drill, MINI, 0o755)
        self.work = os.path.join(root, "lab")
        self.env = dict(os.environ, PATH=self.bin + os.pathsep + os.environ["PATH"], DOCKER_LOG=self.docker_log,
                        LIB=os.path.join(HERE, "lib.sh"), DR_DRILL_WORKDIR=self.work, TMPDIR=root)
        for name in ("MINI_LAB_FAIL", "DR_DRILL_NETWORK", "DR_DRILL_KEEP"):
            self.env.pop(name, None)

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def write(path, text, mode):
        with open(path, "w") as f:
            f.write(text)
        os.chmod(path, mode)

    def run_drill(self, *args, **env):
        cmd = [BASH, self.drill, os.path.join(self.tmp.name, "vtopctl"), *args]
        return subprocess.run(cmd, capture_output=True, text=True, env=dict(self.env, **env), timeout=120)

    def docker_calls(self):
        with open(self.docker_log) as f:
            return f.read()

    def order(self):
        path = os.path.join(self.work, "order")
        if not os.path.exists(path):
            return []
        with open(path) as f:
            return f.read().split("\n")[:-1]

    # ---- the lab directory is this user's alone: --check runs its state file as shell
    def test_a_lab_directory_others_can_write_is_refused(self):
        os.makedirs(self.work)
        os.chmod(self.work, 0o777)
        out = self.run_drill("--handover")
        self.assertEqual(out.returncode, 2, out.stdout + out.stderr)
        self.assertIn("writable by no one else", out.stderr)
        self.assertNotIn("run", self.docker_calls())

    def test_a_state_file_others_can_write_is_not_run(self):
        out = self.run_drill("--handover")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        state = os.path.join(self.work, "handover.state")
        os.chmod(state, 0o666)
        out = self.run_drill("--check")
        self.assertEqual(out.returncode, 2, out.stdout + out.stderr)
        self.assertIn("writable by no one else", out.stderr)

    # ---- a run is unchanged: lab, recover, check, per leg, then the lab is removed
    def test_a_run_goes_through_every_leg_and_tears_the_lab_down(self):
        out = self.run_drill()
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("rm -fv", self.docker_calls())

    def test_a_run_orders_lab_recover_check_for_each_leg(self):
        out = self.run_drill(DR_DRILL_KEEP="1")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual(self.order(), ["shared", "lab one", "recover one", "check one rto=500",
                                        "lab two", "recover two", "check two rto=500"])
        self.assertNotIn("rm -fv", self.docker_calls())  # DR_DRILL_KEEP

    # ---- --handover
    def test_handover_sets_up_and_stops_before_any_recovery(self):
        out = self.run_drill("--handover")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual(self.order(), ["shared", "lab one"])  # the first leg, never recover
        self.assertNotIn("rm -fv", self.docker_calls())  # the lab stays up
        for name in ("handover.state", "lab.env", "HANDOVER.txt"):
            self.assertTrue(os.path.isfile(os.path.join(self.work, name)), name)

    def test_handover_prints_what_the_second_person_needs(self):
        text = self.run_drill("--handover", "--leg", "two").stdout
        self.assertIn("HANDOVER: mini (leg two)", text)
        self.assertIn('section "The mini disaster"', text)
        self.assertIn("leg two is broken; 7 records are lost", text)
        self.assertIn(self.work, text)
        self.assertIn(f". {self.work}/lab.env", text)
        self.assertIn("minioadmin", text)  # the store's credentials, and its endpoint
        self.assertIn("http://127.0.0.1:9000", text)
        self.assertRegex(text, r"--check --leg two")
        self.assertRegex(text, r"--teardown")
        self.assertIn(f"DR_DRILL_WORKDIR={self.work}", text)

    def test_the_labs_environment_is_written_for_sourcing(self):
        self.run_drill("--handover")
        with open(os.path.join(self.work, "lab.env")) as f:
            text = f.read()
        self.assertIn("AWS_ACCESS_KEY_ID=minioadmin", text)
        out = subprocess.run([BASH, "-c", f". {self.work}/lab.env && printf %s \"$MINI_SECRET\""],
                             capture_output=True, text=True)
        self.assertEqual(out.stdout, "hunter 2")
        self.assertEqual(stat.S_IMODE(os.stat(os.path.join(self.work, "lab.env")).st_mode), 0o600)

    def test_a_lab_that_is_not_in_the_expected_state_is_not_handed_over(self):
        out = self.run_drill("--handover", MINI_LAB_FAIL="1")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("not handed over", out.stderr)
        self.assertIn("rm -fv", self.docker_calls())  # and what it started is removed

    def test_a_handover_into_a_used_directory_is_refused(self):
        self.assertEqual(self.run_drill("--handover").returncode, 0)
        out = self.run_drill("--handover")
        self.assertEqual(out.returncode, 2)
        self.assertIn("--teardown", out.stderr)

    # ---- --check
    def test_check_runs_only_the_criteria_and_reports_rpo_and_rto(self):
        self.run_drill("--handover")
        with open(os.path.join(self.work, "state-one"), "w") as f:
            f.write("fixed\n")  # what the person did
        before = self.docker_calls()
        out = self.run_drill("--check")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertEqual(self.order()[-1].split(" rto=")[0], "check one")
        self.assertNotIn("recover", " ".join(self.order()))
        self.assertIn("RPO: 7 record(s) lost", out.stdout)
        self.assertIn("RTO:", out.stdout)
        with open(os.path.join(self.work, "result.json")) as f:
            result = json.load(f)
        self.assertEqual(result["mode"], "check")
        self.assertEqual(result["rpo_records_lost"], 7)
        self.assertGreaterEqual(result["rto_ms"], 0)
        self.assertEqual(result["pass"], True)
        self.assertEqual(self.docker_calls(), before)  # a check starts and removes nothing

    def test_check_fails_on_a_lab_that_was_not_recovered_and_can_be_run_again(self):
        self.run_drill("--handover")
        out = self.run_drill("--check")
        self.assertEqual(out.returncode, 1, out.stdout + out.stderr)
        self.assertIn("leg one is fixed", out.stderr)
        self.assertIn("still up", out.stdout)
        self.assertNotIn("rm -fv", self.docker_calls())
        with open(os.path.join(self.work, "state-one"), "w") as f:
            f.write("fixed\n")
        again = self.run_drill("--check")
        self.assertEqual(again.returncode, 0, again.stdout + again.stderr)

    def test_a_person_s_time_is_reported_against_the_bound_and_not_enforced(self):
        self.run_drill("--handover")
        with open(os.path.join(self.work, "state-one"), "w") as f:
            f.write("fixed\n")
        out = self.run_drill("--check")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("not enforced for a person", out.stdout)
        with open(os.path.join(self.work, "result.json")) as f:
            result = json.load(f)
        self.assertEqual(result["rto_bound_s"], 1)
        self.assertIn(result["rto_within_automated_bound"], (True, False))

    def test_check_without_a_handover_is_refused(self):
        out = self.run_drill("--check")
        self.assertEqual(out.returncode, 2)
        self.assertRegex(out.stderr, r"no lab was handed over|no handed-over lab")

    def test_check_for_another_leg_than_the_one_handed_over_is_refused(self):
        self.run_drill("--handover", "--leg", "two")
        out = self.run_drill("--check", "--leg", "one")
        self.assertNotEqual(out.returncode, 0)
        self.assertIn("handed over for leg two", out.stderr)
        self.assertEqual(self.run_drill("--check").returncode, 1)  # no --leg: the leg handed over (unfixed here)
        self.assertIn("check two", self.order()[-1])

    def test_a_drill_with_no_legs_and_no_shared_lab_hands_over_and_checks(self):
        plain = os.path.join(self.tmp.name, "plain.sh")
        self.write(plain, MINI_PLAIN, 0o755)
        ctl = os.path.join(self.tmp.name, "vtopctl")

        def run(*args):
            return subprocess.run([BASH, plain, ctl, *args], capture_output=True, text=True, env=self.env, timeout=60)

        out = run("--handover")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertNotIn("(leg", out.stdout)
        self.assertEqual(run("--check").returncode, 1)  # not recovered
        with open(os.path.join(self.work, "state"), "w") as f:
            f.write("fixed\n")
        self.assertEqual(run("--check").returncode, 0)
        self.assertEqual(run("--teardown").returncode, 0)
        self.assertFalse(os.path.exists(self.work))

    # ---- --teardown, and options
    def test_teardown_removes_the_lab_and_its_directory(self):
        self.run_drill("--handover")
        out = self.run_drill("--teardown")
        self.assertEqual(out.returncode, 0, out.stdout + out.stderr)
        self.assertIn("rm -fv", self.docker_calls())
        self.assertFalse(os.path.exists(self.work))

    def test_a_leg_the_drill_does_not_have_is_refused(self):
        out = self.run_drill("--handover", "--leg", "three")
        self.assertEqual(out.returncode, 2)
        self.assertIn("no leg three", out.stderr)

    def test_unknown_options_and_a_missing_vtopctl_are_refused(self):
        self.assertEqual(self.run_drill("--bogus").returncode, 2)
        out = subprocess.run([BASH, self.drill, "--handover"], capture_output=True, text=True, env=self.env)
        self.assertEqual(out.returncode, 2)
        self.assertIn("usage", out.stderr + out.stdout)


class WiredIn(unittest.TestCase):
    """Each real drill speaks the lifecycle, and the page says what it says."""

    def text(self, drill):
        with open(os.path.join(HERE, f"{drill}.sh")) as f:
            return f.read()

    def test_every_drill_defines_the_phases_and_runs_through_the_lifecycle(self):
        for drill in DRILLS:
            text = self.text(drill)
            for fn in ("lab", "recover", "check", "handover_text"):
                self.assertRegex(text, rf"(?m)^{fn}\(\) \{{", f"{drill}: {fn}")
            self.assertIn('dr_init %s "$@"' % drill, text)
            self.assertRegex(text, r"(?m)^dr_run_drill$", drill)
            self.assertIn("[--handover | --check | --teardown]", text, drill)

    def test_every_drill_names_the_section_of_the_page_it_follows(self):
        with open(PAGE) as f:
            headings = set(re.findall(r"^#{2,3} (.+)$", f.read(), re.M))
        for drill in DRILLS:
            match = re.search(r'^DR_PAGE_SECTION="([^"]+)"', self.text(drill), re.M)
            self.assertTrue(match, drill)
            self.assertIn(match.group(1), headings, f"{drill}: no heading '{match.group(1)}' on the page")

    def test_the_legs_of_a_drill_are_the_ones_its_header_documents(self):
        for drill, legs in {"ledger-lost": "file kafka kafka-rebuild", "region-lost": "replicated lagging",
                            "disk-full": "work ledger store", "verification-failed": "engine readback rest"}.items():
            self.assertIn(f"DR_LEGS=({legs})", self.text(drill), drill)
            for leg in legs.split():
                self.assertIn(leg, self.text(drill).split("# Usage:")[1].split("\n")[0], f"{drill} usage names {leg}")

    def test_the_page_documents_the_modes(self):
        with open(PAGE) as f:
            page = f.read()
        second = page.split("## The second person", 1)[1]
        for word in ("--handover", "--check", "--teardown", "--leg", "DR_DRILL_WORKDIR"):
            self.assertIn(word, second)

    def test_the_rest_leg_s_destructive_demonstration_is_not_part_of_a_check(self):
        text = self.text("verification-failed")
        body = text.split("check_rest() {", 1)[1].split("\n}\n", 1)[0]
        self.assertNotIn("rm --versions", body)
        self.assertIn("rm --versions", text.split("leg_done() {", 1)[1])

    @unittest.skipUnless(BASH, "bash 5 is needed")
    def test_a_real_drill_refuses_a_check_with_no_lab(self):
        with tempfile.TemporaryDirectory() as tmp:
            ctl = os.path.join(tmp, "vtopctl")
            with open(ctl, "w") as f:
                f.write("#!/bin/sh\n")
            os.chmod(ctl, 0o755)
            shim = os.path.join(tmp, "bin")
            os.makedirs(shim)
            with open(os.path.join(shim, "docker"), "w") as f:
                f.write(DOCKER_SHIM)
            os.chmod(os.path.join(shim, "docker"), 0o755)
            for drill in DRILLS:
                env = dict(os.environ, PATH=shim + os.pathsep + os.environ["PATH"], DOCKER_LOG=os.path.join(tmp, "d.log"),
                           DR_DRILL_WORKDIR=os.path.join(tmp, "absent-" + drill))
                out = subprocess.run([BASH, os.path.join(HERE, f"{drill}.sh"), ctl, "--check"],
                                     capture_output=True, text=True, env=env, timeout=60)
                self.assertEqual(out.returncode, 2, f"{drill}: {out.stdout}{out.stderr}")
                self.assertFalse(os.path.exists(env["DR_DRILL_WORKDIR"]), f"{drill} created the workdir")


if __name__ == "__main__":
    unittest.main()
