"""One published port, and three lookups that have to agree about it (#484).

`VTOP_H3_PORT` moves the host side of the h3 proxy's publish, because 9443 is
an unremarkable choice for somebody else's TLS service and a collision leaves
the profile unable to start. Compose reads that variable the way compose reads
every variable: the invoking SHELL first, then the `benchmarks/.env` it
auto-loads for a `-f` file in that directory, then the literal
`${VTOP_H3_PORT:-9443}` default.

Three other things have to arrive at the same number without asking compose.
`lib/engine.py` decides which endpoint the run dials, whether that endpoint is
the lab's, and how it is translated for a containerized sender; `verify-h3.sh`
decides which port it probes, which Alt-Svc value it demands and which
authority it expects the proxy to forward; `gen-h3-certs.sh` predicts an
identity from the same file. Each of them read only the process environment,
so an override FILED in `.env` — the durable way to state it, and the only way
that survives a new terminal — moved the proxy and left the harness dialling
9443 and the check reporting a lab that was not broken (review).

So the resolution lives in one place per language: `lib/dotenv.sh` for the
shell scripts and `engine._compose_value` for the harness. This module pins the
order in both, pins that `verify-h3.sh` goes through the shared one rather than
growing a fourth, and pins that the two languages answer the same question the
same way.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib import engine  # noqa: E402

BENCH_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOTENV_SH = os.path.join(BENCH_DIR, "lib", "dotenv.sh")
VERIFY_SCRIPT = os.path.join(BENCH_DIR, "verify-h3.sh")

# The compose file's own fallback for the publish, `${VTOP_H3_PORT:-9443}`.
CONTAINER_PORT = 9443
# An arbitrary second port, standing for "the operator moved it".
MOVED_PORT = 9543

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None,
    reason="half the claim under test is two bash scripts")


def resolved_by_the_shell(env_file, key="VTOP_H3_PORT",
                          default=CONTAINER_PORT, **exported):
    """What `lib/dotenv.sh` says compose will resolve, run as real bash."""
    env = {k: v for k, v in os.environ.items() if k != key}
    env.update(exported)
    proc = subprocess.run(
        ["bash", "-c",
         f'source "{DOTENV_SH}"; compose_value "{env_file}" {key} {default}'],
        capture_output=True, text=True, env=env, timeout=30)
    assert proc.returncode == 0, (
        f"the shared lookup itself failed ({proc.returncode}): {proc.stderr.strip()}. "
        "Every port assertion in verify-h3.sh is made against its answer")
    return proc.stdout


@pytest.fixture
def env_file(tmp_path):
    """An `.env` of this test's own, never the operator's."""
    return tmp_path / ".env"


@pytest.fixture(autouse=True)
def no_inherited_port(monkeypatch, tmp_path):
    """Neither language may answer from the shell or file that runs the suite.

    The harness reads the repository's real `benchmarks/.env` in anger, which
    is the entire point of the fix — so a case that did not say otherwise would
    be answered by whatever the developer filed there.
    """
    monkeypatch.delenv("VTOP_H3_PORT", raising=False)
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / "absent.env"))


# --------------------------------------------------------------------------
# The shell: lib/dotenv.sh, shared by gen-h3-certs.sh and verify-h3.sh
# --------------------------------------------------------------------------


def test_the_shell_reads_a_port_filed_in_the_env_file(env_file):
    env_file.write_text(f"VTOP_H3_PORT={MOVED_PORT}\n", encoding="utf-8")
    assert resolved_by_the_shell(env_file) == str(MOVED_PORT), (
        "compose auto-loads this file and publishes the proxy on what it says. A check "
        f"that answered {CONTAINER_PORT} here probes a port with no listener and reports "
        "the lab broken — no QUIC, no Alt-Svc, no log line — when the only thing wrong is "
        "where it looked"
    )


def test_an_exported_port_outranks_the_file_as_it_does_for_compose(env_file):
    env_file.write_text(f"VTOP_H3_PORT={MOVED_PORT}\n", encoding="utf-8")
    assert resolved_by_the_shell(env_file, VTOP_H3_PORT="9643") == "9643", (
        "compose resolves the shell before the file, so a one-off export is what the "
        "proxy was published on; preferring the file here would move the check off the "
        "running proxy in the other direction"
    )


def test_a_blank_export_masks_the_file_and_falls_back_to_the_default(env_file):
    env_file.write_text(f"VTOP_H3_PORT={MOVED_PORT}\n", encoding="utf-8")
    assert resolved_by_the_shell(env_file, VTOP_H3_PORT="") == str(CONTAINER_PORT), (
        "a variable exported blank is PRESENT for compose: it masks the file and leaves "
        f"${{VTOP_H3_PORT:-{CONTAINER_PORT}}} on its default. Reading the file in that "
        "case describes a publish that did not happen"
    )


def test_the_default_stands_when_nothing_names_a_port(env_file):
    env_file.write_text("MINIO_ROOT_USER=someone\n", encoding="utf-8")
    assert resolved_by_the_shell(env_file) == str(CONTAINER_PORT), (
        "the unconfigured lab is the common case, and it must resolve to the same "
        "default the compose file spells"
    )
    absent = env_file.parent / "nothing.env"
    assert resolved_by_the_shell(absent) == str(CONTAINER_PORT), (
        "a missing .env is the state of a fresh checkout, not an error: the lookup must "
        "answer the default rather than fail the script that called it"
    )


# --------------------------------------------------------------------------
# verify-h3.sh must ask the shared lookup, not the shell alone
# --------------------------------------------------------------------------


def test_verify_h3_resolves_the_published_port_the_way_compose_did():
    with open(VERIFY_SCRIPT, encoding="utf-8") as fh:
        lines = fh.read().splitlines()
    assert any("lib/dotenv.sh" in line and "source" in line for line in lines), (
        "verify-h3.sh must source lib/dotenv.sh; it is the one place that knows compose's "
        "order of precedence, and a second copy in this script is how the two drifted apart "
        "in the first place"
    )
    assignments = [line for line in lines if line.startswith("H3_PORT=")]
    assert len(assignments) == 1, (
        f"expected exactly one place in verify-h3.sh to settle the published port, found "
        f"{assignments!r}. Every assertion in that script — the probe, the TCP control, the "
        "advertised port, the forwarded authority — is made against that one number"
    )
    assert "compose_value" in assignments[0] and ".env" in assignments[0], (
        f"{assignments[0].strip()!r} does not go through the shared lookup against the .env "
        "compose auto-loads, so a filed override is invisible to it and the check probes a "
        "port nothing is published on"
    )
    bare = [line for line in lines
            if "${VTOP_H3_PORT" in line and not line.lstrip().startswith("#")]
    assert not bare, (
        f"{bare!r} reads the override straight out of the shell. That expansion is exactly "
        "what compose does NOT do on its own — it consults .env first — and it is what made "
        "a filed override invisible here"
    )


# --------------------------------------------------------------------------
# ... and the harness must answer the same question the same way
# --------------------------------------------------------------------------


def test_the_harness_reads_a_port_filed_in_the_env_file(monkeypatch, env_file):
    env_file.write_text(f"VTOP_H3_PORT={MOVED_PORT}\n", encoding="utf-8")
    monkeypatch.setattr(engine, "_ENV_FILE", str(env_file))
    assert engine.h3_proxy_host_port() == MOVED_PORT, (
        "the run must dial where compose published the proxy. Reading only the process "
        f"environment leaves the scenario aiming at {CONTAINER_PORT} — where the "
        "validation also insists it aims — so the proxied baseline cannot run at all while "
        "the profile is up and healthy"
    )
    scenario = {"backend": "s3_native", "h3_proxy": True, "runner_mode": "host",
                "endpoint_url": f"https://localhost:{CONTAINER_PORT}"}
    assert engine.h3_scenario_endpoint(scenario) == f"https://localhost:{MOVED_PORT}", (
        "and the scenario's endpoint must follow it: the bundled scenario file names the "
        "default, because that is what it is without an override"
    )


def test_the_harness_keeps_the_shell_ahead_of_the_file(monkeypatch, env_file):
    env_file.write_text(f"VTOP_H3_PORT={MOVED_PORT}\n", encoding="utf-8")
    monkeypatch.setattr(engine, "_ENV_FILE", str(env_file))
    monkeypatch.setenv("VTOP_H3_PORT", "9643")
    assert engine.h3_proxy_host_port() == 9643, (
        "an exported value is what compose used, so it is what the run must dial"
    )
    monkeypatch.setenv("VTOP_H3_PORT", "")
    assert engine.h3_proxy_host_port() == CONTAINER_PORT, (
        "and a blank export masks the file for compose too, which then falls back to the "
        "default — the harness must not resolve it to the filed value the stack ignored"
    )


def test_the_two_languages_answer_a_filed_override_identically(monkeypatch, env_file):
    # The claim this module exists for. verify-h3.sh proves the proxy serves
    # HTTP/3 on the port it resolved; the harness measures through the port IT
    # resolved. If those differ, a green check certifies a listener the
    # measurement never reaches — which is worse than either being wrong,
    # because both look right on their own.
    env_file.write_text(f"# the lab, moved off a busy port\nVTOP_H3_PORT={MOVED_PORT}\n",
                        encoding="utf-8")
    monkeypatch.setattr(engine, "_ENV_FILE", str(env_file))
    assert str(engine.h3_proxy_host_port()) == resolved_by_the_shell(env_file), (
        "the harness and verify-h3.sh must resolve the published port to the same value "
        "from the same file, or the run and its verification are about different listeners"
    )
