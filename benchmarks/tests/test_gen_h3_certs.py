"""The identity the h3 proxy runs as has to describe the key it is handed (#484).

`gen-h3-certs.sh` mints a private key at mode 0600 and then writes
`VTOP_BENCH_UID`/`VTOP_BENCH_GID` into `benchmarks/.env`, because the proxy
container runs as exactly that pair and nginx dies on "permission denied"
reading its own key if the pair is not the key's owner.

Those ids are only ever ADDED, never rewritten — an operator who set them has a
reason — and the review round after the service landed found the hole in that:
a value left over from another user or another checkout is not a preference,
and preserving it silently produces a proxy that cannot start, reported as
broken material rather than as a stale line in a file nobody opened. So a
disagreement is refused, and the refusal names both what `.env` says and who
owns the key.

The round after THAT found two more identities the script would file without
being able to work. One is root: minting in a development or CI container
happens as uid 0, and a root nginx under `cap_drop: [ALL]` dies chowning its
temp paths before it ever reads the key. The other is anything the shell
already exports, which compose reads BEFORE `.env` — so the file could be
validated, reported consistent, and then not be what the proxy ran as. Both are
refused here, and the refusal has to say which of the two places the value it
objects to came from, because they are corrected differently.

Asserted by RUNNING the script, because the thing under test is a shell script
and everything that could be wrong about it — the stat spelling, what is
written when, what is left alone on refusal — is only true when it executes.
The script is copied into a temporary directory first: it writes `.env` beside
itself, and a test that clobbered the operator's real one would be a worse bug
than the one it was checking for.
"""

import os
import shutil
import subprocess

import pytest

BENCH_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPT = os.path.join(BENCH_DIR, "gen-h3-certs.sh")

pytestmark = pytest.mark.skipif(
    shutil.which("openssl") is None,
    reason="the script mints its material with openssl; without it there is no key to own")


@pytest.fixture
def lab(tmp_path):
    """The real script, run somewhere its .env cannot reach the repository."""
    script = tmp_path / "gen-h3-certs.sh"
    shutil.copy2(SCRIPT, script)
    # The shared .env resolution is sourced by path from beside the script, so
    # the copy needs it too: a test arrangement the real layout does not have
    # would prove the script works somewhere it is never run.
    (tmp_path / "lib").mkdir()
    shutil.copy2(os.path.join(BENCH_DIR, "lib", "dotenv.sh"), tmp_path / "lib" / "dotenv.sh")
    cert_dir = tmp_path / "tls"

    def run(**exported):
        # The ids are scrubbed from the inherited environment unless the case
        # states one. The script reads what COMPOSE would resolve, and the
        # shell outranks the file — so an operator who exports VTOP_BENCH_UID
        # in the shell that runs pytest would otherwise silently become a
        # condition of every case in this module.
        env = {key: value for key, value in os.environ.items()
               if key not in ("VTOP_BENCH_UID", "VTOP_BENCH_GID")}
        env.update(exported)
        return subprocess.run(["bash", str(script), str(cert_dir)],
                              capture_output=True, text=True, timeout=120, env=env)

    return run, tmp_path / ".env", cert_dir


@pytest.fixture
def a_key_owner_of(tmp_path):
    """Make the script see material owned by <uid>:<gid>, as an env mapping.

    Freshly minted material belongs to whoever ran pytest, and the two owners
    that matter here — root, and anyone the compose default does not describe —
    are not that. The owner only ever reaches the script through `stat`, so
    `stat` is what the case replaces: bash sources BASH_ENV before running a
    non-interactive script, and a function outranks the PATH lookup. (A
    directory of shims on PATH is the obvious way and does not survive a
    `noexec` /tmp, which is where pytest puts its temporary directories.)
    """
    def owner(uid, gid):
        answer = tmp_path / "stat-says.bash"
        answer.write_text(f"stat() {{ printf '{uid} {gid}\\n'; }}\n", encoding="utf-8")
        return {"BASH_ENV": str(answer)}

    return owner


def test_a_first_run_writes_the_owner_of_the_key_it_just_minted(lab):
    run, env_file, cert_dir = lab
    result = run()
    assert result.returncode == 0, (
        f"minting into a clean directory must succeed; it exited "
        f"{result.returncode} saying: {result.stderr}"
    )
    key = cert_dir / "h3-proxy-key.pem"
    stat = key.stat()
    written = dict(line.split("=", 1) for line in
                   env_file.read_text(encoding="utf-8").splitlines() if "=" in line)
    assert written.get("VTOP_BENCH_UID") == str(stat.st_uid), (
        f"the .env names uid {written.get('VTOP_BENCH_UID')!r} but {key} is owned "
        f"by {stat.st_uid}; the proxy runs as what the .env says, and a 0600 key "
        "is readable by its owner alone"
    )
    assert written.get("VTOP_BENCH_GID") == str(stat.st_gid), (
        f"the .env names gid {written.get('VTOP_BENCH_GID')!r} but {key} has group "
        f"{stat.st_gid}; the pair is written together because it is one identity"
    )
    assert f"owner={stat.st_uid}:{stat.st_gid}" in result.stdout, (
        "the machine-readable line must state the identity the run established "
        f"({stat.st_uid}:{stat.st_gid}); it said: {result.stdout.strip()}. The check "
        "is invisible on the path where it passes, and that is the path someone "
        "chasing a proxy that will not start needs to see was taken"
    )


def test_a_re_run_that_agrees_with_the_key_changes_nothing(lab):
    run, env_file, _ = lab
    assert run().returncode == 0
    first = env_file.read_text(encoding="utf-8")
    result = run()
    assert result.returncode == 0, (
        f"a second run over valid material must be a no-op, not a failure; it "
        f"exited {result.returncode} saying: {result.stderr}"
    )
    assert env_file.read_text(encoding="utf-8") == first, (
        "the second run rewrote the .env. The ids are appended once and then "
        "left alone; a file that grows a duplicate line on every invocation is "
        "how an operator's own value gets buried under ours"
    )


def test_an_id_that_cannot_read_the_key_is_refused_and_left_alone(lab):
    run, env_file, cert_dir = lab
    assert run().returncode == 0
    owner = (cert_dir / "h3-proxy-key.pem").stat().st_uid
    stale = owner + 1
    doctored = f"VTOP_BENCH_UID={stale}\nVTOP_BENCH_GID={owner}\n"
    env_file.write_text(doctored, encoding="utf-8")

    result = run()
    assert result.returncode != 0, (
        f"a .env pinning uid {stale} against a key owned by {owner} must be "
        "refused: the proxy would start as a user that cannot read its own key "
        "and report it as broken material. The run exited 0 and said: "
        f"{result.stdout.strip()}"
    )
    assert str(stale) in result.stderr and str(owner) in result.stderr, (
        f"the refusal must name both the stale value ({stale}) and the key's "
        f"actual owner ({owner}), or the operator cannot tell which of the two "
        f"to change. It said: {result.stderr.strip()}"
    )
    assert env_file.read_text(encoding="utf-8") == doctored, (
        "the refusal must leave the .env exactly as it found it. Rewriting an "
        "id an operator set on purpose is the failure the add-only rule exists "
        "to prevent; refusing is how both rules hold at once"
    )


def test_a_stale_group_is_refused_on_the_same_terms(lab):
    # The uid is the half that decides whether nginx can open the key, so a
    # lone gid mismatch would be tempting to wave through. It is not waved
    # through: the pair is one identity, written together, and a half that was
    # written for different material says nothing about it is being checked.
    run, env_file, cert_dir = lab
    assert run().returncode == 0
    stat = (cert_dir / "h3-proxy-key.pem").stat()
    stale = stat.st_gid + 1
    env_file.write_text(f"VTOP_BENCH_UID={stat.st_uid}\nVTOP_BENCH_GID={stale}\n",
                        encoding="utf-8")

    result = run()
    assert result.returncode != 0, (
        f"a .env pinning gid {stale} against material grouped {stat.st_gid} must "
        f"be refused; the run exited 0 and said: {result.stdout.strip()}"
    )
    assert "VTOP_BENCH_GID" in result.stderr, (
        f"the refusal must name the half that disagrees; it said: {result.stderr.strip()}"
    )
    assert "VTOP_BENCH_UID" not in result.stderr.split("The h3 proxy")[0], (
        "the refusal listed VTOP_BENCH_UID as stale as well, but it matches the "
        f"key's owner. A refusal that reports values it did not object to sends "
        f"the operator after the wrong line. It said: {result.stderr.strip()}"
    )


def test_an_operator_value_that_matches_is_not_duplicated(lab):
    # The add-only rule, still intact: a .env that already states the right
    # identity — by hand, or from a previous run in another checkout — is
    # accepted as it stands.
    run, env_file, cert_dir = lab
    assert run().returncode == 0
    stat = (cert_dir / "h3-proxy-key.pem").stat()
    hand_written = (f"# set deliberately\nVTOP_BENCH_UID={stat.st_uid}\n"
                    f"VTOP_BENCH_GID={stat.st_gid}\nMINIO_ROOT_USER=someone\n")
    env_file.write_text(hand_written, encoding="utf-8")

    result = run()
    assert result.returncode == 0, (
        "a .env that already names the key's owner must be accepted; it exited "
        f"{result.returncode} saying: {result.stderr.strip()}"
    )
    assert env_file.read_text(encoding="utf-8") == hand_written, (
        "the script rewrote a .env that already agreed with the material, "
        "including an unrelated credential line beside it; it may only ever "
        "append ids that are missing"
    )


def test_a_key_owned_by_root_is_refused_rather_than_filed_as_an_unusable_identity(lab, a_key_owner_of):
    # Minting inside a development or CI container is commonly done as uid 0,
    # and the material then belongs to root — which is a valid owner as far as
    # the shape check is concerned and was written out as VTOP_BENCH_UID=0.
    # Root is the one identity this proxy cannot run as: its container drops
    # every capability, and a root nginx master chowns its temp paths before it
    # listens (tests/test_compose_h3.py pins both halves). The container then
    # dies naming /var/cache/nginx, which says nothing about the file that
    # chose the identity.
    run, env_file, _ = lab
    result = run(**a_key_owner_of(0, 0))

    assert result.returncode != 0, (
        "material owned by root must be refused: writing VTOP_BENCH_UID=0 files an "
        "identity that cannot start, and the failure then surfaces as a cache directory "
        f"nginx could not chown. The run exited 0 saying: {result.stdout.strip()}"
    )
    assert not env_file.exists(), (
        "the refusal wrote a .env anyway. An id that cannot work is worse than no id: the "
        f"compose default at least names a plausible user. It contains: "
        f"{env_file.read_text(encoding='utf-8') if env_file.exists() else ''!r}"
    )
    assert "uid 0" in result.stderr and "CAP_CHOWN" in result.stderr, (
        "the refusal must say what it saw (a root-owned key) and why root cannot work "
        "(the chown nginx does before it listens, without the capability to do it), or it "
        f"reads as an arbitrary policy. It said: {result.stderr.strip()}"
    )
    assert "chown" in result.stderr and "sudo -u" in result.stderr, (
        "and it must say what to do about it — mint as the unprivileged user, or hand the "
        f"existing material over. It said: {result.stderr.strip()}"
    )


def test_an_exported_id_outranks_the_env_file_and_is_refused_on_the_same_terms(lab):
    # The .env here is this script's own, written moments earlier and correct.
    # Compose still would not use it: an exported variable outranks the file,
    # so validating the file alone reported a consistent lab and left the proxy
    # starting as a uid that cannot read its own 0600 key.
    run, env_file, cert_dir = lab
    assert run().returncode == 0
    filed = env_file.read_text(encoding="utf-8")
    owner = (cert_dir / "h3-proxy-key.pem").stat().st_uid
    stale = owner + 1

    result = run(VTOP_BENCH_UID=str(stale))
    assert result.returncode != 0, (
        f"VTOP_BENCH_UID={stale} is exported over a .env that names the key's owner "
        f"({owner}), and compose reads the shell first — so the run compose performs is the "
        "broken one while this script reports success. It exited 0 saying: "
        f"{result.stdout.strip()}"
    )
    assert str(stale) in result.stderr and str(owner) in result.stderr, (
        f"the refusal must name both the value compose will use ({stale}) and the key's "
        f"owner ({owner}); it said: {result.stderr.strip()}"
    )
    assert "exported" in result.stderr, (
        "the refusal must say the value came from the SHELL. An operator told to correct a "
        "line will edit the .env, which already agrees with the key, and see the refusal "
        f"again. It said: {result.stderr.strip()}"
    )
    assert env_file.read_text(encoding="utf-8") == filed, (
        "the refusal must leave the .env alone; the file is not the half that is wrong"
    )


def test_an_exported_id_that_agrees_with_the_key_is_accepted(lab):
    # The other half of the rule: reading the shell is not a reason to distrust
    # it. An operator who exports the owner's ids — the documented way to run
    # the lab as a second user — must not be refused for having done so.
    run, _, cert_dir = lab
    assert run().returncode == 0
    stat = (cert_dir / "h3-proxy-key.pem").stat()

    result = run(VTOP_BENCH_UID=str(stat.st_uid), VTOP_BENCH_GID=str(stat.st_gid))
    assert result.returncode == 0, (
        "an exported pair that IS the key's owner describes exactly the identity the proxy "
        f"needs; refusing it would make the escape hatch unusable. It said: "
        f"{result.stderr.strip()}"
    )


def test_a_blank_export_masks_the_file_and_is_judged_as_the_compose_default(lab, a_key_owner_of):
    # The subtlest form of the same defect. `VTOP_BENCH_UID=` exported is
    # PRESENT as far as compose is concerned: it masks the .env line and leaves
    # `${VTOP_BENCH_UID:-1000}` on its default. So the identity the proxy gets
    # is 1000 — not the 4242 the file states and the key requires — and a check
    # that treated a blank as "nothing set" would wave it through.
    run, env_file, _ = lab
    as_4242 = a_key_owner_of(4242, 4242)
    assert run(**as_4242).returncode == 0
    filed = env_file.read_text(encoding="utf-8")
    assert "VTOP_BENCH_UID=4242" in filed, filed

    result = run(**as_4242, VTOP_BENCH_UID="")
    assert result.returncode != 0, (
        "a blank export masks a .env that names the key's owner (4242) and compose falls "
        "back to 1000, which cannot read a 0600 key owned by 4242. The run exited 0 saying: "
        f"{result.stdout.strip()}"
    )
    assert "1000" in result.stderr and "4242" in result.stderr, (
        "the refusal must name the identity compose will actually use (1000, the default a "
        "blank export falls back to) beside the owner it must be (4242), or it describes a "
        f"disagreement the operator cannot see. It said: {result.stderr.strip()}"
    )
    assert env_file.read_text(encoding="utf-8") == filed, (
        "the .env states the right identity and must survive: the wrong half is in the shell"
    )


def test_an_env_file_without_a_final_newline_gains_a_new_assignment_not_a_longer_one(lab):
    # Appending to a file whose last line is unterminated concatenates onto
    # that line: the previous value silently absorbs the assignment, compose
    # falls back to uid 1000, and on a host that is not uid 1000 the proxy
    # cannot read the key it was just handed (review). An editor that trims
    # trailing newlines is enough to cause it.
    run, env_file, _ = lab
    env_file.write_text("MINIO_ROOT_USER=someone", encoding="utf-8")  # no final newline

    done = run()
    assert done.returncode == 0, done.stderr

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert lines[0] == "MINIO_ROOT_USER=someone", (
        "the operator's existing value must survive intact; concatenating onto it changes "
        "a credential as a side effect of minting a certificate"
    )
    assert any(line.startswith("VTOP_BENCH_UID=") for line in lines), (
        "and the identity must land as its own assignment, or compose never sees it and the "
        "proxy runs as a uid that cannot read its own key"
    )
