"""Thin driver around the compiled `vtopctl` binary.

The benchmark never imports engine code — it only builds and runs the binary and
parses its JSON output, keeping benchmark logic fully separate from the engine.
"""
from __future__ import annotations

import json
import os
import subprocess
from urllib.parse import urlsplit


def repo_root() -> str:
    # benchmarks/lib/engine.py -> repo root is two levels up.
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def vtopctl_path(build_if_missing: bool = True) -> str:
    # isfile, not exists (review): a container run whose compose step ran
    # before the binary was built can leave a *directory* at this path (the
    # short bind-mount form mkdirs a missing source). `exists` would treat
    # that directory as the binary and skip the build; `isfile` rebuilds so
    # the runner self-heals even if the mount got created empty.
    env = os.environ.get("VTOPCTL_BIN")
    if env and os.path.isfile(env):
        return env
    root = repo_root()
    path = os.path.join(root, "target", "release", "vtopctl")
    if not os.path.isfile(path) and build_if_missing:
        subprocess.run(["cargo", "build", "--release", "--bin", "vtopctl"],
                       cwd=root, check=True)
    return path


def effective_endpoint(scenario) -> str:
    """The endpoint the engine will actually be pointed at: the environment's
    override when set, else the scenario's. Public so a shaped run can check
    it is the proxy's (#403)."""
    return _effective_endpoint(scenario)


# --- runner mode (#476) ------------------------------------------------------
#
# The engine can run as a host process (today's behaviour, the default) or
# inside the lab's compose network, where a middlebox can later sit in its
# L3 path. Everything below exists so the two modes stay the same
# measurement: same binary (mounted in, never rebuilt in an image), same
# credential resolution, same config — only the namespace changes.

RUNNER_MODES = ("host", "container")

# The compose service that wraps the mounted binary, and the file that
# defines it. `exec` into a standing service rather than `run --rm` per
# invocation: a soak calls the engine once per cycle, and the network
# namespace a middlebox shapes must be the SAME one across cycles, not a
# fresh one per process.
CONTAINER_SERVICE = "vtop-engine"
COMPOSE_FILE = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "docker-compose.benchmark.yml")

# The environment the engine's credential/endpooint resolution consumes —
# the exact keys _backend_env manages. A container run forwards these and
# ONLY these through `exec -e`: forwarding the whole host environment would
# leak host paths and credentials into a namespace that needs neither.
_ENGINE_ENV_KEYS = (
    "VTOP_S3_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY",
    # The session token rides with temporary credentials (review): the SDK's
    # credential chain treats key+secret without it as long-lived and fails
    # auth, so omitting it broke external stores in container mode only.
    "AWS_SESSION_TOKEN",
    # The SDK's own endpoint overrides (review): the backend consumes both,
    # the host runner honors them, and dropping them at the boundary made
    # container mode silently aim at a different store. Operator topology —
    # never translated.
    "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3",
    "AWS_REGION", "VTOP_S3_FORCE_PATH_STYLE", "VTOP_S3_VERIFY_TLS",
)


def runner_mode(scenario) -> str:
    """The scenario's launch mode, refused loudly when it names neither mode:
    a typo that fell back to `host` would run an unshaped measurement and
    record it as whatever the scenario claimed."""
    raw = scenario.get("runner_mode", "host")
    # A bool or other non-string is a scenario error, not a silent host
    # default (review): `runner_mode: false` in YAML arrives as False, and
    # `False or "host"` would quietly coerce it. Only a string names a mode.
    if raw is None or raw == "":
        raw = "host"
    if not isinstance(raw, str) or raw not in RUNNER_MODES:
        raise ValueError(
            f"runner_mode must be one of {RUNNER_MODES}, got {raw!r}")
    return raw


def container_wire_endpoint(endpoint: str, shaped: bool = False) -> str:
    """The lab endpoint as the CONTAINERIZED engine reaches it.

    The compose stack publishes MinIO on the host's loopback at :9000, and
    loopback inside the engine container is the engine container. On the
    compose network the same store is `minio:9000` — the service name — so
    exactly the lab loopback endpoint is rewritten and nothing else is: an
    external store's address means the operator brought their own topology,
    and rewriting it would point the engine somewhere the operator never
    named. The credential decision (`_is_lab_endpoint`) keeps judging the
    HOST-view endpoint, so the lab fallbacks follow the store, not the
    spelling of its address.
    """
    if _is_lab_endpoint(endpoint, shaped=shaped):
        parts = urlsplit(endpoint)
        # A SHAPED scenario goes through the proxy service, not around it
        # (review): 9100 is the same lab store behind toxiproxy, and
        # translating it to the store's own name would send the engine
        # around the toxics while the summary said shaped — the exact
        # bypass require_endpoint_through_proxy exists to refuse.
        if parts.port == 9100:
            return f"{parts.scheme or 'http'}://toxiproxy:9100"
        return f"{parts.scheme or 'http'}://minio:9000"
    return endpoint


def preflight_container(config_path: str, input_dir: str, binary: str,
                        scenario=None) -> str | None:
    """Prove the engine container can see this run's files, BEFORE the run.

    The run root reaches the container through a compose variable
    (VTOP_BENCH_RUN_ROOT), and a container recreated without it silently
    mounts the default instead — after which every cycle fails with a
    config-not-found the runner can only count, not explain. Found the hard
    way: a 300-second soak recorded 267 buried errors and a clean exit.
    One exec up front turns that into an immediate refusal naming the knob.

    Returns None when the container sees the file, else the failure text.
    """
    remount = (
        "Bring the stack up with VTOP_BENCH_RUN_ROOT set to the directory "
        "your TMPDIR (and any --seed-dir) lives under, plus the "
        "containerized profile, e.g.\n"
        "  VTOP_BENCH_RUN_ROOT=$TMPDIR docker compose -f "
        "benchmarks/docker-compose.benchmark.yml --profile containerized up -d")
    # Probes are argv, never a shell string (review): a path with a single
    # quote in a `sh -c` program is arbitrary command execution in the
    # engine container. `test` takes the path as one positional argument,
    # so no quoting question arises.
    #
    # The permission the engine actually needs, not mere existence (review,
    # three findings): the config must be READABLE; the input directory
    # readable AND searchable (-r -x, or the engine stats it and sees
    # nothing); a localfs destination must be a WRITABLE directory, or its
    # parent writable when the root does not exist yet.
    probes = [
        (f"read this run's config at {config_path}", ["test", "-r", config_path]),
        (f"read and search this run's input directory {input_dir}",
         ["test", "-r", input_dir, "-a", "-x", input_dir]),
    ]
    if scenario is not None and scenario.get("backend") == "localfs":
        local_path = str(scenario.get("local_path", "") or "")
        if local_path:
            if not os.path.isabs(local_path):
                return (
                    f"localfs local_path {local_path!r} is relative, and would "
                    "resolve against a different working directory in each "
                    "namespace; make it absolute, under the mounted run root")
            parent = os.path.dirname(local_path.rstrip("/")) or "/"
            # An existing root must be a writable directory; a missing root
            # needs a writable parent to be created in. `sh -c` avoided by
            # expressing the either-or as two probes and accepting the run
            # if EITHER holds — checked in Python, not the shell.
            root_exists = subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
                 CONTAINER_SERVICE, "test", "-e", local_path],
                capture_output=True, text=True).returncode == 0
            if root_exists:
                # An EXISTING root must itself be a writable directory
                # (review): a read-only directory, or a regular file where a
                # directory is meant, refuses.
                probe_dir = local_path
                is_dir = subprocess.run(
                    ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
                     CONTAINER_SERVICE, "test", "-d", local_path],
                    capture_output=True, text=True).returncode == 0
                if not is_dir:
                    return (
                        f"the localfs destination {local_path} exists in the "
                        "container but is a file where a directory is meant. "
                        f"{remount}")
            else:
                # A MISSING root is created with create_dir_all, so ANY
                # not-yet-existing depth is fine as long as the FIRST
                # existing ancestor is a writable directory (review, two
                # findings): walk up until something exists, then probe
                # that. LocalFsBackend::store creates the rest.
                probe_dir = parent
                walk = subprocess.run(
                    ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
                     CONTAINER_SERVICE, "sh", "-c",
                     'd="$1"; while [ ! -e "$d" ] && [ "$d" != "/" ] '
                     '&& [ "$d" != "." ]; do d=$(dirname "$d"); done; '
                     'printf %s "$d"', "vtopbench", local_path],
                    capture_output=True, text=True)
                if walk.returncode == 0 and walk.stdout.strip():
                    probe_dir = walk.stdout.strip()
            # An ACTUAL WRITE, not a permission-bit read (review): test -w
            # passes on a directory whose filesystem is mounted read-only,
            # or under other mount-level denials that the bits do not show.
            # Create and remove a probe file; success is the only proof
            # that matters before a soak commits to this destination.
            probe_name = f".vtop-bench-writeprobe-{os.getpid()}"
            write_ok = subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
                 CONTAINER_SERVICE, "sh", "-c",
                 'p="$1/$2"; (set -C; : > "$p") 2>/dev/null && rm -f "$p"',
                 "vtopbench", probe_dir, probe_name],
                capture_output=True, text=True).returncode == 0
            if not write_ok:
                return (
                    f"the localfs destination {local_path} is not writable in "
                    f"the container (probed a real write under {probe_dir}; it "
                    "may be a read-only mount, not just permission bits). The "
                    "container runs read-only except the mounted run root; put "
                    f"local_path under it. {remount}")
    for what, args in probes:
        probe = subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
             CONTAINER_SERVICE] + args,
            capture_output=True, text=True)
        if probe.returncode != 0:
            return (
                f"the engine container cannot {what}: the run root is not "
                "mounted where this run expects it — or the container user "
                "cannot traverse it (the service runs as VTOP_BENCH_UID, "
                "default 1000; set it to your uid when yours differs, since "
                f"mkdtemp directories are 0700). {remount}\n"
                f"compose said: "
                f"{probe.stderr.strip() or probe.stdout.strip() or 'nothing'}")
    # AND THE BYTES MUST RUN THERE (review): a hash match proves identity,
    # not executability — a macOS host binary mounts and matches and then
    # cannot exec in a Linux container, and a no-outcome cycle would bury
    # that. One --version exec proves the platform before anything is
    # measured.
    probe = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
         CONTAINER_SERVICE, "vtopctl", "--version"],
        capture_output=True, text=True)
    if probe.returncode != 0:
        return (
            "the container cannot execute the mounted vtopctl (a host binary "
            "built for another platform mounts and hash-matches but will not "
            "run — container mode needs a Linux build): "
            f"{probe.stderr.strip() or probe.stdout.strip() or 'no output'}")
    # THE SAME BYTES IN BOTH NAMESPACES (review): the compose mount defaults
    # to target/release/vtopctl, so a VTOPCTL_BIN override — or a rebuild
    # that replaced the file after the container mounted its old inode,
    # found live — would measure a different artifact while the results
    # claimed one binary. The hashes must match or the run refuses.
    host_hash = ""
    try:
        with open(binary, "rb") as fh:
            import hashlib
            host_hash = hashlib.sha256(fh.read()).hexdigest()
    except OSError as error:
        return f"cannot hash the host binary {binary}: {error}"
    probe = subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T",
         CONTAINER_SERVICE, "sha256sum", "/usr/local/bin/vtopctl"],
        capture_output=True, text=True)
    container_hash = probe.stdout.split()[0] if probe.returncode == 0 and probe.stdout else ""
    if container_hash != host_hash:
        return (
            "the container's vtopctl is not the binary this run selected "
            f"({binary}): host sha256 {host_hash[:16]}…, container "
            f"{container_hash[:16] or 'unreadable'}…. Recreate the service so "
            "the mount follows the current file (a bind mount keeps the OLD "
            "inode across a rebuild), and export VTOPCTL_BIN before `up` if "
            f"you are selecting a custom build. {remount}")
    return None


def invocation(binary: str, args: list[str], scenario) -> tuple[list[str], dict[str, str]]:
    """The exact (argv, env) a run of the engine uses, in either mode.

    Public and pure so the default path is pinned by a test rather than by
    hope: `host` mode must produce today's command line unchanged, or the
    second mode's existence has already drifted the first.
    """
    env = _backend_env(scenario)
    if runner_mode(scenario) == "host":
        return [binary] + args, env
    # Container mode: the engine's environment crosses the boundary through
    # explicit -e flags — the six keys resolution consumes, with the wire
    # endpoint swapped in for the lab's loopback address. The config file's
    # endpoint line gets the same translation in write_engine_config, so the
    # two channels can never name different stores.
    # SECRETS NEVER RIDE THE COMMAND LINE (review): `-e KEY=VALUE` puts the
    # secret in /proc/*/cmdline for any local observer. `-e KEY` alone tells
    # the docker client to propagate the value from ITS OWN environment, so
    # the value travels through the returned env dict instead.
    argv = ["docker", "compose", "-f", COMPOSE_FILE, "exec", "-T"]
    exec_env = dict(os.environ)
    for key in _ENGINE_ENV_KEYS:
        value = env.get(key, "")
        # EVERY forwarded endpoint variable gets the same view translation
        # (review): the SDK honors its own AWS_ENDPOINT_URL family, and a
        # lab-loopback value forwarded verbatim aims the container at
        # itself — the exact hole the VTOP_S3_ENDPOINT_URL translation
        # closed, one variable over. Operator endpoints pass through.
        if value and key in (
                "VTOP_S3_ENDPOINT_URL", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
            value = container_wire_endpoint(
                value, shaped=_shaped_by_the_bundled_proxy(scenario))
        if value:
            argv += ["-e", key]
            exec_env[key] = value
    argv += [CONTAINER_SERVICE, "vtopctl"] + args
    return argv, exec_env


def _effective_endpoint(scenario) -> str:
    """The one answer to "which store is this run actually pointed at?".

    The engine resolves VTOP_S3_ENDPOINT_URL over its config file
    (vtop-upload s3_native.rs), so the environment must win here in the
    same order: resolving the scenario field first let a run with both set
    write its config for one store while the engine talked to another.
    Everything that keys off the endpoint resolves it through this helper —
    the written config's endpoint line and the lab credential fallbacks in
    _backend_env alike — because an endpoint recognized by one and not the
    other produced a bucket-creating config with credentials the server
    never saw, and every upload failed on credential resolution.
    """
    return os.environ.get("VTOP_S3_ENDPOINT_URL", "") or scenario.get("endpoint_url", "")


def _shaped_by_the_bundled_proxy(scenario) -> bool:
    """True only for a scenario shaped through the bundled `minio` proxy
    (review): the lab credential fallback follows that proxy's name, and
    lib/shaping.py refuses a proxy wearing it that forwards anywhere else."""
    return bool(scenario.get("shaping_api_url")) and str(
        scenario.get("shaping_proxy", "minio") or "minio") == "minio"


def _is_lab_endpoint(endpoint: str, shaped: bool = False) -> bool:
    # The compose stack publishes MinIO on the loopback interface at the
    # FIXED host port 9000 (docker-compose.benchmark.yml pins it; only the
    # bind address is overridable), and that one endpoint is the only one
    # whose credentials this harness can know. Anything else — a remote
    # store, or another loopback service such as localstack on :4566 —
    # means the operator brought an identity, and injecting the lab
    # fallbacks would OUTRANK it: environment keys beat profiles and
    # instance metadata in the SDK's credential chain.
    # A malformed endpoint is NOT the lab, and not this helper's error to
    # raise: urlsplit defers port validation to the .port property, which
    # throws on a non-numeric or out-of-range port — the engine's own config
    # validation is where a broken endpoint should be reported.
    try:
        parts = urlsplit(endpoint)
        port = parts.port
    except ValueError:
        return False
    # 9100 is the same lab MinIO behind the shaped stack's toxiproxy (#403)
    # — but only for a SHAPED scenario: the pipe changes, the store and its
    # lab credentials do not. An unshaped scenario aimed at 9100 is somebody
    # else's service, and must not be handed the lab's keys (review).
    lab_ports = (9000, 9100) if shaped else (9000,)
    return (parts.hostname in ("localhost", "127.0.0.1", "::1")
            and port in lab_ports)


def write_engine_config(scenario, work_dir: str, state_db: str,
                        input_glob: str, config_path: str,
                        key_prefix: str = "") -> str:
    backend = scenario.get("backend", "mock")
    bucket = scenario.get("bucket", "telemetry-data")
    endpoint = _effective_endpoint(scenario)
    # Bucket provisioning is a scenario's EXPLICIT choice, never an inference
    # from the endpoint: a real S3-compatible endpoint keeps CreateBucket out
    # of the runtime identity (SECURITY_MODEL §5.2), so the inferred true
    # either failed least-privilege credentials with AccessDenied or issued a
    # real CreateBucket against production storage. Unset derives true only
    # for backend=minio — inert, since the mc-based backend's ensure_bucket
    # is the base no-op. No bundled scenario opts in: the compose init
    # service provisions every lab bucket, soak included, so per-cycle
    # subprocesses never carry CreateBucket into a measurement.
    requested = scenario.get("create_bucket", "")
    if requested == "":
        create_bucket = "true" if backend == "minio" else "false"
    else:
        # PyYAML and the fallback parser both hand over a bool; a value that
        # skipped the loader arrives as a string. A truthiness test would
        # read the string "false" as an opt-in, so spell both forms out.
        create_bucket = "true" if str(requested).strip().lower() == "true" else "false"
    whole_file = "true" if scenario.get("whole_file") or scenario.get("format") == "binary" else "false"
    checksum = scenario.get("checksum", "sha256")
    # The engine implements sha256 / blake3 / none; record the request as-is.
    if checksum not in ("sha256", "blake3", "none", "disabled"):
        checksum = "sha256"
    lines = [
        "engine:",
        "  name: vtop-bench",
        "  tenant: default",
        f'  state_store: "sqlite://{state_db}"',
        f"  work_dir: {work_dir}",
        "  log_level: warn",
        "batching:",
        f"  max_records: {scenario.get('batch_max_records', 10000)}",
        f"  max_bytes: {scenario.get('batch_max_bytes', 104857600)}",
        f"  max_batch_age_seconds: {scenario.get('batch_max_age_seconds', 60)}",
    ]
    # The #87 pipeline-width knob, surfaced so a scenario grid can vary it
    # (#102: object_upload p95 vs concurrency is the signal that decides
    # whether an adaptive controller has anything to react to). Absent =
    # the engine default, exactly as before. Present — zero included — is
    # written through, so the engine's own validation refuses it loudly
    # (batching.max_concurrent_batches must be > 0, vtop-core config.rs): a
    # truthiness check here ran the default width of 8 while summary.json
    # recorded 0, mislabeling the very concurrency comparison the #102 grid
    # exists to make.
    if scenario.get("max_concurrent_batches") is not None:
        lines.append(
            f"  max_concurrent_batches: {scenario.get('max_concurrent_batches')}")
    lines += [
        "compression:",
        f"  type: {scenario.get('compression', 'gzip')}",
        f"  level: {scenario.get('compression_level', 6)}",
        "checksum:",
        f"  algorithm: {checksum}",
        "sources:",
        "  file:",
        "    enabled: true",
        f"    whole_file: {whole_file}",
        "    paths:",
        f'      - "{input_glob}"',
        "upload:",
        f"  backend: {backend}",
        f'  bucket: "{bucket}"',
        # The bench bucket is durable across runs — the named volume survives
        # `compose down` — so each run namespaces its objects under its
        # run_id: attributable in the bucket, removable with one
        # `mc rm -r --force`.
        f'  prefix: "{key_prefix}"',
        f"  create_bucket: {create_bucket}",
        "  region: us-east-1",
        "  force_path_style: true",
        "  verify_tls: false",
    ]
    if backend == "localfs":
        root = scenario.get("local_path", "") or os.path.join(os.path.dirname(state_db), "objects")
        lines.append(f'  local_path: "{root}"')
    # Command-based backends (awscli/s3cmd/minio) shell out to a CLI, and the
    # engine refuses a non-absolute command_binary — PATH lookup is forbidden
    # (vtop-core config.rs). write_engine_config never wrote a command_* block,
    # so ANY command backend failed config validation on every cycle (#499);
    # pass a scenario-supplied absolute path (and optional alias/profile)
    # through so such a backend can be configured at all.
    if backend in ("awscli", "s3cmd", "minio"):
        command_binary = scenario.get("command_binary", "")
        if command_binary:
            lines.append(f'  command_binary: "{command_binary}"')
        profile = scenario.get("profile", "")
        if profile:
            lines.append(f'  profile: "{profile}"')
        # The engine CLEARS the child environment and restores only the names
        # in command_env_allowlist (CommandPolicy::command_with_environment,
        # vtop-upload/src/command.rs). A command backend that authenticates
        # through env vars (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, ...) fails
        # auth unless its allowlist is serialized too, so a dropped allowlist
        # left the newly configurable backend unusable (#499). Accept a YAML
        # list or a comma/space-separated string.
        allow = scenario.get("command_env_allowlist", "")
        if isinstance(allow, (list, tuple)):
            names = [str(n).strip() for n in allow if str(n).strip()]
        else:
            names = [n for n in str(allow).replace(",", " ").split() if n]
        if names:
            lines.append("  command_env_allowlist:")
            for name in names:
                lines.append(f'    - "{name}"')
    if endpoint:
        # The config the CONTAINERIZED engine reads must name the store as
        # that engine reaches it (#476): loopback inside the container is
        # the container. Same translation, same single place, as the
        # environment override in `invocation` — resolved through
        # `_effective_endpoint` first in both, so the two can never name
        # different stores.
        if runner_mode(scenario) == "container":
            endpoint = container_wire_endpoint(
                endpoint, shaped=_shaped_by_the_bundled_proxy(scenario))
        lines.append(f"  endpoint_url: {endpoint}")
    with open(config_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return config_path


# The .env the BENCHMARK STACK actually honors. Compose resolves a -f file's
# project directory to that file's own directory, so it is benchmarks/.env —
# and deliberately NOT the repository root's — that interpolates the stack's
# credentials. The runner must read the same file, or a filed override would
# reach one side of the authentication and not the other, in either
# direction.
_ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), ".env")


def _dotenv_overrides() -> dict[str, str]:
    """KEY=VALUE pairs from the .env compose reads for the benchmark stack.

    Deliberately minimal (comments and blank lines skipped, one layer of
    matching quotes stripped, no interpolation): the harness is
    dependency-light, and the lab's .env holds simple assignments. The
    boundary of "minimal" is spelled out because reviews keep probing it:
    a quoted value ends at its first closing quote — escape sequences are
    not interpreted, so a credential containing the quote character itself
    is outside this parser's charter (and Compose's escape-expansion rules
    are exactly the complexity this harness refuses to re-implement).

    Inline comments follow Compose's own dotenv rule: an UNQUOTED value
    stops at the first whitespace-preceded ``#``; a quoted value keeps its
    content untouched. The rule matters because these values are
    credentials the server was interpolated with — a parser that keeps
    ``benchmarksecret # local MinIO`` hands the client a password the
    server never saw, and the failure reads as an auth bug.
    """
    values: dict[str, str] = {}
    try:
        with open(_ENV_FILE, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                value = value.strip()
                if value[:1] in ('"', "'"):
                    # A quoted value ends at its CLOSING quote; whatever
                    # follows — an inline comment, stray whitespace — is not
                    # part of it. Checking for a quote PAIR before stripping
                    # the comment left the quotes inside the credential
                    # whenever a comment followed the closing quote
                    # (review). An unterminated quote is left exactly as
                    # written — not this parser's error to invent a meaning
                    # for.
                    closing = value.find(value[0], 1)
                    if closing != -1:
                        value = value[1:closing]
                else:
                    for at in range(1, len(value)):
                        if value[at] == "#" and value[at - 1].isspace():
                            value = value[:at]
                            break
                    value = value.rstrip()
                values[key.strip()] = value
    except OSError:
        pass
    return values


def _backend_env(scenario) -> dict[str, str]:
    env = dict(os.environ)
    endpoint = _effective_endpoint(scenario)
    if scenario.get("backend") == "s3_native" and endpoint:
        # A no-op when the endpoint came from the environment: it is
        # already set there, and setdefault leaves it alone.
        env.setdefault("VTOP_S3_ENDPOINT_URL", endpoint)
    # The lab fallbacks apply to the mc-based backend and to s3_native aimed
    # at the LAB stack only — the loopback endpoint is the one whose
    # credentials this harness can know, and setdefault keeps real AWS
    # credentials (already in the environment) winning over the fallbacks.
    if scenario.get("backend") == "minio" or (
            scenario.get("backend") == "s3_native" and endpoint
            and _is_lab_endpoint(endpoint, shaped=_shaped_by_the_bundled_proxy(scenario))):
        # The benchmark compose lets an operator override the SERVER's
        # credentials via MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (issue #81).
        # The client must follow the same variables THROUGH THE SAME
        # CHANNELS: a literal default here made every upload against an
        # overridden stack fail authentication, and consulting only the
        # process environment repeats that failure for an operator who set
        # the override in .env — which compose auto-loads and a separately
        # launched Python process does not. A blank value falls back to the
        # lab default, exactly as compose's own ${VAR:-default} would.
        dotenv = _dotenv_overrides()

        def _credential(name: str) -> str:
            # A PRESENT shell variable masks the filed one even when blank,
            # exactly as compose's interpolation resolves it: the server
            # sees ${VAR:-minioadmin} with the blank shell value and uses
            # the default, so falling through to .env here would hand the
            # client a credential the server never saw.
            if name in env:
                return env[name] or "minioadmin"
            return dotenv.get(name) or "minioadmin"

        env.setdefault("AWS_ACCESS_KEY_ID", _credential("MINIO_ROOT_USER"))
        env.setdefault("AWS_SECRET_ACCESS_KEY",
                       _credential("MINIO_ROOT_PASSWORD"))
        env.setdefault("AWS_REGION", "us-east-1")
        env.setdefault("VTOP_S3_FORCE_PATH_STYLE", "true")
        env.setdefault("VTOP_S3_VERIFY_TLS", "false")
    return env


def process_once(binary: str, config_path: str, scenario,
                 source: str = "file") -> tuple[int, list[dict], str]:
    """Run `vtopctl process-once --json` and parse the batch outcomes."""
    argv, env = invocation(
        binary, ["--json", "process-once", "--source", source, "--config", config_path],
        scenario)
    proc = subprocess.run(argv, capture_output=True, text=True, env=env)
    outcomes: list[dict] = []
    try:
        outcomes = json.loads(proc.stdout) if proc.stdout.strip() else []
    except json.JSONDecodeError:
        outcomes = []
    return proc.returncode, outcomes, proc.stderr


def replay(binary: str, config_path: str, scenario) -> tuple[int, str]:
    argv, env = invocation(binary, ["replay", "--config", config_path], scenario)
    proc = subprocess.run(argv, capture_output=True, text=True, env=env)
    return proc.returncode, proc.stdout + proc.stderr
