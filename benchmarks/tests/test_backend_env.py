"""The benchmark client follows the same credential overrides as the stack.

The benchmark compose lets an operator override the MinIO server's
credentials via MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (issue #81). The
client used to hardcode its own defaults, so an overridden stack came up
healthy and then refused every upload — a failure that pointed at the
scenario rather than at the one variable pair the operator changed.
"""

from lib.engine import _backend_env


def minio_scenario():
    return {"backend": "minio"}


def test_client_credentials_follow_the_server_override(tmp_path, monkeypatch):
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    monkeypatch.setenv("MINIO_ROOT_USER", "operator")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "override-secret")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "operator", (
        "the client must authenticate with the same override the server was "
        "started with, or scenario 07 fails every upload"
    )
    assert env["AWS_SECRET_ACCESS_KEY"] == "override-secret"


def test_client_credentials_default_to_the_lab_pair(tmp_path, monkeypatch):
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin"
    assert env["AWS_SECRET_ACCESS_KEY"] == "minioadmin"


def test_a_dotenv_file_supplies_the_override_the_shell_did_not(
        tmp_path, monkeypatch):
    # Compose interpolates the benchmark stack from benchmarks/.env (the -f
    # file's own directory is the project directory); the runner must read
    # the SAME file, or a filed override reaches the server and not the
    # client.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text(
        "# lab overrides\nMINIO_ROOT_USER=filed-user\n"
        "MINIO_ROOT_PASSWORD=\"filed secret\"\n",
        encoding="utf-8")
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "filed-user"
    assert env["AWS_SECRET_ACCESS_KEY"] == "filed secret", (
        "one layer of matching quotes is stripped, as compose strips them"
    )


def test_an_inline_comment_is_not_part_of_the_password(
        tmp_path, monkeypatch):
    # Compose strips a whitespace-preceded '#' from an UNQUOTED value, so
    # the server was started with 'benchmarksecret' — a client parser that
    # keeps ' # local MinIO' authenticates with a password the server never
    # saw, and the failure reads as an auth bug in the scenario. A QUOTED
    # value keeps its '#', and a '#' glued to the value (no whitespace
    # before it) is content, not a comment — both per Compose's own rules.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text(
        "MINIO_ROOT_USER=\"user # not a comment\" # a real comment\n"
        "MINIO_ROOT_PASSWORD=benchmarksecret # local MinIO\n",
        encoding="utf-8")
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_SECRET_ACCESS_KEY"] == "benchmarksecret", (
        "the inline comment belongs to the file, not the password"
    )
    assert env["AWS_ACCESS_KEY_ID"] == "user # not a comment", (
        "a quoted value keeps its '#' and ends at its closing quote — the "
        "comment after the quote goes, and so do the quotes themselves"
    )


def test_an_explicitly_empty_filed_value_does_not_crash_the_parser(
        tmp_path, monkeypatch):
    # 'KEY=' files the empty string. The quote check used substring
    # membership ('' in '"\'' is True), so the empty value indexed past its
    # own end and the parser crashed instead of letting the documented
    # blank-value fallback apply (review).
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text(
        "MINIO_ROOT_USER=\nMINIO_ROOT_PASSWORD=\n", encoding="utf-8")
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin", (
        "an explicitly blank filed credential falls back to the lab default"
    )


def test_a_hash_glued_to_the_value_is_content(tmp_path, monkeypatch):
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text(
        "MINIO_ROOT_USER=minioadmin\nMINIO_ROOT_PASSWORD=#secret\n",
        encoding="utf-8")
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_SECRET_ACCESS_KEY"] == "#secret", (
        "compose requires whitespace before an inline comment; a glued '#' "
        "is the value"
    )


def test_the_shell_wins_over_the_dotenv_file(tmp_path, monkeypatch):
    # The same precedence compose applies: an exported variable overrides
    # the filed one.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text("MINIO_ROOT_USER=filed-user\n",
                                   encoding="utf-8")
    monkeypatch.setenv("MINIO_ROOT_USER", "shell-user")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "shell-user"


def test_a_blank_override_falls_back_to_the_lab_default(
        tmp_path, monkeypatch):
    # ${VAR:-default} semantics, deliberately: a blank variable is a common
    # way to 'clear' a value in .env, and treating it as a real credential
    # recreates the authentication mismatch this module exists to prevent.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    monkeypatch.setenv("MINIO_ROOT_USER", "")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin"
    assert env["AWS_SECRET_ACCESS_KEY"] == "minioadmin"


def test_a_blank_shell_variable_masks_the_filed_one(tmp_path, monkeypatch):
    # The case that actually recreates the mismatch: compose lets a PRESENT
    # shell variable mask .env even when blank, then resolves the blank to
    # the default — so the client must do the same, never falling through
    # to the filed value the server was denied.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))
    (tmp_path / ".env").write_text("MINIO_ROOT_USER=filed-user\n"
                                   "MINIO_ROOT_PASSWORD=filed-secret\n",
                                   encoding="utf-8")
    monkeypatch.setenv("MINIO_ROOT_USER", "")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin", (
        "a blank export masks .env for compose, so the server runs on the "
        "default; the client following the filed value would diverge"
    )
    assert env["AWS_SECRET_ACCESS_KEY"] == "minioadmin"


def test_an_endpoint_supplied_only_by_env_still_gets_the_lab_credentials(
        tmp_path, monkeypatch):
    # The engine honors VTOP_S3_ENDPOINT_URL over its config, so a run can be
    # aimed at the lab stack by environment alone — and must then resolve
    # credentials exactly as if the scenario had named the endpoint. Gating on
    # the scenario key only left such a run without the minioadmin fallbacks,
    # failing every upload on credential resolution.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    monkeypatch.setenv("VTOP_S3_ENDPOINT_URL", "http://localhost:9000")
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env({"backend": "s3_native"})
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin", (
        "the lab credential fallbacks must follow the same effective endpoint "
        "the engine resolves, whichever channel supplied it"
    )
    assert env["AWS_SECRET_ACCESS_KEY"] == "minioadmin"
    assert env["VTOP_S3_ENDPOINT_URL"] == "http://localhost:9000"


def test_explicit_aws_credentials_still_win(tmp_path, monkeypatch):
    # setdefault semantics are load-bearing: an operator who points the
    # runner at real S3 with real AWS credentials must not have them
    # overwritten by lab MinIO variables that happen to be exported.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    monkeypatch.setenv("MINIO_ROOT_USER", "operator")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "override-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "real-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "real-secret")
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "real-key"
    assert env["AWS_SECRET_ACCESS_KEY"] == "real-secret"


def test_a_remote_endpoint_never_receives_the_lab_credentials(
        tmp_path, monkeypatch):
    # Environment keys outrank profiles and instance metadata in the SDK's
    # credential chain, so injecting minioadmin for a NON-lab endpoint would
    # replace the identity the operator brought — every upload would
    # authenticate as a user the remote store has never heard of.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    for var in ("VTOP_S3_ENDPOINT_URL", "MINIO_ROOT_USER",
                "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env({"backend": "s3_native",
                        "endpoint_url": "https://rgw.example.net:8443"})
    assert "AWS_ACCESS_KEY_ID" not in env, (
        "a remote endpoint means the operator brought an identity: the lab "
        "fallbacks are for the loopback stack only"
    )
    assert env["VTOP_S3_ENDPOINT_URL"] == "https://rgw.example.net:8443", (
        "the endpoint itself must still reach the engine; only the "
        "credential fallbacks are lab-scoped"
    )


def test_the_environment_endpoint_outranks_the_scenario_field(monkeypatch):
    # The engine resolves VTOP_S3_ENDPOINT_URL over its config file, so the
    # harness must resolve in the same order: preferring the scenario field
    # let a run write its config for one store while the engine talked to
    # another.
    from lib.engine import _effective_endpoint
    monkeypatch.setenv("VTOP_S3_ENDPOINT_URL", "http://localhost:9000")
    assert _effective_endpoint(
        {"endpoint_url": "https://rgw.example.net:8443"}
    ) == "http://localhost:9000", (
        "whichever endpoint the engine will actually use is the one every "
        "harness decision must key off"
    )


def test_another_loopback_service_is_not_the_lab(tmp_path, monkeypatch):
    # Loopback alone is not the signal — the compose stack pins host port
    # 9000, and a different local S3 stand-in (localstack, a second MinIO)
    # has its own credentials that the minioadmin fallbacks would shadow.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    for var in ("VTOP_S3_ENDPOINT_URL", "MINIO_ROOT_USER",
                "MINIO_ROOT_PASSWORD", "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env({"backend": "s3_native",
                        "endpoint_url": "http://localhost:4566"})
    assert "AWS_ACCESS_KEY_ID" not in env, (
        "the lab predicate is host AND port: a loopback service on another "
        "port is somebody else's store with somebody else's credentials"
    )


def test_a_malformed_endpoint_is_not_the_lab_and_does_not_crash(
        tmp_path, monkeypatch):
    # urlsplit defers port validation to the .port property, which raises on
    # a non-numeric port. The harness must not fall over before vtopctl even
    # starts — a broken endpoint is the engine's configuration error to
    # report, and it is certainly not the lab.
    from lib import engine
    monkeypatch.setattr(engine, "_ENV_FILE", str(tmp_path / ".env"))  # absent
    for var in ("VTOP_S3_ENDPOINT_URL", "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env({"backend": "s3_native",
                        "endpoint_url": "http://localhost:not-a-port"})
    assert "AWS_ACCESS_KEY_ID" not in env, (
        "an endpoint the parser cannot even read is nobody's lab: injecting "
        "credentials for it would only mask the real configuration error"
    )
    assert env["VTOP_S3_ENDPOINT_URL"] == "http://localhost:not-a-port", (
        "the broken endpoint must still reach the engine: the configuration "
        "error is the engine's to report, and dropping it here would hide it"
    )
