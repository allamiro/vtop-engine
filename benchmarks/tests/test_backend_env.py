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
