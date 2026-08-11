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


def test_client_credentials_follow_the_server_override(monkeypatch):
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


def test_client_credentials_default_to_the_lab_pair(monkeypatch):
    for var in ("MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD",
                "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(var, raising=False)
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "minioadmin"
    assert env["AWS_SECRET_ACCESS_KEY"] == "minioadmin"


def test_explicit_aws_credentials_still_win(monkeypatch):
    # setdefault semantics are load-bearing: an operator who points the
    # runner at real S3 with real AWS credentials must not have them
    # overwritten by lab MinIO variables that happen to be exported.
    monkeypatch.setenv("MINIO_ROOT_USER", "operator")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "override-secret")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "real-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "real-secret")
    env = _backend_env(minio_scenario())
    assert env["AWS_ACCESS_KEY_ID"] == "real-key"
    assert env["AWS_SECRET_ACCESS_KEY"] == "real-secret"
