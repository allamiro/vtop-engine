"""A minimal Docker Engine API client over the unix socket, standard library only.

The S3 tap and the cgroup sampler run inside the harness container, where the
Docker CLI is not installed; they need only three calls: kill a container with
a signal, read one stats sample, and inspect a container. The socket is
mounted read-write into the harness container (`/var/run/docker.sock`).
"""
from __future__ import annotations

import http.client
import json
import os
import socket
import urllib.parse
from typing import Any


def default_socket() -> str:
    """DOCKER_HOST's unix socket, else the system one, else Docker Desktop's per-user one."""
    host = os.environ.get("DOCKER_HOST", "")
    if host.startswith("unix://"):
        return host[len("unix://"):]
    for path in ("/var/run/docker.sock", os.path.expanduser("~/.docker/run/docker.sock")):
        if os.path.exists(path):
            return path
    return "/var/run/docker.sock"


SOCKET = default_socket()


class DockerError(Exception):
    pass


class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path: str, timeout: float):
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self) -> None:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        sock.connect(self._path)
        self.sock = sock


def request(method: str, path: str, *, socket_path: str = SOCKET, timeout: float = 10.0) -> Any:
    conn = _UnixConnection(socket_path, timeout)
    try:
        conn.request(method, path, headers={"Host": "docker"})
        response = conn.getresponse()
        body = response.read()
    finally:
        conn.close()
    if response.status >= 300:
        raise DockerError(f"{method} {path}: HTTP {response.status}: {body[:300]!r}")
    return json.loads(body) if body.strip() else None


def kill(container: str, signal: str = "KILL", **kw: Any) -> None:
    request("POST", f"/containers/{urllib.parse.quote(container)}/kill?signal={signal}", **kw)


def stats(container: str, **kw: Any) -> dict[str, Any]:
    return request("GET", f"/containers/{urllib.parse.quote(container)}/stats?stream=false&one-shot=true", **kw)


def inspect(container: str, **kw: Any) -> dict[str, Any]:
    return request("GET", f"/containers/{urllib.parse.quote(container)}/json", **kw)
