"""Probe Docker and Postgres state for stack lifecycle tests."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
from urllib.parse import urlparse


def _find_executable(name: str, candidates: list[str]) -> str:
    found = shutil.which(name)
    if found:
        return found
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return name


def _docker_executable() -> str:
    return _find_executable(
        "docker",
        [
            r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
            r"C:\Program Files\Docker\Docker\Docker.exe",
        ],
    )


def _parse_dsn(dsn: str) -> tuple[str, int]:
    parsed = urlparse(dsn)
    if parsed.hostname:
        host = parsed.hostname
        port = parsed.port or 5432
        return host, port

    if ":" in dsn:
        host, port_text = dsn.rsplit(":", 1)
        return host, int(port_text)

    return "localhost", 5432


def postgres_is_up(dsn: str) -> bool:
    """Return True when a Postgres server is accepting TCP connections."""
    host, port = _parse_dsn(dsn)
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def postgres_is_down(dsn: str) -> bool:
    """Return True when no Postgres server is listening at the DSN."""
    return not postgres_is_up(dsn)


def volume_exists(volume_name: str) -> bool:
    """Return True when the named Docker volume exists."""
    proc = subprocess.run(
        [_docker_executable(), "volume", "inspect", volume_name],
        capture_output=True,
        text=True,
    )
    return proc.returncode == 0


def volume_is_gone(volume_name: str) -> bool:
    """Return True when the named Docker volume was removed."""
    return not volume_exists(volume_name)


__all__ = [
    "postgres_is_up",
    "postgres_is_down",
    "volume_exists",
    "volume_is_gone",
]
