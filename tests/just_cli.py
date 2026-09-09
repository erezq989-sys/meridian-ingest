"""Thin wrappers around the project Just recipes for test usage."""

from __future__ import annotations

import json

import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


def _find_executable(name: str, candidates: Sequence[str]) -> str:
    """Return an executable path or a fallback command name."""
    found = shutil.which(name)
    if found:
        return found
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    return name


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _docker_executable() -> str:
    return _find_executable(
        "docker",
        [
            r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
            r"C:\Program Files\Docker\Docker\Docker.exe",
        ],
    )


def _just_executable() -> str:
    return _find_executable(
        "just",
        [
            r"C:\Users\erezq\AppData\Local\Microsoft\WinGet\Links\just.exe",
            r"C:\Users\erezq\AppData\Local\Programs\just\just.exe",
        ],
    )


@dataclass
class CommandResult:
    """Simple result object with exit_code and a readable description."""

    exit_code: int
    stdout: str = ""
    stderr: str = ""
    command: list[str] | None = None

    def describe(self) -> str:
        cmd = " ".join(self.command or [])
        return f"command: {cmd}\nstdout:\n{self.stdout}\nstderr:\n{self.stderr}"


def _run(cmd: list[str]) -> CommandResult:
    env = os.environ.copy()
    env.setdefault("COMPOSE_PROJECT_NAME", "meridian-ingest")
    proc = subprocess.run(cmd, cwd=_repo_root(), capture_output=True, text=True, env=env)
    return CommandResult(
        exit_code=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        command=cmd,
    )


def just_up() -> CommandResult:
    """Run the project `just up` recipe."""
    return _run([_just_executable(), "up"])


def just_down() -> CommandResult:
    """Run the project `just down` recipe."""
    return _run([_just_executable(), "down"])


def just_run(layer: str, job: str, window: str) -> CommandResult:
    """Run a project job via the `just run` recipe."""
    return _run([_just_executable(), "run", layer, job, window])


def just_inspect(layer: str, job: str, window: str) -> dict[str, object]:
    """Inspect a project layer and decode its JSON response."""
    result = _run([_just_executable(), "inspect", layer, job, window])
    assert result.exit_code == 0, result.describe()
    return json.loads(result.stdout)


def just_report(question: str, market: str, station: str, day: str) -> dict[str, object]:
    """Run a project report and decode its JSON response."""
    result = _run([_just_executable(), "report", question, market, station, day])
    assert result.exit_code == 0, result.describe()
    return json.loads(result.stdout)


__all__ = [
    "CommandResult",
    "just_up",
    "just_down",
    "just_run",
    "just_inspect",
    "just_report",
]
