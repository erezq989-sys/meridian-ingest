"""Lifecycle coverage for the project's `just up` and `just down` recipes."""

from __future__ import annotations

import time
from collections.abc import Callable

from tests.just_cli import just_down, just_up
from tests.stack_probe import postgres_is_down, postgres_is_up, volume_exists, volume_is_gone


def wait_for(predicate: Callable[[], bool], iterations: int = 10, delay: int = 2) -> bool:
    """Poll a stack condition for a bounded amount of time."""
    for attempt in range(iterations):
        if predicate():
            return True
        if attempt < iterations - 1:
            time.sleep(delay)
    return False


def test_up_leaves_postgres_available_at_the_agreed_dsn(restored_stack: None, dsn: str) -> None:
    assert just_down().exit_code == 0
    result = just_up()
    assert result.exit_code == 0, result.describe()
    assert wait_for(lambda: postgres_is_up(dsn)), f"nothing accepting connections at {dsn}"


def test_up_creates_the_bronze_data_volume(restored_stack: None, bronze_volume: str) -> None:
    assert just_down().exit_code == 0
    result = just_up()
    assert result.exit_code == 0, result.describe()
    assert wait_for(lambda: volume_exists(bronze_volume)), f"no volume named {bronze_volume}"


def test_down_takes_postgres_down(restored_stack: None, dsn: str) -> None:
    assert postgres_is_up(dsn), "the stack was not up to begin with"
    result = just_down()
    assert result.exit_code == 0, result.describe()
    assert wait_for(lambda: postgres_is_down(dsn)), f"something is still listening at {dsn}"


def test_down_deletes_the_bronze_data_volume(restored_stack: None, bronze_volume: str) -> None:
    assert volume_exists(bronze_volume), "the volume was not there to begin with"
    result = just_down()
    assert result.exit_code == 0, result.describe()
    assert wait_for(lambda: volume_is_gone(bronze_volume)), f"volume {bronze_volume} survived `just down`"
