"""Pytest configuration."""
import os
from pathlib import Path

import pytest

from meridian import db
from tests.just_cli import just_down, just_up
from tests.warehouse import Warehouse


def _project_volume_name() -> str:
    """Return the Docker volume name used for bronze data."""
    project_name = "meridian-ingest"
    return f"{project_name}_bronze_data"


def _running_in_container() -> bool:
    """Return True when tests are executing inside a container."""
    return Path("/.dockerenv").exists()


@pytest.fixture(scope="session", autouse=True)
def test_db_init():
    """Initialize test database at session start."""
    # Use the Compose service name from inside the app container.
    os.environ.setdefault("PGHOST", "postgres" if _running_in_container() else "localhost")
    os.environ.setdefault("PGPORT", "5432")
    os.environ.setdefault("PGUSER", "meridian")
    os.environ.setdefault("PGPASSWORD", "meridian")
    os.environ.setdefault("PGDATABASE", "trips")

    try:
        db.init_schema()
    except Exception as e:
        print(f"Warning: Could not initialize test database: {e}")
        # Tests may fail if database is not available
        pass


@pytest.fixture
def dsn() -> str:
    """The Postgres DSN expected by the stack lifecycle checks."""
    return "postgresql://meridian:meridian@localhost:5432/trips"


@pytest.fixture
def bronze_volume() -> str:
    """The Docker volume used for the bronze layer data."""
    return _project_volume_name()


@pytest.fixture(scope="session")
def warehouse() -> Warehouse:
    """Cache expensive bronze windows across the ingestion tests."""
    return Warehouse()


@pytest.fixture
def restored_stack() -> None:
    """Ensure the Compose stack is in a clean, started state for each test."""
    if _running_in_container():
        pytest.skip("stack lifecycle tests are managed by the outer Docker Compose process")
    just_down()
    result = just_up()
    assert result.exit_code == 0, result.describe()
    yield
    just_down()
