"""Pytest configuration."""
import os
import pytest
from meridian import db


@pytest.fixture(scope="session", autouse=True)
def test_db_init():
    """Initialize test database at session start."""
    # Set test database environment variables
    os.environ.setdefault("PGHOST", "localhost")
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
