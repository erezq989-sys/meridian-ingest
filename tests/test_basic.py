"""Tests for the pipeline."""
import json
import pytest
from datetime import datetime

from meridian import db, trip, report


@pytest.fixture
def clean_db():
    """Set up and tear down test database."""
    db.init_schema()
    yield
    # Cleanup: truncate tables
    with db.get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE bronze_trips, silver_trips, quarantine, station_daily_trips")


def test_schema_detection():
    """Test schema version detection."""
    old_headers = ["start station id", "start station name", "Start Time"]
    new_headers = ["start_station_id", "start_station_name", "start_time"]
    
    assert trip.detect_schema(old_headers) == "old"
    assert trip.detect_schema(new_headers) == "new"


def test_normalize_old_schema():
    """Test normalization of old schema."""
    old_row = {
        "start station id": "JC115",
        "end station id": "JC116",
        "starttime": "2026-06-02 08:00:00",
        "stoptime": "2026-06-02 08:15:00",
    }
    
    normalized = trip.normalize_row(old_row, "old")
    assert normalized["start_station_id"] == "JC115"
    assert normalized["end_station_id"] == "JC116"


def test_validate_trip_valid():
    """Test validation of a valid trip."""
    valid_trip = {
        "start_station_id": "JC115",
        "end_station_id": "JC116",
        "start_time": "2026-06-02 08:00:00",
        "stop_time": "2026-06-02 08:15:00",
    }
    
    # Should not raise
    trip.validate_trip(valid_trip)


def test_validate_trip_missing_station():
    """Test validation rejects missing station."""
    invalid_trip = {
        "start_station_id": "",
        "end_station_id": "JC116",
        "start_time": "2026-06-02 08:00:00",
        "stop_time": "2026-06-02 08:15:00",
    }
    
    with pytest.raises(trip.TripValidationError):
        trip.validate_trip(invalid_trip)


def test_init_schema_creates_tables():
    """Schema initialization should succeed without reserved-keyword conflicts."""
    db.init_schema()

    with db.get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.bronze_trips')")
            assert cur.fetchone()[0] == "bronze_trips"


def test_inspect_bronze_empty(clean_db):
    """Test inspect on empty bronze layer."""
    result = report.inspect_bronze("jc", "2026-06")
    assert result["layer"] == "bronze"
    assert result["rows"] == 0
    assert result["objects"] == 0


def test_inspect_silver_with_quarantine(clean_db):
    """Test inspect silver layer with quarantine reasons."""
    # Insert a raw bronze row
    with db.get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze_trips (market, "window", source_key, source_timestamp, raw_row)
                VALUES (%s, %s, %s, %s, %s)
                """,
                ("jc", "2026-06", "test.zip", datetime.now(), json.dumps({"test": "data"}))
            )
            
            # Manually insert a quarantine
            cur.execute(
                """
                INSERT INTO quarantine (market, "window", reason, raw_row)
                VALUES (%s, %s, %s, %s)
                """,
                ("jc", "2026-06", "never_docked", json.dumps({"test": "data"}))
            )
    
    result = report.inspect_silver("jc", "2026-06")
    assert result["layer"] == "silver"
    assert result["rejects"] == 1
    assert result["reasons"]["never_docked"] == 1
