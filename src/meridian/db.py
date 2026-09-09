"""Database connection and schema management."""
import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg


def get_connection_string() -> str:
    """Build connection string from environment."""
    host = os.getenv("PGHOST", "localhost")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "")
    dbname = os.getenv("PGDATABASE", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"


@contextmanager
def get_db() -> Generator[psycopg.Connection[tuple[Any, ...]], None, None]:
    """Get a database connection."""
    conn = psycopg.connect(get_connection_string())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_schema() -> None:
    """Initialize database schema."""
    with get_db() as conn:
        with conn.cursor() as cur:
            # Bronze layer: raw published bytes
            cur.execute("""
                CREATE TABLE IF NOT EXISTS bronze_trips (
                    id BIGSERIAL PRIMARY KEY,
                    market TEXT NOT NULL,
                    "window" TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    source_timestamp TIMESTAMP NOT NULL,
                    raw_row JSONB NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bronze_unique_payload
                ON bronze_trips (market, "window", source_key, source_timestamp, ((raw_row)::text))
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_bronze_market_window ON bronze_trips (market, \"window\")")
            
            # Silver layer: conformed trips
            cur.execute("""
                CREATE TABLE IF NOT EXISTS silver_trips (
                    id BIGSERIAL PRIMARY KEY,
                    market TEXT NOT NULL,
                    "window" TEXT NOT NULL,
                    trip_id TEXT,
                    start_station_id TEXT NOT NULL,
                    end_station_id TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    user_type TEXT,
                    member_birth_year INT,
                    member_gender INT,
                    bike_id TEXT,
                    UNIQUE (market, "window", trip_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_silver_market_window ON silver_trips (market, \"window\")")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_silver_times ON silver_trips (start_time, end_time)")
            
            # Quarantine layer: rejected trips with reasons
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quarantine (
                    id BIGSERIAL PRIMARY KEY,
                    market TEXT NOT NULL,
                    "window" TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    raw_row JSONB NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_quarantine_market_window ON quarantine (market, \"window\")")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_quarantine_reason ON quarantine (reason)")
            
            # Gold layer: station-day facts
            cur.execute("""
                CREATE TABLE IF NOT EXISTS station_daily_trips (
                    id BIGSERIAL PRIMARY KEY,
                    market TEXT NOT NULL,
                    day DATE NOT NULL,
                    station_id TEXT NOT NULL,
                    departures INT DEFAULT 0,
                    arrivals INT DEFAULT 0,
                    UNIQUE (market, day, station_id)
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_gold_market_day ON station_daily_trips (market, day)")
