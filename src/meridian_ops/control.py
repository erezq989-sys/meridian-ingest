"""Stage 2 control-table access and computed coverage."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from meridian import db
from meridian_ops.config import EARLIEST_WINDOWS, validate_market
from meridian_ops.months import iter_months, month_days, next_month


@dataclass(frozen=True)
class LoadRecord:
    """One successful operational load."""

    market: str
    job: str
    window: str
    loaded_at: datetime


def init_control_schema() -> None:
    """Create operational tables without changing Stage 1 DDL."""
    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS operational_loads (
                market TEXT NOT NULL,
                job TEXT NOT NULL,
                "window" TEXT NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                airflow_run_id TEXT NOT NULL,
                PRIMARY KEY (market, job, "window")
            )
            """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS operational_schedules (
                market TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                changed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """)
        cursor.executemany(
            """
            INSERT INTO operational_schedules (market, enabled)
            VALUES (%s, FALSE)
            ON CONFLICT (market) DO NOTHING
            """,
            [(market,) for market in EARLIEST_WINDOWS],
        )


def record_load(cursor: Any, market: str, job: str, window: str, run_id: str) -> None:
    """Record a successful job; repeated recording is harmless."""
    cursor.execute(
        """
        INSERT INTO operational_loads
            (market, job, "window", loaded_at, airflow_run_id)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s)
        ON CONFLICT (market, job, "window") DO UPDATE SET
            loaded_at = EXCLUDED.loaded_at,
            airflow_run_id = EXCLUDED.airflow_run_id
        """,
        (market, job, window, run_id),
    )


def load_time(cursor: Any, market: str, job: str, window: str) -> datetime | None:
    """Return the last successful load time for one control coordinate."""
    cursor.execute(
        """
        SELECT loaded_at FROM operational_loads
        WHERE market = %s AND job = %s AND "window" = %s
        """,
        (market, job, window),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def set_schedule(market: str, enabled: bool) -> None:
    """Persist the requested schedule state."""
    validate_market(market)
    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO operational_schedules (market, enabled, changed_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (market) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                changed_at = EXCLUDED.changed_at
            """,
            (market, enabled),
        )


def schedule_enabled(market: str) -> bool:
    """Read whether automatic scheduling is enabled for a market."""
    validate_market(market)
    try:
        with db.get_db() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT enabled FROM operational_schedules WHERE market = %s",
                (market,),
            )
            row = cursor.fetchone()
            return bool(row and row[0])
    except Exception:
        # DAG parsing can begin just before operational initialization finishes.
        return False


def _records(market: str) -> list[LoadRecord]:
    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT market, job, "window", loaded_at
            FROM operational_loads
            WHERE market = %s
            ORDER BY loaded_at
            """,
            (market,),
        )
        return [LoadRecord(*row) for row in cursor.fetchall()]


def coverage(market: str) -> dict[str, object]:
    """Compute coverage from successful load records, never stored state."""
    validate_market(market)
    earliest = EARLIEST_WINDOWS[market]
    records = _records(market)
    silver_job = f"transform-to-silver trips:{market}"
    silver = {
        record.window: record.loaded_at
        for record in records
        if record.job == silver_job
    }
    gold = {
        record.window: record.loaded_at
        for record in records
        if record.job == "transform-to-gold station-daily"
    }

    def complete(month: str) -> bool:
        silver_time = silver.get(month)
        return silver_time is not None and all(
            gold.get(day) is not None and gold[day] > silver_time
            for day in month_days(month)
        )

    complete_months = sorted(month for month in silver if complete(month))
    newest_complete = complete_months[-1] if complete_months else None
    watermark: str | None = None
    next_due = earliest
    while complete(next_due):
        watermark = next_due
        next_due = next_month(next_due)

    gaps = (
        [
            month
            for month in iter_months(earliest, newest_complete)
            if not complete(month)
        ]
        if newest_complete is not None
        else []
    )
    return {
        "job": f"trips:{market}",
        "earliest": earliest,
        "watermark": watermark,
        "complete": len(complete_months),
        "gaps": gaps,
        "next": next_due,
    }


def incomplete_gold_days(market: str, month: str) -> list[str]:
    """Return days without a post-silver gold record."""
    records = _records(market)
    silver_job = f"transform-to-silver trips:{market}"
    silver_time = next(
        (
            record.loaded_at
            for record in reversed(records)
            if record.job == silver_job and record.window == month
        ),
        None,
    )
    gold = {
        record.window: record.loaded_at
        for record in records
        if record.job == "transform-to-gold station-daily"
    }
    if silver_time is None:
        return month_days(month)
    return [
        day
        for day in month_days(month)
        if gold.get(day) is None or gold[day] <= silver_time
    ]


def month_for_run(market: str, run_id: str) -> str | None:
    """Resolve the monthly silver window recorded by one Airflow run."""
    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT "window"
            FROM operational_loads
            WHERE market = %s
              AND airflow_run_id = %s
              AND job = %s
            LIMIT 1
            """,
            (market, run_id, f"transform-to-silver trips:{market}"),
        )
        row = cursor.fetchone()
        return str(row[0]) if row else None
