"""Silver-to-gold station-day aggregation job."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from meridian import db


def _markets_for_day(cursor: Any, day: date) -> list[str]:
    """Return markets with silver input or an existing gold coordinate."""
    cursor.execute(
        """
        SELECT DISTINCT market
        FROM silver_trips
        WHERE DATE(start_time) = %s OR DATE(end_time) = %s
        UNION
        SELECT market FROM station_daily_trips WHERE day = %s
        ORDER BY market
        """,
        (day, day, day),
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _replace_station_day(cursor: Any, market: str, day: date) -> None:
    cursor.execute(
        "DELETE FROM station_daily_trips WHERE market = %s AND day = %s",
        (market, day),
    )
    cursor.execute(
        """
        WITH departures AS (
            SELECT start_station_id AS station_id, COUNT(*) AS departures
            FROM silver_trips
            WHERE market = %s AND DATE(start_time) = %s
            GROUP BY start_station_id
        ),
        arrivals AS (
            SELECT end_station_id AS station_id, COUNT(*) AS arrivals
            FROM silver_trips
            WHERE market = %s AND DATE(end_time) = %s
            GROUP BY end_station_id
        )
        INSERT INTO station_daily_trips
            (market, day, station_id, departures, arrivals)
        SELECT
            %s,
            %s,
            COALESCE(departures.station_id, arrivals.station_id),
            COALESCE(departures.departures, 0),
            COALESCE(arrivals.arrivals, 0)
        FROM departures
        FULL OUTER JOIN arrivals USING (station_id)
        """,
        (market, day, market, day, market, day),
    )


def transform_to_gold(day: str) -> None:
    """Atomically replace the complete station-day window across all markets."""
    day_date = datetime.fromisoformat(day).date()
    with db.get_db() as connection, connection.cursor() as cursor:
        for market in _markets_for_day(cursor, day_date):
            _replace_station_day(cursor, market, day_date)
