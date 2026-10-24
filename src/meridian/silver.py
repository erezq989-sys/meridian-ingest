"""Bronze-to-silver conformance job."""

from __future__ import annotations

import json
from typing import Any

from meridian import db
from meridian.trip import ConformedTrip, TripValidationError, conform_trip

BATCH_SIZE = 5_000
SILVER_INSERT = """
    INSERT INTO silver_trips
        (market, "window", trip_id, start_station_id, end_station_id,
         start_time, end_time, user_type, member_birth_year,
         member_gender, bike_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""
QUARANTINE_INSERT = """
    INSERT INTO quarantine (market, "window", reason, raw_row)
    VALUES (%s, %s, %s, %s)
"""


def _silver_row(
    market: str,
    window: str,
    bronze_id: int,
    trip: ConformedTrip,
) -> tuple[object, ...]:
    return (
        market,
        window,
        trip.trip_id or str(bronze_id),
        trip.start_station_id,
        trip.end_station_id,
        trip.start_time,
        trip.end_time,
        trip.user_type,
        trip.member_birth_year,
        trip.member_gender,
        trip.bike_id,
    )


def _flush(
    cursor: Any,
    valid_rows: list[tuple[object, ...]],
    rejected_rows: list[tuple[object, ...]],
) -> None:
    if valid_rows:
        cursor.executemany(SILVER_INSERT, valid_rows)
        valid_rows.clear()
    if rejected_rows:
        cursor.executemany(QUARANTINE_INSERT, rejected_rows)
        rejected_rows.clear()


def _classify_row(
    market: str,
    window: str,
    bronze_id: int,
    raw_row: dict[str, Any],
) -> tuple[str, tuple[object, ...]]:
    try:
        trip = conform_trip(raw_row)
    except TripValidationError as error:
        return (
            "rejected",
            (market, window, error.reason, json.dumps(raw_row)),
        )
    return "valid", _silver_row(market, window, bronze_id, trip)


def transform_to_silver(market: str, window: str) -> None:
    """Atomically replace one conformed market/month and its rejects."""
    with db.get_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                'DELETE FROM silver_trips WHERE market = %s AND "window" = %s',
                (market, window),
            )
            cursor.execute(
                'DELETE FROM quarantine WHERE market = %s AND "window" = %s',
                (market, window),
            )

        valid_rows: list[tuple[object, ...]] = []
        rejected_rows: list[tuple[object, ...]] = []
        with connection.cursor(
            name="silver_source"
        ) as source, connection.cursor() as sink:
            source.execute(
                """
                SELECT id, raw_row
                FROM bronze_trips
                WHERE market = %s AND "window" = %s
                ORDER BY id
                """,
                (market, window),
            )
            for bronze_id, raw_row in source:
                outcome, values = _classify_row(market, window, bronze_id, raw_row)
                if outcome == "valid":
                    valid_rows.append(values)
                else:
                    rejected_rows.append(values)
                if len(valid_rows) >= BATCH_SIZE or len(rejected_rows) >= BATCH_SIZE:
                    _flush(sink, valid_rows, rejected_rows)
            _flush(sink, valid_rows, rejected_rows)
