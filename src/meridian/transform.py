"""Transform jobs: silver (conformance) and gold (facts)."""
from datetime import datetime, date
from typing import Any

import dateutil.parser

from meridian import db
from meridian.trip import detect_schema, normalize_row, validate_trip, TripValidationError


def transform_to_silver(market: str, window: str) -> None:
    """
    Transform bronze to silver: conform schema, validate, quarantine rejects.
    
    A trip is valid if it has start_station_id, end_station_id, start_time, end_time.
    """
    with db.get_db() as conn:
        with conn.cursor() as cur:
            # Get all raw rows for this market/window from bronze
            cur.execute(
                """
                SELECT id, raw_row FROM bronze_trips
                WHERE market = %s AND "window" = %s
                """,
                (market, window)
            )
            
            rows = cur.fetchall()
            
            for bronze_id, raw_row_json in rows:
                raw_row = raw_row_json
                
                # Try to normalize and validate
                try:
                    # Detect schema from the raw row keys
                    schema = detect_schema(list(raw_row.keys()))
                    normalized = normalize_row(raw_row, schema)
                    validate_trip(normalized)
                    
                    # Trip is valid, insert into silver
                    start_time_str = normalized.get("start_time", "")
                    end_time_str = normalized.get("stop_time") or normalized.get("end_time", "")
                    
                    start_time = dateutil.parser.parse(start_time_str)
                    end_time = dateutil.parser.parse(end_time_str)
                    
                    trip_id = normalized.get("trip_id") or f"{bronze_id}"
                    
                    cur.execute(
                        """
                        INSERT INTO silver_trips (market, "window", trip_id, start_station_id, end_station_id,
                                                   start_time, end_time, user_type, member_birth_year, member_gender, bike_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            market,
                            window,
                            trip_id,
                            normalized.get("start_station_id"),
                            normalized.get("end_station_id"),
                            start_time,
                            end_time,
                            normalized.get("user_type"),
                            int(normalized.get("member_birth_year", 0)) if normalized.get("member_birth_year") else None,
                            int(normalized.get("member_gender", 0)) if normalized.get("member_gender") else None,
                            normalized.get("bike_id"),
                        )
                    )
                
                except TripValidationError as e:
                    # Quarantine the reject
                    reason = e.message.split(":")[0] if ":" in e.message else e.message
                    cur.execute(
                        """
                        INSERT INTO quarantine (market, "window", reason, raw_row)
                        VALUES (%s, %s, %s, %s)
                        """,
                        (market, window, reason, raw_row_json)
                    )


def transform_to_gold(market: str, day: str) -> None:
    """
    Transform silver to gold: aggregate station-day trip counts.
    
    Counts departures (by start time) and arrivals (by end time) per station per day.
    """
    # Parse day as ISO date
    day_date = datetime.fromisoformat(day).date()
    
    with db.get_db() as conn:
        with conn.cursor() as cur:
            # Count departures per station
            cur.execute(
                """
                SELECT start_station_id, DATE(start_time), COUNT(*) as count
                FROM silver_trips
                WHERE market = %s AND DATE(start_time) = %s
                GROUP BY start_station_id, DATE(start_time)
                """,
                (market, day_date)
            )
            
            departures = {}
            for row in cur.fetchall():
                if row is not None:
                    station_id, trip_date, count = row
                    departures[station_id] = count
            
            # Count arrivals per station
            cur.execute(
                """
                SELECT end_station_id, DATE(end_time), COUNT(*) as count
                FROM silver_trips
                WHERE market = %s AND DATE(end_time) = %s
                GROUP BY end_station_id, DATE(end_time)
                """,
                (market, day_date)
            )
            
            arrivals = {}
            for row in cur.fetchall():
                if row is not None:
                    station_id, trip_date, count = row
                    arrivals[station_id] = count
            
            # Merge all stations (only if there's activity)
            all_stations = set(departures.keys()) | set(arrivals.keys())
            
            # Upsert into gold layer (only for stations with activity)
            for station_id in all_stations:
                dep_count = departures.get(station_id, 0)
                arr_count = arrivals.get(station_id, 0)
                
                # Only insert if there's activity
                if dep_count > 0 or arr_count > 0:
                    cur.execute(
                        """
                        INSERT INTO station_daily_trips (market, day, station_id, departures, arrivals)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (market, day, station_id) DO UPDATE SET
                            departures = %s,
                            arrivals = %s
                        """,
                        (
                            market,
                            day_date,
                            station_id,
                            dep_count,
                            arr_count,
                            dep_count,
                            arr_count,
                        )
                    )
