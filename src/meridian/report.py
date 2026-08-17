"""Reporting and inspection functions."""
import json
from typing import Any

from meridian import db


def inspect_bronze(market: str, window: str) -> dict[str, Any]:
    """Inspect bronze layer: return object and row counts."""
    with db.get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT source_key) as objects, COUNT(*) as rows
                FROM bronze_trips
                WHERE market = %s AND window = %s
                """,
                (market, window)
            )
            
            result = cur.fetchone()
            objects, rows = result if result else (0, 0)
            
            return {
                "layer": "bronze",
                "job": f"trips:{market}",
                "window": window,
                "objects": objects,
                "rows": rows,
            }


def inspect_silver(market: str, window: str) -> dict[str, Any]:
    """Inspect silver layer: return row counts and quarantine reasons."""
    with db.get_db() as conn:
        with conn.cursor() as cur:
            # Valid rows
            cur.execute(
                """
                SELECT COUNT(*) as rows
                FROM silver_trips
                WHERE market = %s AND window = %s
                """,
                (market, window)
            )
            
            rows = cur.fetchone()[0] if cur.fetchone() else 0
            
            # Quarantine reasons
            cur.execute(
                """
                SELECT reason, COUNT(*) as count
                FROM quarantine
                WHERE market = %s AND window = %s
                GROUP BY reason
                ORDER BY count DESC
                """,
                (market, window)
            )
            
            reasons = {}
            rejects = 0
            for reason, count in cur.fetchall():
                reasons[reason] = count
                rejects += count
            
            return {
                "layer": "silver",
                "job": f"trips:{market}",
                "window": window,
                "rows": rows,
                "rejects": rejects,
                "reasons": reasons,
            }


def report_daily_station_trips(market: str, station: str, day: str) -> dict[str, Any]:
    """Report departures and arrivals for a station on a day."""
    with db.get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT departures, arrivals
                FROM station_daily_trips
                WHERE market = %s AND station_id = %s AND day = %s
                """,
                (market, station, day)
            )
            
            result = cur.fetchone()
            departures, arrivals = result if result else (0, 0)
            
            return {
                "market": market,
                "station": station,
                "day": day,
                "departures": departures,
                "arrivals": arrivals,
            }
