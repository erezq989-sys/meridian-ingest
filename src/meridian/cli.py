"""Command-line interface."""
import json
import sys
from typing import Any

from meridian import db, ingest, transform, report


def validate_window_grain(layer: str, job: str, window: str) -> None:
    """Validate that window grain matches job type."""
    # Extract job type
    if job.startswith("trips:"):
        # trips jobs expect YYYY-MM (monthly)
        if len(window) != 7 or window[4] != "-":
            print(f"ERROR: trips job requires monthly window (YYYY-MM), got {window}", file=sys.stderr)
            sys.exit(1)
    elif job == "station-daily":
        # station-daily job expects YYYY-MM-DD (daily)
        if len(window) != 10 or window[4] != "-" or window[7] != "-":
            print(f"ERROR: station-daily job requires daily window (YYYY-MM-DD), got {window}", file=sys.stderr)
            sys.exit(1)
    else:
        print(f"ERROR: Unknown job {job}", file=sys.stderr)
        sys.exit(1)


def output_json(data: dict[str, Any]) -> None:
    """Output a JSON object to stdout."""
    print(json.dumps(data, default=str))


def run(layer: str, job: str, window: str) -> None:
    """Run a job."""
    validate_window_grain(layer, job, window)
    
    if layer == "ingest-to-bronze":
        if not job.startswith("trips:"):
            print(f"ERROR: ingest expects trips job", file=sys.stderr)
            sys.exit(1)
        market = job.split(":")[1]
        db.init_schema()
        ingest.ingest_to_bronze(market, window)
    
    elif layer == "transform-to-silver":
        if not job.startswith("trips:"):
            print(f"ERROR: silver transform expects trips job", file=sys.stderr)
            sys.exit(1)
        market = job.split(":")[1]
        db.init_schema()
        transform.transform_to_silver(market, window)
    
    elif layer == "transform-to-gold":
        if job != "station-daily":
            print(f"ERROR: gold transform expects station-daily job", file=sys.stderr)
            sys.exit(1)
        db.init_schema()
        # Detect markets that have silver data for this day
        # Then transform to gold for each market
        day_date = window  # window is already validated to be YYYY-MM-DD
        with db.get_db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT market FROM silver_trips
                    WHERE DATE(start_time) = %s OR DATE(end_time) = %s
                    UNION
                    SELECT market FROM station_daily_trips WHERE day = %s
                    """,
                    (day_date, day_date, day_date)
                )
                markets = [row[0] for row in cur.fetchall()]
        
        if markets:
            for market in markets:
                transform.transform_to_gold(market, window)
        # If no silver data exists for this day, that's OK - just no gold to build
    
    else:
        print(f"ERROR: Unknown layer {layer}", file=sys.stderr)
        sys.exit(1)


def inspect(layer: str, job: str, window: str) -> None:
    """Inspect a layer."""
    validate_window_grain(layer, job, window)
    
    db.init_schema()
    
    if layer == "bronze":
        if not job.startswith("trips:"):
            print(f"ERROR: inspect bronze expects trips job", file=sys.stderr)
            sys.exit(1)
        market = job.split(":")[1]
        result = report.inspect_bronze(market, window)
    
    elif layer == "silver":
        if not job.startswith("trips:"):
            print(f"ERROR: inspect silver expects trips job", file=sys.stderr)
            sys.exit(1)
        market = job.split(":")[1]
        result = report.inspect_silver(market, window)
    
    else:
        print(f"ERROR: Cannot inspect {layer}", file=sys.stderr)
        sys.exit(1)
    
    output_json(result)


def report_cmd(question: str, market: str, station: str, day: str) -> None:
    """Report a business question."""
    if question != "daily-station-trips":
        print(f"ERROR: Unknown question {question}", file=sys.stderr)
        sys.exit(1)
    
    db.init_schema()
    result = report.report_daily_station_trips(market, station, day)
    output_json(result)


def main() -> None:
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: meridian run|inspect|report ...", file=sys.stderr)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "run":
        if len(sys.argv) != 5:
            print(f"Usage: meridian run <layer> <job> <window>", file=sys.stderr)
            sys.exit(1)
        run(sys.argv[2], sys.argv[3], sys.argv[4])
    
    elif command == "inspect":
        if len(sys.argv) != 5:
            print(f"Usage: meridian inspect <layer> <job> <window>", file=sys.stderr)
            sys.exit(1)
        inspect(sys.argv[2], sys.argv[3], sys.argv[4])
    
    elif command == "report":
        if len(sys.argv) != 6:
            print(f"Usage: meridian report <question> <market> <station> <day>", file=sys.stderr)
            sys.exit(1)
        report_cmd(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    
    else:
        print(f"ERROR: Unknown command {command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
