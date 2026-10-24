"""Command-line interface over independently callable pipeline jobs."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Sequence
from datetime import date
from typing import Any

from meridian import gold, ingest, report, silver

_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_DAY = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
_MARKETS = {"jc", "nyc"}


class CliUsageError(ValueError):
    """Invalid CLI coordinates or command shape."""


def _valid_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
    except ValueError:
        return False
    return True


def parse_trip_job(job: str) -> str:
    """Return the market encoded by a supported trips job."""
    prefix, separator, market = job.partition(":")
    if prefix != "trips" or not separator or market not in _MARKETS:
        raise CliUsageError(f"expected trips:jc or trips:nyc, got {job}")
    return market


def validate_window_grain(job: str, window: str) -> None:
    """Validate a coordinate at the job's declared grain."""
    if job.startswith("trips:"):
        parse_trip_job(job)
        if not _MONTH.fullmatch(window) or not _valid_date(f"{window}-01"):
            raise CliUsageError(
                f"trips job requires monthly window (YYYY-MM), got {window}"
            )
        return
    if job == "station-daily":
        if not _DAY.fullmatch(window) or not _valid_date(window):
            raise CliUsageError(
                f"station-daily job requires daily window (YYYY-MM-DD), got {window}"
            )
        return
    raise CliUsageError(f"unknown job {job}")


def run(layer: str, job: str, window: str) -> None:
    """Run one layer without terminating the calling Python process."""
    validate_window_grain(job, window)
    if layer == "ingest-to-bronze":
        ingest.ingest_to_bronze(parse_trip_job(job), window)
        return
    if layer == "transform-to-silver":
        silver.transform_to_silver(parse_trip_job(job), window)
        return
    if layer == "transform-to-gold":
        if job != "station-daily":
            raise CliUsageError("gold transform expects station-daily job")
        gold.transform_to_gold(window)
        return
    raise CliUsageError(f"unknown layer {layer}")


def inspect(layer: str, job: str, window: str) -> dict[str, Any]:
    """Read one layer coordinate without running migrations or transformations."""
    validate_window_grain(job, window)
    market = parse_trip_job(job)
    if layer == "bronze":
        return report.inspect_bronze(market, window)
    if layer == "silver":
        return report.inspect_silver(market, window)
    raise CliUsageError(f"cannot inspect {layer}")


def report_answer(question: str, market: str, station: str, day: str) -> dict[str, Any]:
    """Answer a supported business question."""
    if question != "daily-station-trips":
        raise CliUsageError(f"unknown question {question}")
    if market not in _MARKETS:
        raise CliUsageError(f"unknown market {market}")
    if not _DAY.fullmatch(day) or not _valid_date(day):
        raise CliUsageError(f"daily report requires YYYY-MM-DD, got {day}")
    return report.report_daily_station_trips(market, station, day)


def _json(data: dict[str, Any]) -> None:
    print(json.dumps(data, default=str))


def main(argv: Sequence[str] | None = None) -> int:
    """Parse process arguments; only this boundary converts errors to exit codes."""
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        if len(args) == 4 and args[0] == "run":
            run(args[1], args[2], args[3])
            return 0
        if len(args) == 4 and args[0] == "inspect":
            _json(inspect(args[1], args[2], args[3]))
            return 0
        if len(args) == 5 and args[0] == "report":
            _json(report_answer(args[1], args[2], args[3], args[4]))
            return 0
        raise CliUsageError(
            "usage: meridian run|inspect|report with the documented arguments"
        )
    except CliUsageError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
