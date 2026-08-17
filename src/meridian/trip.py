"""Trip data parsing and validation."""
from datetime import datetime
from typing import Any

import dateutil.parser


class TripValidationError(Exception):
    """Raised when a trip row fails validation."""
    def __init__(self, message: str, row: dict[str, Any]) -> None:
        super().__init__(message)
        self.message = message
        self.row = row


def detect_schema(headers: list[str]) -> str:
    """
    Detect schema version from CSV headers.
    
    Old schema (pre-migration): spaces in names like "start station id"
    New schema (post-migration): underscores like "start_station_id"
    """
    # Check for new schema indicators
    if "start_station_id" in headers:
        return "new"
    # Check for old schema indicators
    if "start station id" in headers:
        return "old"
    # Fallback
    return "unknown"


def normalize_row(row: dict[str, Any], schema: str) -> dict[str, Any]:
    """
    Normalize row to a consistent schema.
    
    Returns dict with canonical keys.
    """
    if schema == "old":
        # Map old column names to new
        mapping = {
            "start station id": "start_station_id",
            "start station name": "start_station_name",
            "end station id": "end_station_id",
            "end station name": "end_station_name",
            "starttime": "start_time",
            "stoptime": "stop_time",
            "Start Time": "start_time",
            "Stop Time": "stop_time",
            "User Type": "user_type",
            "Birth Year": "member_birth_year",
            "Gender": "member_gender",
            "Bike ID": "bike_id",
        }
        normalized = {}
        for k, v in row.items():
            normalized[mapping.get(k, k)] = v
        return normalized
    else:
        # New schema or unknown: lowercase keys for consistency
        return {k.lower().replace(" ", "_"): v for k, v in row.items()}


def validate_trip(row: dict[str, Any]) -> None:
    """
    Validate that a trip row contains required fields.
    
    A row is valid if it has:
    - start_station_id (not null, not empty)
    - end_station_id (not null, not empty)
    - start_time (parseable)
    - end_time (parseable)
    """
    # Check start_station_id
    start_id = row.get("start_station_id", "").strip() if isinstance(row.get("start_station_id"), str) else ""
    if not start_id:
        raise TripValidationError("never_docked: missing start_station_id", row)
    
    # Check end_station_id
    end_id = row.get("end_station_id", "").strip() if isinstance(row.get("end_station_id"), str) else ""
    if not end_id:
        raise TripValidationError("never_docked: missing end_station_id", row)
    
    # Check start_time
    start_time = row.get("start_time") or row.get("starttime")
    if not start_time:
        raise TripValidationError("never_docked: missing start_time", row)
    
    # Try to parse start_time
    try:
        _ = dateutil.parser.parse(str(start_time))
    except Exception as e:
        raise TripValidationError(f"never_docked: invalid start_time: {e}", row) from e
    
    # Check end_time
    end_time = row.get("stop_time") or row.get("end_time")
    if not end_time:
        raise TripValidationError("never_docked: missing end_time", row)
    
    # Try to parse end_time
    try:
        _ = dateutil.parser.parse(str(end_time))
    except Exception as e:
        raise TripValidationError(f"never_docked: invalid end_time: {e}", row) from e


def parse_trip_time(time_str: str) -> datetime:
    """Parse trip time string to datetime."""
    return dateutil.parser.parse(time_str)  # type: ignore
