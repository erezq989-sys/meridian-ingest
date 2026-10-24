"""Canonical trip-row normalization and validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import dateutil.parser

# Canonical field -> every normalized publisher header that has represented it.
FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "trip_id": ("trip_id", "ride_id"),
    "start_station_id": ("start_station_id",),
    "start_station_name": ("start_station_name",),
    "end_station_id": ("end_station_id",),
    "end_station_name": ("end_station_name",),
    "start_time": ("start_time", "starttime", "started_at"),
    "end_time": ("end_time", "stop_time", "stoptime", "ended_at"),
    "user_type": ("user_type", "usertype", "member_casual"),
    "member_birth_year": ("member_birth_year", "birth_year"),
    "member_gender": ("member_gender", "gender"),
    "bike_id": ("bike_id", "bikeid"),
}


class TripValidationError(ValueError):
    """A known source-data defect that belongs in quarantine."""

    def __init__(
        self,
        reason: str,
        row: dict[str, Any],
        detail: str | None = None,
    ) -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.row = row


@dataclass(frozen=True)
class ConformedTrip:
    """Typed values ready for insertion into the silver layer."""

    trip_id: str | None
    start_station_id: str
    end_station_id: str
    start_time: datetime
    end_time: datetime
    user_type: str | None
    member_birth_year: int | None
    member_gender: int | None
    bike_id: str | None


def _header_name(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def detect_schema(headers: list[str]) -> str:
    """Identify the publisher era for diagnostics."""
    lowered = {header.strip().lower() for header in headers}
    if {"started_at", "ended_at", "ride_id", "start_station_id"} & lowered:
        return "new"
    if {"starttime", "stoptime", "start station id"} & lowered:
        return "old"
    return "unknown"


def normalize_row(row: dict[str, Any], schema: str | None = None) -> dict[str, Any]:
    """Map every supported publisher era into one canonical field set."""
    del schema  # Detection is informative; aliases, not branches, drive conformance.
    by_header = {_header_name(str(key)): value for key, value in row.items()}
    normalized: dict[str, Any] = {}
    for canonical, aliases in FIELD_ALIASES.items():
        for alias in aliases:
            if alias in by_header:
                normalized[canonical] = by_header[alias]
                break
    return normalized


def _required_text(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise TripValidationError(f"missing_{field}", row)
    return value.strip()


def _required_time(row: dict[str, Any], field: str) -> datetime:
    value = row.get(field)
    if value is None or not str(value).strip():
        raise TripValidationError(f"missing_{field}", row)
    try:
        return dateutil.parser.parse(str(value))
    except (ValueError, TypeError, OverflowError) as error:
        raise TripValidationError(
            f"invalid_{field}", row, f"invalid {field}: {error}"
        ) from error


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    """Coerce optional legacy demographics without rejecting an otherwise valid trip."""
    text = _optional_text(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except (ValueError, TypeError, OverflowError):
        return None


def conform_trip(row: dict[str, Any]) -> ConformedTrip:
    """Normalize and type one source row, raising only known validation defects."""
    normalized = normalize_row(row, detect_schema(list(row)))
    return ConformedTrip(
        trip_id=_optional_text(normalized.get("trip_id")),
        start_station_id=_required_text(normalized, "start_station_id"),
        end_station_id=_required_text(normalized, "end_station_id"),
        start_time=_required_time(normalized, "start_time"),
        end_time=_required_time(normalized, "end_time"),
        user_type=_optional_text(normalized.get("user_type")),
        member_birth_year=_optional_int(normalized.get("member_birth_year")),
        member_gender=_optional_int(normalized.get("member_gender")),
        bike_id=_optional_text(normalized.get("bike_id")),
    )


def validate_trip(row: dict[str, Any]) -> None:
    """Validate a canonical row while preserving the Stage 1 public API."""
    normalized = normalize_row(row)
    _required_text(normalized, "start_station_id")
    _required_text(normalized, "end_station_id")
    _required_time(normalized, "start_time")
    _required_time(normalized, "end_time")
