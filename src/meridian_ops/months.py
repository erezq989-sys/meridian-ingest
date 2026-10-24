"""Calendar-month helpers shared by coverage and task bodies."""

from __future__ import annotations

import calendar
from datetime import date, timedelta


def parse_month(month: str) -> date:
    """Parse an exact YYYY-MM value as the first calendar day."""
    if len(month) != 7 or month[4] != "-":
        raise ValueError(f"expected month YYYY-MM, got {month}")
    try:
        parsed = date.fromisoformat(f"{month}-01")
    except ValueError as error:
        raise ValueError(f"expected month YYYY-MM, got {month}") from error
    if parsed.strftime("%Y-%m") != month:
        raise ValueError(f"expected month YYYY-MM, got {month}")
    return parsed


def next_month(month: str) -> str:
    """Return the month immediately after *month*."""
    parsed = parse_month(month)
    if parsed.month == 12:
        return f"{parsed.year + 1:04d}-01"
    return f"{parsed.year:04d}-{parsed.month + 1:02d}"


def iter_months(start: str, end: str) -> list[str]:
    """Return an inclusive, ascending calendar-month range."""
    if parse_month(start) > parse_month(end):
        raise ValueError(f"month range starts after it ends: {start} > {end}")
    result: list[str] = []
    current = start
    while True:
        result.append(current)
        if current == end:
            return result
        current = next_month(current)


def month_days(month: str) -> list[str]:
    """Return every ISO day in a calendar month."""
    first = parse_month(month)
    count = calendar.monthrange(first.year, first.month)[1]
    return [(first + timedelta(days=offset)).isoformat() for offset in range(count)]
