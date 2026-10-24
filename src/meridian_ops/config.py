"""Committed Stage 2 market configuration."""

EARLIEST_WINDOWS: dict[str, str] = {
    "jc": "2021-01",
    "nyc": "2026-01",
}

DAG_IDS: dict[str, str] = {
    market: f"meridian_{market}_monthly" for market in EARLIEST_WINDOWS
}


def validate_market(market: str) -> str:
    """Return a configured market or fail with a stable message."""
    if market not in EARLIEST_WINDOWS:
        raise ValueError(f"unknown market {market}; expected jc or nyc")
    return market
