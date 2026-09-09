"""`just run transform-to-gold` and `just report` integration tests."""

from __future__ import annotations

import pytest

from tests.just_cli import just_report, just_run
from tests.warehouse import Warehouse

pytestmark = pytest.mark.slow

FACT = "station-daily"
DAY = "2026-06-02"
TRIPS_WINDOW = "2026-06"
EXPECTED = {"departures": 216, "arrivals": 209}


@pytest.fixture(scope="module")
def gold(warehouse: Warehouse) -> Warehouse:
    """Bronze and silver first because gold reads conformed silver data."""
    warehouse.silver("trips:jc", TRIPS_WINDOW)
    warehouse.gold(FACT, DAY)
    return warehouse


def test_transform_to_gold_exits_zero(warehouse: Warehouse) -> None:
    """The gold transform should complete for a valid station-day window."""
    warehouse.silver("trips:jc", TRIPS_WINDOW)
    result = just_run("transform-to-gold", FACT, DAY)
    assert result.exit_code == 0, result.describe()


def test_report_answers_departures_and_arrivals(gold: Warehouse) -> None:
    """The station report should return the expected business answer."""
    answer = just_report("daily-station-trips", "jc", "JC115", DAY)

    assert answer["departures"] == EXPECTED["departures"]
    assert answer["arrivals"] == EXPECTED["arrivals"]


def test_gold_is_idempotent(gold: Warehouse) -> None:
    """Rerunning gold should preserve the station report."""
    first = just_report("daily-station-trips", "jc", "JC115", DAY)

    result = just_run("transform-to-gold", FACT, DAY)
    assert result.exit_code == 0, result.describe()

    second = just_report("daily-station-trips", "jc", "JC115", DAY)
    assert second == first
    assert second["departures"] == EXPECTED["departures"]
    assert second["arrivals"] == EXPECTED["arrivals"]
