"""`just run ingest-to-bronze` integration tests."""

from __future__ import annotations

import pytest

from tests.just_cli import just_inspect, just_run
from tests.warehouse import Warehouse

pytestmark = pytest.mark.slow

PUBLISHED_ROWS = [
    pytest.param("trips:nyc", "2024-01", 1888085, id="standard"),
    pytest.param("trips:jc", "2021-02", 4881, id="after-schema-change"),
    pytest.param("trips:jc", "2021-01", 11624, id="before-schema-change"),
    pytest.param("trips:nyc", "2018-04", 1307543, id="multiple-files-one-window"),
]


def test_ingest_exits_zero() -> None:
    """The bronze recipe should complete successfully for a valid window."""
    result = just_run("ingest-to-bronze", "trips:jc", "2021-01")
    assert result.exit_code == 0, result.describe()


@pytest.mark.parametrize(("job", "window", "published_rows"), PUBLISHED_ROWS)
def test_ingest_lands_the_published_row_count(
    warehouse: Warehouse, job: str, window: str, published_rows: int
) -> None:
    """Each supported archive coordinate should land its published row count."""
    warehouse.bronze(job, window)

    assert just_inspect("bronze", job, window)["rows"] == published_rows


def test_ingest_is_idempotent(warehouse: Warehouse) -> None:
    """Rerunning a bronze window should not change its inspection result."""
    warehouse.bronze("trips:jc", "2021-02")
    first = just_inspect("bronze", "trips:jc", "2021-02")

    result = just_run("ingest-to-bronze", "trips:jc", "2021-02")
    assert result.exit_code == 0, result.describe()

    assert just_inspect("bronze", "trips:jc", "2021-02") == first


def test_ingesting_a_window_leaves_the_others_untouched(warehouse: Warehouse) -> None:
    """Ingesting one market/window must not modify another bronze coordinate."""
    warehouse.bronze("trips:nyc", "2018-04")
    before = just_inspect("bronze", "trips:nyc", "2018-04")

    result = just_run("ingest-to-bronze", "trips:jc", "2021-01")
    assert result.exit_code == 0, result.describe()

    assert just_inspect("bronze", "trips:nyc", "2018-04") == before
