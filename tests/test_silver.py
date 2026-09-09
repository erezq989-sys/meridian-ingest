"""`just run transform-to-silver` integration tests."""

from __future__ import annotations

import pytest

from tests.just_cli import just_inspect, just_run
from tests.warehouse import Warehouse

pytestmark = pytest.mark.slow


def test_conform_exits_zero(warehouse: Warehouse) -> None:
    """The silver transform should complete for a valid bronze window."""
    warehouse.bronze("trips:jc", "2021-01")
    result = just_run("transform-to-silver", "trips:jc", "2021-01")
    assert result.exit_code == 0, result.describe()


def test_conform_counts_rows_and_rejects(warehouse: Warehouse) -> None:
    """Silver counts should match the published rows and quarantine rejects."""
    warehouse.silver("trips:nyc", "2024-01")

    silver = just_inspect("silver", "trips:nyc", "2024-01")

    assert silver["rows"] == 1881977
    assert silver["rejects"] == 6108
    assert sum(silver["reasons"].values()) == silver["rejects"]


def test_no_row_is_lost_between_bronze_and_silver(warehouse: Warehouse) -> None:
    """Every bronze row should become either silver data or a quarantine row."""
    warehouse.silver("trips:jc", "2021-02")

    bronze = just_inspect("bronze", "trips:jc", "2021-02")
    silver = just_inspect("silver", "trips:jc", "2021-02")

    assert silver["rows"] + silver["rejects"] == bronze["rows"]


def test_conform_is_idempotent(warehouse: Warehouse) -> None:
    """Rerunning the silver transform should not change its inspection result."""
    warehouse.silver("trips:jc", "2021-02")
    first = just_inspect("silver", "trips:jc", "2021-02")

    result = just_run("transform-to-silver", "trips:jc", "2021-02")
    assert result.exit_code == 0, result.describe()

    assert just_inspect("silver", "trips:jc", "2021-02") == first
