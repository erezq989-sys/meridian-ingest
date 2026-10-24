"""Unit coverage for Stage 2 operational semantics."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Iterator
from unittest.mock import Mock

import pytest

from meridian_ops import cli, control, jobs
from meridian_ops.control import LoadRecord
from meridian_ops.months import iter_months, month_days, next_month


def _complete_month(market: str, month: str, silver_time: datetime) -> list[LoadRecord]:
    records = [
        LoadRecord(
            market,
            f"transform-to-silver trips:{market}",
            month,
            silver_time,
        )
    ]
    records.extend(
        LoadRecord(
            market,
            "transform-to-gold station-daily",
            day,
            silver_time + timedelta(seconds=1),
        )
        for day in month_days(month)
    )
    return records


def test_month_helpers_follow_calendar_boundaries() -> None:
    assert next_month("2021-12") == "2022-01"
    assert iter_months("2021-11", "2022-02") == [
        "2021-11",
        "2021-12",
        "2022-01",
        "2022-02",
    ]
    assert len(month_days("2021-02")) == 28
    assert len(month_days("2024-02")) == 29


def test_coverage_computes_watermark_gap_and_next(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    records = []
    for offset, month in enumerate(["2021-01", "2021-02", "2021-04"]):
        records.extend(_complete_month("jc", month, base + timedelta(days=offset)))
    monkeypatch.setattr(control, "_records", lambda market: records)

    assert control.coverage("jc") == {
        "job": "trips:jc",
        "earliest": "2021-01",
        "watermark": "2021-02",
        "complete": 3,
        "gaps": ["2021-03"],
        "next": "2021-03",
    }


def test_new_silver_record_invalidates_older_gold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_silver = datetime(2026, 1, 1, tzinfo=UTC)
    records = _complete_month("jc", "2021-01", old_silver)
    records.append(
        LoadRecord(
            "jc",
            "transform-to-silver trips:jc",
            "2021-01",
            old_silver + timedelta(days=2),
        )
    )
    monkeypatch.setattr(control, "_records", lambda market: records)

    result = control.coverage("jc")

    assert result["watermark"] is None
    assert result["complete"] == 0
    assert result["next"] == "2021-01"


def test_named_month_before_market_history_fails_immediately() -> None:
    with pytest.raises(ValueError, match="history starts at 2021-01"):
        jobs.resolve_month("jc", "manual", "2019-06", None)


def test_windowless_manual_run_uses_selected_control_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "month_for_run", lambda market, run_id: "2021-03")
    run = {
        "dag_run_id": "manual-run",
        "run_type": "manual",
        "conf": {},
        "data_interval_start": "2026-09-01T00:00:00Z",
    }

    assert cli._month_from_run(run, "jc") == "2021-03"


def test_skipped_windowless_manual_run_reports_null_month(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "month_for_run", lambda market, run_id: None)
    run = {
        "dag_run_id": "manual-run",
        "run_type": "manual",
        "conf": {},
        "data_interval_start": "2026-09-01T00:00:00Z",
    }

    assert cli._month_from_run(run, "jc") is None


def test_failed_ingest_is_never_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    @contextmanager
    def fake_lock(key: str) -> Iterator[tuple[object, object]]:
        yield object(), object()

    record = Mock()
    monkeypatch.setattr(jobs, "_window_lock", fake_lock)
    monkeypatch.setattr(
        jobs.ingest,
        "ingest_to_bronze",
        Mock(side_effect=RuntimeError("download failed")),
    )
    monkeypatch.setattr(jobs, "record_load", record)

    with pytest.raises(RuntimeError, match="download failed"):
        jobs.run_ingest("jc", {"month": "2021-01", "work": True}, "run-1")

    record.assert_not_called()


def test_gold_finishes_other_days_then_names_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    @contextmanager
    def fake_lock(key: str) -> Iterator[tuple[object, object]]:
        yield object(), object()

    silver_time = datetime(2026, 1, 1, tzinfo=UTC)

    def fake_load_time(cursor: Any, market: str, job: str, window: str) -> Any:
        if job.startswith("transform-to-silver"):
            return silver_time
        return None

    failed_day = "2021-02-03"
    transformed: list[str] = []

    def fake_gold(day: str) -> None:
        transformed.append(day)
        if day == failed_day:
            raise RuntimeError("bad day")

    record = Mock()
    monkeypatch.setattr(jobs, "_window_lock", fake_lock)
    monkeypatch.setattr(jobs, "load_time", fake_load_time)
    monkeypatch.setattr(jobs.gold, "transform_to_gold", fake_gold)
    monkeypatch.setattr(jobs, "record_load", record)

    with pytest.raises(jobs.GoldBatchError) as caught:
        jobs.run_gold_batch("jc", {"month": "2021-02", "work": True}, "run-1")

    assert caught.value.failed_days == [failed_day]
    assert transformed == month_days("2021-02")
    assert record.call_count == 27
