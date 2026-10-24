"""Airflow task bodies that wrap, but do not modify, Stage 1 jobs."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from meridian import db, gold, ingest, s3
from meridian_ops.config import EARLIEST_WINDOWS, validate_market
from meridian_ops.control import coverage, incomplete_gold_days, load_time, record_load
from meridian_ops.months import month_days, parse_month


class GoldBatchError(RuntimeError):
    """One or more gold days failed after the remaining days finished."""

    def __init__(self, failed_days: list[str]) -> None:
        self.failed_days = failed_days
        super().__init__(f"gold batch failed for days: {', '.join(failed_days)}")


@contextmanager
def _window_lock(key: str) -> Generator[tuple[Any, Any], None, None]:
    """Serialize only work that can mutate the same Stage 1 coordinate."""
    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_lock(hashtext(%s))", (key,))
        try:
            yield connection, cursor
        finally:
            cursor.execute("SELECT pg_advisory_unlock(hashtext(%s))", (key,))


def month_is_published(market: str, month: str) -> bool:
    """Return whether the publisher currently exposes an archive for a month."""
    validate_market(market)
    parse_month(month)
    objects = s3.filter_by_market(s3.list_s3_objects(), market)
    return bool(s3.get_month_files(objects, month))


def resolve_month(
    market: str,
    run_type: str,
    explicit_month: str | None,
    interval_month: str | None,
) -> dict[str, object]:
    """Choose and validate exactly one month for an Airflow run."""
    validate_market(market)
    earliest = EARLIEST_WINDOWS[market]
    if explicit_month:
        parse_month(explicit_month)
        if explicit_month < earliest:
            raise ValueError(
                f"{market} history starts at {earliest}; cannot run {explicit_month}"
            )
        return {"month": explicit_month, "work": True}

    if run_type in {"scheduled", "backfill"}:
        if interval_month is None:
            raise ValueError(f"{run_type} run has no monthly data interval")
        parse_month(interval_month)
        if interval_month < earliest:
            raise ValueError(
                f"{market} history starts at {earliest}; cannot run {interval_month}"
            )
        # Backfills are explicit reprocessing requests. Only automatic schedule
        # runs suppress work for an already complete month.
        if run_type == "scheduled" and not incomplete_gold_days(market, interval_month):
            return {"month": interval_month, "work": False}
        return {"month": interval_month, "work": True}

    next_due = str(coverage(market)["next"])
    if not month_is_published(market, next_due):
        return {"month": None, "work": False}
    return {"month": next_due, "work": True}


def publication_ready(market: str, selection: dict[str, object]) -> bool:
    """Sensor predicate: skipped work is ready; real work waits for S3."""
    month = selection.get("month")
    return not selection.get("work") or (
        isinstance(month, str) and month_is_published(market, month)
    )


def _selected_month(selection: dict[str, object]) -> str | None:
    month = selection.get("month")
    return month if isinstance(month, str) else None


def run_ingest(
    market: str, selection: dict[str, object], run_id: str
) -> dict[str, object]:
    """Run Stage 1 bronze in-process, then record success."""
    month = _selected_month(selection)
    if month is None or not selection.get("work"):
        return {"month": month, "work": "skipped"}
    job = f"ingest-to-bronze trips:{market}"
    with _window_lock(f"bronze:{market}:{month}") as (_, cursor):
        ingest.ingest_to_bronze(market, month)
        record_load(cursor, market, job, month, run_id)
    return {"month": month, "work": "complete"}


def run_silver(
    market: str, selection: dict[str, object], run_id: str
) -> dict[str, object]:
    """Run Stage 1 silver in the app container, then record success."""
    month = _selected_month(selection)
    if month is None or not selection.get("work"):
        return {"month": month, "work": "skipped"}
    job = f"transform-to-silver trips:{market}"
    project_dir = Path(os.getenv("MERIDIAN_PROJECT_DIR", "/opt/meridian"))
    environment = os.environ.copy()
    environment.setdefault("COMPOSE_PROJECT_NAME", "meridian-ingest")
    with _window_lock(f"silver:{market}:{month}") as (_, cursor):
        result = subprocess.run(
            ["just", "run", "transform-to-silver", f"trips:{market}", month],
            cwd=project_dir,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "silver subprocess failed "
                f"with exit {result.returncode}: {result.stderr.strip()}"
            )
        record_load(cursor, market, job, month, run_id)
    return {"month": month, "work": "complete"}


def run_gold_batch(
    market: str, selection: dict[str, object], run_id: str
) -> dict[str, object]:
    """Run every missing gold day, continue after failures, and record successes."""
    month = _selected_month(selection)
    days = month_days(month) if month is not None else []
    if month is None or not selection.get("work"):
        return {"month": month, "work": "skipped", "days": len(days), "failed_days": []}

    silver_job = f"transform-to-silver trips:{market}"
    gold_job = "transform-to-gold station-daily"
    failed: list[str] = []
    for day in days:
        try:
            # The Stage 1 day job covers every market, so the lock is day-wide.
            with _window_lock(f"gold:{day}") as (_, cursor):
                silver_time = load_time(cursor, market, silver_job, month)
                if silver_time is None:
                    raise RuntimeError(f"silver was not recorded for {market} {month}")
                existing = load_time(cursor, market, gold_job, day)
                if existing is not None and existing > silver_time:
                    continue
                gold.transform_to_gold(day)
                record_load(cursor, market, gold_job, day, run_id)
        except Exception:
            failed.append(day)
    if failed:
        raise GoldBatchError(failed)
    return {
        "month": month,
        "work": "complete",
        "days": len(days),
        "failed_days": [],
    }


def run_context_values(context: dict[str, Any]) -> tuple[str, str | None, str | None]:
    """Extract run type, configured month, and interval month from Airflow context."""
    dag_run = context["dag_run"]
    raw_type = dag_run.run_type
    run_type = str(getattr(raw_type, "value", raw_type))
    explicit = dag_run.conf.get("month") if dag_run.conf else None
    interval_start = context.get("data_interval_start")
    interval_month = (
        interval_start.strftime("%Y-%m") if interval_start is not None else None
    )
    return run_type, explicit, interval_month
