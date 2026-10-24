"""An interval timetable whose automatic schedule can be switched independently."""

from __future__ import annotations

from typing import Any

from airflow.timetables.base import DataInterval, DagRunInfo, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable
from pendulum import DateTime

from meridian_ops.control import schedule_enabled


class GatedMonthlyTimetable(Timetable):
    """Monthly data intervals gated by the Stage 2 schedule control flag."""

    periodic = True

    def __init__(self, market: str) -> None:
        self.market = market
        self._monthly = CronDataIntervalTimetable("0 0 1 * *", timezone="UTC")

    @property
    def summary(self) -> str:
        return f"monthly for {self.market}; controlled by `just schedule`"

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return self._monthly.infer_manual_data_interval(run_after=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        # Scheduler-created runs have an open-ended restriction. Airflow's
        # explicit backfill command supplies a bounded latest value and must
        # still be able to enumerate intervals while the recurring schedule
        # is disabled.
        if restriction.latest is None and not schedule_enabled(self.market):
            return None
        return self._monthly.next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )

    def serialize(self) -> dict[str, Any]:
        return {"market": self.market}

    @classmethod
    def deserialize(cls, value: dict[str, Any]) -> "GatedMonthlyTimetable":
        return cls(str(value["market"]))
