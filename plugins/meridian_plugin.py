"""Airflow registration for Meridian's schedule-gated timetable."""

from airflow.plugins_manager import AirflowPlugin

from meridian_ops.timetable import GatedMonthlyTimetable


class MeridianPlugin(AirflowPlugin):
    """Register Stage 2 timetable types for DAG serialization."""

    name = "meridian"
    timetables = [GatedMonthlyTimetable]
