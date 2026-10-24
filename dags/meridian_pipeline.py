"""Airflow wiring for Meridian's two independent monthly pipelines."""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG, get_current_context, task

from meridian_ops.config import DAG_IDS, EARLIEST_WINDOWS
from meridian_ops.jobs import (
    publication_ready,
    resolve_month,
    run_context_values,
    run_gold_batch,
    run_ingest,
    run_silver,
)
from meridian_ops.timetable import GatedMonthlyTimetable


def build_market_dag(market: str) -> DAG:
    """Build one monthly DAG for one configured market."""
    earliest = EARLIEST_WINDOWS[market]
    year, month = (int(part) for part in earliest.split("-"))
    with DAG(
        dag_id=DAG_IDS[market],
        description=f"Meridian monthly pipeline for {market}",
        schedule=GatedMonthlyTimetable(market),
        start_date=pendulum.datetime(year, month, 1, tz="UTC"),
        catchup=False,
        is_paused_upon_creation=False,
        max_active_runs=16,
        max_active_tasks=32,
        default_args={"retry_delay": timedelta(seconds=10)},
        tags=["meridian", market],
    ) as dag:

        @task(task_id="which_month", retries=0)
        def which_month() -> dict[str, object]:
            context = get_current_context()
            run_type, explicit, interval = run_context_values(context)
            return resolve_month(market, run_type, explicit, interval)

        selection = which_month()

        wait = PythonSensor(
            task_id="wait_until_published",
            python_callable=publication_ready,
            op_args=[market, selection],
            mode="reschedule",
            poke_interval=int(os.getenv("PUBLISH_POKE_SECONDS", "300")),
            timeout=int(os.getenv("PUBLISH_TIMEOUT_SECONDS", "604800")),
        )

        @task(task_id="ingest_to_bronze", retries=2)
        def bronze(selected: dict[str, object]) -> dict[str, object]:
            context = get_current_context()
            result = run_ingest(market, selected, context["run_id"])
            if result["work"] == "skipped":
                raise AirflowSkipException("month requires no work")
            return result

        @task(task_id="transform_to_silver", retries=2)
        def silver(selected: dict[str, object]) -> dict[str, object]:
            context = get_current_context()
            result = run_silver(market, selected, context["run_id"])
            if result["work"] == "skipped":
                raise AirflowSkipException("month requires no work")
            return result

        @task(task_id="transform_to_gold", retries=2)
        def gold(selected: dict[str, object]) -> dict[str, object]:
            context = get_current_context()
            result = run_gold_batch(market, selected, context["run_id"])
            if result["work"] == "skipped":
                raise AirflowSkipException("month requires no work")
            return result

        bronze_task = bronze(selection)
        silver_task = silver(selection)
        gold_task = gold(selection)
        wait >> bronze_task >> silver_task >> gold_task

    return dag


for configured_market in EARLIEST_WINDOWS:
    globals()[DAG_IDS[configured_market]] = build_market_dag(configured_market)
