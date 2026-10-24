"""Recipe-facing Stage 2 command-line interface."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from meridian_ops.airflow_api import AirflowApi
from meridian_ops.config import DAG_IDS, validate_market
from meridian_ops.control import (
    coverage,
    incomplete_gold_days,
    init_control_schema,
    month_for_run,
    set_schedule,
)
from meridian_ops.months import iter_months, month_days

TERMINAL_STATES = {"success", "failed", "canceled"}
TASK_LABELS: dict[str, Callable[[str], str]] = {
    "ingest_to_bronze": lambda market: f"ingest-to-bronze trips:{market}",
    "transform_to_silver": lambda market: f"transform-to-silver trips:{market}",
    "transform_to_gold": lambda market: "transform-to-gold station-daily",
}


def _month_from_run(run: dict[str, Any], market: str) -> str | None:
    conf = run.get("conf") or {}
    if conf.get("month"):
        return str(conf["month"])
    run_type = str(run.get("run_type", run.get("runType", "")))
    run_id = str(run.get("dag_run_id", run.get("run_id", "")))
    # A windowless manual run chooses its month from control-table coverage,
    # not from the timetable interval Airflow attaches to a manual trigger.
    if run_type == "manual":
        return month_for_run(market, run_id)
    interval = run.get("data_interval_start") or run.get("logical_date")
    if interval:
        return str(interval)[:7]
    return month_for_run(market, run_id)


def _tries(task: dict[str, Any]) -> int:
    value = task.get("try_number", task.get("tryNumber", 0))
    return max(1, int(value or 0))


def _task_summary(
    api: AirflowApi, market: str, run: dict[str, Any]
) -> dict[str, dict[str, object]]:
    run_id = str(run.get("dag_run_id", run.get("run_id")))
    instances = {
        str(task["task_id"]): task
        for task in api.task_instances(DAG_IDS[market], run_id)
    }
    month = _month_from_run(run, market)
    result: dict[str, dict[str, object]] = {}
    for task_id, label_factory in TASK_LABELS.items():
        task = instances.get(task_id, {})
        item: dict[str, object] = {
            "state": task.get("state") or "none",
            "tries": _tries(task),
        }
        if task_id == "transform_to_gold":
            item["days"] = len(month_days(month)) if month else 0
            item["failed_days"] = (
                incomplete_gold_days(market, month)
                if month and task.get("state") == "failed"
                else []
            )
        result[label_factory(market)] = item
    return result


def inspect_runs(market: str) -> dict[str, object]:
    """Return newest-first Airflow run and task states."""
    validate_market(market)
    api = AirflowApi()
    runs: list[dict[str, object]] = []
    for run in api.runs(DAG_IDS[market]):
        run_id = str(run.get("dag_run_id", run.get("run_id")))
        runs.append(
            {
                "month": _month_from_run(run, market),
                "type": run.get("run_type", run.get("runType")),
                "state": run.get("state"),
                "tasks": _task_summary(api, market, {**run, "dag_run_id": run_id}),
            }
        )
    return {"market": market, "runs": runs}


def schedule(market: str, state: str) -> dict[str, object]:
    """Enable or disable one Airflow market schedule."""
    validate_market(market)
    if state not in {"on", "off"}:
        raise ValueError(f"schedule state must be on or off, got {state}")
    enabled = state == "on"
    set_schedule(market, enabled)
    # Airflow does not schedule manual runs for paused DAGs. The timetable gates
    # only automatic runs, so the DAG itself remains unpaused in both states.
    AirflowApi().set_paused(DAG_IDS[market], False)
    return {"market": market, "schedule": state}


def _manual_run(market: str, month: str | None) -> dict[str, Any]:
    api = AirflowApi()
    api.set_paused(DAG_IDS[market], False)
    suffix = month or "next"
    run_id = f"manual__meridian__{suffix}__{uuid.uuid4().hex}"
    conf = {"month": month} if month else {}
    api.trigger(DAG_IDS[market], run_id, conf)
    return api.wait(DAG_IDS[market], run_id)


def _backfill(market: str, start: str, end: str) -> list[dict[str, Any]]:
    expected = iter_months(start, end)
    api = AirflowApi()
    old_ids = {
        str(run.get("dag_run_id", run.get("run_id")))
        for run in api.runs(DAG_IDS[market])
    }
    command = [
        "airflow",
        "backfill",
        "create",
        "--dag-id",
        DAG_IDS[market],
        "--from-date",
        f"{start}-01T00:00:00Z",
        "--to-date",
        f"{end}-01T00:00:00Z",
        "--reprocess-behavior",
        "completed",
        "--max-active-runs",
        "16",
    ]
    created_at = datetime.now(UTC)
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Airflow backfill creation failed: {result.stderr.strip()}")

    selected: dict[str, dict[str, Any]] = {}
    while len(selected) < len(expected):
        for run in api.runs(DAG_IDS[market]):
            run_id = str(run.get("dag_run_id", run.get("run_id")))
            month = _month_from_run(run, market)
            run_type = str(run.get("run_type", run.get("runType", "")))
            if run_id not in old_ids and month in expected and "backfill" in run_type:
                selected[month] = {**run, "dag_run_id": run_id}
        if len(selected) < len(expected):
            if (datetime.now(UTC) - created_at).total_seconds() > 120:
                missing = sorted(set(expected) - set(selected))
                raise TimeoutError(
                    f"Airflow did not create backfill runs for {missing}"
                )
            time.sleep(2)

    return [
        api.wait(DAG_IDS[market], str(selected[month]["dag_run_id"]))
        for month in expected
    ]


def run_pipeline(market: str, windows: list[str]) -> tuple[dict[str, object], int]:
    """Trigger requested Airflow runs, wait, and report aggregate success."""
    validate_market(market)
    if len(windows) > 2:
        raise ValueError("pipeline accepts zero, one, or two month arguments")
    if len(windows) == 2:
        raw_runs = _backfill(market, windows[0], windows[1])
    else:
        raw_runs = [_manual_run(market, windows[0] if windows else None)]

    api = AirflowApi()
    runs: list[dict[str, object]] = []
    failed = False
    for raw in raw_runs:
        run_id = str(raw.get("dag_run_id", raw.get("run_id")))
        month = _month_from_run({**raw, "dag_run_id": run_id}, market)
        tasks = _task_summary(api, market, {**raw, "dag_run_id": run_id})
        state = str(raw.get("state"))
        failed = failed or state != "success"
        work = (
            "skipped"
            if month is None
            or all(task["state"] == "skipped" for task in tasks.values())
            else "complete"
        )
        runs.append({"month": month, "state": state, "work": work})
    payload: dict[str, object] = {"market": market, "runs": runs}
    if len(runs) == 1:
        payload.update(runs[0])
        payload.pop("runs")
    return payload, 1 if failed else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="meridian-ops")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("init")
    schedule_parser = commands.add_parser("schedule")
    schedule_parser.add_argument("market")
    schedule_parser.add_argument("state")
    run_parser = commands.add_parser("run-pipeline")
    run_parser.add_argument("market")
    run_parser.add_argument("windows", nargs="*")
    coverage_parser = commands.add_parser("coverage")
    coverage_parser.add_argument("job")
    runs_parser = commands.add_parser("runs")
    runs_parser.add_argument("market")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "init":
            init_control_schema()
            return 0
        if args.command == "schedule":
            print(json.dumps(schedule(args.market, args.state)))
            return 0
        if args.command == "coverage":
            prefix, separator, market = args.job.partition(":")
            if prefix != "trips" or not separator:
                raise ValueError(
                    f"coverage expects trips:jc or trips:nyc, got {args.job}"
                )
            print(json.dumps(coverage(market)))
            return 0
        if args.command == "runs":
            print(json.dumps(inspect_runs(args.market)))
            return 0
        if args.command == "run-pipeline":
            payload, status = run_pipeline(
                args.market, [window for window in args.windows if window]
            )
            print(json.dumps(payload))
            return status
        raise ValueError(f"unknown command {args.command}")
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
