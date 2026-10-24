"""Small client for Airflow 3's stable REST API."""

from __future__ import annotations

import os
import time
from typing import Any
from urllib.parse import quote

import requests


class AirflowApi:
    """Authenticated Airflow 3 API client for recipes and inspection."""

    def __init__(self) -> None:
        self.base_url = os.getenv("AIRFLOW_API_URL", "http://airflow-api-server:8080")
        token_response = requests.get(f"{self.base_url}/auth/token", timeout=30)
        token_response.raise_for_status()
        token = token_response.json()["access_token"]
        self.headers = {"Authorization": f"Bearer {token}"}

    def request(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = requests.request(
            method,
            f"{self.base_url}/api/v2{path}",
            headers=self.headers,
            json=json,
            params=params,
            timeout=60,
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
        return payload

    def set_paused(self, dag_id: str, paused: bool) -> dict[str, Any]:
        return self.request(
            "PATCH", f"/dags/{quote(dag_id)}", json={"is_paused": paused}
        )

    def trigger(self, dag_id: str, run_id: str, conf: dict[str, Any]) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/dags/{quote(dag_id)}/dagRuns",
            json={"dag_run_id": run_id, "logical_date": None, "conf": conf},
        )

    def run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return self.request(
            "GET", f"/dags/{quote(dag_id)}/dagRuns/{quote(run_id, safe='')}"
        )

    def wait(
        self, dag_id: str, run_id: str, poll_seconds: float = 2.0
    ) -> dict[str, Any]:
        while True:
            run = self.run(dag_id, run_id)
            if run.get("state") in {"success", "failed", "canceled"}:
                return run
            time.sleep(poll_seconds)

    def runs(self, dag_id: str, limit: int = 100) -> list[dict[str, Any]]:
        payload = self.request(
            "GET",
            f"/dags/{quote(dag_id)}/dagRuns",
            params={"limit": limit, "order_by": "-id"},
        )
        rows = payload.get("dag_runs", payload.get("dagRuns", []))
        return list(rows)

    def task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        payload = self.request(
            "GET",
            f"/dags/{quote(dag_id)}/dagRuns/{quote(run_id, safe='')}/taskInstances",
            params={"limit": 100},
        )
        rows = payload.get("task_instances", payload.get("taskInstances", []))
        return list(rows)
