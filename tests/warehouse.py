
"""Landing windows through the interface, at most once per session.

A window costs a download — 2018-04 is a 1.34 GB archive — so the suite lands each coordinate
once and lets the tests that care about idempotency re-run it themselves. Every call here goes
through `just run`; there is no back door that fills a layer faster.
"""

from __future__ import annotations

from tests.just_cli import just_run


class Warehouse:
    def __init__(self) -> None:
        self._done: set[tuple[str, str, str]] = set()

    def _once(self, layer: str, job: str, window: str) -> None:
        key = (layer, job, window)
        if key in self._done:
            return
        result = just_run(layer, job, window)
        assert result.exit_code == 0, result.describe()
        self._done.add(key)

    def bronze(self, job: str, window: str) -> None:
        self._once("ingest-to-bronze", job, window)

    def silver(self, job: str, window: str) -> None:
        """Silver reads what bronze landed, so the window has to be ingested first."""
        self.bronze(job, window)
        self._once("transform-to-silver", job, window)

    def gold(self, fact: str, day: str) -> None:
        self._once("transform-to-gold", fact, day)
