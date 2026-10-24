"""Regression coverage for the Stage 1 review findings."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator
from unittest.mock import MagicMock, Mock

import pytest

from meridian import cli, gold, ingest, s3, silver, trip


def test_new_schema_maps_to_canonical_fields() -> None:
    row = {
        "ride_id": "ride-1",
        "started_at": "2021-02-01 08:00:00",
        "ended_at": "2021-02-01 08:15:00",
        "start_station_id": "JC001",
        "end_station_id": "JC002",
        "member_casual": "member",
    }

    normalized = trip.normalize_row(row, "new")

    assert normalized["trip_id"] == "ride-1"
    assert normalized["start_time"] == row["started_at"]
    assert normalized["end_time"] == row["ended_at"]
    assert normalized["user_type"] == "member"


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (
            {
                "start_station_id": "",
                "end_station_id": "JC002",
                "started_at": "2021-02-01 08:00:00",
                "ended_at": "2021-02-01 08:15:00",
            },
            "missing_start_station_id",
        ),
        (
            {
                "start_station_id": "JC001",
                "end_station_id": "JC002",
                "started_at": "not-a-time",
                "ended_at": "2021-02-01 08:15:00",
            },
            "invalid_start_time",
        ),
    ],
)
def test_validation_defects_have_specific_reasons(
    row: dict[str, str], reason: str
) -> None:
    with pytest.raises(trip.TripValidationError) as caught:
        trip.conform_trip(row)

    assert caught.value.reason == reason


def test_unexpected_silver_bug_is_not_quarantined(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(_: dict[str, object]) -> trip.ConformedTrip:
        raise RuntimeError("programming bug")

    monkeypatch.setattr(silver, "conform_trip", fail)

    with pytest.raises(RuntimeError, match="programming bug"):
        silver._classify_row("jc", "2021-02", 1, {})


def _member(
    source_key: str,
    filename: str,
    timestamp: tuple[int, int, int, int, int, int],
) -> ingest.ArchiveMember:
    return ingest.ArchiveMember(source_key, b"zip", filename, timestamp)


def test_export_selection_keeps_one_newest_folder_whole() -> None:
    candidates = [
        _member("archive.zip", "old/part-1.csv", (2024, 2, 21, 10, 0, 0)),
        _member("archive.zip", "old/part-2.csv", (2024, 2, 21, 10, 0, 0)),
        _member("archive.zip", "new/part-1.csv", (2024, 2, 21, 10, 0, 1)),
        _member("archive.zip", "new/part-2.csv", (2024, 2, 21, 10, 0, 1)),
    ]

    selected = ingest._select_latest_export(candidates)

    assert [member.filename for member in selected] == [
        "new/part-1.csv",
        "new/part-2.csv",
    ]


def test_bronze_deletes_only_its_window_before_reinserting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []

    class Cursor:
        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, statement: str, params: object) -> None:
            calls.append(("execute", (statement, params)))

    class Connection:
        def cursor(self) -> Cursor:
            return Cursor()

    @contextmanager
    def fake_db() -> Iterator[Connection]:
        yield Connection()

    objects = [s3.S3Object("JC-test.zip", 1, datetime(2024, 1, 1))]
    member = _member("JC-test.zip", "run/trips.csv", (2024, 1, 1, 0, 0, 0))
    monkeypatch.setattr(ingest.s3, "list_s3_objects", lambda: objects)
    monkeypatch.setattr(ingest.s3, "filter_by_market", lambda values, market: values)
    monkeypatch.setattr(ingest.s3, "get_month_files", lambda values, window: values)
    monkeypatch.setattr(
        ingest, "_download_latest_export", lambda values, window: [member]
    )
    monkeypatch.setattr(ingest.db, "get_db", fake_db)
    persist = Mock()
    monkeypatch.setattr(ingest, "_persist_member", persist)

    ingest.ingest_to_bronze("jc", "2021-02")

    statement, params = calls[0][1]  # type: ignore[misc]
    assert "DELETE FROM bronze_trips" in statement
    assert params == ("jc", "2021-02")
    persist.assert_called_once()


def test_bronze_insert_preserves_identical_published_rows() -> None:
    cursor = Mock()
    published_row = (
        "jc",
        "2021-02",
        "JC-202102.zip",
        datetime(2021, 2, 1),
        '{"ride_id": "same"}',
        1,
    )

    ingest._insert_rows(cursor, [published_row, published_row])

    statement, rows = cursor.executemany.call_args.args
    assert "ON CONFLICT" not in statement
    assert rows == [published_row, published_row]


def test_cli_job_functions_raise_instead_of_exiting() -> None:
    with pytest.raises(cli.CliUsageError):
        cli.run("not-a-layer", "trips:jc", "2021-02")


def test_cli_delegates_the_complete_gold_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transform = Mock()
    monkeypatch.setattr(cli.gold, "transform_to_gold", transform)

    cli.run("transform-to-gold", "station-daily", "2026-06-02")

    transform.assert_called_once_with("2026-06-02")


def test_gold_replaces_all_markets_in_one_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cursor = MagicMock()

    class Connection:
        def cursor(self) -> MagicMock:
            return cursor

    @contextmanager
    def fake_db() -> Iterator[Connection]:
        yield Connection()

    monkeypatch.setattr(gold.db, "get_db", fake_db)
    monkeypatch.setattr(gold, "_markets_for_day", Mock(return_value=["jc", "nyc"]))
    replace = Mock()
    monkeypatch.setattr(gold, "_replace_station_day", replace)

    gold.transform_to_gold("2026-06-02")

    assert replace.call_count == 2
    expected_day = datetime(2026, 6, 2).date()
    assert replace.call_args_list[0].args[1:] == ("jc", expected_day)
    assert replace.call_args_list[1].args[1:] == ("nyc", expected_day)


@pytest.mark.parametrize(
    "args",
    [
        ["run", "ingest-to-bronze", "trips:jc", "0000-06"],
        ["inspect", "silver", "trips:jc", "0000-06"],
        ["run", "transform-to-gold", "station-daily", "2026-02-29"],
        ["run", "transform-to-gold", "station-daily", "2026-04-31"],
        ["report", "daily-station-trips", "jc", "JC115", "2026-02-31"],
    ],
)
def test_invalid_calendar_dates_fail_before_accessing_storage(
    args: list[str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    storage = Mock(side_effect=AssertionError("invalid date reached storage"))
    monkeypatch.setattr(cli.ingest, "ingest_to_bronze", storage)
    monkeypatch.setattr(cli.report, "inspect_silver", storage)
    monkeypatch.setattr(cli.gold, "transform_to_gold", storage)
    monkeypatch.setattr(cli.report, "report_daily_station_trips", storage)

    assert cli.main(args) == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err.startswith("ERROR:")
    storage.assert_not_called()


@pytest.mark.parametrize("day", ["2024-02-29", "2026-06-02"])
def test_valid_calendar_dates_reach_report(
    day: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    answer = {"departures": 1, "arrivals": 2}
    stored_report = Mock(return_value=answer)
    monkeypatch.setattr(cli.report, "report_daily_station_trips", stored_report)

    assert cli.report_answer("daily-station-trips", "jc", "JC115", day) == answer
    stored_report.assert_called_once_with("jc", "JC115", day)
