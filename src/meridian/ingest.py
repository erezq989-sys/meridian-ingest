"""Fetch one publisher export and replace its bronze window atomically."""

from __future__ import annotations

import csv
import io
import json
import posixpath
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Any, NamedTuple

import requests

from meridian import db, s3

INSERT_BATCH_SIZE = 5_000


class ArchiveMember(NamedTuple):
    source_key: str
    archive: bytes
    filename: str
    date_time: tuple[int, int, int, int, int, int]


def _archive_members(obj: s3.S3Object, window: str) -> list[ArchiveMember]:
    """Download one archive and return its relevant, non-metadata CSV members."""
    response = requests.get(f"https://s3.amazonaws.com/tripdata/{obj.key}", timeout=120)
    response.raise_for_status()
    archive = response.content
    with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
        infos = [
            info
            for info in zipped.infolist()
            if not info.is_dir()
            and info.filename.lower().endswith(".csv")
            and "__MACOSX" not in info.filename.split("/")
            and not posixpath.basename(info.filename).startswith("._")
        ]
        selected = set(
            s3.select_month_members([info.filename for info in infos], window)
        )
        return [
            ArchiveMember(obj.key, archive, info.filename, info.date_time)
            for info in infos
            if info.filename in selected
        ]


def _select_latest_export(candidates: list[ArchiveMember]) -> list[ArchiveMember]:
    """Select every member from one folder-based export run."""
    groups: dict[tuple[str, str], list[ArchiveMember]] = defaultdict(list)
    for member in candidates:
        folder = posixpath.dirname(member.filename)
        groups[(member.source_key, folder)].append(member)

    if not groups:
        return []

    def group_order(key: tuple[str, str]) -> tuple[datetime, str, str]:
        newest_member = max(datetime(*member.date_time) for member in groups[key])
        return newest_member, key[0], key[1]

    selected_key = max(groups, key=group_order)
    return sorted(groups[selected_key], key=lambda member: member.filename)


def _download_latest_export(
    objects: list[s3.S3Object], window: str
) -> list[ArchiveMember]:
    """Download candidates and choose one complete publisher export."""
    candidates: list[ArchiveMember] = []
    for obj in objects:
        if obj.key.lower().endswith(".zip"):
            candidates.extend(_archive_members(obj, window))
    return _select_latest_export(candidates)


def _clean_row(row: dict[str | None, str | None]) -> dict[str, str | None]:
    """Remove NUL bytes while retaining the publisher's row values."""
    cleaned: dict[str, str | None] = {}
    for key, value in row.items():
        clean_key = "" if key is None else key.replace("\x00", "")
        cleaned[clean_key] = value.replace("\x00", "") if value is not None else None
    return cleaned


def _insert_rows(cur: Any, rows: list[tuple[object, ...]]) -> None:
    """Insert one bounded bronze batch."""
    cur.executemany(
        """
        INSERT INTO bronze_trips
            (market, "window", source_key, source_timestamp,
             raw_row, bronze_object_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def _persist_member(cur: Any, member: ArchiveMember, market: str, window: str) -> None:
    """Store exact member bytes and all CSV rows using the caller's transaction."""
    with zipfile.ZipFile(io.BytesIO(member.archive)) as zipped:
        csv_bytes = zipped.read(member.filename)
    source_timestamp = datetime(*member.date_time)
    cur.execute(
        """
        INSERT INTO bronze_objects
            (source_key, source_member, source_timestamp, raw_bytes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (source_key, source_member, source_timestamp)
        DO UPDATE SET
            raw_bytes = EXCLUDED.raw_bytes,
            loaded_at = CURRENT_TIMESTAMP
        RETURNING id
        """,
        (member.source_key, member.filename, source_timestamp, csv_bytes),
    )
    object_row = cur.fetchone()
    if object_row is None:
        raise RuntimeError("failed to persist bronze source object")
    bronze_object_id = object_row[0]

    stream = io.TextIOWrapper(
        io.BytesIO(csv_bytes), encoding="utf-8-sig", errors="replace"
    )
    reader = csv.DictReader(stream)
    if reader.fieldnames is None:
        return

    batch: list[tuple[object, ...]] = []
    for row in reader:
        batch.append(
            (
                market,
                window,
                member.source_key,
                source_timestamp,
                json.dumps(_clean_row(row)),
                bronze_object_id,
            )
        )
        if len(batch) >= INSERT_BATCH_SIZE:
            _insert_rows(cur, batch)
            batch.clear()
    if batch:
        _insert_rows(cur, batch)


def ingest_to_bronze(market: str, window: str) -> None:
    """Replace one market/month with one complete published export."""
    objects = s3.get_month_files(
        s3.filter_by_market(s3.list_s3_objects(), market), window
    )
    if not objects:
        raise ValueError(f"No files found for {market} in {window}")

    members = _download_latest_export(objects, window)
    if not members:
        raise ValueError(f"No CSV files found for {market} in {window}")

    with db.get_db() as connection, connection.cursor() as cursor:
        cursor.execute(
            'DELETE FROM bronze_trips WHERE market = %s AND "window" = %s',
            (market, window),
        )
        for member in members:
            _persist_member(cursor, member, market, window)
