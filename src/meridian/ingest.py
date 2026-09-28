"""Ingest job: fetch from S3 and land in the bronze layer."""
import csv
import io
import json
import zipfile
from datetime import datetime
from typing import NamedTuple

import requests

from meridian import db, s3


INSERT_BATCH_SIZE = 5_000


class ArchiveMember(NamedTuple):
    source_key: str
    archive: bytes
    filename: str
    date_time: tuple[int, int, int, int, int, int]


def _download_latest_export(objects: list[s3.S3Object], window: str) -> list[ArchiveMember]:
    """Choose one complete publisher export using ZIP member timestamps."""
    candidates: list[ArchiveMember] = []
    for obj in objects:
        if not obj.key.lower().endswith(".zip"):
            continue
        response = requests.get(
            f"https://s3.amazonaws.com/tripdata/{obj.key}", timeout=120
        )
        response.raise_for_status()
        archive = response.content
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            csv_names = [
                info.filename
                for info in zf.infolist()
                if not info.is_dir()
                and info.filename.lower().endswith(".csv")
                and "__MACOSX" not in info.filename.split("/")
                and not info.filename.rsplit("/", 1)[-1].startswith("._")
            ]
            selected = set(s3.select_month_members(csv_names, window))
            for info in zf.infolist():
                if info.filename in selected:
                    candidates.append(
                        ArchiveMember(obj.key, archive, info.filename, info.date_time)
                    )

    if not candidates:
        return []

    latest_date = max(datetime(*item.date_time).date() for item in candidates)
    latest = [
        item for item in candidates
        if datetime(*item.date_time).date() == latest_date
    ]

    # Keep distinct split parts, but only one directory copy per basename.
    unique: dict[str, ArchiveMember] = {}
    for item in latest:
        basename = item.filename.rsplit("/", 1)[-1]
        current = unique.get(basename)
        if current is None or (item.date_time, item.filename) > (
            current.date_time,
            current.filename,
        ):
            unique[basename] = item
    return [unique[name] for name in sorted(unique)]


def _insert_rows(cur: object, rows: list[tuple[object, ...]]) -> None:
    """Insert one bounded bronze batch."""
    cur.executemany(  # type: ignore[attr-defined]
        """
        INSERT INTO bronze_trips
            (market, "window", source_key, source_timestamp,
             raw_row, bronze_object_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        rows,
    )


def ingest_to_bronze(market: str, window: str) -> None:
    """Land one complete market/month export with exact source bytes."""
    db.init_schema()
    objects = s3.get_month_files(
        s3.filter_by_market(s3.list_s3_objects(), market), window
    )
    if not objects:
        raise ValueError(f"No files found for {market} in {window}")
    members = _download_latest_export(objects, window)
    if not members:
        raise ValueError(f"No CSV files found for {market} in {window}")

    with db.get_db() as conn:
        with conn.cursor() as cur:
            for member in members:
                with zipfile.ZipFile(io.BytesIO(member.archive)) as zf:
                    csv_bytes = zf.read(member.filename)
                source_timestamp = datetime(*member.date_time)
                cur.execute(
                    """
                    INSERT INTO bronze_objects
                        (source_key, source_member, source_timestamp, raw_bytes)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_key, source_member, source_timestamp)
                    DO UPDATE SET raw_bytes = EXCLUDED.raw_bytes
                    RETURNING id
                    """,
                    (member.source_key, member.filename, source_timestamp, csv_bytes),
                )
                object_row = cur.fetchone()
                if object_row is None:
                    raise RuntimeError("failed to persist bronze source object")
                bronze_object_id = object_row[0]

                text_stream = io.TextIOWrapper(
                    io.BytesIO(csv_bytes), encoding="utf-8", errors="replace"
                )
                reader = csv.DictReader(text_stream)
                if reader.fieldnames is None:
                    continue

                insert_rows: list[tuple[object, ...]] = []
                for row in reader:
                    raw_json = json.dumps(
                        {
                            key.replace("\x00", "") if isinstance(key, str) else key:
                            value.replace("\x00", "") if isinstance(value, str) else value
                            for key, value in row.items()
                        }
                    )
                    insert_rows.append(
                        (
                            market,
                            window,
                            member.source_key,
                            source_timestamp,
                            raw_json,
                            bronze_object_id,
                        )
                    )
                    if len(insert_rows) == INSERT_BATCH_SIZE:
                        _insert_rows(cur, insert_rows)
                        insert_rows.clear()
                if insert_rows:
                    _insert_rows(cur, insert_rows)
