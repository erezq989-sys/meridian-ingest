"""S3 listing and archive parsing."""
import calendar
import re
from datetime import datetime
from typing import NamedTuple
from xml.etree import ElementTree as ET

import requests


class S3Object(NamedTuple):
    """S3 object metadata."""
    key: str
    size: int
    last_modified: datetime


def list_s3_objects() -> list[S3Object]:
    """List every object in the bucket, following S3 v1 pagination."""
    url = "https://s3.amazonaws.com/tripdata/"
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    objects: list[S3Object] = []
    marker: str | None = None
    while True:
        if marker is None:
            resp = requests.get(url, timeout=30)
        else:
            resp = requests.get(url, params={"marker": marker}, timeout=30)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        page_keys: list[str] = []
        for contents in root.findall("s3:Contents", ns):
            key_elem = contents.find("s3:Key", ns)
            size_elem = contents.find("s3:Size", ns)
            modified_elem = contents.find("s3:LastModified", ns)
            if key_elem is None:
                continue
            key = key_elem.text or ""
            if key:
                page_keys.append(key)
            if not key or size_elem is None or modified_elem is None:
                continue
            modified_str = modified_elem.text or ""
            if modified_str.endswith("Z"):
                modified_str = modified_str[:-1]
            objects.append(S3Object(key, int(size_elem.text or 0), datetime.fromisoformat(modified_str)))

        truncated = root.find("s3:IsTruncated", ns)
        if truncated is None or (truncated.text or "").strip().lower() != "true":
            break
        next_marker = root.find("s3:NextMarker", ns)
        new_marker = (next_marker.text or "").strip() if next_marker is not None else ""
        if not new_marker and page_keys:
            new_marker = page_keys[-1]
        if not new_marker or new_marker == marker:
            raise RuntimeError("S3 listing was truncated without a usable next marker")
        marker = new_marker
    return objects


def filter_by_market(objects: list[S3Object], market: str) -> list[S3Object]:
    """Filter S3 objects by market (jc or nyc)."""
    if market == "jc":
        # JC files start with "JC-"
        return [o for o in objects if o.key.startswith("JC-")]
    elif market == "nyc":
        # NYC files don't have "JC-" prefix and don't contain special markers
        return [o for o in objects if not o.key.startswith("JC-") and o.key.endswith(".zip")]
    else:
        raise ValueError(f"Unknown market: {market}")


def get_month_files(objects: list[S3Object], month: str) -> list[S3Object]:
    """Return monthly objects plus yearly archives that contain the month."""
    year, month_number = month.split("-")
    month_pattern = re.compile(rf"(?<!\d){re.escape(year + month_number)}(?!\d)")
    yearly_pattern = re.compile(rf"(?<!\d){re.escape(year)}(?!\d)")
    result: list[S3Object] = []
    for obj in objects:
        filename = obj.key.rsplit("/", 1)[-1]
        if month_pattern.search(filename) or (
            (filename.startswith(year + "-") or yearly_pattern.search(filename))
            and filename.lower().endswith(".zip")
        ):
            result.append(obj)
    return result


def select_month_members(names: list[str], month: str) -> list[str]:
    """Return CSV members whose names identify the requested month."""
    year, month_number = month.split("-")
    month_name = calendar.month_name[int(month_number)].lower()
    tokens = {year + month_number, f"{year}-{month_number}", month_name, f"{int(month_number)}_{month_name}"}
    return [
        name for name in names
        if name.lower().endswith(".csv")
        and any(token in name.lower() for token in tokens)
    ]


def select_export_run(objects: list[S3Object]) -> list[S3Object]:
    """
    From multiple export runs of the same month, select the newest one.
    
    Export runs are identified by archive member timestamp. The publisher may
    re-export a month and store it multiple places with the same timestamp.
    We want exactly one export run (all files from that run), identified by
    the latest (newest) timestamp.
    
    Returns: All objects from the latest export run.
    """
    if not objects:
        return []
    
    # Group by timestamp (which identifies an export run)
    # Take the latest timestamp
    latest_timestamp = max(obj.last_modified for obj in objects)
    
    # Return all objects from the latest export run
    return [obj for obj in objects if obj.last_modified == latest_timestamp]
