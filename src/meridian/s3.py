"""S3 listing and archive parsing."""
import re
from datetime import datetime
from typing import NamedTuple
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

import requests


class S3Object(NamedTuple):
    """S3 object metadata."""
    key: str
    size: int
    last_modified: datetime


def list_s3_objects() -> list[S3Object]:
    """List all objects in the tripdata bucket."""
    url = "https://s3.amazonaws.com/tripdata/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    
    root = ET.fromstring(resp.content)
    # S3 XML namespace
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    
    objects = []
    for contents in root.findall("s3:Contents", ns):
        key_elem = contents.find("s3:Key", ns)
        size_elem = contents.find("s3:Size", ns)
        modified_elem = contents.find("s3:LastModified", ns)
        
        if key_elem is not None and size_elem is not None and modified_elem is not None:
            key = key_elem.text or ""
            size = int(size_elem.text or 0)
            # Parse ISO 8601 datetime
            modified_str = modified_elem.text or ""
            # Remove timezone suffix for simplicity
            if modified_str.endswith("Z"):
                modified_str = modified_str[:-1]
            modified = datetime.fromisoformat(modified_str)
            
            objects.append(S3Object(key=key, size=size, last_modified=modified))
    
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
    """Filter objects for a specific month (YYYY-MM)."""
    # Month pattern: YYYYMM or YYYY-MM in filename
    month_pattern = month.replace("-", "")
    pattern = re.compile(f"{month_pattern}|{month.replace('-', '')}")
    return [o for o in objects if pattern.search(o.key)]


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
