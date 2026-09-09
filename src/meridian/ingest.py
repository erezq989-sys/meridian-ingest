"""Ingest job: fetch from S3 and land in bronze layer."""
import csv
import io
import json
import zipfile
from datetime import datetime
from typing import Any

import requests

from meridian import db, s3
from meridian.trip import detect_schema, normalize_row


def ingest_to_bronze(market: str, window: str) -> None:
    """
    Ingest trips from S3 for a market and month window into bronze layer.
    
    Bronze layer: store the raw bytes exactly as published, with provenance.
    This job must handle:
    - Schema drift (old vs new column names)
    - Multiple export runs of same month (choose latest by archive timestamp)
    - Files inside yearly zips (e.g., April inside 2018.zip)
    
    Args:
        market: "jc" or "nyc"
        window: ISO 8601 month like "2026-06"
    """
    db.init_schema()
    
    # List S3 objects
    all_objects = s3.list_s3_objects()
    market_objects = s3.filter_by_market(all_objects, market)
    month_objects = s3.get_month_files(market_objects, window)
    
    if not month_objects:
        raise ValueError(f"No files found for {market} in {window}")
    
    # Select the latest export run (by timestamp)
    # This ensures we don't load the same month multiple times from different exports
    month_objects = s3.select_export_run(month_objects)
    
    # Download and extract CSVs
    with db.get_db() as conn:
        with conn.cursor() as cur:
            for obj in month_objects:
                if not obj.key.endswith(".zip"):
                    continue
                
                # Download the zip from S3
                url = f"https://s3.amazonaws.com/tripdata/{obj.key}"
                resp = requests.get(url, timeout=120)
                resp.raise_for_status()
                
                # Extract and process CSVs in the zip
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    # Process files in the zip
                    for file_info in zf.filelist:
                        filename = file_info.filename
                        
                        # Skip directories
                        if filename.endswith("/"):
                            continue
                        
                        # Skip non-CSV files
                        if not filename.lower().endswith(".csv"):
                            continue
                        
                        # For root-level monthly files, process them
                        # For yearly files with subdirectories, prefer newer exports
                        # For now: load root-level CSVs and root directory CSVs
                        # The select_export_run filter should have already chosen the latest run
                        
                        # Read CSV with proper parsing
                        with zf.open(filename) as f:
                            text_stream = io.TextIOWrapper(f, encoding="utf-8")
                            reader = csv.DictReader(text_stream)
                            
                            if reader.fieldnames is None:
                                continue
                            
                            headers = list(reader.fieldnames)
                            
                            for row in reader:
                                # Store raw row in bronze
                                raw_json = json.dumps(row)
                                cur.execute(
                                    """
                                    INSERT INTO bronze_trips (market, "window", source_key, source_timestamp, raw_row)
                                    VALUES (%s, %s, %s, %s, %s)
                                    ON CONFLICT DO NOTHING
                                    """,
                                    (market, window, obj.key, obj.last_modified, raw_json)
                                )
