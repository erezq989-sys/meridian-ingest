# Meridian Bikes S3 Data Pipeline

A three-layer data warehouse pipeline for Meridian Bikes trip data, reading from public S3 archives and producing station-day aggregates.

## Architecture

### Three Layers

| Layer | Purpose | Immutable? | Source |
|-------|---------|-----------|--------|
| **Bronze** | Raw S3 data with provenance | Yes | S3 tripdata bucket |
| **Silver** | Conformed, validated trips | No* | Bronze |
| **Gold** | Station-day facts (departures/arrivals) | No* | Silver |

*Rebuildable from their source layer

### Key Features

- **Schema migration handling**: Detects old vs. new column naming (spaces vs. underscores)
- **Export run deduplication**: Chooses latest export by archive timestamp, not by date
- **Quarantine with reasons**: Invalid trips kept with business-friendly reasons
- **Idempotent jobs**: Safe to rerun without duplication
- **Separate, orchestrable jobs**: Each layer is independently runnable

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+ (for local testing)
- `just` (task runner, optional - can use Docker Compose directly)

### Setup

```bash
# Initialize database and start stack
docker compose up -d

# Wait for Postgres to be ready
docker compose exec postgres pg_isready -U meridian
```

### Example Workflow

```bash
# Ingest a month of Jersey City trips into bronze
docker compose exec app python -m meridian.cli run ingest-to-bronze trips:jc 2026-06

# Validate and conform trips to silver
docker compose exec app python -m meridian.cli run transform-to-silver trips:jc 2026-06

# Aggregate to station-day facts in gold
docker compose exec app python -m meridian.cli run transform-to-gold station-daily 2026-06-02

# Check bronze layer
docker compose exec app python -m meridian.cli inspect bronze trips:jc 2026-06

# Check silver layer and see quarantine reasons
docker compose exec app python -m meridian.cli inspect silver trips:jc 2026-06

# Query the business answer
docker compose exec app python -m meridian.cli report daily-station-trips jc JC115 2026-06-02
```

### With `just` (if installed)

```bash
just up
just run ingest-to-bronze trips:jc 2026-06
just run transform-to-silver trips:jc 2026-06
just run transform-to-gold station-daily 2026-06-02
just inspect bronze trips:jc 2026-06
just inspect silver trips:jc 2026-06
just report daily-station-trips jc JC115 2026-06-02
just down
```

## Understanding the Pipeline

### Command Structure

All commands follow a consistent pattern:

```
python -m meridian.cli <command> <args...>
```

### Commands

#### `run <layer> <job> <window>`

Run a job to populate a layer.

- `layer`: `ingest-to-bronze | transform-to-silver | transform-to-gold`
- `job`: 
  - Bronze/Silver: `trips:jc | trips:nyc`
  - Gold: `station-daily`
- `window`: ISO-8601 timestamp at job's grain
  - Trips (monthly): `2026-06`
  - Station-daily (daily): `2026-06-02`

**Grain validation**: Wrong-grain windows are rejected with non-zero exit and stderr message.

#### `inspect <layer> <job> <window>`

View what's in a layer at a coordinate.

Returns JSON with row counts and (for silver) quarantine reasons.

#### `report <question> <market> <station> <day>`

Query the warehouse.

- `question`: `daily-station-trips` (the only question in Stage 1)
- `market`: `jc | nyc`
- `station`: Source station identifier (e.g., `JC115`)
- `day`: ISO-8601 day (e.g., `2026-06-02`)

Returns JSON with `departures` and `arrivals` counts.

## Implementation Details

### Schema Detection

Automatically detects schema version from CSV headers:

- **Old schema** (pre-migration): Spaces in names (`start station id`)
- **New schema** (post-migration): Underscores (`start_station_id`)

Each market migrated independently:
- Jersey City (JC): Migrated at 2021-02
- New York (NYC): Migrated at 2020-01

### Quarantine Reasons

Trips missing required fields are quarantined:

- **`never_docked`**: Missing start_station_id, end_station_id, start_time, or end_time

Note: Missing station *names* are **not** reasons for quarantine. Referential constraints (station ID not in inferred station list) are also **not** quarantine reasons.

### Export Run Selection

When a month appears multiple times in S3 (different export runs):

1. Group by archive member timestamp
2. Select the group with the latest timestamp
3. Load all files from that export run

This avoids:
- Duplication (loading same month twice from different timestamps)
- Mismatches (floating-point precision differences between exports)
- Partial loads (selecting subset of re-exported files)

Example: April 2018 (NYC) exists in three places with timestamps:
- `2018-09-06` (original)
- `2024-02-21` (re-export, stored twice)

Pipeline selects `2024-02-21`, loads all files from that run, gets exactly 1,307,543 rows.

## Testing

Run the test suite locally (requires Postgres):

```bash
# With dependencies installed
uv run pytest

# Skip e2e tests (those requiring Docker)
uv run pytest -m "not e2e"
```

## Project Structure

```
.
├── docker-compose.yml      # Container orchestration
├── Dockerfile              # App container image
├── justfile                # Task recipes
├── pyproject.toml          # Python project config
├── README.md               # This file
│
├── src/meridian/           # Main package
│   ├── cli.py              # Command-line interface
│   ├── db.py               # Database connection & schema
│   ├── ingest.py           # Bronze layer (S3 fetch & load)
│   ├── s3.py               # S3 listing & filtering
│   ├── transform.py        # Silver & gold layers
│   ├── trip.py             # Trip validation & normalization
│   └── report.py           # Inspect & report functions
│
└── tests/                  # Test suite
    ├── conftest.py         # Pytest configuration
    └── test_basic.py       # Unit tests
```

## Database Schema

### bronze_trips

Raw published bytes, exactly as downloaded:

```sql
id BIGSERIAL PRIMARY KEY
market TEXT
window TEXT
source_key TEXT           -- S3 object key
source_timestamp TIMESTAMP -- Archive member timestamp
raw_row JSONB             -- Unparsed CSV row as JSON
loaded_at TIMESTAMP
```

### silver_trips

Conformed, typed, validated trips:

```sql
id BIGSERIAL PRIMARY KEY
market TEXT
window TEXT
trip_id TEXT
start_station_id TEXT NOT NULL
end_station_id TEXT NOT NULL
start_time TIMESTAMP NOT NULL
end_time TIMESTAMP NOT NULL
user_type TEXT
member_birth_year INT
member_gender INT
bike_id TEXT
```

### quarantine

Rejected trips with business reason:

```sql
id BIGSERIAL PRIMARY KEY
market TEXT
window TEXT
reason TEXT         -- e.g., "never_docked"
raw_row JSONB
loaded_at TIMESTAMP
```

### station_daily_trips

Station-day facts:

```sql
id BIGSERIAL PRIMARY KEY
market TEXT
day DATE NOT NULL
station_id TEXT NOT NULL
departures INT
arrivals INT
```

## Typical Validation Numbers

| Coordinate | Published | Conformed | Quarantine |
|---|---:|---:|---:|
| `trips:jc 2019-06` | 39,430 | 39,430 | 0 |
| `trips:jc 2026-06` | 109,897 | 109,510 | 387 |
| `trips:nyc 2018-04` | 1,307,543 | 1,307,543 | 0 |

Note: NYC April 2018 has both published and conformed at 1,307,543 because:
- Bronze correctly selects one export run
- No duplication reaches silver
- No quarantine needed (all 1,307,543 rows are valid)

## Troubleshooting

### "ERROR: <layer> layer requires <grain> window"

The window grain doesn't match the job. Examples:
- ❌ `just run ingest-to-bronze trips:jc 2026-06-02` (day instead of month)
- ✅ `just run ingest-to-bronze trips:jc 2026-06` (month)
- ❌ `just run transform-to-gold station-daily 2026-06` (month instead of day)
- ✅ `just run transform-to-gold station-daily 2026-06-02` (day)

### "No files found for {market} in {window}"

No S3 objects match the market and month. Verify:
- Market is `jc` or `nyc`
- Month exists in the S3 bucket
- Network access to S3

### Database connection errors

Verify Postgres is running:
```bash
docker compose ps
docker compose logs postgres
```

Ensure environment variables are set (defaults to localhost:5432):
```bash
echo $PGHOST $PGPORT $PGUSER $PGDATABASE
```
