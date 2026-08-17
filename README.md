# Meridian Bikes Data Pipeline

A three-layer data warehouse for Meridian Bikes: bronze (raw S3), silver (conformed), gold (station-day facts).

## Quick Start

```bash
# Start the stack
docker compose up -d

# Example: ingest June 2026 Jersey City trips
docker compose exec app python -m meridian.cli run ingest-to-bronze trips:jc 2026-06
docker compose exec app python -m meridian.cli run transform-to-silver trips:jc 2026-06
docker compose exec app python -m meridian.cli run transform-to-gold station-daily 2026-06-02

# Query the answer
docker compose exec app python -m meridian.cli report daily-station-trips jc JC115 2026-06-02
# -> {"market":"jc","station":"JC115","day":"2026-06-02","departures":216,"arrivals":209}
```

See [IMPLEMENTATION.md](IMPLEMENTATION.md) for full documentation, architecture, and usage.

## Key Features

- **Three-layer pipeline**: Bronze (raw), Silver (conformed), Gold (facts)
- **Schema detection**: Handles old vs. new column names automatically
- **Export deduplication**: Chooses latest export run by archive timestamp
- **Quarantine with reasons**: Invalid trips preserved with business reasons
- **Idempotent & separable**: Each job runs independently and safely re-runnable
- **JSON reporting**: All output is structured JSON for easy parsing

## Development

```bash
# Run tests (requires Postgres)
uv run pytest

# Run linter
uv run mypy src/
```

## Spec

Implementation of Stage 1 of the Meridian Bikes data engineering exercise.

