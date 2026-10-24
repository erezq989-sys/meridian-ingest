# Meridian Bikes Data Pipeline

A three-layer data warehouse for Meridian Bikes with an Airflow 3 operational layer: bronze (raw S3), silver (conformed), and gold (station-day facts).

## Quick Start

```bash
# Start the warehouse and Airflow (both market schedules start off)
just up

# Run one month, the next incomplete month, or an inclusive range
just run pipeline jc 2026-06
just run pipeline jc
just run pipeline jc 2021-01 2021-02

# Operate and inspect Airflow
just schedule jc on
just schedule jc off
just inspect coverage trips:jc
just inspect runs jc

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
- **Airflow 3 orchestration**: Independent monthly DAGs for Jersey City and New York
- **Operational coverage**: A computed watermark, gaps, and next-due month backed by successful-load records
- **Safe retries**: Duplicate-safe monthly work and resumable per-day gold batches

## Development

```bash
# Run tests (requires Postgres)
uv run pytest

# Run linter
uv run mypy src/
```

`just down` is a full Stage 1 reset: it stops the stack and removes the
warehouse volume. Run `just up` to recreate the schema through the one-shot
`schema` service.

## Spec

Implementation of Stages 1 and 2 of the Meridian Bikes data engineering exercise.

