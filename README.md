# Meridian Bikes Data Pipeline

A three-layer data warehouse pipeline for Meridian Bikes trip data:
- **Bronze**: Raw S3 data with provenance (immutable)
- **Silver**: Conformed, deduplicated trips with quarantine logic
- **Gold**: Business-ready facts (station-day departures/arrivals)

## Setup

```bash
# Start the stack
just up

# Run the full pipeline
just run ingest-to-bronze    trips:jc      2026-06
just run transform-to-silver trips:jc      2026-06
just run transform-to-gold   station-daily 2026-06-02

# Inspect layers
just inspect bronze trips:jc 2026-06
just inspect silver trips:jc 2026-06

# Query business answers
just report daily-station-trips jc JC115 2026-06-02

# Stop the stack
just down
```

## Implementation Notes

- Handles schema migrations (old vs new column names)
- Deduplicates by choosing latest export run (by archive member timestamp)
- Quarantines invalid trips with business-friendly reasons
- Each job is independently runnable and idempotent
- Only `ingest-to-bronze` accesses the network
