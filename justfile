set shell := ["bash", "-cu"]

# Start the stack
up:
    if command -v docker >/dev/null 2>&1; then docker compose up -d --remove-orphans; else docker-compose up -d --remove-orphans; fi

# Stop the stack
down:
    if command -v docker >/dev/null 2>&1; then docker compose down -v --remove-orphans; else docker-compose down -v --remove-orphans; fi

# Run a job: layer, job name, window
run layer job window:
    if command -v docker >/dev/null 2>&1; then docker compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}; else docker-compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}; fi

# Inspect a layer
inspect layer job window:
    if command -v docker >/dev/null 2>&1; then docker compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}; else docker-compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}; fi

# Query the business question
report question market station day:
    if command -v docker >/dev/null 2>&1; then docker compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}; else docker-compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}; fi
