set shell := ["bash", "-cu"]

# Start the stack
up:
    if docker info >/dev/null 2>&1; then docker compose up -d --remove-orphans; elif command -v docker.exe >/dev/null 2>&1; then docker.exe compose up -d --remove-orphans; else docker-compose up -d --remove-orphans; fi

# Stop the stack
down:
    if docker info >/dev/null 2>&1; then docker compose down --remove-orphans; elif command -v docker.exe >/dev/null 2>&1; then docker.exe compose down --remove-orphans; else docker-compose down --remove-orphans; fi

# Run a job: layer, job name, window
run layer job window:
    if docker info >/dev/null 2>&1; then docker compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}; elif command -v docker.exe >/dev/null 2>&1; then docker.exe compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}; else docker-compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}; fi

# Inspect a layer
inspect layer job window:
    if docker info >/dev/null 2>&1; then docker compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}; elif command -v docker.exe >/dev/null 2>&1; then docker.exe compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}; else docker-compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}; fi

# Query the business question
report question market station day:
    if docker info >/dev/null 2>&1; then docker compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}; elif command -v docker.exe >/dev/null 2>&1; then docker.exe compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}; else docker-compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}; fi
