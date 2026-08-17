set shell := ["powershell", "-Command"]

# Start the stack
up:
    docker compose up -d

# Stop the stack
down:
    docker compose down

# Run a job: layer, job name, window
run layer job window:
    docker compose exec app python -m meridian.cli run {{layer}} {{job}} {{window}}

# Inspect a layer
inspect layer job window:
    docker compose exec app python -m meridian.cli inspect {{layer}} {{job}} {{window}}

# Query the business question
report question market station day:
    docker compose exec app python -m meridian.cli report {{question}} {{market}} {{station}} {{day}}
