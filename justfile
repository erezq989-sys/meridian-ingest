set shell := ["bash", "-cu"]

docker := if os_family() == "windows" { "docker.exe" } else { "docker" }

# Start a fresh-capable stack; the schema service owns all DDL.
up:
    {{docker}} compose up -d --build --remove-orphans

# Stop the stack and remove warehouse volumes, as Stage 1 reset semantics require.
down:
    {{docker}} compose down -v --remove-orphans

# Run a Stage 1 job or a Stage 2 Airflow pipeline.
run target arg1 arg2="" arg3="":
    if [[ "{{target}}" == "pipeline" ]]; then \
      {{docker}} compose exec -T airflow-scheduler python -m meridian_ops.cli run-pipeline "{{arg1}}" "{{arg2}}" "{{arg3}}"; \
    else \
      {{docker}} compose exec -T app python -m meridian.cli run "{{target}}" "{{arg1}}" "{{arg2}}"; \
    fi

# Inspect a Stage 1 layer or Stage 2 coverage/runs.
inspect target arg1 arg2="":
    if [[ "{{target}}" == "coverage" ]]; then \
      {{docker}} compose exec -T airflow-scheduler python -m meridian_ops.cli coverage "{{arg1}}"; \
    elif [[ "{{target}}" == "runs" ]]; then \
      {{docker}} compose exec -T airflow-scheduler python -m meridian_ops.cli runs "{{arg1}}"; \
    else \
      {{docker}} compose exec -T app python -m meridian.cli inspect "{{target}}" "{{arg1}}" "{{arg2}}"; \
    fi

# Enable or disable one market's Airflow schedule.
schedule market state:
    {{docker}} compose exec -T airflow-scheduler python -m meridian_ops.cli schedule "{{market}}" "{{state}}"

# Query the business question.
report question market station day:
    {{docker}} compose exec -T app python -m meridian.cli report "{{question}}" "{{market}}" "{{station}}" "{{day}}"

