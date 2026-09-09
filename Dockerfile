FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    just \
    docker-compose \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Install Python dependencies, including pytest for running the suite in Docker
RUN pip install -e . pytest

# Set PYTHONUNBUFFERED to ensure output is logged immediately
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "meridian.cli"]
