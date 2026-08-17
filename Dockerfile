FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY src/ src/

# Install Python dependencies
RUN pip install -e .

# Set PYTHONUNBUFFERED to ensure output is logged immediately
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "meridian.cli"]
