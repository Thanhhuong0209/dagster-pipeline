FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose Dagster ports
EXPOSE 3000

# Set environment variables
ENV DAGSTER_HOME=/app/.dagster
ENV PYTHONUNBUFFERED=1

# Default VictoriaMetrics URL (can be overridden)
ENV VICTORIAMETRICS_URL=http://victoriametrics:8428

# Run Dagster
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]

