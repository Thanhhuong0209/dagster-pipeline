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

# Expose Prefect ports
EXPOSE 4200

# Set environment variables
ENV PREFECT_HOME=/app/.prefect
ENV PYTHONUNBUFFERED=1

# Default VictoriaMetrics URL (can be overridden)
ENV VICTORIAMETRICS_URL=http://victoriametrics:8428

# Run Prefect server with API enabled
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"]

