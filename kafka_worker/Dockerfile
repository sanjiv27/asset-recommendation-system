FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy worker files
COPY kafka_worker.py .
COPY recommendation_engine.py .
COPY db.py .
COPY retrain.py .

# Copy tests and pytest config
COPY tests ./tests
COPY pytest.ini ./pytest.ini

# Run tests (|| true to continue on failure)
RUN pytest tests/unit/ -v || true

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run the worker
CMD ["python", "kafka_worker.py"]

