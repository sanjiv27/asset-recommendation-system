FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy server files into /app/server
COPY . ./server/

# Copy tests and pytest config
COPY tests ./tests
COPY pytest.ini ./pytest.ini

# Run tests (|| true to continue on failure)
RUN pytest tests/unit/ -v || true

# Expose the port the app runs on
EXPOSE 8000

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run the application
CMD ["uvicorn", "server.server:app", "--host", "0.0.0.0", "--port", "8000"]

