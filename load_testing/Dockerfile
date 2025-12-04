FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY locustfile.py .
COPY collect_metrics.py .
COPY visualize.py .

# Create results directory
RUN mkdir -p /results

ENV PYTHONUNBUFFERED=1

# Default command (can be overridden)
CMD ["locust", "-f", "locustfile.py", "--host=http://server:8000"]
