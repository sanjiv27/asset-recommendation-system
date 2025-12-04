# Load Testing

## Quick Start

### 1. Start Load Testing Container
```bash
docker compose --profile loadtest up -d loadtest
```

### 2. Run Locust Load Test
Access Locust Web UI: **http://localhost:8089**

- Number of users: Start with 10, increase to 50, 100
- Spawn rate: 5 users/second
- Host: http://server:8000 (pre-configured)

Click "Start Swarming" and let it run for 2-5 minutes.

### 3. Collect Metrics (In Parallel)
In another terminal:
```bash
docker exec asset-recommendation-loadtest python collect_metrics.py
```

Let this run during the entire load test. Press Ctrl+C when done.

### 4. Generate Visualizations
```bash
docker exec asset-recommendation-loadtest python visualize.py
```

### 5. View Results
Results are saved to `./load_testing/results/`:
- `kafka_lag.png` - Kafka consumer lag over time
- `api_latency.png` - API response times
- `throughput.png` - Interactions and recommendations
- `db_connections.png` - Database connection usage
- `summary.txt` - Performance summary statistics
- `metrics.csv` - Raw data

## Test Scenarios

### Scenario 1: Baseline (10 users)
- Tests normal operation
- Establishes baseline metrics

### Scenario 2: Medium Load (50 users)
- Tests system under moderate load
- Identifies first bottlenecks

### Scenario 3: High Load (100+ users)
- Tests system limits
- Shows Kafka's benefit in handling spikes

## Metrics Explained

**Kafka Lag**: Number of messages waiting to be processed. Higher lag = worker can't keep up.

**API Latency**: Time to respond to requests. Should stay <100ms for good UX.

**Throughput**: Rate of processing interactions and generating recommendations.

**DB Connections**: Active database connections. Too many = connection pool exhaustion.

## Cleanup
```bash
docker compose --profile loadtest down
```
