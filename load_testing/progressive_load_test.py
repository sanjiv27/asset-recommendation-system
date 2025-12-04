import subprocess
import json
import time

user_counts = [10, 25, 50, 100, 150, 200]
results = []

for users in user_counts:
    print(f"\n{'='*60}")
    print(f"Running load test with {users} concurrent users...")
    print(f"{'='*60}\n")
    
    # Run locust test
    subprocess.run([
        "locust", "-f", "/app/locustfile.py",
        "--host=http://asset-recommendation-server:8000",
        f"--users={users}",
        "--spawn-rate=5",
        "--run-time=2m",
        "--headless",
        "--csv=/results/loadtest",
        "--only-summary"
    ])
    
    # Collect final metrics
    time.sleep(5)
    
    # Parse CSV results
    with open("/results/loadtest_stats.csv", "r") as f:
        lines = [l for l in f.readlines() if "Aggregated" in l]
        if lines:
            aggregated = lines[0].split(",")
            results.append({
                "users": users,
                "total_requests": int(aggregated[2]),
                "failure_rate": float(aggregated[3].strip('%')),
                "avg_latency": float(aggregated[4]),
                "max_latency": float(aggregated[6]),
                "requests_per_sec": float(aggregated[8])
            })
    
    # Parse metrics.csv for Kafka lag
    with open("/results/metrics.csv", "r") as f:
        lines = f.readlines()[1:]  # Skip header
        kafka_lags = [int(line.split(",")[1]) for line in lines]
        results[-1]["max_kafka_lag"] = max(kafka_lags)
        results[-1]["avg_kafka_lag"] = sum(kafka_lags) / len(kafka_lags)
    
    print(f"✓ Completed {users} users: {results[-1]['requests_per_sec']:.1f} req/s, "
          f"{results[-1]['avg_latency']:.1f}ms latency, "
          f"max Kafka lag: {results[-1]['max_kafka_lag']}")
    
    time.sleep(10)  # Cool down between tests

# Save results
with open("/results/progressive_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n" + "="*60)
print("Progressive load test complete!")
print("="*60)
