import subprocess
import json

user_counts = [25, 50, 100, 150]
results = []

for users in user_counts:
    print(f"\n{'='*50}")
    print(f"Testing {users} users...")
    print(f"{'='*50}")
    
    subprocess.run([
        "locust", "-f", "/app/locustfile.py",
        "--host=http://asset-recommendation-server:8000",
        f"--users={users}",
        "--spawn-rate=10",
        "--run-time=1m",
        "--headless",
        "--csv=/results/test",
        "--only-summary"
    ])
    
    with open("/results/test_stats.csv", "r") as f:
        for line in f:
            if "Aggregated" in line:
                parts = line.split(",")
                results.append({
                    "users": users,
                    "requests": int(parts[2]),
                    "avg_latency": float(parts[4]),
                    "req_per_sec": float(parts[8])
                })
    
    with open("/results/metrics.csv", "r") as f:
        lags = [int(l.split(",")[1]) for l in f.readlines()[1:]]
        results[-1]["max_kafka_lag"] = max(lags) if lags else 0

with open("/results/progressive_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n" + "="*50)
for r in results:
    print(f"{r['users']:3d} users: {r['req_per_sec']:6.1f} req/s, "
          f"{r['avg_latency']:5.1f}ms latency, lag: {r['max_kafka_lag']}")
print("="*50)
