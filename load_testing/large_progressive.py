import subprocess
import json
import time

user_counts = [50, 100, 200, 300, 400, 500]
results = []

for i, users in enumerate(user_counts, 1):
    print(f"\n{'='*60}")
    print(f"[{i}/{len(user_counts)}] Testing {users} concurrent users...")
    print(f"{'='*60}")
    
    start = time.time()
    subprocess.run([
        "locust", "-f", "/app/locustfile.py",
        "--host=http://asset-recommendation-server:8000",
        f"--users={users}",
        "--spawn-rate=20",
        "--run-time=90s",
        "--headless",
        "--csv=/results/test",
        "--only-summary"
    ])
    elapsed = time.time() - start
    
    print(f"\n✓ Test completed in {elapsed:.0f}s")
    
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
                print(f"  → {parts[8].strip()} req/s, {parts[4].strip()}ms avg latency")
    
    with open("/results/metrics.csv", "r") as f:
        lags = [int(l.split(",")[1]) for l in f.readlines()[1:]]
        results[-1]["max_kafka_lag"] = max(lags) if lags else 0
        print(f"  → Max Kafka lag: {results[-1]['max_kafka_lag']} messages")

with open("/results/progressive_results.json", "w") as f:
    json.dump(results, f, indent=2)

print("\n" + "="*60)
print("SUMMARY")
print("="*60)
for r in results:
    print(f"{r['users']:3d} users: {r['req_per_sec']:6.1f} req/s, "
          f"{r['avg_latency']:5.1f}ms, lag: {r['max_kafka_lag']}")
print("="*60)
