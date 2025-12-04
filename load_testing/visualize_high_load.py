import json
import matplotlib.pyplot as plt
import seaborn as sns

with open("/results/high_load_results.json", "r") as f:
    results = json.load(f)

users = [r["users"] for r in results]
avg_latency = [r["avg_latency"] for r in results]
max_kafka_lag = [r["max_kafka_lag"] for r in results]
req_per_sec = [r["req_per_sec"] for r in results]
failures = [r["failures"] for r in results]

sns.set_style("whitegrid")

# Latency vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, avg_latency, marker='o', linewidth=2, markersize=10, color='#2E86AB')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Avg Latency (ms)", fontsize=12)
plt.title("API Latency vs Concurrent Users (High Load)", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/high_load_latency.png", dpi=150, bbox_inches='tight')
print("✓ Saved: high_load_latency.png")
plt.close()

# Kafka Lag vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, max_kafka_lag, marker='s', linewidth=2, markersize=10, color='#F77F00')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Max Kafka Lag (messages)", fontsize=12)
plt.title("Kafka Consumer Lag vs Concurrent Users (High Load)", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/high_load_kafka_lag.png", dpi=150, bbox_inches='tight')
print("✓ Saved: high_load_kafka_lag.png")
plt.close()

# Throughput vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, req_per_sec, marker='^', linewidth=2, markersize=10, color='#06A77D')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Requests/Second", fontsize=12)
plt.title("System Throughput vs Concurrent Users (High Load)", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/high_load_throughput.png", dpi=150, bbox_inches='tight')
print("✓ Saved: high_load_throughput.png")
plt.close()

# Failures vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, failures, marker='x', linewidth=2, markersize=12, color='#D62828')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Number of Failures", fontsize=12)
plt.title("Request Failures vs Concurrent Users (High Load)", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/high_load_failures.png", dpi=150, bbox_inches='tight')
print("✓ Saved: high_load_failures.png")
plt.close()

print("\n✅ All high load graphs generated successfully!")
