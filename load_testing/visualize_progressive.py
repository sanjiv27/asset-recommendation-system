import json
import matplotlib.pyplot as plt
import seaborn as sns

with open("/results/progressive_results.json", "r") as f:
    results = json.load(f)

users = [r["users"] for r in results]
avg_latency = [r["avg_latency"] for r in results]
max_kafka_lag = [r["max_kafka_lag"] for r in results]
req_per_sec = [r["req_per_sec"] for r in results]

sns.set_style("whitegrid")

# Latency vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, avg_latency, marker='o', linewidth=2, markersize=10, color='#2E86AB')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Avg Latency (ms)", fontsize=12)
plt.title("API Latency vs Concurrent Users", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/latency_vs_users.png", dpi=150, bbox_inches='tight')
print("✓ Saved: latency_vs_users.png")
plt.close()

# Kafka Lag vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, max_kafka_lag, marker='s', linewidth=2, markersize=10, color='#F77F00')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Max Kafka Lag (messages)", fontsize=12)
plt.title("Kafka Consumer Lag vs Concurrent Users", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/kafka_lag_vs_users.png", dpi=150, bbox_inches='tight')
print("✓ Saved: kafka_lag_vs_users.png")
plt.close()

# Throughput vs Users
plt.figure(figsize=(10, 6))
plt.plot(users, req_per_sec, marker='^', linewidth=2, markersize=10, color='#06A77D')
plt.xlabel("Concurrent Users", fontsize=12)
plt.ylabel("Requests/Second", fontsize=12)
plt.title("System Throughput vs Concurrent Users", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("/results/throughput_vs_users.png", dpi=150, bbox_inches='tight')
print("✓ Saved: throughput_vs_users.png")
plt.close()

# Latency vs Throughput
plt.figure(figsize=(10, 6))
scatter = plt.scatter(req_per_sec, avg_latency, s=200, alpha=0.6, c=users, cmap='viridis', edgecolors='black', linewidth=1.5)
plt.xlabel("Requests/Second", fontsize=12)
plt.ylabel("Avg Latency (ms)", fontsize=12)
plt.title("Latency vs Throughput", fontsize=14, fontweight='bold')
plt.grid(True, alpha=0.3)
cbar = plt.colorbar(scatter)
cbar.set_label("Concurrent Users", fontsize=11)
plt.tight_layout()
plt.savefig("/results/latency_vs_throughput.png", dpi=150, bbox_inches='tight')
print("✓ Saved: latency_vs_throughput.png")
plt.close()

print("\n✅ All graphs generated successfully!")
