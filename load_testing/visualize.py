import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)

def load_data():
    """Load metrics from CSV"""
    df = pd.read_csv('/results/metrics.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['time_elapsed'] = (df['timestamp'] - df['timestamp'].min()).dt.total_seconds()
    return df

def plot_kafka_lag(df):
    """Plot Kafka consumer lag over time"""
    plt.figure(figsize=(12, 6))
    plt.plot(df['time_elapsed'], df['kafka_lag'], linewidth=2, color='#e74c3c')
    plt.fill_between(df['time_elapsed'], df['kafka_lag'], alpha=0.3, color='#e74c3c')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Consumer Lag (messages)', fontsize=12)
    plt.title('Kafka Consumer Lag During Load Test', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('/results/kafka_lag.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: kafka_lag.png")

def plot_api_latency(df):
    """Plot API response time"""
    plt.figure(figsize=(12, 6))
    plt.plot(df['time_elapsed'], df['api_latency_ms'], linewidth=2, color='#3498db')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Latency (ms)', fontsize=12)
    plt.title('API Response Time During Load Test', fontsize=14, fontweight='bold')
    plt.axhline(y=100, color='orange', linestyle='--', label='Target: 100ms')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('/results/api_latency.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: api_latency.png")

def plot_throughput(df):
    """Plot system throughput (interactions and recommendations)"""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Interactions over time
    ax1.plot(df['time_elapsed'], df['db_interactions'], linewidth=2, color='#2ecc71')
    ax1.set_xlabel('Time (seconds)', fontsize=12)
    ax1.set_ylabel('Total Interactions', fontsize=12)
    ax1.set_title('User Interactions Over Time', fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Recommendations over time
    ax2.plot(df['time_elapsed'], df['db_recommendations'], linewidth=2, color='#9b59b6')
    ax2.set_xlabel('Time (seconds)', fontsize=12)
    ax2.set_ylabel('Total Recommendations', fontsize=12)
    ax2.set_title('Recommendations Generated Over Time', fontsize=14, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/results/throughput.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: throughput.png")

def plot_db_connections(df):
    """Plot database active connections"""
    plt.figure(figsize=(12, 6))
    plt.plot(df['time_elapsed'], df['db_active_conns'], linewidth=2, color='#f39c12')
    plt.xlabel('Time (seconds)', fontsize=12)
    plt.ylabel('Active Connections', fontsize=12)
    plt.title('Database Active Connections', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('/results/db_connections.png', dpi=300, bbox_inches='tight')
    print("✓ Saved: db_connections.png")

def generate_summary(df):
    """Generate summary statistics"""
    summary = f"""
Load Test Summary Report
========================
Duration: {df['time_elapsed'].max():.0f} seconds
Data Points: {len(df)}

Kafka Metrics:
- Max Consumer Lag: {df['kafka_lag'].max():.0f} messages
- Avg Consumer Lag: {df['kafka_lag'].mean():.2f} messages
- Total Messages Processed: {df['kafka_end_offset'].max():.0f}

API Performance:
- Avg Latency: {df['api_latency_ms'].mean():.2f} ms
- Max Latency: {df['api_latency_ms'].max():.2f} ms
- Min Latency: {df['api_latency_ms'].min():.2f} ms
- P95 Latency: {df['api_latency_ms'].quantile(0.95):.2f} ms

System Throughput:
- Total Interactions: {df['db_interactions'].max():.0f}
- Total Recommendations: {df['db_recommendations'].max():.0f}
- Avg DB Connections: {df['db_active_conns'].mean():.2f}
"""
    
    with open('/results/summary.txt', 'w') as f:
        f.write(summary)
    
    print(summary)
    print("✓ Saved: summary.txt")

def main():
    """Generate all visualizations"""
    print("Loading metrics data...")
    df = load_data()
    
    print(f"Loaded {len(df)} data points")
    print("\nGenerating visualizations...")
    
    plot_kafka_lag(df)
    plot_api_latency(df)
    plot_throughput(df)
    plot_db_connections(df)
    generate_summary(df)
    
    print("\n✅ All visualizations generated successfully!")
    print("Results saved to /results/")

if __name__ == "__main__":
    main()
