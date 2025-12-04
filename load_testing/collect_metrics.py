import time
import csv
from datetime import datetime
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
import psycopg2
import requests

# Configuration
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "userprofile"
KAFKA_GROUP = "asset_recommendation_worker_group"
API_URL = "http://server:8000"
DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "database": "asset_recommendation",
    "user": "admin",
    "password": "123"
}

def collect_kafka_metrics():
    """Collect Kafka consumer lag"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=[KAFKA_BROKER],
            group_id=KAFKA_GROUP
        )
        
        tp = TopicPartition(KAFKA_TOPIC, 0)
        consumer.assign([tp])
        
        # Get current offset (where consumer is)
        committed = consumer.committed(tp) or 0
        
        # Get end offset (latest message)
        consumer.seek_to_end(tp)
        end_offset = consumer.position(tp)
        
        lag = end_offset - committed
        consumer.close()
        
        return {
            "committed_offset": committed,
            "end_offset": end_offset,
            "lag": lag
        }
    except Exception as e:
        print(f"Error collecting Kafka metrics: {e}")
        return {"committed_offset": 0, "end_offset": 0, "lag": 0}

def collect_db_metrics():
    """Collect database metrics"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Count recommendations
        cursor.execute("SELECT COUNT(*) FROM recommendations")
        rec_count = cursor.fetchone()[0]
        
        # Count interactions
        cursor.execute("SELECT COUNT(*) FROM user_interactions")
        interaction_count = cursor.fetchone()[0]
        
        # Active connections
        cursor.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'active'")
        active_conns = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return {
            "recommendations_count": rec_count,
            "interactions_count": interaction_count,
            "active_connections": active_conns
        }
    except Exception as e:
        print(f"Error collecting DB metrics: {e}")
        return {"recommendations_count": 0, "interactions_count": 0, "active_connections": 0}

def collect_api_metrics():
    """Test API response time"""
    try:
        start = time.time()
        response = requests.get(f"{API_URL}/health", timeout=5)
        latency = (time.time() - start) * 1000  # Convert to ms
        
        return {
            "api_status": response.status_code,
            "api_latency_ms": latency
        }
    except Exception as e:
        print(f"Error collecting API metrics: {e}")
        return {"api_status": 0, "api_latency_ms": 0}

def main():
    """Main metrics collection loop"""
    output_file = "/results/metrics.csv"
    
    # Create CSV with headers
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'timestamp', 'kafka_lag', 'kafka_committed', 'kafka_end_offset',
            'db_recommendations', 'db_interactions', 'db_active_conns',
            'api_status', 'api_latency_ms'
        ])
    
    print("Starting metrics collection... (Press Ctrl+C to stop)")
    
    try:
        while True:
            timestamp = datetime.now().isoformat()
            
            kafka_metrics = collect_kafka_metrics()
            db_metrics = collect_db_metrics()
            api_metrics = collect_api_metrics()
            
            # Write to CSV
            with open(output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    timestamp,
                    kafka_metrics['lag'],
                    kafka_metrics['committed_offset'],
                    kafka_metrics['end_offset'],
                    db_metrics['recommendations_count'],
                    db_metrics['interactions_count'],
                    db_metrics['active_connections'],
                    api_metrics['api_status'],
                    api_metrics['api_latency_ms']
                ])
            
            print(f"[{timestamp}] Kafka Lag: {kafka_metrics['lag']}, "
                  f"API Latency: {api_metrics['api_latency_ms']:.2f}ms")
            
            time.sleep(5)  # Collect every 5 seconds
            
    except KeyboardInterrupt:
        print("\nMetrics collection stopped.")
        print(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
