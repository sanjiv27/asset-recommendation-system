import time
import threading
import schedule
from recommendation_engine import RecommendationEngine
from kafka import KafkaConsumer
from db import save_recommendations
import json

# 1. Initialize Global Engine
engine = RecommendationEngine()
consumer = KafkaConsumer('userprofile', bootstrap_servers=['kafka:9092'], auto_offset_reset='earliest')

def run_scheduler():
    """Background thread to update models every X minutes"""
    while True:
        schedule.run_pending()
        time.sleep(1)

def refresh_job():
    print("--- Scheduled Job: Refreshing Data & Models ---")
    engine.refresh_models()

def process_kafka_message(msg):
    """
    Mock function for processing a Kafka message.
    Message: {'customer_id': '123', action:'request_recs'}
    Message: {'customer_id': '123', action:'refresh_recs'}
    """
    action = msg.get('action')
    customer_id = msg.get('customer_id')
    if action == 'request_recs':
        if engine.is_ready:
            recs = engine.get_recommendation(customer_id, top_n=10)
            save_recommendations(customer_id, recs)
            print(f"Recommendations for {customer_id}: {recs}")
            # Produce result back to Kafka or save to DB
        else:
            print("System warming up, cannot recommend yet.")

    elif action == 'refresh_recs':
        engine.refresh_models()
        print("Recommendations refreshed.")
    else:
        print("Invalid action.")

def main():
    # 1. Initial Load (Blocking)
    print("Initializing Worker... Loading Data...")
    engine.refresh_models()
    
    # 2. Schedule Retraining (e.g., every 6 hours)
    # This runs in a separate thread so it doesn't block Kafka consumption
    schedule.every(10).minutes.do(refresh_job)
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    # 3. Kafka Consumer Loop (Pseudo-code)
    print("Worker started. Listening for messages...")
    
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        process_kafka_message(json.loads(msg.value()))
    
if __name__ == "__main__":
    main()