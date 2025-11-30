import time
import threading
import schedule
from recommendation_engine import RecommendationEngine
from kafka import KafkaConsumer
from db import save_recommendations, get_recent_user_interactions
import json
from kafka.errors import NoBrokersAvailable

# 1. Initialize Global Engine
engine = RecommendationEngine()


def get_kafka_consumer():
    retries = 0
    while retries < 15:
        try:
            consumer = KafkaConsumer(
                'userprofile', 
                bootstrap_servers=['kafka:9092'],
                group_id='asset_recommendation_worker_group',
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print("Worker connected to Kafka!")
            return consumer
        except NoBrokersAvailable:
            print("Kafka Broker not available. Retrying in 2 seconds...")
            time.sleep(2)
            retries += 1
    raise Exception("Failed to connect to Kafka")


consumer = get_kafka_consumer()
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
    """
    # Defensive programming: Check if msg is actually a dict
    if not isinstance(msg, dict):
        print(f"Error: Received invalid message format: {type(msg)}")
        return

    # In your new architecture, the payload is the user profile itself
    # So we assume the action is implied or we check the payload structure
    
    # Adapt this to match your UserProfilePayload from server.py
    customer_id = msg.get('customer_id')
    
    # If the message contains history_isins, it's a recommendation request
    if customer_id and engine.is_ready:
        print(f"Generating recommendations for {customer_id}...")
        
        # Note: You might want to pass the specific user profile data 
        # from 'msg' into get_recommendation if you implemented the 
        # optimizations we discussed previously.
        recs = engine.get_recommendation(customer_id, top_n=10, recent_interactions=get_recent_user_interactions(customer_id))
        
        save_recommendations(customer_id, recs)
        print(f"Saved recommendations for {customer_id}")
    else:
        print("System warming up or invalid message.")

def main():
    # 1. Initial Load (Blocking)
    print("Initializing Worker... Loading Data...")
    engine.refresh_models()
    
    # 2. Schedule Retraining
    schedule.every(10).minutes.do(refresh_job)
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    # 3. Kafka Consumer Loop
    print("Worker started. Listening for messages...")
    
    try:
        while True:
            # poll returns a Dictionary: { TopicPartition: [List of Records] }
            msg_batch = consumer.poll(timeout_ms=1000)
            
            # Check if batch is empty
            if not msg_batch:
                continue
            
            # Iterate over partitions and their lists of messages
            for partition, messages in msg_batch.items():
                for record in messages:
                    # record.value is now a Dict because of value_deserializer above
                    try: 
                        process_kafka_message(record.value)
                    except Exception as e:
                        print(f"Error processing message: {e}")
                        continue
            consumer.commit()
    except KeyboardInterrupt:
        print("Worker stopping...")
    finally:
        consumer.close()
    
if __name__ == "__main__":
    main()