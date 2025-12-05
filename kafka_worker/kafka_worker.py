import time
import threading
import schedule
from recommendation_engine import RecommendationEngine
from kafka import KafkaConsumer
from db import save_recommendations, get_recent_user_interactions
import json
from kafka.errors import NoBrokersAvailable
from flask import Flask, jsonify
import os

# Flask app for internal API
app = Flask(__name__)

@app.route('/models', methods=['GET'])
def list_models():
    """List available models and their metadata."""
    import pickle
    models = []
    
    models_dir = "/app/models"
    if not os.path.exists(models_dir):
        return jsonify({"status": "ok", "models": []})
    
    # Get active model
    active_link = os.path.join(models_dir, 'active_model.pkl')
    active_model = None
    if os.path.islink(active_link):
        active_model = os.path.basename(os.readlink(active_link))
    
    # List all versioned models
    for filename in sorted(os.listdir(models_dir)):
        if filename.startswith('model_v') and filename.endswith('.pkl'):
            model_path = os.path.join(models_dir, filename)
            try:
                with open(model_path, 'rb') as f:
                    model_data = pickle.load(f)
                metadata = model_data.get('metadata', {})
                models.append({
                    "name": metadata.get('name', filename.replace('.pkl', '')),
                    "filename": filename,
                    "type": "SVD",
                    "status": "active" if filename == active_model else "inactive",
                    "n_users": metadata.get('n_users', 'N/A'),
                    "n_items": metadata.get('n_items', 'N/A'),
                    "explained_variance": f"{metadata.get('explained_variance', 0)*100:.2f}%",
                    "trained_on": metadata.get('trained_on', 'Unknown')
                })
            except:
                pass
    
    return jsonify({"status": "ok", "models": models})

@app.route('/models/<model_name>/activate', methods=['POST'])
def activate_model(model_name):
    """Set a model as active."""
    models_dir = "/app/models"
    model_path = os.path.join(models_dir, model_name)
    
    if not os.path.exists(model_path):
        return jsonify({"status": "error", "message": "Model not found"}), 404
    
    active_link = os.path.join(models_dir, 'active_model.pkl')
    if os.path.exists(active_link):
        os.remove(active_link)
    os.symlink(model_path, active_link)
    
    # Reload engine
    engine.refresh_models()
    
    return jsonify({"status": "ok", "message": f"Activated {model_name}"})

@app.route('/retrain', methods=['POST'])
def trigger_retrain():
    """Internal endpoint to trigger model retraining."""
    try:
        from retrain import retrain_model
        result = retrain_model()
        # Reload the engine after retraining
        engine.refresh_models()
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def run_flask():
    """Run Flask server in background thread."""
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

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
    action = msg.get('action')
    # If the message contains history_isins, it's a recommendation request
    if customer_id and engine.is_ready and action == 'request_recs':
        print(f"Generating recommendations for {customer_id}...")
        recs = engine.get_recommendation(customer_id, top_n=10, recent_interactions=get_recent_user_interactions(customer_id))
        save_recommendations(customer_id, recs)
        print(f"Saved recommendations for {customer_id}")
    elif action == 'refresh_recs':
        print(f"Refreshing recommendations for {customer_id}...")
        recs = engine.get_recommendation(customer_id, top_n=10, recent_interactions=get_recent_user_interactions(customer_id))
        save_recommendations(customer_id, recs)
        print(f"Saved refreshed recommendations for {customer_id}")
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
    # Start Flask API in background
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("Internal API started on port 5000")
    
    main()