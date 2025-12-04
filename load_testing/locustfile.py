from locust import HttpUser, task, between
import random

# Sample customer IDs (you can expand this list)
CUSTOMER_IDS = [
    "DED5BF19E23CCCFEE322",
    "C001", "C002", "C003", "C004", "C005"
]

# Sample ISINs for interactions
SAMPLE_ISINS = [
    "GRS003003035", "GRS247003007", "GRS320313000",
    "GRC1451184D4", "GRC2451227D9", "GRC419120AD7"
]

class RecommendationUser(HttpUser):
    wait_time = between(1, 3)  # Wait 1-3 seconds between tasks
    
    def on_start(self):
        """Called when a simulated user starts"""
        self.customer_id = random.choice(CUSTOMER_IDS)
    
    @task(10)  # Weight: 10 (most common action)
    def get_recommendations(self):
        """Fetch existing recommendations"""
        self.client.get(
            f"/recommendations/{self.customer_id}",
            name="/recommendations/[customer_id]"
        )
    
    @task(5)  # Weight: 5
    def log_click(self):
        """Log a click interaction"""
        self.client.post(
            "/user_interactions",
            json={
                "customer_id": self.customer_id,
                "isin": random.choice(SAMPLE_ISINS),
                "type": "click"
            }
        )
    
    @task(2)  # Weight: 2
    def request_new_recommendations(self):
        """Request new recommendations (triggers Kafka)"""
        self.client.post(
            "/recommendations",
            json={
                "customer_id": self.customer_id,
                "action": "request_recs"
            }
        )
    
    @task(1)  # Weight: 1 (least common)
    def add_to_watchlist(self):
        """Add asset to watchlist"""
        isin = random.choice(SAMPLE_ISINS)
        self.client.post(f"/watchlist/{self.customer_id}/{isin}")
    
    @task(8)  # Weight: 8
    def health_check(self):
        """Health check endpoint"""
        self.client.get("/health")
