import requests
from typing import Optional

API_BASE_URL = "http://server:8000"

def get_recommendations(customer_id: str):
    """Fetch recommendations for a customer."""
    response = requests.get(f"{API_BASE_URL}/recommendations/{customer_id}")
    return response.json()

def request_recommendations(customer_id: str, action: str = "request_recs"):
    """Request new or refresh recommendations."""
    response = requests.post(
        f"{API_BASE_URL}/recommendations",
        json={"customer_id": customer_id, "action": action}
    )
    return response.json()

def log_interaction(customer_id: str, isin: str, interaction_type: str = "click"):
    """Log user interaction with an asset."""
    response = requests.post(
        f"{API_BASE_URL}/user_interactions",
        json={"customer_id": customer_id, "isin": isin, "type": interaction_type}
    )
    return response.json()

def check_health():
    """Check API health."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        return response.json()
    except:
        return {"status": "error"}
