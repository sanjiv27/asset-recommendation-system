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

def log_interaction(customer_id: str, isin: str, interaction_type: str = "click", weight: int = None):
    """Log user interaction with an asset."""
    payload = {"customer_id": customer_id, "isin": isin, "type": interaction_type}
    if weight is not None:
        payload["weight"] = weight
    
    response = requests.post(f"{API_BASE_URL}/user_interactions", json=payload)
    return response.json()

def check_health():
    """Check API health."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=2)
        return response.json()
    except:
        return {"status": "error"}

def add_to_watchlist(customer_id: str, isin: str):
    """Add asset to watchlist."""
    response = requests.post(f"{API_BASE_URL}/watchlist/{customer_id}/{isin}")
    return response.json()

def remove_from_watchlist(customer_id: str, isin: str):
    """Remove asset from watchlist."""
    response = requests.delete(f"{API_BASE_URL}/watchlist/{customer_id}/{isin}")
    return response.json()

def get_watchlist(customer_id: str):
    """Get customer's watchlist."""
    response = requests.get(f"{API_BASE_URL}/watchlist/{customer_id}")
    return response.json()
