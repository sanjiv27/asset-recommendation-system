import requests
import random
import time
import sys

# Configuration
API_URL = "http://localhost:8000"  # Adjust if your server is on a different port/host

# ==============================================================================
# DATA MAPPING
# IMPORTANT: These ISINs must exist in your 'asset_information' table!
# ==============================================================================
SECTOR_ISINS = {
    "Technology": [
        "US0378331005", # Apple (Example)
        "US5949181045", # Microsoft (Example)
        "US5951121038", # Micron (Example)
        "US9581021055", # Western Digital Corp (Example)
    ],
    "Healthcare": [
        "US4781601046", # Johnson & Johnson
        "US7170811035", # Pfizer
        "FR0000120578", # Sanofi SA
    ],
}

def simulate_user_interest(user_id: str, target_sector: str, num_clicks: int = 5):
    """
    Simulates a user clicking on multiple assets within a specific sector.
    """
    
    # 1. Validate Sector
    if target_sector not in SECTOR_ISINS:
        print(f"❌ Error: Sector '{target_sector}' not defined in script mapping.")
        print(f"Available sectors: {list(SECTOR_ISINS.keys())}")
        return

    print(f"\n--- Simulating {num_clicks} clicks for User: {user_id} in Sector: {target_sector} ---")
    
    valid_isins = SECTOR_ISINS[target_sector]

    # 2. Loop and Send Requests
    for i in range(num_clicks):
        # Pick a random asset from this sector
        chosen_isin = random.choice(valid_isins)
        
        payload = {
            "customer_id": user_id,
            "isin": chosen_isin,
            "type": "click" # Matches your UserInteraction model
        }

        try:
            response = requests.post(f"{API_URL}/user_interactions", json=payload)
            
            if response.status_code == 200:
                print(f"✅ [{i+1}/{num_clicks}] Clicked {chosen_isin}")
            else:
                print(f"❌ [{i+1}/{num_clicks}] Failed: {response.text}")

        except Exception as e:
            print(f"❌ Connection Error: {e}")
            break
            
        # Optional: slight delay to simulate real behavior (timestamp variance)
        time.sleep(0.2)

    print("--- Interaction Logging Complete ---")

if __name__ == "__main__":
    # CLI Arguments: python generate_interactions.py <UserID> <Sector>
    if len(sys.argv) < 3:
        print("Usage: python generate_interactions.py <USER_ID> <SECTOR> [NUM_CLICKS]")
        print("Example: python generate_interactions.py Kai_Le Technology 10")
        sys.exit(1)

    user_id_arg = sys.argv[1]
    sector_arg = sys.argv[2]
    clicks_arg = int(sys.argv[3]) if len(sys.argv) > 3 else 5

    simulate_user_interest(user_id_arg, sector_arg, clicks_arg)
