# asset-recommendation-system
An asset recommendation system with tunable hybrid approach. 


## Milestone 1

Tech Stack: Kafka, Python, FastAPI

### Foundational Model Training Flow

![Diagram](media/foundation_training_flow.png?raw=true "diagram")


### Retraining Model Flow

![diagram](media/retraining_flow.png?raw=true "diagram")

### Setup Dev Environment

- Initialize all services: Postgres, Server. **Reminder:** We are using docker image to containerize all services, thus if you make any changes in any files, you have to rerun this command to see changes.

`docker compose up --build -d`

- Load foundational FAR-trans data into Postgres

`python FAR-Trans/script.py`

## Final Milestone

### Final Retraining Flow

![Diagram](media/final_retraining_flow.png?raw=true "diagram")

### Final Inference Flow

![Diagram](media/final_inference_flow.png?raw=true "diagram")

### API documentation

#### 1. Generate Recommendations
Triggers the recommendation engine via Kafka. This endpoint does not return recommendations immediately; it queues a job for the background worker.

##### /recommendations
* **Method:** `POST`
###### Request Body Schemas (JSON)
```
{
  "customer_id": "string",
  "action": "request_recs/refresh_recs"
}
```
###### Response Example
```
{
  "status": "ok"
}
```

#### 2. Retrieve Recommendations
Fetches the latest calculated recommendations for a specific customer from the database.

##### /recommendations/{customer_id}
* **Method:** `GET`
###### Response Example
```
{
    "status": "ok",
    "recommendations": [
        {
            "isin": "GRC1451184D4",
            "assetname": "GEK TERNA HOLDING, REAL ESTATE, CONSTRUCTION S.A.",
            "assetcategory": "Bond",
            "sector": "Corporate",
            "industry": "Miscellaneous Construction",
            "current_price": 98.8814,
            "profitability": -0.011186,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRC2451227D9",
            "assetname": "LAMDA DEVELOPMENT SA",
            "assetcategory": "Bond",
            "sector": "Corporate",
            "industry": "Miscellaneous Construction",
            "current_price": 99.8,
            "profitability": -0.002,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRC419120AD7",
            "assetname": "GREEK ORGANISATION OF FOOTBALL PROGNOSTICS S.A.",
            "assetcategory": "Bond",
            "sector": "Corporate",
            "industry": "Hotels and Lodging",
            "current_price": 92.71,
            "profitability": -0.0729,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRC5091217D9",
            "assetname": "PRODEA R.E.I.C. S.A.",
            "assetcategory": "Bond",
            "sector": "Corporate",
            "industry": "Real Estate Investment and Services",
            "current_price": 83.9667,
            "profitability": -0.160333,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS003003035",
            "assetname": "NATIONAL BANK OF GREECE",
            "assetcategory": "Stock",
            "sector": null,
            "industry": null,
            "current_price": 3.76,
            "profitability": 0.160494,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS247003007",
            "assetname": "INTERTECH S.A.  INTERNATIONAL TECHNOLOGIES (CR)",
            "assetcategory": "Stock",
            "sector": "Technology",
            "industry": "Electronics & Computer Distribution",
            "current_price": 1.05,
            "profitability": 0.858407,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS320313000",
            "assetname": "PLAISIO COMPUTERS S.A. (CR)",
            "assetcategory": "Stock",
            "sector": "Technology",
            "industry": "Computer Hardware",
            "current_price": 3.01,
            "profitability": -0.283333,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS396003006",
            "assetname": "QUALITY AND RELIABILITY S.A.(CR)",
            "assetcategory": "Stock",
            "sector": "Technology",
            "industry": "Software - Application",
            "current_price": 0.449,
            "profitability": 0.669145,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS402003008",
            "assetname": "SPACE HELLAS S.A. (CR)",
            "assetcategory": "Stock",
            "sector": "Technology",
            "industry": "Software - Application",
            "current_price": 6.32,
            "profitability": 0.564356,
            "recommendation_date": "2025-11-30T22:00:00"
        },
        {
            "isin": "GRS439003005",
            "assetname": "EUROCONSULTANTS S.A. (CR)",
            "assetcategory": "Stock",
            "sector": "Technology",
            "industry": "Information Technology Services",
            "current_price": 0.606,
            "profitability": 0.415888,
            "recommendation_date": "2025-11-30T22:00:00"
        }
    ]
}
```
#### 3. Log User Interactions
Right now we only support click interactions for users, as a proof of concept, we provides script to fill user's interactions.

##### Generate User Click
Usage: python generate_interactions.py <USER_ID> <SECTOR> [NUM_CLICKS]

`python script/generate_interactions.py 85583A768E48870A546D Technology 10`

##### /user_interactions
* **Method:** `POST`
###### Request Body Schemas (JSON)
```
{
  "customer_id": "string",
  "isin": "string",
  "type": "click" 
}
```
###### Response Example
```
{
  "status": "ok"
}
```