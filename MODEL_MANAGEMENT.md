# Model Management

## Overview
The Model Management page provides a centralized interface for managing recommendation models in the system.

## Features

### 1. Model Listing
- View all available models with their metadata
- Display model status (active/inactive)
- Show training statistics:
  - Number of users
  - Number of items
  - Explained variance
  - Training timestamp

### 2. Model Retraining
- Retrain models using latest user interaction data
- Uses SVD (Singular Value Decomposition) matrix factorization
- Automatically reloads the model after retraining
- Displays training details and statistics

## Architecture

### Components
1. **Frontend**: Streamlit page at `frontend/pages/2_Model_Management.py`
2. **Server API**: FastAPI endpoints in `server/server.py`
   - `GET /models` - List available models
   - `POST /retrain` - Trigger model retraining
3. **Worker API**: Flask endpoints in `kafka_worker/kafka_worker.py`
   - `GET /models` - Return model metadata from worker
   - `POST /retrain` - Execute retraining logic

### Model Storage
- Models are stored in persistent volume: `./kafka_worker/models/`
- Current model: `retrained_svd_model.pkl`
- Survives container restarts via Docker volume mount

## Usage

### Access the Model Management Page
1. Navigate to http://localhost:8501
2. Click "Model Management" in the sidebar
3. View available models and their status

### Retrain a Model
1. Click the "🚀 Retrain Model" button
2. Wait for retraining to complete (typically 1-5 minutes)
3. View training details in the success message
4. Page auto-refreshes to show updated model metadata

## API Endpoints

### GET /models
Returns list of available models with metadata.

**Response:**
```json
{
  "status": "ok",
  "models": [
    {
      "name": "Retrained SVD Model",
      "path": "/app/models/retrained_svd_model.pkl",
      "type": "SVD",
      "status": "active",
      "n_users": 6,
      "n_items": 12,
      "explained_variance": "98.90%",
      "trained_on": "2025-12-04 23:42:15"
    }
  ]
}
```

### POST /retrain
Triggers model retraining with latest interaction data.

**Response:**
```json
{
  "status": "ok",
  "message": "Model retrained in 2.3s - 56193 samples, 98.90% variance explained",
  "details": {
    "training_samples": 56193,
    "n_users": 6,
    "n_items": 12,
    "explained_variance": 0.989
  }
}
```

## Model Architecture

The system uses **SVD (Singular Value Decomposition)** for collaborative filtering:

1. **Input**: User-item interaction matrix from `user_interactions` table
2. **Processing**: 
   - Aggregate interactions by user and item
   - Apply SVD decomposition with 10 components
   - Generate predicted ratings for all user-item pairs
3. **Output**: Serialized model containing:
   - Rating matrix
   - SVD model
   - Predicted ratings DataFrame
   - Metadata (users, items, variance, timestamp)

## Notes

- Retraining uses the same architecture as the current model
- Model automatically loads on worker startup
- Retraining can be triggered manually or scheduled
- All user interactions (clicks, watchlist additions) are used for training
