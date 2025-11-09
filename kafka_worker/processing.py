import json
import os
import time
from typing import Any, Dict, List, Optional

import mlflow
import numpy as np
import pandas as pd
from sklearn.decomposition import NMF
from sklearn.preprocessing import StandardScaler

import db


# MLFlow configuration
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment("asset-recommendation")


def process_userprofile(msg: Dict[str, Any]) -> None:
    """
    Process userprofile topic: Generate standard matrix for MLFlow.
    
    Expected message format:
    {
        "user_name": str,
        "parameters": {
            "risk_level": str,
            "investment_capacity": float,
            "preferences": dict,
            "historical_interactions": list,
            ...
        }
    }
    """
    user_name = msg.get("user_name")
    parameters = msg.get("parameters", {})
    
    if not user_name:
        raise ValueError("user_name is required")
    
    # Generate feature matrix from parameters
    feature_matrix = _generate_user_profile_matrix(parameters)
    
    # Log to MLFlow
    with mlflow.start_run(run_name=f"userprofile-{user_name}-{int(time.time())}"):
        # Log parameters
        mlflow.log_params({
            "user_name": user_name,
            "risk_level": parameters.get("risk_level", "unknown"),
            "investment_capacity": parameters.get("investment_capacity", 0),
        })
        
        # Log feature matrix as artifact (numpy array)
        import tempfile
        import os as os_module
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.npy', delete=False) as f:
            np.save(f.name, feature_matrix)
            mlflow.log_artifact(f.name, "matrices")
            os_module.unlink(f.name)
        
        # Log matrix metadata
        mlflow.log_metrics({
            "matrix_shape_rows": feature_matrix.shape[0],
            "matrix_shape_cols": feature_matrix.shape[1] if len(feature_matrix.shape) > 1 else 1,
            "matrix_mean": float(np.mean(feature_matrix)),
            "matrix_std": float(np.std(feature_matrix)),
        })
        
        # Log matrix as JSON for easier inspection
        matrix_dict = {
            "shape": list(feature_matrix.shape),
            "data": feature_matrix.tolist() if feature_matrix.size < 1000 else "matrix_too_large",
        }
        mlflow.log_dict(matrix_dict, "matrix_info.json")


def process_retraining(msg: Dict[str, Any]) -> None:
    """
    Process retraining topic: Retrain matrices based on user interactions.
    
    Expected message format:
    {
        "user_name": str,
        "aggregate_interactions": [
            {
                "interaction_type": str,
                "asset_id": str,
                "timestamp": str,
                "metadata": dict
            },
            ...
        ]
    }
    """
    user_name = msg.get("user_name")
    aggregate_interactions = msg.get("aggregate_interactions", [])
    
    if not user_name:
        raise ValueError("user_name is required")
    
    # Get past recommendations from database
    past_recommendations = db.get_user_recommendations(user_name)
    
    # Get existing interactions from database
    existing_interactions = db.get_user_interactions(user_name)
    
    # Combine with new interactions (convert to same format)
    new_interactions = []
    for inter in aggregate_interactions:
        new_interactions.append({
            "user_name": user_name,
            "interaction_type": inter.get("interaction_type"),
            "asset_id": inter.get("asset_id"),
            "timestamp": inter.get("timestamp"),
            "metadata": inter.get("metadata", {}),
        })
    
    all_interactions = existing_interactions + new_interactions
    
    # Generate user-item interaction matrix
    interaction_matrix = _generate_interaction_matrix(user_name, all_interactions, past_recommendations)
    
    # Train/retrain recommendation model (using NMF as example)
    model, user_features, item_features = _train_recommendation_model(interaction_matrix)
    
    # Log to MLFlow
    with mlflow.start_run(run_name=f"retraining-{user_name}-{int(time.time())}"):
        # Log parameters
        mlflow.log_params({
            "user_name": user_name,
            "num_interactions": len(all_interactions),
            "num_past_recommendations": len(past_recommendations),
            "matrix_shape": list(interaction_matrix.shape),
        })
        
        # Log model
        mlflow.sklearn.log_model(model, "recommendation_model")
        
        # Log matrices
        import tempfile
        import os as os_module
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.npy', delete=False) as f:
            np.save(f.name, user_features)
            mlflow.log_artifact(f.name, "matrices")
            os_module.unlink(f.name)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.npy', delete=False) as f:
            np.save(f.name, item_features)
            mlflow.log_artifact(f.name, "matrices")
            os_module.unlink(f.name)
        
        # Log metrics
        mlflow.log_metrics({
            "model_components": model.n_components,
            "user_features_shape": user_features.shape,
            "item_features_shape": item_features.shape,
        })


def process_interactions(msg: Dict[str, Any]) -> None:
    """
    Process interactions topic: Save user interactions to Postgres in batch.
    
    Expected message format:
    {
        "interactions": [
            {
                "user_name": str,
                "interaction_type": str,  # e.g., "click", "view", "purchase", "like"
                "asset_id": str,
                "timestamp": str,  # ISO format
                "metadata": dict  # optional additional data
            },
            ...
        ]
    }
    """
    interactions = msg.get("interactions", [])
    
    if not interactions:
        return
    
    # Add interactions to buffer (will auto-flush when batch size reached)
    for interaction in interactions:
        user_name = interaction.get("user_name")
        interaction_type = interaction.get("interaction_type")
        asset_id = interaction.get("asset_id")
        timestamp = interaction.get("timestamp")
        metadata = interaction.get("metadata", {})
        
        if not all([user_name, interaction_type, asset_id, timestamp]):
            continue  # Skip invalid interactions
        
        # Convert metadata to JSON string for storage (db.py will handle JSONB conversion)
        metadata_str = json.dumps(metadata) if metadata else "{}"
        
        db.add_interaction_to_buffer(user_name, interaction_type, asset_id, timestamp, metadata_str)
    
    # Force flush to ensure data is saved
    db.flush_interactions_buffer()


def _generate_user_profile_matrix(parameters: Dict[str, Any]) -> np.ndarray:
    """
    Generate a standard feature matrix from user profile parameters.
    This is a template - customize based on your feature engineering needs.
    """
    features = []
    
    # Risk level encoding
    risk_levels = {"low": 0, "medium": 1, "high": 2}
    risk_level = parameters.get("risk_level", "medium")
    features.append(risk_levels.get(risk_level.lower(), 1))
    
    # Investment capacity (normalized)
    investment_capacity = parameters.get("investment_capacity", 0)
    features.append(float(investment_capacity) / 1000000.0)  # Normalize to millions
    
    # Preferences (one-hot or encoded)
    preferences = parameters.get("preferences", {})
    features.append(preferences.get("stocks", 0))
    features.append(preferences.get("bonds", 0))
    features.append(preferences.get("etf", 0))
    features.append(preferences.get("crypto", 0))
    
    # Historical interaction counts
    historical = parameters.get("historical_interactions", [])
    features.append(len(historical))
    
    # Additional features can be added here
    # For now, create a simple feature vector
    feature_vector = np.array(features, dtype=np.float32)
    
    # Reshape to matrix format (1 x n_features)
    return feature_vector.reshape(1, -1)


def _generate_interaction_matrix(
    user_name: str,
    interactions: List[Dict[str, Any]],
    past_recommendations: List[Dict[str, Any]],
) -> np.ndarray:
    """
    Generate user-item interaction matrix from interactions and recommendations.
    """
    # Get unique assets
    asset_ids = set()
    for inter in interactions:
        if inter.get("asset_id"):
            asset_ids.add(inter.get("asset_id"))
    for rec in past_recommendations:
        recommended_assets = rec.get("recommended_assets", [])
        if isinstance(recommended_assets, list):
            asset_ids.update(recommended_assets)
        elif isinstance(recommended_assets, str):
            asset_ids.add(recommended_assets)
    
    asset_ids = sorted(list(asset_ids))
    asset_to_idx = {asset: idx for idx, asset in enumerate(asset_ids)}
    
    # Create interaction matrix (1 user x n items)
    matrix = np.zeros((1, len(asset_ids)), dtype=np.float32)
    
    # Fill from interactions (weighted by type)
    interaction_weights = {
        "purchase": 5.0,
        "like": 3.0,
        "click": 1.0,
        "view": 0.5,
    }
    
    for inter in interactions:
        asset_id = inter.get("asset_id")
        interaction_type = inter.get("interaction_type", "view")
        if asset_id in asset_to_idx:
            weight = interaction_weights.get(interaction_type, 1.0)
            matrix[0, asset_to_idx[asset_id]] += weight
    
    # Fill from past recommendations
    for rec in past_recommendations:
        recommended_assets = rec.get("recommended_assets", [])
        if isinstance(recommended_assets, list):
            for asset_id in recommended_assets:
                if asset_id in asset_to_idx:
                    matrix[0, asset_to_idx[asset_id]] += 0.5
        elif isinstance(recommended_assets, str) and recommended_assets in asset_to_idx:
            matrix[0, asset_to_idx[recommended_assets]] += 0.5
    
    return matrix


def _train_recommendation_model(interaction_matrix: np.ndarray, n_components: int = 10) -> tuple:
    """
    Train a recommendation model (NMF) from interaction matrix.
    Returns (model, user_features, item_features).
    """
    # Normalize matrix
    scaler = StandardScaler()
    matrix_scaled = scaler.fit_transform(interaction_matrix.T).T
    
    # Train NMF model
    model = NMF(n_components=n_components, random_state=42, max_iter=200)
    user_features = model.fit_transform(matrix_scaled)
    item_features = model.components_.T
    
    return model, user_features, item_features
