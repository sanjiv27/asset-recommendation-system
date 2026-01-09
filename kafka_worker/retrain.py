"""
Retraining module that can be called directly by the worker.
"""
import psycopg2
import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD
from datetime import datetime
import pickle
import os

def retrain_model():
    """Retrain the model with latest data and return results."""
    
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        database=os.getenv("POSTGRES_DB", "asset_recommendation"),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", "123")
    )
    
    # Fetch transaction data
    transactions_df = pd.read_sql("""
        SELECT customerid, isin, transactiontype, timestamp
        FROM transactions
        WHERE transactiontype = 'buy'
    """, conn)
    
    # Fetch interaction data
    interactions_df = pd.read_sql("""
        SELECT customerid, isin, interactiontype, weight, timestamp
        FROM user_interactions
    """, conn)
    
    conn.close()
    
    # Combine data
    txn_ratings = transactions_df.groupby(['customerid', 'isin']).size().reset_index(name='rating')
    interaction_ratings = interactions_df.groupby(['customerid', 'isin'])['weight'].sum().reset_index(name='rating')
    
    all_ratings = pd.concat([txn_ratings, interaction_ratings])
    combined_ratings = all_ratings.groupby(['customerid', 'isin'])['rating'].sum().reset_index()
    
    # Build rating matrix
    rating_matrix = combined_ratings.pivot_table(
        index='customerid',
        columns='isin',
        values='rating',
        fill_value=0
    )
    
    # Train SVD
    n_components = min(5, min(rating_matrix.shape) - 1)
    svd_model = TruncatedSVD(n_components=n_components, random_state=42)
    latent_matrix = svd_model.fit_transform(rating_matrix)
    pred_ratings = np.dot(latent_matrix, svd_model.components_)
    pred_ratings_df = pd.DataFrame(
        pred_ratings,
        index=rating_matrix.index,
        columns=rating_matrix.columns
    )
    
    # Determine next version number
    os.makedirs('/app/models', exist_ok=True)
    existing_models = [f for f in os.listdir('/app/models') if f.startswith('model_v') and f.endswith('.pkl')]
    version = len(existing_models) + 1
    
    model_name = f"model_v{version}.pkl"
    model_path = f'/app/models/{model_name}'
    
    # Prepare model data with metadata
    model_data = {
        'rating_matrix': rating_matrix,
        'svd_model': svd_model,
        'pred_ratings_df': pred_ratings_df,
        'rating_df': combined_ratings,  # Adding this for content-based filtering
        'metadata': {
            'version': version,
            'name': f"v{version}",
            'n_components': n_components,
            'trained_on': datetime.now().isoformat(),
            'training_samples': len(combined_ratings),
            'n_users': rating_matrix.shape[0],
            'n_items': rating_matrix.shape[1],
            'explained_variance': float(svd_model.explained_variance_ratio_.sum())
        }
    }
    
    # Save versioned model
    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    
    # Update active model symlink
    active_link = '/app/models/active_model.pkl'
    if os.path.exists(active_link):
        os.remove(active_link)
    os.symlink(model_path, active_link)
    
    metadata = model_data['metadata']
    return {
        "status": "ok",
        "message": "Model retrained successfully",
        "version": version,
        "training_samples": metadata['training_samples'],
        "n_users": metadata['n_users'],
        "n_items": metadata['n_items'],
        "explained_variance": f"{metadata['explained_variance']:.2%}",
        "trained_on": metadata['trained_on']
    }
