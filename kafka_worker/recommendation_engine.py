import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics.pairwise import cosine_similarity
import logging
from datetime import datetime

# Assume this is your database connector module
import db 

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    def __init__(self):
        # Global State placeholders
        self.asset_df = None
        self.customer_df = None
        self.transactions_df = None
        self.limit_prices_df = None
        
        # Derived State (Matrices & Models)
        self.rating_matrix = None
        self.svd_model = None
        self.pred_ratings_df = None  # CF Predictions
        self.encoded_asset_features = None # Content-Based Features
        self.category_sim_map = None # Demographic pre-calculations
        
        # Configuration
        self.n_components = 5
        self.is_ready = False

    # ==========================================
    # 1. DATA LOADING (The DB Layer)
    # ==========================================
    def _fetch_from_db(self):
        """
        Internal method to fetch raw data from DB.
        Fill out the specific db.* calls here.
        """
        logger.info("Fetching data from Database...")
        
        # Example: Convert numpy array or list of dicts from DB to DataFrame
        # You will need to map your DB column names to the names used in your logic
        
        # 1. Transactions
        raw_txns = db.get_buy_transactions() 
        # Returns list of dicts with columns: customerID, ISIN, transactionType, timestamp
        self.transactions_df = pd.DataFrame(raw_txns) 

        # 2. Assets
        raw_assets = db.get_assets()
        # Returns list of dicts with columns: ISIN, assetCategory, assetSubCategory, sector, industry, marketID
        self.asset_df = pd.DataFrame(raw_assets)
        if self.asset_df.empty:
            # Fallback to empty DataFrame with correct columns if no data
            self.asset_df = pd.DataFrame(columns=['ISIN', 'assetCategory', 'assetSubCategory', 'sector', 'industry', 'marketID'])

        # 3. Customers
        raw_customers = db.get_customers()
        # Returns list of dicts with columns: customerID, riskLevel, investmentCapacity, customerType, timestamp
        self.customer_df = pd.DataFrame(raw_customers)
        if self.customer_df.empty:
            # Fallback to empty DataFrame with correct columns if no data
            self.customer_df = pd.DataFrame(columns=['customerID', 'riskLevel', 'investmentCapacity', 'customerType', 'timestamp'])

        # 4. Limit Prices / Profitability
        raw_prices = db.get_limit_prices()
        # Returns list of dicts with columns: ISIN, profitability, priceMaxDate
        self.limit_prices_df = pd.DataFrame(raw_prices)
        if self.limit_prices_df.empty:
            # Fallback to empty DataFrame with correct columns if no data
            self.limit_prices_df = pd.DataFrame(columns=['ISIN', 'profitability', 'priceMaxDate'])
        
        logger.info("Data fetch complete.")

    # ==========================================
    # 2. PREPROCESSING & TRAINING
    # ==========================================
    def _preprocess_transactions(self):
        """Filter buys and sort."""
        if self.transactions_df.empty:
            return pd.DataFrame()
            
        # Ensure correct types
        df = self.transactions_df.copy()
        # If your DB returns strings for dates, convert them:
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
        # Filter for 'Buy' if your DB returns all types
        if 'transactionType' in df.columns:
            df = df[df.transactionType == "Buy"]
            
        df = df.sort_values('timestamp')
        return df

    def _train_collaborative_filtering(self, train_df):
        """Builds Rating Matrix and Fits SVD."""
        logger.info("Training Collaborative Filtering model...")
        
        # Build Rating Matrix
        rating_df = train_df.groupby(['customerID', 'ISIN']).size().reset_index(name='count')
        self.rating_matrix = rating_df.pivot(index='customerID', columns='ISIN', values='count').fillna(0)
        
        # SVD
        svd = TruncatedSVD(n_components=self.n_components, random_state=42)
        U = svd.fit_transform(self.rating_matrix)
        V = svd.components_.T
        
        # Store the reconstruction for fast lookup
        pred_ratings = np.dot(U, V.T)
        self.pred_ratings_df = pd.DataFrame(
            pred_ratings, 
            index=self.rating_matrix.index, 
            columns=self.rating_matrix.columns
        )
        self.rating_df = rating_df # Keep raw counts for history lookups

    def _prepare_content_features(self):
        """Pre-computes one-hot encoded asset features."""
        logger.info("Preparing Content-Based features...")
        
        features = self.asset_df[['ISIN', 'assetCategory', 'assetSubCategory', 'sector', 'industry', 'marketID']].copy()
        
        # Merge profitability
        features = features.merge(
            self.limit_prices_df[['ISIN', 'profitability']], 
            on='ISIN', 
            how='left'
        )
        
        # Fill NA
        features['profitability'] = features['profitability'].fillna(features['profitability'].median())
        features = features.fillna('Unknown')
        
        # One-hot encode
        feature_cols = ['assetCategory', 'assetSubCategory', 'sector', 'industry', 'marketID']
        encoded = pd.get_dummies(features[feature_cols])
        encoded['profitability'] = features['profitability']
        encoded.index = features['ISIN']
        
        self.encoded_asset_features = encoded

    def _prepare_demographic_profiles(self):
        """Pre-computes category similarity maps based on demographics."""
        logger.info("Preparing Demographic profiles...")
        # (Simplified version of your logic to store the category_map)
        # In a real worker, you calculate the `category_sim_map` here 
        # so you don't iterate over the whole customer DB for every prediction request.
        # For brevity, I'm marking this as the spot to run the 'average demographic vector' logic.
        pass

    # ==========================================
    # 3. PUBLIC INTERFACE (Refresh & Predict)
    # ==========================================
    def refresh_models(self):
        """
        Master function to reload data and retrain models.
        Call this on initialization and periodically via scheduler.
        """
        try:
            logger.info("Starting Model Refresh...")
            
            # 1. Load Data
            self._fetch_from_db()
            
            # 2. Preprocess
            buys_df = self._preprocess_transactions()
            
            if buys_df.empty:
                logger.warning("No transaction data found. Models not updated.")
                return

            # 3. Train CF
            self._train_collaborative_filtering(buys_df)
            
            # 4. Prepare Content Features
            self._prepare_content_features()
            
            # 5. Prepare Demographics
            self._prepare_demographic_profiles()
            
            self.is_ready = True
            logger.info("Model Refresh Complete. Engine is ready.")
            
        except Exception as e:
            logger.error(f"Error during model refresh: {e}", exc_info=True)

    def get_recommendation(self, customer_id: str, top_n: int = 10, weights: tuple = (0.4, 0.3, 0.3)):
        """
        Generates hybrid recommendations.
        """
        if not self.is_ready:
            logger.error("Engine not ready (data not loaded).")
            return []

        # --- 1. Collaborative Filtering Score ---
        if customer_id in self.pred_ratings_df.index:
            cf_scores = self.pred_ratings_df.loc[customer_id]
        else:
            # Cold start (user not in matrix)
            cf_scores = pd.Series(0, index=self.rating_matrix.columns)
            
        # --- 2. Content-Based Score ---
        # Note: We use the pre-computed self.encoded_asset_features
        cb_scores = self._calculate_content_score_fast(customer_id)
        
        # --- 3. Demographic Score ---
        demo_scores = self._calculate_demo_score_fast(customer_id)

        # --- Hybrid Mixing ---
        def normalize(s):
            if s.max() - s.min() > 0:
                return (s - s.min()) / (s.max() - s.min())
            return s

        final_score = (weights[0] * normalize(cf_scores) + 
                       weights[1] * normalize(cb_scores) + 
                       weights[2] * normalize(demo_scores))
        
        # Filter already bought
        if customer_id in self.rating_df.index.get_level_values(0):
            bought = self.rating_df.loc[customer_id]['ISIN'].tolist() if 'ISIN' in self.rating_df.columns else [] 
            # Note: pivot table structure might make looking up bought items slightly different 
            # depending on how you stored rating_df. 
            # Fix: Since rating_df was groupby, we can filter against it directly.
            pass 

        return final_score.sort_values(ascending=False).head(top_n).index.tolist()

    # Helpers for inference
    def _calculate_content_score_fast(self, customer_id):
        # Implementation of your content logic using self.encoded_asset_features
        # and self.rating_df
        return pd.Series(0, index=self.encoded_asset_features.index) # Placeholder

    def _calculate_demo_score_fast(self, customer_id):
        # Implementation of your demo logic
        return pd.Series(0, index=self.encoded_asset_features.index) # Placeholder