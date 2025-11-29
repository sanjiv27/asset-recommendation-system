import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.metrics.pairwise import cosine_similarity
import logging

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
        logger.info("Fetching data from Database...")

        # --- Helper to fix Postgres lowercase columns ---
        def fix_columns(df, required_map):
            """
            Renames columns from Postgres lowercase/snake_case 
            to the CamelCase expected by the engine.
            """
            # 1. Normalize current columns to strict lowercase to match easily
            df.columns = [c.lower() for c in df.columns]
            
            # 2. Rename based on the map
            # We use a dictionary where Key = lowercase_db_name, Value = ExpectedName
            # We filter the map to only include columns that actually exist in the df
            actual_rename = {k: v for k, v in required_map.items() if k in df.columns}
            df.rename(columns=actual_rename, inplace=True)
            return df

        # -----------------------------------------------
        # 1. Transactions
        # -----------------------------------------------
        raw_txns = db.get_buy_transactions()
        self.transactions_df = pd.DataFrame(raw_txns)
        
        # logic needs: customerID, ISIN, transactionType, timestamp
        self.transactions_df = fix_columns(self.transactions_df, {
            'customerid': 'customerID',
            'customer_id': 'customerID', 
            'isin': 'ISIN',
            'transactiontype': 'transactionType',
            'transaction_type': 'transactionType',
            'timestamp': 'timestamp'
        })

        # -----------------------------------------------
        # 2. Assets
        # -----------------------------------------------
        raw_assets = db.get_assets()
        self.asset_df = pd.DataFrame(raw_assets)
        
        # logic needs: ISIN, assetCategory, assetSubCategory, sector, industry, marketID
        self.asset_df = fix_columns(self.asset_df, {
            'isin': 'ISIN',
            'assetcategory': 'assetCategory',
            'asset_category': 'assetCategory',
            'assetsubcategory': 'assetSubCategory',
            'asset_sub_category': 'assetSubCategory',
            'sector': 'sector',
            'industry': 'industry',
            'marketid': 'marketID',
            'market_id': 'marketID'
        })

        # -----------------------------------------------
        # 3. Customers
        # -----------------------------------------------
        raw_customers = db.get_customers()
        self.customer_df = pd.DataFrame(raw_customers)
        
        # logic needs: customerID, riskLevel, investmentCapacity, customerType
        self.customer_df = fix_columns(self.customer_df, {
            'customerid': 'customerID',
            'customer_id': 'customerID',
            'risklevel': 'riskLevel',
            'risk_level': 'riskLevel',
            'investmentcapacity': 'investmentCapacity',
            'investment_capacity': 'investmentCapacity',
            'customertype': 'customerType',
            'customer_type': 'customerType'
        })

        # -----------------------------------------------
        # 4. Limit Prices
        # -----------------------------------------------
        raw_prices = db.get_limit_prices()
        self.limit_prices_df = pd.DataFrame(raw_prices)
        
        # logic needs: ISIN, profitability, priceMaxDate
        self.limit_prices_df = fix_columns(self.limit_prices_df, {
            'isin': 'ISIN',
            'profitability': 'profitability',
            'pricemaxdate': 'priceMaxDate',
            'price_max_date': 'priceMaxDate'
        })

        logger.info(f"Data fetch complete. Txn cols: {self.transactions_df.columns.tolist()}")

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
        """
        Pre-computes the 'Average Demographic Vector' for each Asset Category.
        
        Logic:
        1. Map all customers in DB to numeric vectors (Risk, Capacity, Type).
        2. Join Transactions -> Assets -> Customers.
        3. Group by 'assetCategory' and calculate the mean vector.
        4. Store this lookup table for fast inference.
        """
        logger.info("Preparing Demographic profiles (Category Centroids)...")

        if self.transactions_df.empty or self.customer_df.empty:
            logger.warning("Insufficient data to build demographic profiles.")
            self.category_vectors = pd.DataFrame()
            return

        # --- 1. Define Mappings (Strings to Ints) ---
        risk_map = {"Conservative": 1, "Income": 2, "Balanced": 3, "Aggressive": 4}
        cap_map = {"CAP_LT30K": 1, "CAP_30K_80K": 2, "CAP_80K_300K": 3, "CAP_GT300K": 4}

        # Helper to clean "Predicted_" prefixes
        def clean_label(val):
            if pd.isna(val) or val == "Not_Available": return None
            return str(val).replace("Predicted_", "")

        # --- 2. Vectorize Customer DB ---
        # Work on a copy to avoid mutating global state
        cust_vecs = self.customer_df.copy()
        
        # Apply cleaning and mapping
        cust_vecs['risk_clean'] = cust_vecs['riskLevel'].apply(clean_label)
        cust_vecs['cap_clean'] = cust_vecs['investmentCapacity'].apply(clean_label)
        
        # Map to numerics
        cust_vecs['risk_val'] = cust_vecs['risk_clean'].map(risk_map)
        cust_vecs['cap_val'] = cust_vecs['cap_clean'].map(cap_map)
        
        # One-hot-ish encodings for customer type
        cust_vecs['is_premium'] = (cust_vecs['customerType'] == 'Premium').astype(int)
        cust_vecs['is_professional'] = (cust_vecs['customerType'] == 'Professional').astype(int)

        # Drop users where we couldn't parse risk or cap
        cust_vecs = cust_vecs.dropna(subset=['risk_val', 'cap_val'])

        # --- 3. Merge to link Categories to Customer Vectors ---
        # We need: Transaction -> ISIN -> Asset Category
        #          Transaction -> CustomerID -> Customer Vector
        
        # Merge Transactions with Assets to get Category
        txn_with_cat = self.transactions_df[['customerID', 'ISIN']].merge(
            self.asset_df[['ISIN', 'assetCategory']], 
            on='ISIN', 
            how='inner'
        )

        # Merge result with Customer Vectors
        full_profile = txn_with_cat.merge(
            cust_vecs[['customerID', 'risk_val', 'cap_val', 'is_premium', 'is_professional']],
            on='customerID',
            how='inner'
        )

        # --- 4. GroupBy Category and Calculate Mean Vector ---
        # Result index: assetCategory
        # Result columns: risk_val, cap_val, is_premium, is_professional
        self.category_vectors = full_profile.groupby('assetCategory')[
            ['risk_val', 'cap_val', 'is_premium', 'is_professional']
        ].mean()

        logger.info(f"Demographic profiles built for {len(self.category_vectors)} categories.")

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
        
        return final_score.sort_values(ascending=False).head(top_n).index.tolist()

    # Helpers for inference
    def _calculate_content_score_fast(self, customer_id):
        """
        Calculates Content-Based scores using pre-computed asset features.
        
        Logic:
        1. Look up items the user bought.
        2. Create a 'User Profile' by averaging the features of those items.
        3. Compute Cosine Similarity between User Profile and ALL assets.
        """
        # 1. Safety Checks
        if self.encoded_asset_features is None or self.encoded_asset_features.empty:
            # Fallback if features weren't loaded
            return pd.Series(0, index=[]) 

        # 2. Get User History
        # We access the raw rating dataframe to find what this user bought.
        # (Assumes self.rating_df has 'customerID' and 'ISIN' columns)
        if customer_id not in self.rating_df['customerID'].values:
            # Cold Start: User has no history. Return neutral score.
            return pd.Series(0.5, index=self.encoded_asset_features.index)

        user_history = self.rating_df[self.rating_df['customerID'] == customer_id]
        bought_isins = user_history['ISIN'].unique()

        # 3. Filter for Valid ISINs
        # We only care about assets that exist in our feature matrix
        valid_isins = [isin for isin in bought_isins if isin in self.encoded_asset_features.index]

        if not valid_isins:
            # History exists but items are missing from feature set (rare edge case)
            return pd.Series(0.5, index=self.encoded_asset_features.index)

        # 4. Build User Profile Vector (The Centroid)
        # We grab the feature rows for every asset the user bought
        user_assets_matrix = self.encoded_asset_features.loc[valid_isins]
        
        # We take the mean across the rows (axis=0) to get one vector representing the user
        user_profile_vector = user_assets_matrix.mean(axis=0).values.reshape(1, -1)

        # 5. Calculate Similarity
        # Compare the User Vector against the WHOLE asset feature matrix
        # cosine_similarity returns shape (1, n_assets)
        sim_matrix = cosine_similarity(user_profile_vector, self.encoded_asset_features)

        # 6. Return as Pandas Series
        # Flatten the result [0] to get a 1D array
        return pd.Series(sim_matrix[0], index=self.encoded_asset_features.index)


    def _calculate_demo_score_fast(self, customer_id):
        """
        Calculates similarity between the user (looked up by ID) and 
        the pre-computed category vectors.
        """
        # 1. Safety Check: Do we have category vectors?
        if self.category_vectors is None or self.category_vectors.empty:
             return pd.Series(0.5, index=self.asset_df['ISIN'])

        # 2. Look up the user in the loaded dataframe
        # We need to find the row for this customerID
        user_row = self.customer_df[self.customer_df['customerID'] == customer_id]
        
        if user_row.empty:
            # User not found in local DB (Cold Start) -> Return Neutral
            return pd.Series(0.5, index=self.asset_df['ISIN'])

        # 3. Extract attributes
        # We use .iloc[0] because the filter returns a DataFrame, we need the Series
        risk_str = user_row.iloc[0]['riskLevel']
        cap_str = user_row.iloc[0]['investmentCapacity']
        ctype_str = user_row.iloc[0]['customerType']

        # 4. Define Mappings (Same as in _prepare_demographic_profiles)
        risk_map = {"Conservative": 1, "Income": 2, "Balanced": 3, "Aggressive": 4}
        cap_map = {"CAP_LT30K": 1, "CAP_30K_80K": 2, "CAP_80K_300K": 3, "CAP_GT300K": 4}
        
        # Helper to clean labels (remove "Predicted_" if present)
        def clean(val):
            return str(val).replace("Predicted_", "") if val else ""

        # 5. Construct Target Vector
        # We map the strings to integers. 
        # get(..., 2.5) provides a safe default if the specific value is unknown.
        risk_val = risk_map.get(clean(risk_str), 2.5)
        cap_val = cap_map.get(clean(cap_str), 2.5)
        is_premium = 1 if ctype_str == "Premium" else 0
        is_professional = 1 if ctype_str == "Professional" else 0

        target_vec = np.array([risk_val, cap_val, is_premium, is_professional])

        # 6. Weights for distance calculation (Risk, Cap, Prem, Prof)
        weights = np.array([0.4, 0.3, 0.2, 0.1])
        # Normalization factor (Max theoretical distance)
        max_dist_const = np.sqrt(np.sum(weights * np.array([3, 3, 1, 1]) ** 2))

        # 7. Calculate Similarity for all categories (Vectorized)
        # diff = (Category_Matrix - Target_Vector)
        diff = self.category_vectors - target_vec
        
        # Weighted Euclidean Distance
        dist = np.sqrt(((diff ** 2) * weights).sum(axis=1))
        
        # Similarity = 1 - Normalized Distance
        sim_scores = 1 - (dist / max_dist_const)
        
        # 8. Map Category Scores back to individual Assets
        # self.asset_df has 'assetCategory', we map the score to it.
        # We set index to ISIN so the result is a Series keyed by ISIN
        asset_scores = self.asset_df.set_index('ISIN')['assetCategory'].map(sim_scores)
        
        return asset_scores.fillna(0.5)