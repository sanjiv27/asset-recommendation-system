import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
import sys

sys.path.insert(0, '/app')

with patch('psycopg2.pool.SimpleConnectionPool'):
    from recommendation_engine import RecommendationEngine
    from tests.sample_data import (
        get_sample_transactions,
        get_sample_assets,
        get_sample_customers,
        get_sample_limit_prices,
        get_sample_interactions
    )


@pytest.fixture
def engine():
    return RecommendationEngine()


@pytest.fixture
def mock_db_calls():
    """Fixture that patches all DB calls and returns the patch objects."""
    with patch('recommendation_engine.db.get_buy_transactions') as mock_txns, \
         patch('recommendation_engine.db.get_assets') as mock_assets, \
         patch('recommendation_engine.db.get_customers') as mock_customers, \
         patch('recommendation_engine.db.get_limit_prices') as mock_prices:
        
        mock_txns.return_value = get_sample_transactions()
        mock_assets.return_value = get_sample_assets()
        mock_customers.return_value = get_sample_customers()
        mock_prices.return_value = get_sample_limit_prices()
        
        yield {
            'transactions': mock_txns,
            'assets': mock_assets,
            'customers': mock_customers,
            'prices': mock_prices
        }


@pytest.fixture
def trained_engine(engine, mock_db_calls):
    """Fixture that returns a fully trained engine."""
    engine.refresh_models()
    return engine


class TestRecommendationEngine:
    def test_initialization(self, engine):
        assert engine.is_ready is False
        assert engine.n_components == 5
        assert engine.asset_df is None

    def test_fetch_from_db(self, engine, mock_db_calls):
        engine._fetch_from_db()

        assert engine.transactions_df is not None
        assert len(engine.transactions_df) >= 4
        assert 'customerID' in engine.transactions_df.columns

    def test_preprocess_transactions(self, engine):
        engine.transactions_df = pd.DataFrame(get_sample_transactions())
        result = engine._preprocess_transactions()

        assert len(result) >= 4
        assert result['transactionType'].unique()[0] == 'Buy'

    def test_preprocess_empty_transactions(self, engine):
        engine.transactions_df = pd.DataFrame()
        result = engine._preprocess_transactions()
        
        assert result.empty

    def test_train_collaborative_filtering(self, engine, mock_db_calls):
        engine._fetch_from_db()
        train_df = engine._preprocess_transactions()
        engine._train_collaborative_filtering(train_df)

        assert engine.rating_matrix is not None
        assert engine.pred_ratings_df is not None
        assert 'C001' in engine.rating_matrix.index

    def test_prepare_content_features(self, engine, mock_db_calls):
        engine._fetch_from_db()
        engine._prepare_content_features()

        assert engine.encoded_asset_features is not None
        assert 'profitability' in engine.encoded_asset_features.columns

    def test_prepare_demographic_profiles(self, engine, mock_db_calls):
        engine._fetch_from_db()
        engine._prepare_demographic_profiles()

        assert engine.category_vectors is not None

    def test_refresh_models(self, engine, mock_db_calls):
        engine.refresh_models()

        assert engine.is_ready is True
        assert engine.rating_matrix is not None

    def test_get_recommendation_existing_user(self, trained_engine):
        recs = trained_engine.get_recommendation('C001', top_n=2)

        assert len(recs) <= 2
        assert all(isinstance(isin, str) for isin in recs)

    def test_get_recommendation_with_interactions(self, trained_engine):
        interactions = get_sample_interactions()
        recs = trained_engine.get_recommendation('C001', top_n=2, recent_interactions=interactions)

        assert len(recs) <= 2

    def test_get_recommendation_not_ready(self, engine):
        recs = engine.get_recommendation('C001')
        
        assert recs == []

    def test_calculate_content_score_cold_start(self, trained_engine):
        scores = trained_engine._calculate_content_score_fast('C999')

        assert all(scores == 0.5)

    def test_calculate_demo_score_cold_start(self, trained_engine):
        scores = trained_engine._calculate_demo_score_fast('C999')

        assert all(scores == 0.5)
