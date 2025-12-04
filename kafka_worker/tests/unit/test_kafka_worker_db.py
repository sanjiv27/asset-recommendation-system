import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import sys

sys.path.insert(0, '/app')

with patch('psycopg2.pool.SimpleConnectionPool'):
    import db


@pytest.fixture
def mock_pool():
    """Fixture that mocks the database connection pool."""
    with patch('db._pool') as mock:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock.getconn.return_value = mock_conn
        yield mock, mock_conn, mock_cursor


class TestDatabaseQueries:
    @pytest.mark.parametrize("function,columns,data,expected_key,expected_value", [
        (
            db.get_buy_transactions,
            [('customerID',), ('ISIN',), ('transactionType',), ('timestamp',)],
            [('C001', 'GRS003003035', 'Buy', datetime(2025, 1, 1))],
            'transactionType',
            'Buy'
        ),
        (
            db.get_assets,
            [('ISIN',), ('assetCategory',), ('assetSubCategory',), ('sector',), ('industry',), ('marketID',)],
            [('GRS003003035', 'Stock', 'Common', 'Finance', 'Banking', 'XATH')],
            'assetCategory',
            'Stock'
        ),
        (
            db.get_customers,
            [('customerID',), ('riskLevel',), ('investmentCapacity',), ('customerType',), ('timestamp',)],
            [('C001', 'Aggressive', 'CAP_GT300K', 'Premium', datetime(2025, 1, 1))],
            'riskLevel',
            'Aggressive'
        ),
        (
            db.get_limit_prices,
            [('ISIN',), ('profitability',), ('priceMaxDate',)],
            [('GRS003003035', 0.15, 100.0)],
            'profitability',
            0.15
        ),
    ])
    def test_get_data(self, mock_pool, function, columns, data, expected_key, expected_value):
        _, _, mock_cursor = mock_pool
        mock_cursor.description = columns
        mock_cursor.fetchall.return_value = data
        
        result = function()
        
        assert len(result) == 1
        assert result[0][expected_key] == expected_value


class TestDatabaseWrites:
    def test_save_recommendations(self, mock_pool):
        _, _, mock_cursor = mock_pool
        recs = ['GRS003003035', 'GRS247003007']
        
        result = db.save_recommendations('C001', recs)
        
        assert result is True
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO recommendations" in call_args[0]

    def test_get_recent_interactions(self, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.description = [
            ('ISIN',), ('interactionType',), ('weight',), 
            ('assetCategory',), ('sector',), ('industry',)
        ]
        mock_cursor.fetchall.return_value = [
            ('GRS003003035', 'click', 1, 'Stock', 'Finance', 'Banking')
        ]
        
        result = db.get_recent_user_interactions('C001', days=1)
        
        assert len(result) == 1
        assert result[0]['ISIN'] == 'GRS003003035'
        assert result[0]['interactionType'] == 'click'
