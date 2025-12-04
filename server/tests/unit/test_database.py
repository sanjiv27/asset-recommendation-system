import pytest
from unittest.mock import MagicMock, patch
from datetime import date, datetime
import sys

sys.path.insert(0, '/app')

with patch('psycopg2.pool.SimpleConnectionPool'):
    from server.database import (
        execute_upsert,
        insert_customer_information,
        insert_asset_information,
        batch_insert_customer_information,
        batch_insert_close_prices,
        display_recommendations_details,
        log_interaction
    )


@pytest.fixture
def mock_pool():
    """Fixture that mocks the database connection pool."""
    with patch('server.database._pool') as mock:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock.getconn.return_value = mock_conn
        yield mock, mock_conn, mock_cursor


class TestExecuteUpsert:
    def test_execute_upsert_success(self, mock_pool):
        _, _, mock_cursor = mock_pool
        sql = "INSERT INTO test (id, name) VALUES (%s, %s)"
        params = ("1", "test")
        
        execute_upsert(sql, params)
        
        mock_cursor.execute.assert_called_once_with(sql, params)


class TestInsertOperations:
    def test_insert_customer(self, mock_pool):
        _, _, mock_cursor = mock_pool
        
        insert_customer_information(
            "C001", "Premium", "Aggressive", "CAP_GT300K",
            date(2025, 1, 1), date(2025, 1, 1)
        )
        
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO customer_information" in call_args[0]
        assert call_args[1][0] == "C001"

    def test_insert_asset(self, mock_pool):
        _, _, mock_cursor = mock_pool
        
        insert_asset_information(
            "GRS003003035", "Test Asset", "TA", "Stock", "Common",
            "XATH", "Finance", "Banking", date(2025, 1, 1)
        )
        
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO asset_information" in call_args[0]


class TestBatchOperations:
    @pytest.mark.parametrize("function,table_name,row_count", [
        (batch_insert_customer_information, "customer_information", 2),
        (batch_insert_close_prices, "close_prices", 2),
    ])
    def test_batch_insert(self, mock_pool, function, table_name, row_count):
        _, _, mock_cursor = mock_pool
        
        if table_name == "customer_information":
            rows = [
                ("C001", "Premium", "Aggressive", "CAP_GT300K", date(2025, 1, 1), date(2025, 1, 1)),
                ("C002", "Mass", "Conservative", "CAP_LT30K", date(2025, 1, 1), date(2025, 1, 1))
            ]
        else:  # close_prices
            rows = [
                ("GRS003003035", date(2025, 1, 1), 100.0),
                ("GRS247003007", date(2025, 1, 1), 50.0)
            ]
        
        function(rows)
        
        assert mock_cursor.executemany.called
        call_args = mock_cursor.executemany.call_args[0]
        assert f"INSERT INTO {table_name}" in call_args[0]
        assert len(call_args[1]) == row_count


class TestRecommendations:
    def test_display_recommendations_found(self, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchone.return_value = (
            ['GRS003003035', 'GRS247003007'],
            datetime(2025, 1, 1)
        )
        mock_cursor.description = [
            ('ISIN',), ('assetName',), ('assetCategory',), 
            ('sector',), ('industry',), ('current_price',), ('profitability',)
        ]
        mock_cursor.fetchall.return_value = [
            ('GRS003003035', 'Test Asset', 'Stock', 'Finance', 'Banking', 100.0, 0.15)
        ]
        
        result = display_recommendations_details('C001')
        
        assert len(result) == 1
        assert result[0]['ISIN'] == 'GRS003003035'

    def test_display_recommendations_not_found(self, mock_pool):
        _, _, mock_cursor = mock_pool
        mock_cursor.fetchone.return_value = None
        
        result = display_recommendations_details('C999')
        
        assert result == []


class TestInteractions:
    def test_log_interaction(self, mock_pool):
        _, _, mock_cursor = mock_pool
        
        log_interaction('C001', 'GRS003003035', 'click')
        
        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0]
        assert "INSERT INTO user_interactions" in call_args[0]
        assert call_args[1] == ('C001', 'GRS003003035', 'click', 1)
