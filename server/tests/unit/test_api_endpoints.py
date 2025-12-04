import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from datetime import date
import sys

sys.path.insert(0, '/app')

with patch('psycopg2.pool.SimpleConnectionPool'), \
     patch('kafka.KafkaProducer'):
    from server.server import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def mock_kafka_producer():
    with patch('server.server.producer') as mock:
        mock_future = mock.return_value
        mock_metadata = mock_future.get.return_value
        mock_metadata.topic = 'userprofile'
        mock_metadata.partition = 0
        mock.send.return_value = mock_future
        yield mock


@pytest.fixture
def sample_customer_payload():
    return {
        "customerID": "C001",
        "customerType": "Premium",
        "riskLevel": "Aggressive",
        "investmentCapacity": "CAP_GT300K",
        "lastQuestionnaireDate": "2025-01-01",
        "timestamp": "2025-01-01"
    }


@pytest.fixture
def sample_asset_payload():
    return {
        "ISIN": "GRS003003035",
        "assetName": "Test Asset",
        "assetShortName": "TA",
        "assetCategory": "Stock",
        "assetSubCategory": "Common",
        "marketID": "XATH",
        "sector": "Finance",
        "industry": "Banking",
        "timestamp": "2025-01-01"
    }


class TestHealthEndpoint:
    def test_health_check(self, client):
        response = client.get("/health")
        
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestRecommendationsEndpoint:
    @pytest.mark.parametrize("action", ["request_recs", "refresh_recs"])
    def test_request_recommendations(self, client, mock_kafka_producer, action):
        payload = {"customer_id": "C001", "action": action}
        
        response = client.post("/recommendations", json=payload)
        
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        mock_kafka_producer.send.assert_called_once()

    @pytest.mark.parametrize("payload,expected_status", [
        ({"customer_id": "C001", "action": "invalid_action"}, 422),
        ({"action": "request_recs"}, 422),
    ])
    def test_invalid_requests(self, client, payload, expected_status):
        response = client.post("/recommendations", json=payload)
        
        assert response.status_code == expected_status


class TestGetRecommendationsEndpoint:
    @patch('server.server.display_recommendations_details')
    def test_get_recommendations_success(self, mock_display, client):
        mock_display.return_value = [{
            'ISIN': 'GRS003003035',
            'assetName': 'Test Asset',
            'assetCategory': 'Stock',
            'current_price': 100.0,
            'profitability': 0.15
        }]
        
        response = client.get("/recommendations/C001")
        
        assert response.status_code == 200
        data = response.json()
        assert data['status'] == 'ok'
        assert len(data['recommendations']) == 1

    @patch('server.server.display_recommendations_details')
    def test_get_recommendations_empty(self, mock_display, client):
        mock_display.return_value = []
        
        response = client.get("/recommendations/C001")
        
        assert response.status_code == 200
        assert response.json()['recommendations'] == []


class TestUserInteractionsEndpoint:
    @patch('server.server.log_interaction')
    def test_log_interaction_success(self, mock_log, client):
        payload = {
            "customer_id": "C001",
            "isin": "GRS003003035",
            "type": "click"
        }
        
        response = client.post("/user_interactions", json=payload)
        
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
        mock_log.assert_called_once_with("C001", "GRS003003035", "click")

    def test_log_interaction_invalid_type(self, client):
        payload = {
            "customer_id": "C001",
            "isin": "GRS003003035",
            "type": "invalid_type"
        }
        
        response = client.post("/user_interactions", json=payload)
        
        assert response.status_code == 422


class TestDataIngestionEndpoints:
    @patch('server.server.insert_customer_information')
    def test_ingest_customer(self, mock_insert, client, sample_customer_payload):
        response = client.post("/data/training/customer_information", json=sample_customer_payload)
        
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @patch('server.server.insert_asset_information')
    def test_ingest_asset(self, mock_insert, client, sample_asset_payload):
        response = client.post("/data/training/asset_information", json=sample_asset_payload)
        
        assert response.status_code == 200

    @patch('server.server.batch_insert_customer_information')
    def test_batch_ingest(self, mock_batch, client, sample_customer_payload):
        payload = [
            sample_customer_payload,
            {**sample_customer_payload, "customerID": "C002", "customerType": "Mass"}
        ]
        
        response = client.post("/data/training/customer_information/batch", json=payload)
        
        assert response.status_code == 200
        assert response.json()['rows_inserted'] == 2

    def test_ingest_unsupported_dataset(self, client):
        response = client.post("/data/training/unsupported_dataset", json={"test": "data"})
        
        assert response.status_code == 422
