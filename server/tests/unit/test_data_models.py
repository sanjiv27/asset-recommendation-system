import pytest
from datetime import date
from pydantic import ValidationError
import sys
import os

sys.path.insert(0, '/app')
os.chdir('/app')

from server import data_class


class TestCustomerInformationRow:
    def test_valid_customer(self):
        customer = data_class.CustomerInformationRow(
            customerID="C001",
            customerType="Premium",
            riskLevel="Aggressive",
            investmentCapacity="CAP_GT300K",
            lastQuestionnaireDate=date(2025, 1, 1),
            timestamp=date(2025, 1, 1)
        )
        
        assert customer.customerID == "C001"
        assert customer.customerType == "Premium"

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            data_class.CustomerInformationRow(customerID="C001")

    def test_optional_fields(self):
        customer = data_class.CustomerInformationRow(
            customerID="C001",
            timestamp=date(2025, 1, 1)
        )
        
        assert customer.customerType is None
        assert customer.riskLevel is None


class TestAssetInformationRow:
    def test_valid_asset(self):
        asset = data_class.AssetInformationRow(
            ISIN="GRS003003035",
            assetName="Test Asset",
            assetCategory="Stock",
            timestamp=date(2025, 1, 1)
        )
        
        assert asset.ISIN == "GRS003003035"
        assert asset.assetName == "Test Asset"

    def test_missing_isin(self):
        with pytest.raises(ValidationError):
            data_class.AssetInformationRow(timestamp=date(2025, 1, 1))


class TestTransactionsRow:
    @pytest.mark.parametrize("transaction_type", ["Buy", "Sell"])
    def test_valid_transaction(self, transaction_type):
        txn = data_class.TransactionsRow(
            customerID="C001",
            ISIN="GRS003003035",
            transactionID="T001",
            transactionType=transaction_type,
            timestamp=date(2025, 1, 1)
        )
        
        assert txn.transactionType == transaction_type

    def test_invalid_transaction_type(self):
        with pytest.raises(ValidationError):
            data_class.TransactionsRow(
                customerID="C001",
                ISIN="GRS003003035",
                transactionID="T001",
                transactionType="Invalid",
                timestamp=date(2025, 1, 1)
            )


class TestRecommendationRequest:
    @pytest.mark.parametrize("action", ["request_recs", "refresh_recs"])
    def test_valid_request(self, action):
        req = data_class.RecommendationRequest(
            customer_id="C001",
            action=action
        )
        
        assert req.customer_id == "C001"
        assert req.action == action

    def test_invalid_action(self):
        with pytest.raises(ValidationError):
            data_class.RecommendationRequest(
                customer_id="C001",
                action="invalid_action"
            )


class TestUserInteraction:
    def test_valid_interaction(self):
        interaction = data_class.UserInteraction(
            customer_id="C001",
            isin="GRS003003035",
            type=data_class.InteractionType.CLICK
        )
        
        assert interaction.weight == 1

    @pytest.mark.parametrize("weight", [1, 5, 10])
    def test_custom_weight(self, weight):
        interaction = data_class.UserInteraction(
            customer_id="C001",
            isin="GRS003003035",
            type=data_class.InteractionType.CLICK,
            weight=weight
        )
        
        assert interaction.weight == weight
