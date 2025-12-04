import pandas as pd
from datetime import datetime, date

def get_sample_transactions():
    return [
        {'customerID': 'C001', 'ISIN': 'GRS003003035', 'transactionType': 'Buy', 'timestamp': datetime(2025, 1, 1)},
        {'customerID': 'C001', 'ISIN': 'GRS247003007', 'transactionType': 'Buy', 'timestamp': datetime(2025, 1, 2)},
        {'customerID': 'C002', 'ISIN': 'GRS003003035', 'transactionType': 'Buy', 'timestamp': datetime(2025, 1, 3)},
        {'customerID': 'C002', 'ISIN': 'GRS320313000', 'transactionType': 'Buy', 'timestamp': datetime(2025, 1, 4)},
    ]

def get_sample_assets():
    return [
        {'ISIN': 'GRS003003035', 'assetCategory': 'Stock', 'assetSubCategory': 'Common', 'sector': 'Finance', 'industry': 'Banking', 'marketID': 'XATH'},
        {'ISIN': 'GRS247003007', 'assetCategory': 'Stock', 'assetSubCategory': 'Common', 'sector': 'Technology', 'industry': 'Software', 'marketID': 'XATH'},
        {'ISIN': 'GRS320313000', 'assetCategory': 'Stock', 'assetSubCategory': 'Common', 'sector': 'Technology', 'industry': 'Hardware', 'marketID': 'XATH'},
    ]

def get_sample_customers():
    return [
        {'customerID': 'C001', 'riskLevel': 'Aggressive', 'investmentCapacity': 'CAP_GT300K', 'customerType': 'Premium'},
        {'customerID': 'C002', 'riskLevel': 'Conservative', 'investmentCapacity': 'CAP_LT30K', 'customerType': 'Mass'},
    ]

def get_sample_limit_prices():
    return [
        {'ISIN': 'GRS003003035', 'profitability': 0.15, 'priceMaxDate': 100.0},
        {'ISIN': 'GRS247003007', 'profitability': 0.25, 'priceMaxDate': 50.0},
        {'ISIN': 'GRS320313000', 'profitability': -0.05, 'priceMaxDate': 75.0},
    ]

def get_sample_interactions():
    return [
        {'ISIN': 'GRS003003035', 'interactionType': 'click', 'weight': 1, 'assetCategory': 'Stock', 'sector': 'Finance', 'industry': 'Banking'},
        {'ISIN': 'GRS247003007', 'interactionType': 'click', 'weight': 1, 'assetCategory': 'Stock', 'sector': 'Technology', 'industry': 'Software'},
    ]
