import os
from collections import deque
from typing import List, Tuple
import json
from psycopg2.pool import SimpleConnectionPool
from datetime import datetime


def get_env(name: str, default: str) -> str:
    """Get environment variable or return default value."""
    value = os.getenv(name)
    return value if value not in (None, "") else default


# Database configuration
DB_HOST = get_env("POSTGRES_HOST", "db")
DB_PORT = int(get_env("POSTGRES_PORT", "5432"))
DB_NAME = get_env("POSTGRES_DB", "asset_recommendation")
DB_USER = get_env("POSTGRES_USER", "admin")
DB_PASSWORD = get_env("POSTGRES_PASSWORD", "123")


# Connection pool
_pool: SimpleConnectionPool = SimpleConnectionPool(
    1,
    10,
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
)

def get_buy_transactions() -> List[dict]:
    """
    Get buy transactions from database.
    Returns list of dicts with columns: customerID, ISIN, transactionType, timestamp
    """
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT customerID, ISIN, transactionType, timestamp "
                    "FROM transactions "
                    "WHERE transactionType = 'Buy' "
                    "ORDER BY timestamp DESC"
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _pool.putconn(connection)


def get_assets() -> List[dict]:
    """
    Get asset information from database.
    Returns list of dicts with columns: ISIN, assetCategory, assetSubCategory, sector, industry, marketID
    Returns the latest record for each ISIN (by timestamp).
    """
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (ISIN) 
                        ISIN, assetCategory, assetSubCategory, sector, industry, marketID
                    FROM asset_information
                    ORDER BY ISIN, timestamp DESC
                    """
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _pool.putconn(connection)


def get_customers() -> List[dict]:
    """
    Get customer information from database.
    Returns list of dicts with columns: customerID, riskLevel, investmentCapacity, customerType, timestamp
    Returns the latest record for each customerID (by timestamp).
    """
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT DISTINCT ON (customerID)
                        customerID, riskLevel, investmentCapacity, customerType, timestamp
                    FROM customer_information
                    ORDER BY customerID, timestamp DESC
                    """
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _pool.putconn(connection)


def get_limit_prices() -> List[dict]:
    """
    Get limit prices and profitability from database.
    Returns list of dicts with columns: ISIN, profitability, priceMaxDate
    """
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ISIN, profitability, priceMaxDate "
                    "FROM limit_prices"
                )
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _pool.putconn(connection)

def save_recommendations(customer_id: str, recommendations: List[dict]):
    """
    Save recommendations to database.
    Returns list of dicts with columns: customerID, recommendations
    """
    recommendations_json = json.dumps(recommendations)
    timestamp = datetime.now().replace(second=0, microsecond=0)    
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                        "INSERT INTO recommendations (customerID, recommendations, timestamp) VALUES (%s, %s, %s)",
                    (customer_id, recommendations_json, timestamp)
                )
                connection.commit()
    finally:
        _pool.putconn(connection)
    return True

def get_recent_user_interactions(customer_id: str, days: int = 1):
    """
    Fetches interactions for the last X days.
    """
    conn = _pool.getconn()
    try:
        with conn.cursor() as cursor:
            # We join with asset_information immediately to get the sector/category of what they clicked
            query = """
                SELECT 
                    i.ISIN, 
                    i.interactionType,
                    i.weight,
                    a.assetCategory,
                    a.sector,
                    a.industry
                FROM user_interactions i
                JOIN asset_information a ON i.ISIN = a.ISIN
                WHERE i.customerID = %s 
                  AND i.timestamp >= NOW() - INTERVAL '%s days'
            """
            cursor.execute(query, (customer_id, days))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _pool.putconn(conn)