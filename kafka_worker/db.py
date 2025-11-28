import os
from collections import deque
from typing import List, Tuple
import json
from psycopg2.pool import SimpleConnectionPool


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


# Batch buffer for interactions
_interactions_buffer: deque = deque(maxlen=1000)
_batch_size = 100


def execute_batch_insert(sql: str, params_list: List[Tuple]) -> None:
    """Execute a batch INSERT query."""
    if not params_list:
        return
    
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, params_list)
    finally:
        _pool.putconn(connection)


def batch_insert_interactions(rows: List[Tuple]) -> None:
    """Batch insert user interactions."""
    # Convert metadata to JSONB format
    rows_with_jsonb = []
    for row in rows:
        if len(row) == 5:
            user_name, interaction_type, asset_id, timestamp, metadata = row
            # If metadata is already a dict, convert to JSON string for JSONB
            if isinstance(metadata, dict):
                metadata = json.dumps(metadata)
            elif metadata is None:
                metadata = "{}"
            rows_with_jsonb.append((user_name, interaction_type, asset_id, timestamp, metadata))
        else:
            rows_with_jsonb.append(row)
    
    sql = (
        "INSERT INTO user_interactions (user_name, interaction_type, asset_id, timestamp, metadata) "
        "VALUES (%s, %s, %s, %s, %s::jsonb) "
        "ON CONFLICT (user_name, interaction_type, asset_id, timestamp) DO UPDATE SET "
        "metadata = EXCLUDED.metadata"
    )
    execute_batch_insert(sql, rows_with_jsonb)


def add_interaction_to_buffer(user_name: str, interaction_type: str, asset_id: str, timestamp: str, metadata: str) -> None:
    """Add interaction to buffer and flush if batch size reached."""
    _interactions_buffer.append((user_name, interaction_type, asset_id, timestamp, metadata))
    
    if len(_interactions_buffer) >= _batch_size:
        flush_interactions_buffer()


def flush_interactions_buffer() -> None:
    """Flush all buffered interactions to database."""
    if not _interactions_buffer:
        return
    
    rows = list(_interactions_buffer)
    _interactions_buffer.clear()
    batch_insert_interactions(rows)


def get_user_interactions(user_name: str, limit: int = 1000) -> List[dict]:
    """Get user interactions from database."""
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT user_name, interaction_type, asset_id, timestamp, metadata::text "
                    "FROM user_interactions "
                    "WHERE user_name = %s "
                    "ORDER BY timestamp DESC "
                    "LIMIT %s",
                    (user_name, limit),
                )
                columns = [desc[0] for desc in cursor.description]
                results = []
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row))
                    # Parse JSONB metadata back to dict
                    if row_dict.get("metadata"):
                        try:
                            row_dict["metadata"] = json.loads(row_dict["metadata"])
                        except (json.JSONDecodeError, TypeError):
                            row_dict["metadata"] = {}
                    else:
                        row_dict["metadata"] = {}
                    results.append(row_dict)
                return results
    finally:
        _pool.putconn(connection)

def get_dataset(dataset_name: str) -> List[dict]:
    """Get dataset from database."""
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM %s",
                    (dataset_name,),
                )
                return cursor.fetchall()
    finally:
        _pool.putconn(connection)


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