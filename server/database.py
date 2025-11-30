import os
from typing import Tuple, List, Dict, Any

from psycopg2.pool import SimpleConnectionPool


def get_env(name: str, default: str) -> str:
    """Get environment variable or return default value."""
    value = os.getenv(name)
    return value if value is not None and value != "" else default


# Database configuration
DB_HOST = get_env("POSTGRES_HOST", "localhost")
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


def execute_upsert(sql: str, params: Tuple) -> None:
    """Execute an INSERT ... ON CONFLICT UPDATE query."""
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
    finally:
        _pool.putconn(connection)


def execute_batch_upsert(sql: str, params_list: list[Tuple]) -> None:
    """Execute a batch INSERT ... ON CONFLICT UPDATE query."""
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.executemany(sql, params_list)
    finally:
        _pool.putconn(connection)


def insert_customer_information(
    customer_id: str,
    customer_type: str,
    risk_level: str,
    investment_capacity: str,
    last_questionnaire_date,
    timestamp,
) -> None:
    """Insert or update customer information."""
    sql = (
        "INSERT INTO customer_information (customerID, customerType, riskLevel, "
        "investmentCapacity, lastQuestionnaireDate, timestamp) VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (customerID, timestamp) DO UPDATE SET "
        "customerType = EXCLUDED.customerType, "
        "riskLevel = EXCLUDED.riskLevel, "
        "investmentCapacity = EXCLUDED.investmentCapacity, "
        "lastQuestionnaireDate = EXCLUDED.lastQuestionnaireDate"
    )
    params = (
        customer_id,
        customer_type,
        risk_level,
        investment_capacity,
        last_questionnaire_date,
        timestamp,
    )
    execute_upsert(sql, params)


def insert_asset_information(
    isin: str,
    asset_name: str,
    asset_short_name: str,
    asset_category: str,
    asset_sub_category: str,
    market_id: str,
    sector: str,
    industry: str,
    timestamp,
) -> None:
    """Insert or update asset information."""
    sql = (
        "INSERT INTO asset_information (ISIN, assetName, assetShortName, assetCategory, "
        "assetSubCategory, marketID, sector, industry, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (ISIN, timestamp) DO UPDATE SET "
        "assetName = EXCLUDED.assetName, "
        "assetShortName = EXCLUDED.assetShortName, "
        "assetCategory = EXCLUDED.assetCategory, "
        "assetSubCategory = EXCLUDED.assetSubCategory, "
        "marketID = EXCLUDED.marketID, "
        "sector = EXCLUDED.sector, "
        "industry = EXCLUDED.industry"
    )
    params = (
        isin,
        asset_name,
        asset_short_name,
        asset_category,
        asset_sub_category,
        market_id,
        sector,
        industry,
        timestamp,
    )
    execute_upsert(sql, params)


def insert_markets(
    exchange_id: str,
    market_id: str,
    name: str,
    description: str,
    country: str,
    trading_days: str,
    trading_hours: str,
    market_class: str,
) -> None:
    """Insert or update market information."""
    sql = (
        "INSERT INTO markets (exchangeID, marketID, name, description, country, tradingDays, tradingHours, marketClass) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (exchangeID) DO UPDATE SET "
        "marketID = EXCLUDED.marketID, name = EXCLUDED.name, description = EXCLUDED.description, "
        "country = EXCLUDED.country, tradingDays = EXCLUDED.tradingDays, tradingHours = EXCLUDED.tradingHours, "
        "marketClass = EXCLUDED.marketClass"
    )
    params = (
        exchange_id,
        market_id,
        name,
        description,
        country,
        trading_days,
        trading_hours,
        market_class,
    )
    execute_upsert(sql, params)


def insert_close_prices(isin: str, timestamp, close_price: float) -> None:
    """Insert or update close price."""
    sql = (
        "INSERT INTO close_prices (ISIN, timestamp, closePrice) VALUES (%s, %s, %s) "
        "ON CONFLICT (ISIN, timestamp) DO UPDATE SET closePrice = EXCLUDED.closePrice"
    )
    params = (isin, timestamp, close_price)
    execute_upsert(sql, params)


def batch_insert_close_prices(rows: list[tuple]) -> None:
    """Batch insert or update close prices."""
    sql = (
        "INSERT INTO close_prices (ISIN, timestamp, closePrice) VALUES (%s, %s, %s) "
        "ON CONFLICT (ISIN, timestamp) DO UPDATE SET closePrice = EXCLUDED.closePrice"
    )
    execute_batch_upsert(sql, rows)


def insert_limit_prices(
    isin: str,
    min_date,
    max_date,
    price_min_date: float,
    price_max_date: float,
    profitability: float,
) -> None:
    """Insert or update limit prices."""
    sql = (
        "INSERT INTO limit_prices (ISIN, minDate, maxDate, priceMinDate, priceMaxDate, profitability) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (ISIN) DO UPDATE SET "
        "minDate = EXCLUDED.minDate, maxDate = EXCLUDED.maxDate, priceMinDate = EXCLUDED.priceMinDate, "
        "priceMaxDate = EXCLUDED.priceMaxDate, profitability = EXCLUDED.profitability"
    )
    params = (
        isin,
        min_date,
        max_date,
        price_min_date,
        price_max_date,
        profitability,
    )
    execute_upsert(sql, params)


def batch_insert_customer_information(rows: list[tuple]) -> None:
    """Batch insert or update customer information."""
    sql = (
        "INSERT INTO customer_information (customerID, customerType, riskLevel, "
        "investmentCapacity, lastQuestionnaireDate, timestamp) VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (customerID, timestamp) DO UPDATE SET "
        "customerType = EXCLUDED.customerType, "
        "riskLevel = EXCLUDED.riskLevel, "
        "investmentCapacity = EXCLUDED.investmentCapacity, "
        "lastQuestionnaireDate = EXCLUDED.lastQuestionnaireDate"
    )
    execute_batch_upsert(sql, rows)


def batch_insert_limit_prices(rows: list[tuple]) -> None:
    """Batch insert or update limit prices."""
    sql = (
        "INSERT INTO limit_prices (ISIN, minDate, maxDate, priceMinDate, priceMaxDate, profitability) "
        "VALUES (%s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (ISIN) DO UPDATE SET "
        "minDate = EXCLUDED.minDate, maxDate = EXCLUDED.maxDate, priceMinDate = EXCLUDED.priceMinDate, "
        "priceMaxDate = EXCLUDED.priceMaxDate, profitability = EXCLUDED.profitability"
    )
    execute_batch_upsert(sql, rows)


def insert_transactions(
    customer_id: str,
    isin: str,
    transaction_id: str,
    transaction_type: str,
    timestamp,
    total_value: float,
    units: float,
    channel: str,
    market_id: str,
) -> None:
    """Insert or update transaction."""
    sql = (
        "INSERT INTO transactions (customerID, ISIN, transactionID, transactionType, timestamp, totalValue, units, channel, marketID) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (transactionID) DO UPDATE SET "
        "customerID = EXCLUDED.customerID, ISIN = EXCLUDED.ISIN, transactionType = EXCLUDED.transactionType, "
        "timestamp = EXCLUDED.timestamp, totalValue = EXCLUDED.totalValue, units = EXCLUDED.units, "
        "channel = EXCLUDED.channel, marketID = EXCLUDED.marketID"
    )
    params = (
        customer_id,
        isin,
        transaction_id,
        transaction_type,
        timestamp,
        total_value,
        units,
        channel,
        market_id,
    )
    execute_upsert(sql, params)


def batch_insert_transactions(rows: list[tuple]) -> None:
    """Batch insert or update transactions."""
    sql = (
        "INSERT INTO transactions (customerID, ISIN, transactionID, transactionType, timestamp, totalValue, units, channel, marketID) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "ON CONFLICT (transactionID) DO UPDATE SET "
        "customerID = EXCLUDED.customerID, ISIN = EXCLUDED.ISIN, transactionType = EXCLUDED.transactionType, "
        "timestamp = EXCLUDED.timestamp, totalValue = EXCLUDED.totalValue, units = EXCLUDED.units, "
        "channel = EXCLUDED.channel, marketID = EXCLUDED.marketID"
    )
    execute_batch_upsert(sql, rows)

def display_recommendations_details(customer_id: str) -> List[Dict[str, Any]]:
    """
    Fetches the latest recommendations for a user and retrieves 
    full asset details (Name, Price, Category, etc.) for those items.
    """
    connection = _pool.getconn()
    try:
        with connection:
            with connection.cursor() as cursor:
                # 1. Get the list of ISINs from the most recent recommendation
                cursor.execute(
                    """
                    SELECT recommendations, timestamp 
                    FROM recommendations 
                    WHERE customerID = %s 
                    ORDER BY timestamp DESC 
                    LIMIT 1
                    """,
                    (customer_id,)
                )
                row = cursor.fetchone()
                
                # If no recommendations found, return empty list
                if not row:
                    return []
                
                # Psycopg2 usually converts JSONB directly to a Python list. 
                # If it comes back as a string, we might need json.loads(row[0])
                rec_isins = row[0] 
                rec_date = row[1]

                if not rec_isins:
                    return []

                # 2. Query Asset Details for these ISINs
                # We use the ANY(%s) syntax which works perfectly with Python lists in Postgres
                query = """
                    SELECT 
                        a.ISIN, 
                        a.assetName, 
                        a.assetCategory, 
                        a.sector, 
                        a.industry,
                        l.priceMaxDate as current_price,
                        l.profitability
                    FROM asset_information a
                    LEFT JOIN limit_prices l ON a.ISIN = l.ISIN
                    WHERE a.ISIN = ANY(%s)
                """
                
                cursor.execute(query, (rec_isins,))
                
                # Convert rows to list of dictionaries
                columns = [desc[0] for desc in cursor.description]
                results = []
                for asset_row in cursor.fetchall():
                    asset_dict = dict(zip(columns, asset_row))
                    # Add the recommendation timestamp for context
                    asset_dict['recommendation_date'] = rec_date
                    results.append(asset_dict)
                
                return results

    except Exception as e:
        print(f"Error fetching recommendation details: {e}")
        return []
        
    finally:
        _pool.putconn(connection)
