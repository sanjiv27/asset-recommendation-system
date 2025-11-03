from typing import Literal

from fastapi import FastAPI, HTTPException, Path

from .database import (
    insert_asset_information,
    insert_customer_information,
    execute_upsert,
)
from .data_class import (
    CustomerInformationRow,
    AssetInformationRow,
    MarketsRow,
    ClosePricesRow,
    LimitPricesRow,
    TransactionsRow,
)

app = FastAPI(title="Asset Recommendation")



DatasetName = Literal[
    "customer_information",
    "asset_information",
    "markets",
    "close_prices",
    "limit_prices",
    "transactions",
]


@app.post("/data/training/{dataset}")
def ingest_row(
    dataset: DatasetName = Path(..., description="Target table to insert into"),
    payload: dict = None,
):
    if payload is None:
        raise HTTPException(status_code=400, detail="Request body is required")

    if dataset == "customer_information":
        row = CustomerInformationRow(**payload)
        insert_customer_information(
            row.customerID,
            row.customerType,
            row.riskLevel,
            row.investmentCapacity,
            row.lastQuestionnaireDate,
            row.timestamp,
        )
        return {"status": "ok"}

    if dataset == "asset_information":
        row = AssetInformationRow(**payload)
        insert_asset_information(
            row.ISIN,
            row.assetName,
            row.assetShortName,
            row.assetCategory,
            row.assetSubCategory,
            row.marketID,
            row.sector,
            row.industry,
            row.timestamp,
        )
        return {"status": "ok"}

    if dataset == "markets":
        row = MarketsRow(**payload)
        sql = (
            "INSERT INTO markets (exchangeID, marketID, name, description, country, tradingDays, tradingHours, marketClass) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (exchangeID) DO UPDATE SET "
            "marketID = EXCLUDED.marketID, name = EXCLUDED.name, description = EXCLUDED.description, "
            "country = EXCLUDED.country, tradingDays = EXCLUDED.tradingDays, tradingHours = EXCLUDED.tradingHours, "
            "marketClass = EXCLUDED.marketClass"
        )
        params = (
            row.exchangeID,
            row.marketID,
            row.name,
            row.description,
            row.country,
            row.tradingDays,
            row.tradingHours,
            row.marketClass,
        )
        execute_upsert(sql, params)
        return {"status": "ok"}

    if dataset == "close_prices":
        row = ClosePricesRow(**payload)
        sql = (
            "INSERT INTO close_prices (ISIN, timestamp, closePrice) VALUES (%s, %s, %s) "
            "ON CONFLICT (ISIN, timestamp) DO UPDATE SET closePrice = EXCLUDED.closePrice"
        )
        params = (row.ISIN, row.timestamp, row.closePrice)
        execute_upsert(sql, params)
        return {"status": "ok"}

    if dataset == "limit_prices":
        row = LimitPricesRow(**payload)
        sql = (
            "INSERT INTO limit_prices (ISIN, minDate, maxDate, priceMinDate, priceMaxDate, profitability) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (ISIN) DO UPDATE SET "
            "minDate = EXCLUDED.minDate, maxDate = EXCLUDED.maxDate, priceMinDate = EXCLUDED.priceMinDate, "
            "priceMaxDate = EXCLUDED.priceMaxDate, profitability = EXCLUDED.profitability"
        )
        params = (
            row.ISIN,
            row.minDate,
            row.maxDate,
            row.priceMinDate,
            row.priceMaxDate,
            row.profitability,
        )
        execute_upsert(sql, params)
        return {"status": "ok"}

    if dataset == "transactions":
        row = TransactionsRow(**payload)
        sql = (
            "INSERT INTO transactions (customerID, ISIN, transactionID, transactionType, timestamp, totalValue, units, channel, marketID) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (transactionID) DO UPDATE SET "
            "customerID = EXCLUDED.customerID, ISIN = EXCLUDED.ISIN, transactionType = EXCLUDED.transactionType, "
            "timestamp = EXCLUDED.timestamp, totalValue = EXCLUDED.totalValue, units = EXCLUDED.units, "
            "channel = EXCLUDED.channel, marketID = EXCLUDED.marketID"
        )
        params = (
            row.customerID,
            row.ISIN,
            row.transactionID,
            row.transactionType,
            row.timestamp,
            row.totalValue,
            row.units,
            row.channel,
            row.marketID,
        )
        execute_upsert(sql, params)
        return {"status": "ok"}

    raise HTTPException(status_code=400, detail="Unsupported dataset")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


