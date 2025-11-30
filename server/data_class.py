from typing import Literal, Optional, List
from pydantic import BaseModel, Field
from datetime import date, datetime
from enum import Enum

class CustomerInformationRow(BaseModel):
    customerID: str
    customerType: Optional[str] = None
    riskLevel: Optional[str] = None
    investmentCapacity: Optional[str] = None
    lastQuestionnaireDate: Optional[date] = None
    timestamp: date


class AssetInformationRow(BaseModel):
    ISIN: str
    assetName: Optional[str] = None
    assetShortName: Optional[str] = None
    assetCategory: Optional[str] = None
    assetSubCategory: Optional[str] = None
    marketID: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    timestamp: date


class MarketsRow(BaseModel):
    exchangeID: str
    marketID: str
    name: Optional[str] = None
    description: Optional[str] = None
    country: Optional[str] = None
    tradingDays: Optional[str] = None
    tradingHours: Optional[str] = None
    marketClass: Optional[str] = None


class ClosePricesRow(BaseModel):
    ISIN: str
    timestamp: date
    closePrice: Optional[float] = Field(default=None)


class LimitPricesRow(BaseModel):
    ISIN: str
    minDate: Optional[date] = None
    maxDate: Optional[date] = None
    priceMinDate: Optional[float] = None
    priceMaxDate: Optional[float] = None
    profitability: Optional[float] = None


class TransactionsRow(BaseModel):
    customerID: str
    ISIN: str
    transactionID: str
    transactionType: Literal["Buy", "Sell"]
    timestamp: date
    totalValue: Optional[float] = None
    units: Optional[float] = None
    channel: Optional[str] = None
    marketID: Optional[str] = None

class RiskLevel(str, Enum):
    CONSERVATIVE = "Conservative"
    INCOME = "Income"
    BALANCED = "Balanced"
    AGGRESSIVE = "Aggressive"

class InvestCapacity(str, Enum):
    LOW = "CAP_LT30K"
    MEDIUM = "CAP_30K_80K"
    HIGH = "CAP_80K_300K"
    VERY_HIGH = "CAP_GT300K"

class CustomerType(str, Enum):
    MASS = "Mass"
    PREMIUM = "Premium"
    PROFESSIONAL = "Professional"

class RecommendationRequest(BaseModel):
    customer_id: str = Field(..., description="Unique user identifier")
    action: Literal["request_recs", "refresh_recs"] = Field(..., description="Action to perform")

class InteractionType(str, Enum):
    CLICK = "click"
    VIEW_DETAILS = "view_details"
    ADD_WATCHLIST = "add_watchlist"

class UserInteraction(BaseModel):
    customer_id: str = Field(..., description="Unique user identifier")
    isin: str = Field(..., description="ISIN of the asset")
    type: InteractionType = Field(..., description="Type of interaction")
    weight: Optional[int] = Field(default=1, description="Weight of the interaction")