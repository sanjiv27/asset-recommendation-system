-- Create database (if not exists)

-- Customer Information Table
CREATE TABLE IF NOT EXISTS customer_information (
    customerID VARCHAR(50) NOT NULL,
    customerType VARCHAR(50),
    riskLevel VARCHAR(50),
    investmentCapacity VARCHAR(50),
    lastQuestionnaireDate DATE,
    timestamp DATE NOT NULL,
    PRIMARY KEY (customerID, timestamp)
);

-- Asset Information Table
CREATE TABLE IF NOT EXISTS asset_information (
    ISIN VARCHAR(20) NOT NULL,
    assetName TEXT,
    assetShortName VARCHAR(50),
    assetCategory VARCHAR(50),
    assetSubCategory VARCHAR(100),
    marketID VARCHAR(20),
    sector VARCHAR(100),
    industry VARCHAR(100),
    timestamp DATE NOT NULL,
    PRIMARY KEY (ISIN, timestamp)
);

-- Markets Table
CREATE TABLE IF NOT EXISTS markets (
    exchangeID VARCHAR(20) PRIMARY KEY,
    marketID VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    description TEXT,
    country VARCHAR(100),
    tradingDays VARCHAR(100),
    tradingHours VARCHAR(50),
    marketClass VARCHAR(100)
);

-- Close Prices Table
CREATE TABLE IF NOT EXISTS close_prices (
    ISIN VARCHAR(20) NOT NULL,
    timestamp DATE NOT NULL,
    closePrice DECIMAL(15, 4),
    PRIMARY KEY (ISIN, timestamp)
);

-- Limit Prices Table
CREATE TABLE IF NOT EXISTS limit_prices (
    ISIN VARCHAR(20) PRIMARY KEY,
    minDate DATE,
    maxDate DATE,
    priceMinDate DECIMAL(15, 4),
    priceMaxDate DECIMAL(15, 4),
    profitability DECIMAL(10, 6)
);

-- Transactions Table
CREATE TABLE IF NOT EXISTS transactions (
    customerID VARCHAR(50) NOT NULL,
    ISIN VARCHAR(20) NOT NULL,
    transactionID VARCHAR(50) PRIMARY KEY,
    transactionType VARCHAR(10) CHECK (transactionType IN ('Buy', 'Sell')),
    timestamp DATE NOT NULL,
    totalValue DECIMAL(15, 2),
    units DECIMAL(15, 4),
    channel VARCHAR(50),
    marketID VARCHAR(20)
);

-- User Interactions Table
-- Stores user interactions with assets (clicks, views, purchases, etc.)
CREATE TABLE IF NOT EXISTS user_interactions (
    user_name VARCHAR(100) NOT NULL,
    interaction_type VARCHAR(50) NOT NULL,
    asset_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    metadata JSONB,
    PRIMARY KEY (user_name, interaction_type, asset_id, timestamp)
);