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

-- Recommendations Table
CREATE TABLE IF NOT EXISTS recommendations (
    customerID VARCHAR(50) NOT NULL,
    recommendations JSONB,
    timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (customerID, timestamp)
);

-- User Interactions Table
CREATE TABLE IF NOT EXISTS user_interactions (
    interactionID SERIAL PRIMARY KEY,
    customerID VARCHAR(50) NOT NULL,
    ISIN VARCHAR(20) NOT NULL,
    interactionType VARCHAR(20) NOT NULL, -- 'click', 'view_details', 'add_watchlist'
    weight INT DEFAULT 1,                 -- 1 for click, 3 for watchlist, etc.
    timestamp TIMESTAMP NOT NULL
);

-- Index for fast retrieval during recommendation generation
CREATE INDEX idx_interactions_user_time ON user_interactions(customerID, timestamp DESC);

-- Watchlist Table
CREATE TABLE IF NOT EXISTS watchlist (
    watchlistID SERIAL PRIMARY KEY,
    customerID VARCHAR(50) NOT NULL,
    ISIN VARCHAR(20) NOT NULL,
    addedDate TIMESTAMP DEFAULT NOW(),
    UNIQUE(customerID, ISIN)
);

-- Index for fast watchlist retrieval
CREATE INDEX idx_watchlist_customer ON watchlist(customerID);