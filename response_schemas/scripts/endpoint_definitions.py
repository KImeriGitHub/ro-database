"""
All Alpha Vantage API endpoints with the parameters needed for schema
inference and validation.  Derived from AlphaVantageDocs.md.

Each entry carries:
    function        - AV function name (used as schema filename)
    params          - query params for the *inference* call
    alt_params      - query params for the *validation* call (different symbol)
    premium         - True if the endpoint requires a premium key
    csv_only        - True if the endpoint only returns CSV (no JSON)
    category        - grouping label for --category filtering
"""

BASE_URL = "https://www.alphavantage.co/query"

ENDPOINTS = [
    # ------------------------------------------------------------------
    # CORE STOCK TIME SERIES
    # ------------------------------------------------------------------
    {
        "function": "TIME_SERIES_INTRADAY",
        "params": {"symbol": "IBM", "interval": "5min"},
        "alt_params": {"symbol": "AAPL", "interval": "5min"},
        "premium": True,
        "category": "stock_time_series",
    },
    {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": True,
        "category": "stock_time_series",
    },
    {
        "function": "SYMBOL_SEARCH",
        "params": {"keywords": "microsoft"},
        "alt_params": {"keywords": "apple"},
        "premium": False,
        "category": "stock_time_series",
    },
    {
        "function": "MARKET_STATUS",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "stock_time_series",
    },

    # ------------------------------------------------------------------
    # ALPHA INTELLIGENCE
    # ------------------------------------------------------------------
    {
        "function": "NEWS_SENTIMENT",
        "params": {"tickers": "AAPL"},
        "alt_params": {"tickers": "MSFT"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "EARNINGS_CALL_TRANSCRIPT",
        "params": {"symbol": "IBM", "quarter": "2024Q1"},
        "alt_params": {"symbol": "AAPL", "quarter": "2024Q1"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "INSIDER_TRANSACTIONS",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "INSTITUTIONAL_HOLDINGS",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": True,
        "category": "alpha_intelligence",
    },

    # ------------------------------------------------------------------
    # FUNDAMENTAL DATA
    # ------------------------------------------------------------------
    {
        "function": "OVERVIEW",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "ETF_PROFILE",
        "params": {"symbol": "QQQ"},
        "alt_params": {"symbol": "SPY"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "INCOME_STATEMENT",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "BALANCE_SHEET",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "CASH_FLOW",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "SHARES_OUTSTANDING",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS_ESTIMATES",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "LISTING_STATUS",
        "params": {},
        "alt_params": {},
        "premium": False,
        "csv_only": True,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS_CALENDAR",
        "params": {"symbol": "IBM"},
        "alt_params": {"symbol": "AAPL"},
        "premium": False,
        "csv_only": True,
        "category": "fundamental",
    },

    # ------------------------------------------------------------------
    # FOREX
    # ------------------------------------------------------------------
    {
        "function": "CURRENCY_EXCHANGE_RATE",
        "params": {"from_currency": "USD", "to_currency": "JPY"},
        "alt_params": {"from_currency": "EUR", "to_currency": "GBP"},
        "premium": False,
        "category": "forex",
    },
    {
        "function": "FX_INTRADAY",
        "params": {"from_symbol": "EUR", "to_symbol": "USD", "interval": "5min"},
        "alt_params": {"from_symbol": "GBP", "to_symbol": "JPY", "interval": "5min"},
        "premium": True,
        "category": "forex",
    },
    {
        "function": "FX_DAILY",
        "params": {"from_symbol": "EUR", "to_symbol": "USD"},
        "alt_params": {"from_symbol": "GBP", "to_symbol": "JPY"},
        "premium": False,
        "category": "forex",
    },

    # ------------------------------------------------------------------
    # CRYPTOCURRENCIES
    # ------------------------------------------------------------------
    {
        "function": "DIGITAL_CURRENCY_DAILY",
        "params": {"symbol": "BTC", "market": "USD"},
        "alt_params": {"symbol": "ETH", "market": "USD"},
        "premium": False,
        "category": "crypto",
    },

    # ------------------------------------------------------------------
    # COMMODITIES
    # ------------------------------------------------------------------
    {
        "function": "WTI",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "BRENT",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "NATURAL_GAS",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COPPER",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "ALUMINUM",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "WHEAT",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "CORN",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COTTON",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "SUGAR",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COFFEE",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "ALL_COMMODITIES",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "GOLD_SILVER_SPOT",
        "params": {"symbol": "GOLD"},
        "alt_params": {"symbol": "SILVER"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "GOLD_SILVER_HISTORY",
        "params": {"symbol": "GOLD", "interval": "daily"},
        "alt_params": {"symbol": "SILVER", "interval": "daily"},
        "premium": False,
        "category": "commodities",
    },

    # ------------------------------------------------------------------
    # INDEX DATA
    # ------------------------------------------------------------------
    {
        "function": "INDEX_CATALOG",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "index",
    },
    {
        # NOTE: requires higher-tier premium subscription; may fail with current key
        "function": "INDEX_DATA",
        "params": {"symbol": "SPX", "interval": "daily"},
        "alt_params": {"symbol": "DJI", "interval": "weekly"},
        "premium": True,
        "category": "index",
    },

    # ------------------------------------------------------------------
    # ECONOMIC INDICATORS
    # ------------------------------------------------------------------
    {
        "function": "REAL_GDP",
        "params": {"interval": "annual"},
        "alt_params": {"interval": "quarterly"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "REAL_GDP_PER_CAPITA",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "TREASURY_YIELD",
        "params": {"interval": "weekly", "maturity": "10year"},
        "alt_params": {"interval": "monthly", "maturity": "2year"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "FEDERAL_FUNDS_RATE",
        "params": {"interval": "weekly"},
        "alt_params": {"interval": "monthly"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "CPI",
        "params": {"interval": "monthly"},
        "alt_params": {"interval": "semiannual"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "INFLATION",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "RETAIL_SALES",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "DURABLES",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "UNEMPLOYMENT",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "NONFARM_PAYROLL",
        "params": {},
        "alt_params": {},
        "premium": False,
        "category": "economic",
    },
]

ALL_CATEGORIES = sorted({e["category"] for e in ENDPOINTS})
