"""
All Alpha Vantage API endpoints with the parameters needed for schema
inference and validation.  Derived from AlphaVantageDocs.md.

Each entry carries:
    function        - AV function name (used as schema filename)
    params          - query params for the *inference* call
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
        "params": {"symbol": "IBM", "interval": "1min"},
        "premium": True,
        "category": "stock_time_series",
    },
    {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "params": {"symbol": "IBM"},
        "premium": True,
        "category": "stock_time_series",
    },
    {
        "function": "SYMBOL_SEARCH",
        "params": {"keywords": "microsoft"},
        "premium": False,
        "category": "stock_time_series",
    },
    {
        "function": "MARKET_STATUS",
        "params": {},
        "premium": False,
        "category": "stock_time_series",
    },

    # ------------------------------------------------------------------
    # ALPHA INTELLIGENCE
    # ------------------------------------------------------------------
    {
        "function": "NEWS_SENTIMENT",
        "params": {"tickers": "AAPL"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "EARNINGS_CALL_TRANSCRIPT",
        "params": {"symbol": "IBM", "quarter": "2024Q1"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "INSIDER_TRANSACTIONS",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "alpha_intelligence",
    },
    {
        "function": "INSTITUTIONAL_HOLDINGS",
        "params": {"symbol": "IBM"},
        "premium": True,
        "category": "alpha_intelligence",
    },

    # ------------------------------------------------------------------
    # FUNDAMENTAL DATA
    # ------------------------------------------------------------------
    {
        "function": "OVERVIEW",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "ETF_PROFILE",
        "params": {"symbol": "QQQ"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "INCOME_STATEMENT",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "BALANCE_SHEET",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "CASH_FLOW",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "SHARES_OUTSTANDING",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS_ESTIMATES",
        "params": {"symbol": "IBM"},
        "premium": False,
        "category": "fundamental",
    },
    {
        "function": "LISTING_STATUS",
        "params": {},
        "premium": False,
        "csv_only": True,
        "category": "fundamental",
    },
    {
        "function": "EARNINGS_CALENDAR",
        "params": {"symbol": "IBM"},
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
        "premium": False,
        "category": "forex",
    },
    {
        "function": "FX_INTRADAY",
        "params": {"from_symbol": "EUR", "to_symbol": "USD", "interval": "1min"},
        "premium": True,
        "category": "forex",
    },
    {
        "function": "FX_DAILY",
        "params": {"from_symbol": "EUR", "to_symbol": "USD"},
        "premium": False,
        "category": "forex",
    },

    # ------------------------------------------------------------------
    # CRYPTOCURRENCIES
    # ------------------------------------------------------------------
    {
        "function": "DIGITAL_CURRENCY_DAILY",
        "params": {"symbol": "BTC", "market": "USD"},
        "premium": False,
        "category": "crypto",
    },

    # ------------------------------------------------------------------
    # COMMODITIES
    # ------------------------------------------------------------------
    {
        "function": "WTI",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "BRENT",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "NATURAL_GAS",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COPPER",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "ALUMINUM",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "WHEAT",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "CORN",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COTTON",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "SUGAR",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "COFFEE",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "ALL_COMMODITIES",
        "params": {"interval": "daily"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "GOLD_SILVER_SPOT",
        "params": {"symbol": "GOLD"},
        "premium": False,
        "category": "commodities",
    },
    {
        "function": "GOLD_SILVER_HISTORY",
        "params": {"symbol": "GOLD", "interval": "daily"},
        "premium": False,
        "category": "commodities",
    },

    # ------------------------------------------------------------------
    # INDEX DATA
    # ------------------------------------------------------------------
    {
        "function": "INDEX_CATALOG",
        "params": {},
        "premium": False,
        "category": "index",
    },
    {
        # NOTE: requires higher-tier premium subscription; may fail with current key
        "function": "INDEX_DATA",
        "params": {"symbol": "SPX", "interval": "daily"},
        "premium": True,
        "category": "index",
    },

    # ------------------------------------------------------------------
    # ECONOMIC INDICATORS
    # ------------------------------------------------------------------
    {
        "function": "REAL_GDP",
        "params": {"interval": "annual"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "REAL_GDP_PER_CAPITA",
        "params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "TREASURY_YIELD",
        "params": {"interval": "weekly", "maturity": "10year"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "FEDERAL_FUNDS_RATE",
        "params": {"interval": "weekly"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "CPI",
        "params": {"interval": "monthly"},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "INFLATION",
        "params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "RETAIL_SALES",
        "params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "DURABLES",
        "params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "UNEMPLOYMENT",
        "params": {},
        "premium": False,
        "category": "economic",
    },
    {
        "function": "NONFARM_PAYROLL",
        "params": {},
        "premium": False,
        "category": "economic",
    },
]

ALL_CATEGORIES = sorted({e["category"] for e in ENDPOINTS})
