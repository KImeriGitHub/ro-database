================================================================================
  ALPHA VANTAGE API - COMPACT REFERENCE
  Base URL: https://www.alphavantage.co/query
  All requests require: &apikey=YOUR_KEY
  Default output: JSON (add &datatype=csv for CSV)
================================================================================

--------------------------------------------------------------------------------
1. CORE STOCK TIME SERIES
--------------------------------------------------------------------------------
TIME_SERIES_INTRADAY          [PREMIUM]
  Required: function, symbol, interval (1min|5min|15min|30min|60min), apikey
  Optional: adjusted (true*), extended_hours (true*), month (YYYY-MM),
            outputsize (compact*|full), datatype (json*|csv),
            entitlement (realtime|delayed)
  Example:  ?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=1min&apikey=demo

SYMBOL_SEARCH  (ticker lookup / autocomplete)
  Required: function, keywords, apikey
  Optional: datatype
  Example:  ?function=SYMBOL_SEARCH&keywords=microsoft&apikey=demo

MARKET_STATUS  (global market open/close)
  Required: function, apikey
  Example:  ?function=MARKET_STATUS&apikey=demo

Global exchange suffixes: .LON .TRT .TRV .DEX .BSE .SHH .SHZ

--------------------------------------------------------------------------------
2. ALPHA INTELLIGENCE
--------------------------------------------------------------------------------
NEWS_SENTIMENT                [also works with ETFs]
  Required: function, apikey
  Optional: tickers (e.g. IBM or COIN,CRYPTO:BTC,FOREX:USD),
            topics (blockchain|earnings|ipo|mergers_and_acquisitions|
                    financial_markets|economy_fiscal|economy_monetary|
                    economy_macro|energy_transportation|finance|
                    life_sciences|manufacturing|real_estate|
                    retail_wholesale|technology),
            time_from/time_to (YYYYMMDDTHHMM), sort (LATEST*|EARLIEST|RELEVANCE),
            limit (50*|1000)
  Example:  ?function=NEWS_SENTIMENT&tickers=AAPL&apikey=demo

EARNINGS_CALL_TRANSCRIPT      [equities only, not ETFs]
  Required: function, symbol, quarter (YYYYqM e.g. 2024Q1, min=2010Q1), apikey
  Example:  ?function=EARNINGS_CALL_TRANSCRIPT&symbol=IBM&quarter=2024Q1&apikey=demo

INSIDER_TRANSACTIONS          [equities only, not ETFs]
  Required: function, symbol, apikey
  Example:  ?function=INSIDER_TRANSACTIONS&symbol=IBM&apikey=demo

INSTITUTIONAL_HOLDINGS        [PREMIUM] [equities only, not ETFs]
  Required: function, symbol, apikey
  Example:  ?function=INSTITUTIONAL_HOLDINGS&symbol=IBM&apikey=demo

--------------------------------------------------------------------------------
1. FUNDAMENTAL DATA
--------------------------------------------------------------------------------
OVERVIEW              ?function=OVERVIEW&symbol=IBM&apikey=demo               [equities only, not ETFs]
ETF_PROFILE           ?function=ETF_PROFILE&symbol=QQQ&apikey=demo            [ETFs only]
INCOME_STATEMENT      ?function=INCOME_STATEMENT&symbol=IBM&apikey=demo       [equities only, not ETFs]
BALANCE_SHEET         ?function=BALANCE_SHEET&symbol=IBM&apikey=demo          [equities only, not ETFs]
CASH_FLOW             ?function=CASH_FLOW&symbol=IBM&apikey=demo              [equities only, not ETFs]
SHARES_OUTSTANDING    ?function=SHARES_OUTSTANDING&symbol=IBM&apikey=demo     [equities only, not ETFs]
EARNINGS              ?function=EARNINGS&symbol=IBM&apikey=demo               [equities only, not ETFs]
EARNINGS_ESTIMATES    ?function=EARNINGS_ESTIMATES&symbol=IBM&apikey=demo     [equities only, not ETFs]
LISTING_STATUS        ?function=LISTING_STATUS&apikey=demo
                        Optional: date (YYYY-MM-DD), state (active*|delisted)
EARNINGS_CALENDAR     ?function=EARNINGS_CALENDAR&symbol=IBM&apikey=demo
                        Optional: horizon (3month*|6month|12month)

--------------------------------------------------------------------------------
4. FOREX (FX)
--------------------------------------------------------------------------------
CURRENCY_EXCHANGE_RATE
  Required: function, from_currency, to_currency, apikey
  Example:  ?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency=JPY&apikey=demo

FX_INTRADAY           [PREMIUM]
  Required: function, from_symbol, to_symbol, interval, apikey
  Optional: outputsize, datatype
  Example:  ?function=FX_INTRADAY&from_symbol=EUR&to_symbol=USD&interval=1min&apikey=demo

FX_DAILY
  Required: function, from_symbol, to_symbol, apikey
  Optional: outputsize, datatype
  Example:  ?function=FX_DAILY&from_symbol=EUR&to_symbol=USD&apikey=demo

--------------------------------------------------------------------------------
5. CRYPTOCURRENCIES
--------------------------------------------------------------------------------
CURRENCY_EXCHANGE_RATE (crypto)
  Example:  ?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=USD&apikey=demo

DIGITAL_CURRENCY_DAILY
  Required: function, symbol, market, apikey
  Example:  ?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey=demo

--------------------------------------------------------------------------------
6. COMMODITIES
--------------------------------------------------------------------------------
  All: Required: function, apikey  |  Optional: interval, datatype

GOLD_SILVER_SPOT      ?function=GOLD_SILVER_SPOT&symbol=GOLD&apikey=demo
                        Required: symbol (GOLD|XAU|SILVER|XAG)
                        No interval param (live spot price)
GOLD_SILVER_HISTORY   ?function=GOLD_SILVER_HISTORY&symbol=GOLD&interval=daily&apikey=demo
                        Required: symbol (GOLD|XAU|SILVER|XAG), interval (daily*|weekly|monthly)
WTI                   ?function=WTI&interval=daily&apikey=demo
                        interval: daily*|weekly|monthly
BRENT                 ?function=BRENT&interval=daily&apikey=demo
                        interval: daily*|weekly|monthly
NATURAL_GAS           ?function=NATURAL_GAS&interval=daily&apikey=demo
                        interval: daily*|weekly|monthly
COPPER                ?function=COPPER&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
ALUMINUM              ?function=ALUMINUM&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
WHEAT                 ?function=WHEAT&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
CORN                  ?function=CORN&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
COTTON                ?function=COTTON&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
SUGAR                 ?function=SUGAR&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
COFFEE                ?function=COFFEE&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)
ALL_COMMODITIES       ?function=ALL_COMMODITIES&interval=monthly&apikey=demo
                        interval: monthly*|quarterly|annual  (NO daily)

--------------------------------------------------------------------------------
7. INDEX DATA
--------------------------------------------------------------------------------
INDEX_CATALOG  (list all available index symbols)
  Required: function, apikey
  Returns:  JSON mapping of ticker symbols to index names (~400+ indices)
  Coverage: Dow Jones, S&P 500, NASDAQ, Russell, Cboe VIX, sector indices,
            VIX futures, options-based strategy indices
  Example:  ?function=INDEX_CATALOG&apikey=demo

INDEX_DATA  (historical OHLC for market indices)              [PREMIUM]
  Required: function, symbol, interval, apikey
  symbol:   Index ticker from INDEX_CATALOG (e.g. DJI, SPX, COMP, NDX, VIX)
  interval: daily | weekly | monthly
  Optional: datatype (json*|csv)
  Returns:  Decades of historical open, high, low, close (OHLC) time series
  Example:  ?function=INDEX_DATA&symbol=SPX&interval=daily&apikey=demo

--------------------------------------------------------------------------------
8. ECONOMIC INDICATORS
--------------------------------------------------------------------------------
  All: Required: function, apikey  |  Optional: interval, datatype

REAL_GDP              interval: annual*|quarterly
REAL_GDP_PER_CAPITA   interval: annual*
TREASURY_YIELD        interval: daily|weekly*|monthly  |  maturity: 3month|2year|5year|7year|10year*|30year
FEDERAL_FUNDS_RATE    interval: daily|weekly*|monthly
CPI                   interval: monthly*|semiannual
INFLATION             interval: annual*
RETAIL_SALES          interval: monthly*
DURABLES              interval: monthly*
UNEMPLOYMENT          interval: monthly*
NONFARM_PAYROLL       interval: monthly*

Examples:
  ?function=REAL_GDP&interval=annual&apikey=demo
  ?function=TREASURY_YIELD&interval=weekly&maturity=10year&apikey=demo
  ?function=CPI&interval=monthly&apikey=demo

================================================================================
NOTES
- * = default value
- outputsize=compact → latest 100 points (free); outputsize=full → 20+ years (premium)
- entitlement=realtime → live data (premium); entitlement=delayed → 15-min delay (premium)
- Rate limits: free = 25 req/day; premium = 75–1200 req/min depending on plan
- Docs: https://www.alphavantage.co/documentation/
================================================================================