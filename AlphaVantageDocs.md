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
  Example:  ?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey=demo

TIME_SERIES_DAILY_ADJUSTED    [PREMIUM]
  Required: function, symbol, apikey
  Optional: outputsize, datatype, entitlement
  Example:  ?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=demo

SYMBOL_SEARCH  (ticker lookup / autocomplete)
  Required: function, keywords, apikey
  Optional: datatype
  Example:  ?function=SYMBOL_SEARCH&keywords=microsoft&apikey=demo

MARKET_STATUS  (global market open/close)
  Required: function, apikey
  Example:  ?function=MARKET_STATUS&apikey=demo

Global exchange suffixes: .LON .TRT .TRV .DEX .BSE .SHH .SHZ

--------------------------------------------------------------------------------
1. ALPHA INTELLIGENCE
--------------------------------------------------------------------------------
NEWS_SENTIMENT
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

EARNINGS_CALL_TRANSCRIPT
  Required: function, symbol, quarter (YYYYqM e.g. 2024Q1, min=2010Q1), apikey
  Example:  ?function=EARNINGS_CALL_TRANSCRIPT&symbol=IBM&quarter=2024Q1&apikey=demo

INSIDER_TRANSACTIONS
  Required: function, symbol, apikey
  Example:  ?function=INSIDER_TRANSACTIONS&symbol=IBM&apikey=demo

INSTITUTIONAL_HOLDINGS        [PREMIUM]
  Required: function, symbol, apikey
  Example:  ?function=INSTITUTIONAL_HOLDINGS&symbol=IBM&apikey=demo

--------------------------------------------------------------------------------
1. FUNDAMENTAL DATA
--------------------------------------------------------------------------------
OVERVIEW              ?function=OVERVIEW&symbol=IBM&apikey=demo
ETF_PROFILE           ?function=ETF_PROFILE&symbol=QQQ&apikey=demo
INCOME_STATEMENT      ?function=INCOME_STATEMENT&symbol=IBM&apikey=demo
BALANCE_SHEET         ?function=BALANCE_SHEET&symbol=IBM&apikey=demo
CASH_FLOW             ?function=CASH_FLOW&symbol=IBM&apikey=demo
SHARES_OUTSTANDING    ?function=SHARES_OUTSTANDING&symbol=IBM&apikey=demo
EARNINGS              ?function=EARNINGS&symbol=IBM&apikey=demo
EARNINGS_ESTIMATES    ?function=EARNINGS_ESTIMATES&symbol=IBM&apikey=demo
LISTING_STATUS        ?function=LISTING_STATUS&apikey=demo
                        Optional: date (YYYY-MM-DD), state (active*|delisted)
EARNINGS_CALENDAR     ?function=EARNINGS_CALENDAR&symbol=IBM&apikey=demo
                        Optional: horizon (3month*|6month|12month)

--------------------------------------------------------------------------------
1. FOREX (FX)
--------------------------------------------------------------------------------
CURRENCY_EXCHANGE_RATE
  Required: function, from_currency, to_currency, apikey
  Example:  ?function=CURRENCY_EXCHANGE_RATE&from_currency=USD&to_currency=JPY&apikey=demo

FX_INTRADAY           [PREMIUM]
  Required: function, from_symbol, to_symbol, interval, apikey
  Optional: outputsize, datatype
  Example:  ?function=FX_INTRADAY&from_symbol=EUR&to_symbol=USD&interval=5min&apikey=demo

FX_DAILY
  Required: function, from_symbol, to_symbol, apikey
  Optional: outputsize, datatype
  Example:  ?function=FX_DAILY&from_symbol=EUR&to_symbol=USD&apikey=demo

--------------------------------------------------------------------------------
1. CRYPTOCURRENCIES
--------------------------------------------------------------------------------
CURRENCY_EXCHANGE_RATE (crypto)
  Example:  ?function=CURRENCY_EXCHANGE_RATE&from_currency=BTC&to_currency=USD&apikey=demo

DIGITAL_CURRENCY_DAILY
  Required: function, symbol, market, apikey
  Example:  ?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=USD&apikey=demo

--------------------------------------------------------------------------------
1. COMMODITIES
--------------------------------------------------------------------------------
  All: Required: function, apikey  |  Optional: interval (monthly*|weekly|daily|quarterly|annual), datatype

GOLD_SPOT             ?function=GOLD_SPOT&apikey=demo      (realtime spot)
SILVER_SPOT           ?function=SILVER_SPOT&apikey=demo
GOLD_HISTORY          ?function=GOLD_HISTORY&apikey=demo
SILVER_HISTORY        ?function=SILVER_HISTORY&apikey=demo
WTI                   ?function=WTI&interval=monthly&apikey=demo
BRENT                 ?function=BRENT&interval=monthly&apikey=demo
NATURAL_GAS           ?function=NATURAL_GAS&interval=monthly&apikey=demo
COPPER                ?function=COPPER&interval=monthly&apikey=demo
ALUMINUM              ?function=ALUMINUM&interval=monthly&apikey=demo
WHEAT                 ?function=WHEAT&interval=monthly&apikey=demo
CORN                  ?function=CORN&interval=monthly&apikey=demo
COTTON                ?function=COTTON&interval=monthly&apikey=demo
SUGAR                 ?function=SUGAR&interval=monthly&apikey=demo
COFFEE                ?function=COFFEE&interval=monthly&apikey=demo
ALL_COMMODITIES       ?function=ALL_COMMODITIES&interval=monthly&apikey=demo

--------------------------------------------------------------------------------
8. INDEX DATA
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
  Example:  ?function=INDEX_DATA&symbol=SPX&interval=daily&apikey=YOUR_KEY

--------------------------------------------------------------------------------
9. ECONOMIC INDICATORS
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