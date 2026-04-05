================================================================================
  FIRSTRATEDATA - COMPLETE STOCKS & ETFs BUNDLE - FORMAT REFERENCE
  Source: https://firstratedata.com
================================================================================

--------------------------------------------------------------------------------
COVERAGE
--------------------------------------------------------------------------------
- US Stocks : 16,245 most liquid (Russell 3000, S&P 500, Nasdaq 100, DJI)
- US ETFs   : 5,150 most liquid
- Includes  : 7,000+ delisted tickers
- Updates   : daily (files available by 3am ET next trading day)
              new tickers added end of every week

--------------------------------------------------------------------------------
TIMEFRAMES AVAILABLE
--------------------------------------------------------------------------------
  1min | 5min | 30min | 1hour | 1day

  NOTE: Zero-volume bars are omitted — gaps in sequence = no trades that period.

--------------------------------------------------------------------------------
BAR FORMAT
--------------------------------------------------------------------------------
  Columns (CSV): DateTime, Open, High, Low, Close, Volume

  DateTime  : yyyy-MM-dd HH:mm:ss
  OHLC      : price values (float)
  Volume    : integer, in individual shares (not lots)
  Timezone  : US Eastern Time (ET)
  Timestamp : marks START of bar period
                e.g. 1min bar at 09:30 covers 09:30:00 → 09:30:59

  Example row:
    2024-03-15 09:30:00,185.20,185.75,185.10,185.60,1234567

--------------------------------------------------------------------------------
ADJUSTMENT
--------------------------------------------------------------------------------
  The bundle contains ONLY split+dividend-adjusted prices.
  No unadjusted or split-only variants are included.

  All OHLC values are adjusted for both splits and dividends.
  There are NO separate Dividends or Splits columns in the data.
  To get per-event dividend and split values, an external source
  (e.g. Alpha Vantage TIME_SERIES_DAILY_ADJUSTED) is required.

  Adjustment details: https://firstratedata.com/about/price_adjustment

--------------------------------------------------------------------------------
TICKER LIST FORMAT  (supplied separately)
--------------------------------------------------------------------------------
  One ticker per line in the format:
    SYMBOL (Company Full Name) Start Date:YYYY-MM-DD

  Fields:
    SYMBOL      : exchange ticker string (e.g. AAPL, SPY)
    Name        : full company/fund name in parentheses (may be empty)
    Start Date  : earliest available data date for that ticker (YYYY-MM-DD)

  Tickers are grouped into two sections:
    "Stock Tickers"  →  equities
    "ETF Tickers"    →  exchange-traded funds

--------------------------------------------------------------------------------
PRACTICAL NOTES
--------------------------------------------------------------------------------
- Large files: do NOT open directly in Excel — use a text editor or chunking
- License: https://firstratedata.com/about/license
- Market hours: regular session 09:30–16:00 ET
                pre/post-market data included where traded

================================================================================
