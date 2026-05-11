# Algo Trading Database

A bias-aware market data infrastructure for quantitative strategy research, backtesting, and live trading. Built on Alpha Vantage as the sole required data provider — for both historical setup and ongoing daily updates — with a homegrown point-in-time snapshot pipeline. Optionally enhanced with FirstRate Data (survivorship bias-free prices for 16k+ tickers including delisted securities) to fill gaps that Alpha Vantage alone cannot cover.

> Looking for the implementation details, folder layout, or setup steps? See [SPEC.md](SPEC.md).

## Why this project exists

Building a reliable algo trading database is harder than it looks. After evaluating 14 data providers — Alpha Vantage, Norgate Data, CRSP, Compustat, Kibot, Polygon.io, Databento, Tiingo, EODHD, Finnhub, Financial Modeling Prep, Nasdaq Data Link (Quandl), FirstRate Data, and QuantConnect — we found that no single affordable provider solves all the problems a serious quant needs solved.

The critical issues:

- **Survivorship bias in prices.** Most providers only include currently-listed stocks. Backtests on this data are overly optimistic because they exclude companies that went bankrupt, were acquired, or delisted.
- **Look-ahead bias in fundamentals.** Every retail-accessible fundamental data provider (Alpha Vantage, FMP, Finnhub, Tiingo, EODHD) serves the *latest* version of financial statements, silently overwriting restated values. Your backtest uses corrected numbers that weren't available at the time.
- **Point-in-time (PIT) fundamentals are institutional-only.** True PIT data — where original and restated values are preserved with timestamps — is only available from Compustat PIT ($5k–25k+/yr via WRDS), LSEG/Refinitiv PIT, or S&P Capital IQ Premium. None are accessible to independent researchers at retail pricing.

This project addresses these gaps with a practical, layered approach:

1. **Alpha Vantage** as the sole required data provider — used for both the initial historical data setup (downloading full price and fundamental history) and all ongoing daily updates (prices, fundamentals, alternative data), with a custom daily snapshot pipeline that builds a homegrown PIT layer over time.
2. **FirstRate Data** (optional) to enhance the Alpha Vantage historical data — adds survivorship bias-free prices (intraday + daily, including 7,000+ delisted tickers back to 2000) that Alpha Vantage does not cover. One-time purchase; no ongoing subscription.

The historical setup and the daily raw data pipeline are independent concerns. You can build a fully functional historical database from Alpha Vantage alone, and separately configure the daily pipeline. FirstRate Data, if purchased, is layered on top of the Alpha Vantage historical data to add delisted securities and intraday granularity.

## The point-in-time problem (and our solution)

### The problem

Every affordable fundamental data provider serves "latest available" financials. When a company restates its 2022 Q3 earnings in 2024, the data provider replaces the old Q3 2022 values with the corrected ones. Your backtest in 2022 then uses numbers that didn't exist until 2024.

Beyond restatements, there is a more fundamental issue: **financial data is never available on the fiscal period end date.** A company's fiscal quarter may end on December 31, but the 10-Q isn't filed with the SEC until weeks or months later — often mid-February or later. Without PIT awareness, a backtest can use December earnings data in a January trading signal, even though no market participant could have known those numbers yet. This reporting-lag bias means any backtest on fundamentals that doesn't account for actual data availability dates is effectively using future information to make past decisions.

Research by S&P Global shows this can move a company from the 5th percentile to the 88th percentile in ROE ranking — completely inverting a factor signal.

True PIT databases (Compustat PIT, LSEG/Refinitiv PIT, S&P Capital IQ Premium) cost $5k–25k+/year with institutional contracts.

### Our solution: build it ourselves

We run two daily snapshot pipelines:

**Fundamentals PIT pipeline:**
1. Pulls fundamental data from Alpha Vantage for every ticker in our universe.
2. Stores each API return. Clearly indicates the `observed_date`.
3. Never overwrites previous values.

After several years of collection, this produces a genuine PIT dataset for the covered period. The pipeline becomes productive once roughly 3 months of `daily/` snapshots have accumulated; before that the PIT layer is too sparse to use directly. See [data_transformation/SPEC.md](data_transformation/SPEC.md#lookahead-bias-on-the-historical-period).

## Data sources and rationale

### FirstRate Data (optional historical enhancement — one-time load)

**Why FirstRate Data:** It is the best combination of survivorship bias-free data, intraday granularity, data quality, and pricing to supplement the Alpha Vantage historical data. Key features:

- 16,245 stock tickers including 7,000+ delisted securities with full price history back to Jan 2000
- Covers all current and former S&P 500, NASDAQ 100, DJIA, and Russell 3000 members
- 1-minute, 5-minute, 30-minute, 1-hour, and daily bars
- Tick data available (10 years)
- Daily bars in three variants (unadjusted, split-adjusted, split+dividend-adjusted); 1-minute bars are unadjusted
- Out-of-hours (pre/post market) trades included
- Data sourced directly from major exchanges and 4 dark pools
- 5,150+ ETFs, 130 futures, 115 US indices, 110 international indices, 70 FX crosses, 50 crypto
- Used by NBER, Boston/Chicago Federal Reserve, Cambridge, NYU, Stanford
- Dedicated QA team since 2023; daily screening for gaps, duplicates, anomalies
- One-time purchase model — you own the data files permanently

**Our usage:** FirstRate Data is an optional one-time purchase that supplements the Alpha Vantage historical data. It adds survivorship bias-free prices for 7,000+ delisted securities and intraday bars that Alpha Vantage does not provide. After ingestion, we do not maintain a FirstRate subscription — Alpha Vantage handles all ongoing daily updates.

**Why not Norgate Data (the other survivorship-free option):**
Norgate also offers survivorship bias-free data with excellent historical index constituents, but requires Windows (or a Windows VM) for the NDU updater application, provides end-of-day data only (no intraday), and locks you into a subscription where data access stops if you lapse. FirstRate offers deeper intraday history, runs on any OS via flat CSV files, and uses a one-time purchase model.

### Alpha Vantage (sole required provider — historical setup + ongoing daily data)

**Why Alpha Vantage:** Broad coverage across prices, fundamentals, and alternative data at accessible pricing. NASDAQ-licensed for commercial use. Alpha Vantage is the only required data provider — it powers both the initial historical data setup (full price and fundamental history for all active tickers) and all ongoing daily updates. It provides pre- and post-market daily data.

**Data we pull:**

| Category | Endpoints | Update frequency |
|---|---|---|
| Intraday prices | `TIME_SERIES_INTRADAY` | Daily (active universe only) |
| Daily prices | `TIME_SERIES_DAILY_ADJUSTED` | Daily (active universe only) |
| Fundamentals | `INCOME_STATEMENT`, `BALANCE_SHEET`, `CASH_FLOW`, `EARNINGS`, `EARNINGS_ESTIMATES` | Daily snapshot (PIT pipeline) |
| Insider transactions | `INSIDER_TRANSACTIONS` | Daily |
| Market News & Sentiment | `NEWS_SENTIMENT` | Daily |
| Indices | `INDEX_DATA` — direct index prices (S&P 500, DJIA, VIX, etc.). Universe discovered via `INDEX_CATALOG`. | Daily |
| ETF profiles | `ETF_PROFILE` — net assets, holdings, expense ratio. Used to filter out low-net-asset ETFs. No historical PIT; latest snapshot only. | Daily |
| Commodities | `WTI`, `BRENT`, `NATURAL_GAS`, gold, silver, copper | Daily |
| Economic indicators | `REAL_GDP`, `CPI`, `UNEMPLOYMENT`, `FEDERAL_FUNDS_RATE`, treasury yields | Per release schedule |

### Providers we evaluated but did not select

| Provider | Reason not selected |
|---|---|
| **Norgate Data** | Excellent survivorship-free EOD and historical index constituents. But Windows-only (NDU updater), no intraday data, subscription-only (data inaccessible if lapsed), fundamentals are not PIT. FirstRate Data provides better intraday coverage on any OS. |
| **CRSP** | Gold standard for academic research, survivorship-free back to 1925. But ~$5k–25k+/yr institutional pricing, requires WRDS access. Not accessible to independents. |
| **Compustat** | Best fundamental database (99k securities, back to 1950). Offers true PIT from 1987. But same institutional pricing/access barrier as CRSP. |
| **Kibot** | Deep intraday history (28+ years, one-time ~$990). But mixed data quality reviews, no survivorship bias handling, no fundamentals, 8–12 hour update delay. FirstRate Data is higher quality with delisted tickers included. |
| **Polygon.io** | Excellent real-time/low-latency data. But $29–$499/mo subscription, no survivorship bias handling. Better suited as a live-trading feed, not a database backbone. |
| **Databento** | Institutional-grade tick data with nanosecond timestamps. But $199/mo+ for live, no fundamentals. Overkill for non-HFT strategies. |
| **Tiingo** | Great budget option ($10/mo), clean data. But no survivorship bias handling, limited fundamentals. |
| **EODHD** | Best value for global coverage ($19.99–$79.99/mo). But no PIT fundamentals, only partial survivorship handling. |
| **Finnhub** | Best free tier (60 calls/min), unique alternative data (congressional trades, lobbying). But shallow historical depth, no survivorship bias, unreliable WebSocket news, poor support. |
| **FMP** | Comprehensive fundamentals from SEC EDGAR. Has "as-reported" endpoints. But not true PIT. Survivorship-free EOD exists but is secondary to their fundamentals focus. |
| **QuantConnect** | All-in-one platform. But tick timestamps capped at milliseconds, fundamental survivorship bias gaps for delisted stocks, you don't own the data. |

## Key design decisions

1. **Alpha Vantage as the sole required provider.** Alpha Vantage handles both the initial historical data setup and all ongoing daily updates. The system is fully functional with Alpha Vantage alone.
2. **Historical setup is independent of the daily pipeline.** You can set up history without configuring daily pulls, and vice versa.
3. **FirstRate Data as an optional historical enhancement.** If purchased, it adds survivorship bias-free prices (7,000+ delisted tickers, intraday bars back to 2000). No ongoing subscription needed.
4. **FirstRate Data over Norgate Data.** Both offer survivorship bias-free prices. FirstRate wins on: any OS (no Windows lock-in), intraday data (Norgate is EOD-only), deeper intraday history. Norgate wins on: historical index constituents and history depth (back to 1950 vs 2000).
5. **Homegrown PIT over paying for institutional data.** True PIT databases cost $5k–25k+/year. Our daily snapshot approach builds PIT organically. The trade-off is a cold-start period of several years.
6. **Append-only storage for raw data.** Fundamental data is never updated in place. New values create new rows. This is the foundation of the PIT layer.
7. **GCP container for fetching, local for processing.** Daily ingestion runs in a GCP Cloud container that writes to a single append-only GCS bucket. A local sync script mirrors the bucket contents for further processing.
8. **Raw data archived immutably in GCS.** Every API response is processed once into parquet and never modified. Schema violations are logged before data enters the pipeline.
9. **Yield-aware API call management.** The asset catalog tracks which tickers return data for each Alpha Vantage endpoint. Tickers with no data are skipped daily and re-checked weekly. Avoids wasting API calls on tickers where Alpha Vantage has no coverage.

## Estimated costs

| Item | Cost | Notes |
|---|---|---|
| Alpha Vantage (paid plan) | ~$600/yr | Sole required provider — historical setup + ongoing daily data. |
| GCP Cloud Storage | ~$20/yr | Parquet files (historical + daily) |
| GCP Cloud Run | ~$5–20/yr | Daily ingestion container (low usage, mostly free tier) |
| FirstRate Data (optional) | ~$300–400 one-time | Adds 16k+ tickers (7k+ delisted), 26 years of 1-min data. |
| **Total year 1 (AV only)** | **~$625–640** | No FirstRate purchase needed |
| **Total year 1 (with FirstRate)** | **~$925–1,040** | Includes optional one-time FirstRate purchase |
| **Total year 2+** | **~$625–640/yr** | Recurring only |

## Future considerations

- **Add Norgate Data** for historical index constituent data beyond the S&P 500 (Russell 3000, NASDAQ 100, etc.).
- **Add Finnhub** for congressional trading data, insider sentiment, and ESG scores as supplementary alternative data.
- **Add Polygon.io or Databento** if the project evolves toward live trading or HFT requiring real-time streaming or order book depth.
- **EDGAR XBRL ingestion** as a direct SEC filing pipeline to cross-validate Alpha Vantage fundamentals and capture restatements at the source.
- **Consistency tests** that validate raw and transformed data against independent sources.
- **If institutional access becomes available**, integrate CRSP/Compustat via WRDS to replace the homegrown PIT layer.
- **Schedule local sync** of the GCS bucket before market open.
- **GCS lifecycle rules** to move raw data older than 1 year to Nearline/Coldline storage.

## Getting started

See [SPEC.md](SPEC.md) for prerequisites, setup steps, folder structure, and the full implementation specification.

## License

This project is for personal research use. Data from FirstRate Data and Alpha Vantage is subject to their respective terms of service and cannot be redistributed. GCP resources are billed to your own GCP account.

## References

- FirstRate Data: [firstratedata.com](https://firstratedata.com)
- Alpha Vantage: [alphavantage.co](https://www.alphavantage.co)
