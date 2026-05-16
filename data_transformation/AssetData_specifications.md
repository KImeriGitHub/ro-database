## StockData specifications

## Conventions

These apply to every `AssetData` instance and every frame below. They
are part of the contract; downstream code in the next repo should not
re-derive them.

### Currency

**All amounts are denominated in USD.** Prices, dividends,
fundamentals (income statement / balance sheet / cash flow), sentiment
inputs, etf net assets, commodity values, and economic indicator
values are USD only. Forex pairs are catalogued as `XXXUSD` (USD as
the quote currency); `USDUSD` is excluded. Multi-currency assets are
out of scope and would need a follow-up phase.

### Timezone

The schema does **not** carry timezone information per column. The
following conventions hold instead:

| Frame | Time column | dtype | Wall-clock convention |
|---|---|---|---|
| `shareprice_daily`, `price_daily` (stocks, etfs, indices, commodities, economic) | `Date` | `pl.Date` | US/Eastern trading day |
| `price_daily` (forex, cryptocurrencies) | `Date` | `pl.Date` | UTC trading day |
| `shareprice_intraday` | `Datetime` | `pl.Datetime` (tz-naive) | US/Eastern wall-clock; tz stripped at transformation |
| `sentiment_df` | `Datetime` | `pl.Datetime` (tz-naive) | UTC wall-clock |
| `etf_profile`, `insider_df` | `Date` | `pl.Date` | calendar date as reported (no intraday context) |
| `financials_*` | `Date` | `pl.Date` | row axis = `shareprice_daily.Date`; same convention as that frame |

Source-side, Alpha Vantage's stocks / etfs intraday + daily endpoints
are validated against `Time Zone == "US/Eastern"`, and forex / crypto
against `Time Zone == "UTC"` (a mismatch logs `timezone_mismatch` in
the ingest report). Indices / commodities / economic responses do not
expose a `Time Zone` field and are not validated; they are treated as
US/Eastern by convention. Consumers that need true UTC for
`shareprice_intraday` must localise the tz-naive `Datetime` to
`US/Eastern` themselves before converting.

### Listing / delisting

- **Stocks and ETFs support delisted symbols.** A delisted ticker
  remains in the catalog and produces an `AssetData` instance, but
  `shareprice_daily.Date` ends at (or near) the delisting date. As a
  rule of thumb a symbol with `max(shareprice_daily.Date) < today` is
  delisted; rare exceptions are stale data feeds. There is no
  dedicated `delisted` boolean — consumers infer it from the date
  axis.
- **Forex, indices, cryptocurrencies, commodities, economic do not
  carry listing / delisting metadata.** A stale `price_daily` series
  for these asset types typically indicates an API gap rather than a
  delisting, and the date-based heuristic above does not apply.

### Known asset cadence (sub-daily symbols on the daily axis)

Several `commodities` and `economic` symbols are exposed through the
daily-shaped `price_daily` frame but their **source cadence is
sub-daily** (monthly, quarterly, or annual). The frame's
`Date : pl.Date` still holds, but the series contains far fewer rows
than a daily series and the `Date` values are the AV-published
period-end dates (typically a month start). Consumers must not assume
one row per trading day for these symbols.

#### commodities

| Symbol | Source cadence | AV function |
|---|---|---|
| `WTI` | daily | `WTI` |
| `BRENT` | daily | `BRENT` |
| `NATURAL_GAS` | daily | `NATURAL_GAS` |
| `XAU` | daily | `GOLD_SILVER_HISTORY` (symbol=XAU) |
| `XAG` | daily | `GOLD_SILVER_HISTORY` (symbol=XAG) |
| `COPPER` | monthly | `COPPER` |
| `ALUMINUM` | monthly | `ALUMINUM` |
| `WHEAT` | monthly | `WHEAT` |
| `CORN` | monthly | `CORN` |
| `COTTON` | monthly | `COTTON` |
| `SUGAR` | monthly | `SUGAR` |
| `COFFEE` | monthly | `COFFEE` |
| `ALL_COMMODITIES` | monthly | `ALL_COMMODITIES` |

#### economic

| Symbol | Source cadence | AV function |
|---|---|---|
| `TREASURY_YIELD_30Y` | daily | `TREASURY_YIELD` (maturity=30year) |
| `TREASURY_YIELD_10Y` | daily | `TREASURY_YIELD` (maturity=10year) |
| `TREASURY_YIELD_7Y`  | daily | `TREASURY_YIELD` (maturity=7year) |
| `TREASURY_YIELD_5Y`  | daily | `TREASURY_YIELD` (maturity=5year) |
| `TREASURY_YIELD_2Y`  | daily | `TREASURY_YIELD` (maturity=2year) |
| `TREASURY_YIELD_3M`  | daily | `TREASURY_YIELD` (maturity=3month) |
| `FEDERAL_FUNDS_RATE` | daily | `FEDERAL_FUNDS_RATE` |
| `CPI` | monthly | `CPI` |
| `RETAIL_SALES` | monthly | `RETAIL_SALES` |
| `DURABLES` | monthly | `DURABLES` |
| `UNEMPLOYMENT` | monthly | `UNEMPLOYMENT` |
| `NONFARM_PAYROLL` | monthly | `NONFARM_PAYROLL` |
| `REAL_GDP` | quarterly | `REAL_GDP` |
| `REAL_GDP_PER_CAPITA` | quarterly | `REAL_GDP_PER_CAPITA` |
| `INFLATION` | annual | `INFLATION` |

### Scalars

ticker: str
The symbol associated with the asset.

about: str
The name of the symbol. And additional information.

sector: int
Index of the list given by 
CANONICAL_SECTORS: Basic Materials, Communication Services, Consumer Cyclical, 
Consumer Defensive, Energy, Financial Services, Healthcare, Industrials, 
Real Estate, Technology, Utilities, Other

### shareprice_daily: pl.DataFrame
Columns 
 'Date'              : pl.Date
 'Open'              : pl.Float32
 'High'              : pl.Float32
 'Low'               : pl.Float32
 'Close'             : pl.Float32
 'Volume'            : pl.Float32
 'DividendAmount'    : pl.Float32
 'SplitCoefficient'  : pl.Float32
 'AdjFactor'         : pl.Float32

`AdjFactor` is a **single-day** multiplier (no cumulative product).
For row `i >= 1`,
`Close[i] * AdjFactor[i] / Close[i-1] - 1` is the gross total return
across the `i-1 -> i` transition (splits and dividends absorbed). On
days with no split and no dividend, `AdjFactor[i] = 1.0`. The
formula is documented in `AssetData_design_choices.md` section 6;
cumulative use (e.g. an `AdjClose` series) is the consumer's
responsibility.

`AdjFactor[0] = 1.0` by convention (no preceding `Close` to anchor
against).

OHLCV columns are the **raw, unadjusted** values from the source.

### shareprice_intraday: pl.DataFrame
Columns 
 'Datetime'  : pl.Datetime
 'Open'      : pl.Float32
 'High'      : pl.Float32
 'Low'       : pl.Float32
 'Close'     : pl.Float32
 'Volume'    : pl.Float32

OHLCV are the **raw, unadjusted** intraday bars. There is no
intraday-side adjustment column; consumers that need adjusted
intraday returns join `shareprice_daily.AdjFactor` on the calendar
date of `Datetime`.

### price_daily: pl.DataFrame
Columns 
 'Date'         : pl.Date
 'Open'         : pl.Float32
 'High'         : pl.Float32
 'Low'          : pl.Float32
 'Close'        : pl.Float32
 'Volume'       : pl.Float32

### insider_df: pl.DataFrame
Columns 
 'Date'              : pl.Date
 'Executive_role'    : pl.Categorical  (label from CANONICAL_INSIDER_ROLES; see below)
 'AcqDis'            : pl.Categorical  ('A' for acquisition, 'D' for disposal)
 'Shares'            : pl.Float32
 '_executive'        : pl.Utf8         (raw composite-key component; build-side scaffolding)
 '_security_type'    : pl.Utf8         (raw composite-key component; build-side scaffolding)

`Date` is the raw `transactionDate` from INSIDER_TRANSACTIONS; the frame
is a chronological list of transactions (not a per-trading-date snapshot).
Avoiding lookahead leakage when consuming this frame is the responsibility
of the feature-generation step.

The underscore-prefixed columns (`_executive`, `_security_type`) carry the
raw composite-key components that drove the dedup. They are not part of the
modelling surface (downstream features should key on
`(Date, Executive_role, AcqDis)`); they exist so the incremental build path
in `data_transformation/transform.py` can dedup new daily rows against the
saved frame without re-reading every historical / daily source file. Treat
them as internal scaffolding.

CANONICAL_INSIDER_ROLES: CAO, General Counsel, CFO, COO, CTO_CIO,
VP, CEO, Other C-Suite, Chairman, Director, 10% Owner, Officer, Other

`Executive_role` is derived from the raw `executive_title` string returned by
INSIDER_TRANSACTIONS. Mapping is by case-insensitive regex match against an
ordered rule list; the first rule that matches wins. A title that matches no
rule (including empty / null) falls through to `Other`.

The rule order IS the spec. It is arranged most-specific first so compound
titles route correctly. Worth noting:

 - VP precedes CEO so "Vice President" hits VP rather than CEO's `president`
   pattern.
 - CEO precedes the catch-all `chief ` (Other C-Suite) so "Chief Executive
   Officer" hits CEO rather than the generic chief rule.
 - Bare acronyms (`cfo`, `coo`, `cto`, `cio`, `vp`, `ceo`) use regex word
   boundaries (`\b...\b`) so they do not match inside unrelated words
   (e.g. "Director" contains the substring "cto" but `\bcto\b` does not match).

Match rules (index : label : regex pattern, applied to lowercased title):
 0  CAO             : `chief accounting|controller|principal accounting`
 1  General Counsel : `general counsel|chief legal|secretary`
 2  CFO             : `\bcfo\b|chief financial|treasurer|principal financial`
 3  COO             : `\bcoo\b|chief operating`
 4  CTO_CIO         : `\bcto\b|\bcio\b|chief technology|chief information|chief digital`
 5  VP              : `\bvp\b|vice president|executive vice|senior vice`
 6  CEO             : `\bceo\b|chief executive|president`
 7  Other C-Suite   : `chief `   (catch-all for CMO, CHRO, CRO, CSO, ...)
 8  Chairman        : `chairman|chair of`
 9  Director        : `director`
 10 10% Owner       : `10%|beneficial owner`
 11 Officer         : `officer`
 12 Other           : (no pattern; default fallthrough, also catches empty/null)

### sentiment_df: pl.DataFrame
 'Datetime'                   : pl.Datetime
 'source'                     : pl.Categorical  News-source label (Reuters, Bloomberg, ...) from NEWS_SENTIMENT.
 'ticker_relevance_score'     : pl.Float32
 'ticker_sentiment_score'     : pl.Float32
 'overall_sentiment_score'    : pl.Float32
 'blockchain'                 : pl.Float32     Topic relevance score (null if topic absent)
 'earnings'                   : pl.Float32
 'ipo'                        : pl.Float32
 'mergers_and_acquisitions'   : pl.Float32
 'financial_markets'          : pl.Float32
 'economy_fiscal'             : pl.Float32
 'economy_monetary'           : pl.Float32
 'economy_macro'              : pl.Float32
 'energy_transportation'      : pl.Float32
 'finance'                    : pl.Float32
 'life_sciences'              : pl.Float32
 'manufacturing'              : pl.Float32
 'real_estate'                : pl.Float32
 'retail_wholesale'           : pl.Float32
 'technology'                 : pl.Float32
 '_url'                       : pl.Utf8         Raw article url; build-side scaffolding for the incremental dedup.

`source` is the upstream news-source label (`Reuters`, `Bloomberg`, ...).
The label space is small and bounded, hence Categorical.

`_url` is the raw article url renamed with an underscore prefix; it carries
the second component of the `(Datetime, url)` dedup key so the incremental
build path can dedup new daily rows against the saved frame without
re-reading every historical / daily sentiment file. Like the
`_executive` / `_security_type` columns on `insider_df`, this is internal
scaffolding rather than a modelling feature.

### etf_profile: pl.DataFrame
 'Date'                       : pl.Date
 'information_technology'     : pl.Float32           sector weight 
 'communication_services'     : pl.Float32 
 'consumer_discretionary'     : pl.Float32 
 'consumer_staples'           : pl.Float32 
 'healthcare'                 : pl.Float32 
 'industrials'                : pl.Float32 
 'utilities'                  : pl.Float32 
 'materials'                  : pl.Float32 
 'energy'                     : pl.Float32 
 'financials'                 : pl.Float32 
 'real_estate'                : pl.Float32 
 'other'                      : pl.Float32           Sum of unknown sector weights
 'holdings'                   : pl.List(pl.Struct)   {symbol: Utf8, weight: Float32} -- these are for further feature generation
 'net_assets'                 : pl.Float32 
 'net_expense_ratio'          : pl.Float32 
 'portfolio_turnover'         : pl.Float32           Often null (API returns "n/a")
 'dividend_yield'             : pl.Float32  
 'leveraged'                  : pl.Categorical       "YES" or "NO"

### financials_quarterly: pl.DataFrame
Note: 
 - 'Date' is sourced from `reportedDate` (EARNINGS.quarterlyEarnings) and is not duplicated as its own field below.
 - qm: quarterly minus, qp: quarterly plus
 - m is in {1,2,...,16}
 - n encodes a signed quarterly offset: `m{|k|}` for k<0, `p{k}` for k>0, `0` for k=0.
   Range: {m8, m7, m6, m5, m4, m3, m2, m1, 0, p1, p2, p3, p4}.
   Regex: `_qp_(m|p)?(\d+)$` (sign letter absent => zero).
Columns
 'Date'                                                             : pl.Date
 'days_to_fiscalDateEnding_qm0'                                     : pl.Float32
 'days_to_fiscalDateEnding_qm{m}'                                   : pl.Float32
 'days_to_reportedDate_qm0'                                         : pl.Float32
 'days_to_reportedDate_qm{m}'                                       : pl.Float32
 'reportTime_qm0'                                                   : pl.Categorical  ('pre-market', 'post-market', 'other')
 'reportTime_qm{m}'                                                 : pl.Categorical  ('pre-market', 'post-market', 'other')
 'accumulatedDepreciationAmortizationPPE_qm{m}'                     : pl.Float32
 'capitalExpenditures_qm{m}'                                        : pl.Float32
 'capitalLeaseObligations_qm{m}'                                    : pl.Float32
 'cashAndCashEquivalentsAtCarryingValue_qm{m}'                      : pl.Float32
 'cashAndShortTermInvestments_qm{m}'                                : pl.Float32
 'cashflowFromFinancing_qm{m}'                                      : pl.Float32
 'cashflowFromInvestment_qm{m}'                                     : pl.Float32
 'changeInCashAndCashEquivalents_qm{m}'                             : pl.Float32
 'changeInExchangeRate_qm{m}'                                       : pl.Float32
 'changeInInventory_qm{m}'                                          : pl.Float32
 'changeInOperatingAssets_qm{m}'                                    : pl.Float32
 'changeInOperatingLiabilities_qm{m}'                               : pl.Float32
 'changeInReceivables_qm{m}'                                        : pl.Float32
 'commonStock_qm{m}'                                                : pl.Float32
 'commonStockSharesOutstanding_qm{m}'                               : pl.Float32
 'comprehensiveIncomeNetOfTax_qm{m}'                                : pl.Float32
 'costOfRevenue_qm{m}'                                              : pl.Float32
 'costofGoodsAndServicesSold_qm{m}'                                 : pl.Float32
 'currentAccountsPayable_qm{m}'                                     : pl.Float32
 'currentDebt_qm{m}'                                                : pl.Float32
 'currentLongTermDebt_qm{m}'                                        : pl.Float32
 'currentNetReceivables_qm{m}'                                      : pl.Float32
 'deferredRevenue_qm{m}'                                            : pl.Float32
 'depreciation_qm{m}'                                               : pl.Float32
 'depreciationAndAmortization_qm{m}'                                : pl.Float32
 'depreciationDepletionAndAmortization_qm{m}'                       : pl.Float32
 'dividendPayout_qm{m}'                                             : pl.Float32
 'dividendPayoutCommonStock_qm{m}'                                  : pl.Float32
 'dividendPayoutPreferredStock_qm{m}'                               : pl.Float32
 'ebit_qm{m}'                                                       : pl.Float32
 'ebitda_qm{m}'                                                     : pl.Float32
 'estimatedEPS_qm{m}'                                               : pl.Float32
 'goodwill_qm{m}'                                                   : pl.Float32
 'grossProfit_qm{m}'                                                : pl.Float32
 'incomeBeforeTax_qm{m}'                                            : pl.Float32
 'incomeTaxExpense_qm{m}'                                           : pl.Float32
 'intangibleAssets_qm{m}'                                           : pl.Float32
 'intangibleAssetsExcludingGoodwill_qm{m}'                          : pl.Float32
 'interestAndDebtExpense_qm{m}'                                     : pl.Float32
 'interestExpense_qm{m}'                                            : pl.Float32
 'interestIncome_qm{m}'                                             : pl.Float32
 'inventory_qm{m}'                                                  : pl.Float32
 'investmentIncomeNet_qm{m}'                                        : pl.Float32
 'investments_qm{m}'                                                : pl.Float32
 'longTermDebt_qm{m}'                                               : pl.Float32
 'longTermDebtNoncurrent_qm{m}'                                     : pl.Float32
 'longTermInvestments_qm{m}'                                        : pl.Float32
 'netIncome_qm{m}'                                                  : pl.Float32
 'netIncomeFromContinuingOperations_qm{m}'                          : pl.Float32
 'netInterestIncome_qm{m}'                                          : pl.Float32
 'nonInterestIncome_qm{m}'                                          : pl.Float32
 'operatingCashflow_qm{m}'                                          : pl.Float32
 'operatingExpenses_qm{m}'                                          : pl.Float32
 'operatingIncome_qm{m}'                                            : pl.Float32
 'otherCurrentAssets_qm{m}'                                         : pl.Float32
 'otherCurrentLiabilities_qm{m}'                                    : pl.Float32
 'otherNonCurrentAssets_qm{m}'                                      : pl.Float32
 'otherNonCurrentLiabilities_qm{m}'                                 : pl.Float32
 'otherNonOperatingIncome_qm{m}'                                    : pl.Float32
 'paymentsForOperatingActivities_qm{m}'                             : pl.Float32
 'paymentsForRepurchaseOfCommonStock_qm{m}'                         : pl.Float32
 'paymentsForRepurchaseOfEquity_qm{m}'                              : pl.Float32
 'paymentsForRepurchaseOfPreferredStock_qm{m}'                      : pl.Float32
 'proceedsFromIssuanceOfCommonStock_qm{m}'                          : pl.Float32
 'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet_qm{m}'  : pl.Float32
 'proceedsFromIssuanceOfPreferredStock_qm{m}'                       : pl.Float32
 'proceedsFromOperatingActivities_qm{m}'                            : pl.Float32
 'proceedsFromRepaymentsOfShortTermDebt_qm{m}'                      : pl.Float32
 'proceedsFromRepurchaseOfEquity_qm{m}'                             : pl.Float32
 'proceedsFromSaleOfTreasuryStock_qm{m}'                            : pl.Float32
 'profitLoss_qm{m}'                                                 : pl.Float32
 'propertyPlantEquipment_qm{m}'                                     : pl.Float32
 'reportedEPS_qm{m}'                                                : pl.Float32
 'researchAndDevelopment_qm{m}'                                     : pl.Float32
 'retainedEarnings_qm{m}'                                           : pl.Float32
 'sellingGeneralAndAdministrative_qm{m}'                            : pl.Float32
 'shortLongTermDebtTotal_qm{m}'                                     : pl.Float32
 'shortTermDebt_qm{m}'                                              : pl.Float32
 'shortTermInvestments_qm{m}'                                       : pl.Float32
 'stockBasedCompensation_qm{m}'                                     : pl.Float32
 'surprise_qm{m}'                                                   : pl.Float32
 'surprisePercentage_qm{m}'                                         : pl.Float32
 'totalAssets_qm{m}'                                                : pl.Float32
 'totalCurrentAssets_qm{m}'                                         : pl.Float32
 'totalCurrentLiabilities_qm{m}'                                    : pl.Float32
 'totalLiabilities_qm{m}'                                           : pl.Float32
 'totalNonCurrentAssets_qm{m}'                                      : pl.Float32
 'totalNonCurrentLiabilities_qm{m}'                                 : pl.Float32
 'totalRevenue_qm{m}'                                               : pl.Float32
 'totalShareholderEquity_qm{m}'                                     : pl.Float32
 'treasuryStock_qm{m}'                                              : pl.Float32

 'eps_estimate_analyst_count_qp_{n}'                                 : pl.Float32
 'eps_estimate_average_qp_{n}'                                       : pl.Float32
 'eps_estimate_average_30_days_ago_qp_{n}'                           : pl.Float32
 'eps_estimate_average_60_days_ago_qp_{n}'                           : pl.Float32
 'eps_estimate_average_7_days_ago_qp_{n}'                            : pl.Float32
 'eps_estimate_average_90_days_ago_qp_{n}'                           : pl.Float32
 'eps_estimate_high_qp_{n}'                                          : pl.Float32
 'eps_estimate_low_qp_{n}'                                           : pl.Float32
 'eps_estimate_revision_down_trailing_30_days_qp_{n}'                : pl.Float32
 'eps_estimate_revision_down_trailing_7_days_qp_{n}'                 : pl.Float32
 'eps_estimate_revision_up_trailing_30_days_qp_{n}'                  : pl.Float32
 'eps_estimate_revision_up_trailing_7_days_qp_{n}'                   : pl.Float32
 'revenue_estimate_analyst_count_qp_{n}'                             : pl.Float32
 'revenue_estimate_average_qp_{n}'                                   : pl.Float32
 'revenue_estimate_high_qp_{n}'                                      : pl.Float32
 'revenue_estimate_low_qp_{n}'                                       : pl.Float32


### financials_annually: pl.DataFrame
Note: 
 - annual EARNINGS does not provide `reportedDate`, `reportTime`, `estimatedEPS`, `surprise`, or `surprisePercentage`; those are quarterly-only.
 - am: annually minus, ap: annually plus
 - m is in {1,2,3,4}
 - n encodes a signed annual offset: `m{|k|}` for k<0, `p{k}` for k>0, `0` for k=0.
   Range: {m2, m1, 0, p1}.
   Regex: `_ap_(m|p)?(\d+)$` (sign letter absent => zero).
Columns
 'Date'                                                             : pl.Date
 'days_to_fiscalDateEnding_am0'                                     : pl.Float32
 'days_to_fiscalDateEnding_am{m}'                                   : pl.Float32
 'days_to_reportedDate_am0'                                         : pl.Float32
 'days_to_reportedDate_am{m}'                                       : pl.Float32
 'accumulatedDepreciationAmortizationPPE_am{m}'                     : pl.Float32
 'capitalExpenditures_am{m}'                                        : pl.Float32
 'capitalLeaseObligations_am{m}'                                    : pl.Float32
 'cashAndCashEquivalentsAtCarryingValue_am{m}'                      : pl.Float32
 'cashAndShortTermInvestments_am{m}'                                : pl.Float32
 'cashflowFromFinancing_am{m}'                                      : pl.Float32
 'cashflowFromInvestment_am{m}'                                     : pl.Float32
 'changeInCashAndCashEquivalents_am{m}'                             : pl.Float32
 'changeInExchangeRate_am{m}'                                       : pl.Float32
 'changeInInventory_am{m}'                                          : pl.Float32
 'changeInOperatingAssets_am{m}'                                    : pl.Float32
 'changeInOperatingLiabilities_am{m}'                               : pl.Float32
 'changeInReceivables_am{m}'                                        : pl.Float32
 'commonStock_am{m}'                                                : pl.Float32
 'commonStockSharesOutstanding_am{m}'                               : pl.Float32
 'comprehensiveIncomeNetOfTax_am{m}'                                : pl.Float32
 'costOfRevenue_am{m}'                                              : pl.Float32
 'costofGoodsAndServicesSold_am{m}'                                 : pl.Float32
 'currentAccountsPayable_am{m}'                                     : pl.Float32
 'currentDebt_am{m}'                                                : pl.Float32
 'currentLongTermDebt_am{m}'                                        : pl.Float32
 'currentNetReceivables_am{m}'                                      : pl.Float32
 'deferredRevenue_am{m}'                                            : pl.Float32
 'depreciation_am{m}'                                               : pl.Float32
 'depreciationAndAmortization_am{m}'                                : pl.Float32
 'depreciationDepletionAndAmortization_am{m}'                       : pl.Float32
 'dividendPayout_am{m}'                                             : pl.Float32
 'dividendPayoutCommonStock_am{m}'                                  : pl.Float32
 'dividendPayoutPreferredStock_am{m}'                               : pl.Float32
 'ebit_am{m}'                                                       : pl.Float32
 'ebitda_am{m}'                                                     : pl.Float32
 'goodwill_am{m}'                                                   : pl.Float32
 'grossProfit_am{m}'                                                : pl.Float32
 'incomeBeforeTax_am{m}'                                            : pl.Float32
 'incomeTaxExpense_am{m}'                                           : pl.Float32
 'intangibleAssets_am{m}'                                           : pl.Float32
 'intangibleAssetsExcludingGoodwill_am{m}'                          : pl.Float32
 'interestAndDebtExpense_am{m}'                                     : pl.Float32
 'interestExpense_am{m}'                                            : pl.Float32
 'interestIncome_am{m}'                                             : pl.Float32
 'inventory_am{m}'                                                  : pl.Float32
 'investmentIncomeNet_am{m}'                                        : pl.Float32
 'investments_am{m}'                                                : pl.Float32
 'longTermDebt_am{m}'                                               : pl.Float32
 'longTermDebtNoncurrent_am{m}'                                     : pl.Float32
 'longTermInvestments_am{m}'                                        : pl.Float32
 'netIncome_am{m}'                                                  : pl.Float32
 'netIncomeFromContinuingOperations_am{m}'                          : pl.Float32
 'netInterestIncome_am{m}'                                          : pl.Float32
 'nonInterestIncome_am{m}'                                          : pl.Float32
 'operatingCashflow_am{m}'                                          : pl.Float32
 'operatingExpenses_am{m}'                                          : pl.Float32
 'operatingIncome_am{m}'                                            : pl.Float32
 'otherCurrentAssets_am{m}'                                         : pl.Float32
 'otherCurrentLiabilities_am{m}'                                    : pl.Float32
 'otherNonCurrentAssets_am{m}'                                      : pl.Float32
 'otherNonCurrentLiabilities_am{m}'                                 : pl.Float32
 'otherNonOperatingIncome_am{m}'                                    : pl.Float32
 'paymentsForOperatingActivities_am{m}'                             : pl.Float32
 'paymentsForRepurchaseOfCommonStock_am{m}'                         : pl.Float32
 'paymentsForRepurchaseOfEquity_am{m}'                              : pl.Float32
 'paymentsForRepurchaseOfPreferredStock_am{m}'                      : pl.Float32
 'proceedsFromIssuanceOfCommonStock_am{m}'                          : pl.Float32
 'proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet_am{m}'  : pl.Float32
 'proceedsFromIssuanceOfPreferredStock_am{m}'                       : pl.Float32
 'proceedsFromOperatingActivities_am{m}'                            : pl.Float32
 'proceedsFromRepaymentsOfShortTermDebt_am{m}'                      : pl.Float32
 'proceedsFromRepurchaseOfEquity_am{m}'                             : pl.Float32
 'proceedsFromSaleOfTreasuryStock_am{m}'                            : pl.Float32
 'profitLoss_am{m}'                                                 : pl.Float32
 'propertyPlantEquipment_am{m}'                                     : pl.Float32
 'reportedEPS_am{m}'                                                : pl.Float32
 'researchAndDevelopment_am{m}'                                     : pl.Float32
 'retainedEarnings_am{m}'                                           : pl.Float32
 'sellingGeneralAndAdministrative_am{m}'                            : pl.Float32
 'shortLongTermDebtTotal_am{m}'                                     : pl.Float32
 'shortTermDebt_am{m}'                                              : pl.Float32
 'shortTermInvestments_am{m}'                                       : pl.Float32
 'stockBasedCompensation_am{m}'                                     : pl.Float32
 'totalAssets_am{m}'                                                : pl.Float32
 'totalCurrentAssets_am{m}'                                         : pl.Float32
 'totalCurrentLiabilities_am{m}'                                    : pl.Float32
 'totalLiabilities_am{m}'                                           : pl.Float32
 'totalNonCurrentAssets_am{m}'                                      : pl.Float32
 'totalNonCurrentLiabilities_am{m}'                                 : pl.Float32
 'totalRevenue_am{m}'                                               : pl.Float32
 'totalShareholderEquity_am{m}'                                     : pl.Float32
 'treasuryStock_am{m}'                                              : pl.Float32

 'eps_estimate_analyst_count_ap_{n}'                                 : pl.Float32
 'eps_estimate_average_ap_{n}'                                       : pl.Float32
 'eps_estimate_average_30_days_ago_ap_{n}'                           : pl.Float32
 'eps_estimate_average_60_days_ago_ap_{n}'                           : pl.Float32
 'eps_estimate_average_7_days_ago_ap_{n}'                            : pl.Float32
 'eps_estimate_average_90_days_ago_ap_{n}'                           : pl.Float32
 'eps_estimate_high_ap_{n}'                                          : pl.Float32
 'eps_estimate_low_ap_{n}'                                           : pl.Float32
 'eps_estimate_revision_down_trailing_30_days_ap_{n}'                : pl.Float32
 'eps_estimate_revision_down_trailing_7_days_ap_{n}'                 : pl.Float32
 'eps_estimate_revision_up_trailing_30_days_ap_{n}'                  : pl.Float32
 'eps_estimate_revision_up_trailing_7_days_ap_{n}'                   : pl.Float32
 'revenue_estimate_analyst_count_ap_{n}'                             : pl.Float32
 'revenue_estimate_average_ap_{n}'                                   : pl.Float32
 'revenue_estimate_high_ap_{n}'                                      : pl.Float32
 'revenue_estimate_low_ap_{n}'                                       : pl.Float32