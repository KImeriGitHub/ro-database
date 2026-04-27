## StockData specifications

### General
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
 'AdjClose'          : pl.Float32
 'Volume'            : pl.Float32
 'AdjVolume'         : pl.Float32
 'DividendAmount'    : pl.Float32
 'SplitCoefficient'  : pl.Float32

### shareprice_intraday: pl.DataFrame
Columns 
 'Datetime'     : pl.Datetime
 'AdjOpen'      : pl.Float32
 'AdjHigh'      : pl.Float32
 'AdjLow'       : pl.Float32
 'AdjClose'     : pl.Float32
 'AdjVolume'    : pl.Float32

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
 'TransactionDate'   : pl.Date
 'Executive_role'    : pl.Categorical  (index into CANONICAL_INSIDER_ROLES; see below)
 'AcqDis'            : pl.Categorical  (1 for 'A' and -1 for 'D')
 'Shares'            : pl.Float32

CANONICAL_INSIDER_ROLES: CAO, General Counsel, CFO, COO, CTO_CIO,
Other C-Suite, CEO, Chairman, Director, VP, 10% Owner, Officer, Other

`executive_role` is derived from the raw `executive_title` string returned by
INSIDER_TRANSACTIONS. Mapping is by case-insensitive substring match against
an ordered rule list; the first rule that matches wins. A title that matches
no rule (including empty / null) falls through to `Other`.

Order is most-specific first so that compound titles route correctly
(e.g. "Chief Accounting Officer" hits CAO before the generic `chief ` rule;
"President & CFO" hits CFO before CEO). Adjust the order if a different
priority is desired -- the rule order IS the spec.

Match rules (index : label : substring patterns):
 0  CAO             : 'chief accounting', 'controller', 'principal accounting'
 1  General Counsel : 'general counsel', 'chief legal', 'secretary'
 2  CFO             : 'cfo', 'chief financial', 'treasurer', 'principal financial'
 3  COO             : 'coo', 'chief operating'
 4  CTO_CIO         : 'cto', 'cio', 'chief technology', 'chief information', 'chief digital'
 5  Other C-Suite   : 'chief '   (catch-all for CMO, CHRO, CRO, CSO, ...)
 6  CEO             : 'ceo', 'chief executive', 'president'
 7  Chairman        : 'chairman', 'chair of'
 8  Director        : 'director'
 9  VP              : 'vp', 'vice president', 'executive vice', 'senior vice'
 10 10% Owner       : '10%', 'beneficial owner'
 11 Officer         : 'officer'
 12 Other           : (no pattern; default fallthrough, also catches empty/null)

### sentiment_df: pl.DataFrame
 'Datetime'                   : pl.Datetime
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
 - m is in {0,1,2,...,16}
 - n encodes a signed quarterly offset: `m{|k|}` for k<0, `p{k}` for k>0, `0` for k=0.
   Range: {m8, m7, m6, m5, m4, m3, m2, m1, 0, p1, p2, p3, p4}.
   Regex: `_qp_(m|p)?(\d+)$` (sign letter absent => zero).
Columns
 'Date'                                                             : pl.Date
 'days_to_fiscalDateEnding_qm{m}'                                   : pl.Float32
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

 'earnings_estimate_days_diff_qp_{n}'                                : pl.Float32
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
 - m is in {0,1,2,3,4}
 - n encodes a signed annual offset: `m{|k|}` for k<0, `p{k}` for k>0, `0` for k=0.
   Range: {m2, m1, 0, p1}.
   Regex: `_ap_(m|p)?(\d+)$` (sign letter absent => zero).
Columns
 'Date'                                                             : pl.Date
 'days_to_fiscalDateEnding_am{m}'                                   : pl.Date
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

 'earnings_estimate_days_diff_ap_{n}'                                : pl.Float32
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