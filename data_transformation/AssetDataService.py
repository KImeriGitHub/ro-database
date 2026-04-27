import json
from pathlib import Path
from typing import Any

import polars as pl


_QM_BASE_FIELDS: list[tuple[str, Any]] = [
    ("days_to_fiscalDateEnding", pl.Float32),
    ("reportTime", pl.Categorical),
    ("accumulatedDepreciationAmortizationPPE", pl.Float32),
    ("capitalExpenditures", pl.Float32),
    ("capitalLeaseObligations", pl.Float32),
    ("cashAndCashEquivalentsAtCarryingValue", pl.Float32),
    ("cashAndShortTermInvestments", pl.Float32),
    ("cashflowFromFinancing", pl.Float32),
    ("cashflowFromInvestment", pl.Float32),
    ("changeInCashAndCashEquivalents", pl.Float32),
    ("changeInExchangeRate", pl.Float32),
    ("changeInInventory", pl.Float32),
    ("changeInOperatingAssets", pl.Float32),
    ("changeInOperatingLiabilities", pl.Float32),
    ("changeInReceivables", pl.Float32),
    ("commonStock", pl.Float32),
    ("commonStockSharesOutstanding", pl.Float32),
    ("comprehensiveIncomeNetOfTax", pl.Float32),
    ("costOfRevenue", pl.Float32),
    ("costofGoodsAndServicesSold", pl.Float32),
    ("currentAccountsPayable", pl.Float32),
    ("currentDebt", pl.Float32),
    ("currentLongTermDebt", pl.Float32),
    ("currentNetReceivables", pl.Float32),
    ("deferredRevenue", pl.Float32),
    ("depreciation", pl.Float32),
    ("depreciationAndAmortization", pl.Float32),
    ("depreciationDepletionAndAmortization", pl.Float32),
    ("dividendPayout", pl.Float32),
    ("dividendPayoutCommonStock", pl.Float32),
    ("dividendPayoutPreferredStock", pl.Float32),
    ("ebit", pl.Float32),
    ("ebitda", pl.Float32),
    ("estimatedEPS", pl.Float32),
    ("goodwill", pl.Float32),
    ("grossProfit", pl.Float32),
    ("incomeBeforeTax", pl.Float32),
    ("incomeTaxExpense", pl.Float32),
    ("intangibleAssets", pl.Float32),
    ("intangibleAssetsExcludingGoodwill", pl.Float32),
    ("interestAndDebtExpense", pl.Float32),
    ("interestExpense", pl.Float32),
    ("interestIncome", pl.Float32),
    ("inventory", pl.Float32),
    ("investmentIncomeNet", pl.Float32),
    ("investments", pl.Float32),
    ("longTermDebt", pl.Float32),
    ("longTermDebtNoncurrent", pl.Float32),
    ("longTermInvestments", pl.Float32),
    ("netIncome", pl.Float32),
    ("netIncomeFromContinuingOperations", pl.Float32),
    ("netInterestIncome", pl.Float32),
    ("nonInterestIncome", pl.Float32),
    ("operatingCashflow", pl.Float32),
    ("operatingExpenses", pl.Float32),
    ("operatingIncome", pl.Float32),
    ("otherCurrentAssets", pl.Float32),
    ("otherCurrentLiabilities", pl.Float32),
    ("otherNonCurrentAssets", pl.Float32),
    ("otherNonCurrentLiabilities", pl.Float32),
    ("otherNonOperatingIncome", pl.Float32),
    ("paymentsForOperatingActivities", pl.Float32),
    ("paymentsForRepurchaseOfCommonStock", pl.Float32),
    ("paymentsForRepurchaseOfEquity", pl.Float32),
    ("paymentsForRepurchaseOfPreferredStock", pl.Float32),
    ("proceedsFromIssuanceOfCommonStock", pl.Float32),
    ("proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet", pl.Float32),
    ("proceedsFromIssuanceOfPreferredStock", pl.Float32),
    ("proceedsFromOperatingActivities", pl.Float32),
    ("proceedsFromRepaymentsOfShortTermDebt", pl.Float32),
    ("proceedsFromRepurchaseOfEquity", pl.Float32),
    ("proceedsFromSaleOfTreasuryStock", pl.Float32),
    ("profitLoss", pl.Float32),
    ("propertyPlantEquipment", pl.Float32),
    ("reportedEPS", pl.Float32),
    ("researchAndDevelopment", pl.Float32),
    ("retainedEarnings", pl.Float32),
    ("sellingGeneralAndAdministrative", pl.Float32),
    ("shortLongTermDebtTotal", pl.Float32),
    ("shortTermDebt", pl.Float32),
    ("shortTermInvestments", pl.Float32),
    ("stockBasedCompensation", pl.Float32),
    ("surprise", pl.Float32),
    ("surprisePercentage", pl.Float32),
    ("totalAssets", pl.Float32),
    ("totalCurrentAssets", pl.Float32),
    ("totalCurrentLiabilities", pl.Float32),
    ("totalLiabilities", pl.Float32),
    ("totalNonCurrentAssets", pl.Float32),
    ("totalNonCurrentLiabilities", pl.Float32),
    ("totalRevenue", pl.Float32),
    ("totalShareholderEquity", pl.Float32),
    ("treasuryStock", pl.Float32),
]

_AM_EXCLUDE = {"reportTime", "estimatedEPS", "surprise", "surprisePercentage"}
_AM_BASE_FIELDS: list[tuple[str, Any]] = [
    (name, dtype) for name, dtype in _QM_BASE_FIELDS if name not in _AM_EXCLUDE
]

_QP_BASE_FIELDS: list[tuple[str, Any]] = [
    ("earnings_estimate_days_diff", pl.Float32),
    ("eps_estimate_analyst_count", pl.Float32),
    ("eps_estimate_average", pl.Float32),
    ("eps_estimate_average_30_days_ago", pl.Float32),
    ("eps_estimate_average_60_days_ago", pl.Float32),
    ("eps_estimate_average_7_days_ago", pl.Float32),
    ("eps_estimate_average_90_days_ago", pl.Float32),
    ("eps_estimate_high", pl.Float32),
    ("eps_estimate_low", pl.Float32),
    ("eps_estimate_revision_down_trailing_30_days", pl.Float32),
    ("eps_estimate_revision_down_trailing_7_days", pl.Float32),
    ("eps_estimate_revision_up_trailing_30_days", pl.Float32),
    ("eps_estimate_revision_up_trailing_7_days", pl.Float32),
    ("revenue_estimate_analyst_count", pl.Float32),
    ("revenue_estimate_average", pl.Float32),
    ("revenue_estimate_high", pl.Float32),
    ("revenue_estimate_low", pl.Float32),
]

_AP_BASE_FIELDS = list(_QP_BASE_FIELDS)


def _signed_suffix(n: int) -> str:
    if n < 0:
        return f"m{-n}"
    if n > 0:
        return f"p{n}"
    return "0"


def _build_quarterly_schema() -> dict:
    schema: dict = {"Date": pl.Date}
    for m in range(0, 17):
        for name, dtype in _QM_BASE_FIELDS:
            schema[f"{name}_qm{m}"] = dtype
    for n in range(-8, 5):
        suffix = _signed_suffix(n)
        for name, dtype in _QP_BASE_FIELDS:
            schema[f"{name}_qp_{suffix}"] = dtype
    return schema


def _build_annual_schema() -> dict:
    schema: dict = {"Date": pl.Date}
    for m in range(0, 5):
        for name, dtype in _AM_BASE_FIELDS:
            schema[f"{name}_am{m}"] = dtype
    for n in range(-2, 2):
        suffix = _signed_suffix(n)
        for name, dtype in _AP_BASE_FIELDS:
            schema[f"{name}_ap_{suffix}"] = dtype
    return schema


SCHEMAS: dict[str, dict] = {
    "shareprice_daily": {
        "Date": pl.Date,
        "Open": pl.Float32,
        "High": pl.Float32,
        "Low": pl.Float32,
        "Close": pl.Float32,
        "AdjClose": pl.Float32,
        "Volume": pl.Float32,
        "AdjVolume": pl.Float32,
        "DividendAmount": pl.Float32,
        "SplitCoefficient": pl.Float32,
    },
    "shareprice_intraday": {
        "Datetime": pl.Datetime,
        "AdjOpen": pl.Float32,
        "AdjHigh": pl.Float32,
        "AdjLow": pl.Float32,
        "AdjClose": pl.Float32,
        "AdjVolume": pl.Float32,
    },
    "price_daily": {
        "Date": pl.Date,
        "Open": pl.Float32,
        "High": pl.Float32,
        "Low": pl.Float32,
        "Close": pl.Float32,
        "Volume": pl.Float32,
    },
    "insider_df": {
        "Date": pl.Date,
        "TransactionDate": pl.Date,
        "Executive_role": pl.Categorical,
        "AcqDis": pl.Categorical,
        "Shares": pl.Float32,
    },
    "sentiment_df": {
        "Datetime": pl.Datetime,
        "ticker_relevance_score": pl.Float32,
        "ticker_sentiment_score": pl.Float32,
        "overall_sentiment_score": pl.Float32,
        "blockchain": pl.Float32,
        "earnings": pl.Float32,
        "ipo": pl.Float32,
        "mergers_and_acquisitions": pl.Float32,
        "financial_markets": pl.Float32,
        "economy_fiscal": pl.Float32,
        "economy_monetary": pl.Float32,
        "economy_macro": pl.Float32,
        "energy_transportation": pl.Float32,
        "finance": pl.Float32,
        "life_sciences": pl.Float32,
        "manufacturing": pl.Float32,
        "real_estate": pl.Float32,
        "retail_wholesale": pl.Float32,
        "technology": pl.Float32,
    },
    "etf_profile": {
        "Date": pl.Date,
        "information_technology": pl.Float32,
        "communication_services": pl.Float32,
        "consumer_discretionary": pl.Float32,
        "consumer_staples": pl.Float32,
        "healthcare": pl.Float32,
        "industrials": pl.Float32,
        "utilities": pl.Float32,
        "materials": pl.Float32,
        "energy": pl.Float32,
        "financials": pl.Float32,
        "real_estate": pl.Float32,
        "other": pl.Float32,
        "holdings": pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32})),
        "net_assets": pl.Float32,
        "net_expense_ratio": pl.Float32,
        "portfolio_turnover": pl.Float32,
        "dividend_yield": pl.Float32,
        "leveraged": pl.Categorical,
    },
    "financials_quarterly": _build_quarterly_schema(),
    "financials_annually": _build_annual_schema(),
}


ASSET_LAYOUT: dict[str, dict] = {
    "StockData": {
        "scalars": ["ticker", "about", "sector"],
        "frames": {
            "shareprice_daily":     "shareprice_daily",
            "shareprice_intraday":  "shareprice_intraday",
            "financials_quarterly": "financials_quarterly",
            "financials_annually":  "financials_annually",
            "insider_df":           "insider_df",
            "sentiment_df":         "sentiment_df",
        },
    },
    "ETFData": {
        "scalars": ["ticker", "about"],
        "frames": {
            "shareprice_daily":    "shareprice_daily",
            "shareprice_intraday": "shareprice_intraday",
            "etf_profile":         "etf_profile",
        },
    },
    "IndexData": {
        "scalars": ["ticker", "about"],
        "frames": {"price_daily": "price_daily"},
    },
    "ForexData": {
        "scalars": ["ticker", "about"],
        "frames": {"price_daily": "price_daily"},
    },
    "CryptocurrenciesData": {
        "scalars": ["ticker", "about"],
        "frames": {"price_daily": "price_daily"},
    },
    "CommoditiesData": {
        "scalars": ["ticker", "about"],
        "frames": {"price_daily": "price_daily"},
    },
    "EconomicData": {
        "scalars": ["ticker", "about"],
        "frames": {"price_daily": "price_daily"},
    },
}


class AssetDataMixin:
    """Generic dict/parquet serialization for the asset dataclasses.

    Dispatch is by class name string into ASSET_LAYOUT, which keeps this
    module free of imports from AssetData and avoids a cycle.
    """

    @classmethod
    def default_instance(cls):
        layout = ASSET_LAYOUT[cls.__name__]
        kwargs: dict[str, Any] = {
            frame: pl.DataFrame(schema=SCHEMAS[schema_name])
            for frame, schema_name in layout["frames"].items()
        }
        return cls(**kwargs)

    def to_dict(self) -> dict:
        layout = ASSET_LAYOUT[type(self).__name__]
        out: dict[str, Any] = {"_asset_type": type(self).__name__}
        for scalar in layout["scalars"]:
            out[scalar] = getattr(self, scalar)
        for frame in layout["frames"]:
            out[frame] = getattr(self, frame).to_dicts()
        return out

    @classmethod
    def from_dict(cls, d: dict):
        layout = ASSET_LAYOUT[cls.__name__]
        kwargs: dict[str, Any] = {scalar: d[scalar] for scalar in layout["scalars"]}
        for frame, schema_name in layout["frames"].items():
            rows = d.get(frame, [])
            kwargs[frame] = pl.DataFrame(rows, schema=SCHEMAS[schema_name])
        return cls(**kwargs)

    def copy(self):
        layout = ASSET_LAYOUT[type(self).__name__]
        kwargs: dict[str, Any] = {s: getattr(self, s) for s in layout["scalars"]}
        for frame in layout["frames"]:
            kwargs[frame] = getattr(self, frame).clone()
        return type(self)(**kwargs)

    def save_to(self, dir) -> None:
        path = Path(dir)
        path.mkdir(parents=True, exist_ok=True)
        layout = ASSET_LAYOUT[type(self).__name__]
        metadata: dict[str, Any] = {"_asset_type": type(self).__name__}
        for scalar in layout["scalars"]:
            metadata[scalar] = getattr(self, scalar)
        (path / "metadata.json").write_text(json.dumps(metadata, indent=2))
        for frame in layout["frames"]:
            getattr(self, frame).write_parquet(path / f"{frame}.parquet")

    @classmethod
    def load_from(cls, dir):
        path = Path(dir)
        layout = ASSET_LAYOUT[cls.__name__]
        metadata = json.loads((path / "metadata.json").read_text())
        kwargs: dict[str, Any] = {s: metadata[s] for s in layout["scalars"]}
        for frame, schema_name in layout["frames"].items():
            file = path / f"{frame}.parquet"
            if file.exists():
                kwargs[frame] = pl.read_parquet(file)
            else:
                kwargs[frame] = pl.DataFrame(schema=SCHEMAS[schema_name])
        return cls(**kwargs)
