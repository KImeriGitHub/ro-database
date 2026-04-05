import pandas as pd
import polars as pl
import re
from typing import Dict
from dataclasses import fields
from pandas.api.types import is_float_dtype
    
from AssetData import AssetData

import logging

logger = logging.getLogger(__name__)

# Common constants used for validation
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VALID_REPORT_TIMES = {"pre-market", "post-market"}

# Define dtypes for shareprice
shareprice_dtypes = {
    'Date': 'string',
    'Open': 'Float64',
    'High': 'Float64',
    'Low': 'Float64',
    'Close': 'Float64',
    'AdjClose': 'Float64',
    'Volume': 'Float64',
    'Dividends': 'Float64',
    'Splits': 'Float64'
}
# Define dtypes for quarterly financials
quarterly_dtypes = {
    'fiscalDateEnding': 'string',
    'reportedDate': 'string',
    'reportedEPS': 'Float64',
    'estimatedEPS': 'Float64',
    'surprise': 'Float64',
    'surprisePercentage': 'Float64',
    'reportTime': 'string',
    'grossProfit': 'Float64',
    'totalRevenue': 'Float64',
    'ebit': 'Float64',
    'ebitda': 'Float64',
    'totalAssets': 'Float64',
    'totalCurrentLiabilities': 'Float64',
    'totalShareholderEquity': 'Float64',
    'commonStockSharesOutstanding': 'Float64',
    'operatingCashflow': 'Float64'
}
# Define dtypes for annual financials
annual_dtypes = {
    'fiscalDateEnding': 'string',
    'reportedEPS': 'Float64',
    'grossProfit': 'Float64',
    'totalRevenue': 'Float64',
    'ebit': 'Float64',
    'ebitda': 'Float64',
    'totalAssets': 'Float64',
    'totalCurrentLiabilities': 'Float64',
    'totalShareholderEquity': 'Float64',
    'operatingCashflow': 'Float64'
}

class AssetDataService:
    def __init__(self):
        pass

    @staticmethod
    def defaultInstance(ticker: str = "", isin: str = "") -> AssetData:
        # Create empty DataFrames with specified dtypes
        empty_shareprice = pd.DataFrame({col: pd.Series(dtype=dt)
            for col, dt in shareprice_dtypes.items()})
        empty_quarterly = pd.DataFrame({col: pd.Series(dtype=dt)
            for col, dt in quarterly_dtypes.items()})
        empty_annual = pd.DataFrame({col: pd.Series(dtype=dt)
            for col, dt in annual_dtypes.items()})

        return AssetData(
            ticker=ticker,
            isin=isin,
            shareprice=empty_shareprice,
            about={},
            sector="",
            financials_quarterly=empty_quarterly,
            financials_annually=empty_annual
        )

    @staticmethod
    def to_dict(asset: AssetData) -> Dict:
        # Basic fields
        data = {
            "ticker": asset.ticker,
            "isin": asset.isin or "",
            "about": asset.about or {},
            "sector": asset.sector or "",
        }
        # Fallback empty instance for missing DataFrames
        empty = AssetDataService.defaultInstance(ticker=asset.ticker, isin=asset.isin)
        # Convert DataFrames to dict (default orient='dict')
        data['shareprice'] = asset.shareprice.to_dict() if isinstance(asset.shareprice, pd.DataFrame) else empty.shareprice.to_dict()
        data['financials_quarterly'] = asset.financials_quarterly.to_dict() if isinstance(asset.financials_quarterly, pd.DataFrame) else empty.financials_quarterly.to_dict()
        data['financials_annually'] = asset.financials_annually.to_dict() if isinstance(asset.financials_annually, pd.DataFrame) else empty.financials_annually.to_dict()
        return data
    
    @staticmethod
    def from_dict(assetdict: dict) -> AssetData:
        # Validate required fields
        ticker = assetdict.get("ticker")
        if not ticker:
            raise ValueError("Ticker symbol is required to construct AssetData.")

        # Create empty instance to get schema defaults
        instance = AssetDataService.defaultInstance(ticker=ticker, isin=assetdict.get("isin", ""))

        # Populate basic fields
        instance.about = assetdict.get("about", {}) or {}
        instance.sector = assetdict.get("sector", "") or ""

        # Load shareprice
        sp_dict = assetdict.get("shareprice", {})
        instance.shareprice = pd.DataFrame(sp_dict)
        instance.shareprice = instance.shareprice.astype(shareprice_dtypes)

        # Load financials quarterly
        fq_dict = assetdict.get("financials_quarterly", {})
        instance.financials_quarterly = pd.DataFrame(fq_dict)
        instance.financials_quarterly = instance.financials_quarterly.astype(quarterly_dtypes)

        # Load financials annually
        fa_dict = assetdict.get("financials_annually", {})
        instance.financials_annually = pd.DataFrame(fa_dict)
        instance.financials_annually = instance.financials_annually.astype(annual_dtypes)

        return instance
    
    @staticmethod
    def copy(ad: AssetData) -> AssetData:
        # Create a new instance of AssetData with the same attributes
        return AssetData(
            ticker=ad.ticker,
            isin=ad.isin,
            about=ad.about.copy() if ad.about else {},
            sector=ad.sector,
            shareprice=ad.shareprice.copy(),
            financials_quarterly=ad.financials_quarterly.copy(),
            financials_annually=ad.financials_annually.copy()
        )

    # ------------------------------------------------------------------
    # Validation helper methods
    # ------------------------------------------------------------------

    @staticmethod
    def _check_string_col(df: pd.DataFrame, col: str, prefix: str = "VALIDATION") -> None:
        """Ensure column has pandas string dtype."""
        if df[col].dtype != pd.StringDtype(storage="python"):
            logger.error(
                f"{prefix} ERROR: {col} must be dtype string, got {df[col].dtype}"
            )

    @staticmethod
    def _check_date_col(df: pd.DataFrame, col: str, prefix: str = "VALIDATION") -> None:
        AssetDataService._check_string_col(df, col)
        bad = df[~df[col].astype(str).str.match(DATE_RE)]
        if not bad.empty:
            logger.error(
                f"{prefix} ERROR: Column {col} has invalid dates:\n{bad[col].unique()}"
            )

    @staticmethod
    def _check_float_col(df: pd.DataFrame, col: str, prefix: str = "VALIDATION") -> None:
        if df[col].dtype != pd.Float64Dtype():
            logger.error(
                f"{prefix} ERROR: {col} must be float dtype, got {df[col].dtype}"
            )

    @staticmethod
    def validate_shareprice_df(df: pd.DataFrame, prefix: str = "VALIDATION") -> None:
        if df is None:
            return
        if df.empty:
            logger.error(f"{prefix} ERROR: Shareprice data is empty.")

        exp = [
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
            "AdjClose",
            "Volume",
            "Dividends",
            "Splits",
        ]
        if list(df.columns) != exp:
            logger.error(f"{prefix} ERROR: shareprice.columns must be {exp}")

        AssetDataService._check_date_col(df, "Date")
        for c in exp[1:]:
            AssetDataService._check_float_col(df, c)

        price_columns = ["Open", "High", "Low", "Close", "AdjClose"]
        if not df.empty and (df[price_columns] < 0).any().any():
            logger.error(
                f"{prefix} ERROR: Float Shareprice data contains negative values."
            )

    @staticmethod
    def validate_financials_df(finquar: pd.DataFrame, finann: pd.DataFrame, prefix: str = "VALIDATION") -> None:
        if finquar is not None:
            exp_q = [
                "fiscalDateEnding",
                "reportedDate",
                "reportedEPS",
                "estimatedEPS",
                "surprise",
                "surprisePercentage",
                "reportTime",
                "grossProfit",
                "totalRevenue",
                "ebit",
                "ebitda",
                "totalAssets",
                "totalCurrentLiabilities",
                "totalShareholderEquity",
                "commonStockSharesOutstanding",
                "operatingCashflow",
            ]
            if list(finquar.columns) != exp_q:
                logger.error(
                    f"{prefix} ERROR: financials_quarterly.columns must be {exp_q}"
                )

            AssetDataService._check_date_col(finquar, "fiscalDateEnding")
            AssetDataService._check_date_col(finquar, "reportedDate")

            rt = finquar["reportTime"]
            AssetDataService._check_string_col(finquar, "reportTime")
            rtset = set(rt.unique()) - set([pd.NA])
            if not rt.empty and (
                rt.dtype != pd.StringDtype(storage="python")
                or not rtset.issubset(VALID_REPORT_TIMES)
            ):
                logger.error(
                    f"{prefix} ERROR: reportTime must be in {VALID_REPORT_TIMES}, got {set(rt.unique())}"
                )

            for c in exp_q:
                if c not in {"fiscalDateEnding", "reportedDate", "reportTime"}:
                    AssetDataService._check_float_col(finquar, c)

        if finann is not None:
            exp_a = [
                "fiscalDateEnding",
                "reportedEPS",
                "grossProfit",
                "totalRevenue",
                "ebit",
                "ebitda",
                "totalAssets",
                "totalCurrentLiabilities",
                "totalShareholderEquity",
                "operatingCashflow",
            ]
            if list(finann.columns) != exp_a:
                logger.error(
                    f"{prefix} ERROR: financials_annually.columns must be {exp_a}. Got {list(finann.columns)}"
                )
            AssetDataService._check_date_col(finann, "fiscalDateEnding")
            for c in exp_a[1:]:
                AssetDataService._check_float_col(finann, c)

    @staticmethod
    def validate_asset_data(ad: AssetData, prefix: str = "VALIDATION"):
        
        # 1. Field names
        dc_fields = {f.name for f in fields(AssetData)}
        inst_fields = set(ad.__dict__)
        if dc_fields != inst_fields:
            extra = inst_fields - dc_fields
            missing = dc_fields - inst_fields
            logger.error(f"{prefix} ERROR: Field mismatch - extra: {extra}, missing: {missing}")

        # 2. Attribute types
        if not isinstance(ad.ticker, str):
            logger.error(f"{prefix} ERROR: ticker must be str")
        if not isinstance(ad.isin, str):
            logger.error(f"{prefix} ERROR: isin must be str")
        if not isinstance(ad.sector, str):
            logger.error(f"{prefix} ERROR: sector must be str")
        if ad.about is not None and not isinstance(ad.about, dict):
            logger.error(f"{prefix} ERROR: about must be dict or None")

        # 3. shareprice and financials
        AssetDataService.validate_shareprice_df(ad.shareprice, prefix=prefix)
        AssetDataService.validate_financials_df(ad.financials_quarterly, ad.financials_annually, prefix=prefix)