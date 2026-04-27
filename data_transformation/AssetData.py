import polars as pl
from dataclasses import dataclass, field

from data_transformation.AssetDataService import AssetDataMixin

CANONICAL_SECTORS = [
    "Basic Materials",
    "Communication Services",
    "Consumer Cyclical",
    "Consumer Defensive",
    "Energy",
    "Financial Services",
    "Healthcare",
    "Industrials",
    "Real Estate",
    "Technology",
    "Utilities",
    "Other",
]

_OTHER_SECTOR = CANONICAL_SECTORS.index("Other")


@dataclass
class StockData(AssetDataMixin):
    ticker: str = ""
    about: str = ""
    sector: int = _OTHER_SECTOR

    shareprice_daily: pl.DataFrame = field(default_factory=pl.DataFrame)
    shareprice_intraday: pl.DataFrame = field(default_factory=pl.DataFrame)
    financials_quarterly: pl.DataFrame = field(default_factory=pl.DataFrame)
    financials_annually: pl.DataFrame = field(default_factory=pl.DataFrame)

    insider_df: pl.DataFrame = field(default_factory=pl.DataFrame)
    sentiment_df: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class ETFData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    shareprice_daily: pl.DataFrame = field(default_factory=pl.DataFrame)
    shareprice_intraday: pl.DataFrame = field(default_factory=pl.DataFrame)

    etf_profile: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class IndexData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    price_daily: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class ForexData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    price_daily: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class CryptocurrenciesData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    price_daily: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class CommoditiesData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    price_daily: pl.DataFrame = field(default_factory=pl.DataFrame)


@dataclass
class EconomicData(AssetDataMixin):
    ticker: str = ""
    about: str = ""

    price_daily: pl.DataFrame = field(default_factory=pl.DataFrame)
