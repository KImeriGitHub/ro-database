"""Tests for the AssetDataMixin (default_instance, to_dict/from_dict, copy, save_to/load_from)
and the schema registry in AssetDataService.
"""

import re
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from data_transformation.AssetData import (
    CANONICAL_SECTORS,
    CommoditiesData,
    CryptocurrenciesData,
    EconomicData,
    ETFData,
    ForexData,
    IndexData,
    StockData,
)
from data_transformation.AssetDataService import (
    ASSET_LAYOUT,
    SCHEMAS,
)


ALL_CLASSES = [
    StockData,
    ETFData,
    IndexData,
    ForexData,
    CryptocurrenciesData,
    CommoditiesData,
    EconomicData,
]


# ── default_instance ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_default_instance_returns_correct_type(cls):
    inst = cls.default_instance()
    assert isinstance(inst, cls)


@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_default_instance_scalars_are_defaults(cls):
    inst = cls.default_instance()
    layout = ASSET_LAYOUT[cls.__name__]
    if "ticker" in layout["scalars"]:
        assert inst.ticker == ""
    if "about" in layout["scalars"]:
        assert inst.about == ""
    if "sector" in layout["scalars"]:
        assert inst.sector == CANONICAL_SECTORS.index("Other")


@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_default_instance_frames_are_empty_with_correct_schema(cls):
    inst = cls.default_instance()
    layout = ASSET_LAYOUT[cls.__name__]
    for frame_field, schema_name in layout["frames"].items():
        df = getattr(inst, frame_field)
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0
        assert dict(df.schema) == SCHEMAS[schema_name]


def test_stockdata_sector_default_is_other():
    s = StockData.default_instance()
    assert CANONICAL_SECTORS[s.sector] == "Other"


# ── Schema registry ───────────────────────────────────────────────────────────

def test_quarterly_schema_has_signed_qp_columns():
    q = SCHEMAS["financials_quarterly"]
    assert "eps_estimate_high_qp_m8" in q
    assert "eps_estimate_high_qp_0" in q
    assert "eps_estimate_high_qp_p4" in q
    assert q["eps_estimate_high_qp_m8"] == pl.Float32


def test_annual_schema_has_signed_ap_columns():
    a = SCHEMAS["financials_annually"]
    assert "eps_estimate_average_ap_m2" in a
    assert "eps_estimate_average_ap_0" in a
    assert "eps_estimate_average_ap_p1" in a


def test_no_hyphen_columns_in_financials_schemas():
    """Sign letters replaced literal minus signs; nothing should still contain '-'."""
    bad_q = [c for c in SCHEMAS["financials_quarterly"] if "-" in c]
    bad_a = [c for c in SCHEMAS["financials_annually"] if "-" in c]
    assert bad_q == []
    assert bad_a == []


def test_qp_ap_columns_match_documented_regex():
    qp_re = re.compile(r"_qp_(m|p)?(\d+)$")
    ap_re = re.compile(r"_ap_(m|p)?(\d+)$")
    qp_cols = [c for c in SCHEMAS["financials_quarterly"] if "_qp_" in c]
    ap_cols = [c for c in SCHEMAS["financials_annually"] if "_ap_" in c]
    # Sanity: schemas actually contain qp/ap columns
    assert qp_cols
    assert ap_cols
    for c in qp_cols:
        assert qp_re.search(c), c
    for c in ap_cols:
        assert ap_re.search(c), c


def test_quarterly_qm_includes_categorical_reportTime():
    q = SCHEMAS["financials_quarterly"]
    assert q["reportTime_qm0"] == pl.Categorical
    # Annual variant excludes reportTime per spec
    a = SCHEMAS["financials_annually"]
    assert not any(c.startswith("reportTime_") for c in a)


def test_annual_days_to_fiscalDateEnding_is_float32():
    a = SCHEMAS["financials_annually"]
    assert a["days_to_fiscalDateEnding_am0"] == pl.Float32


def test_etf_profile_holdings_is_list_of_struct():
    holdings_dtype = SCHEMAS["etf_profile"]["holdings"]
    expected = pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32}))
    assert holdings_dtype == expected


# ── to_dict / from_dict ───────────────────────────────────────────────────────

@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_to_dict_from_dict_roundtrip_preserves_scalars(cls):
    inst = cls.default_instance()
    inst.ticker = "TEST"
    inst.about = "Test asset"
    if hasattr(inst, "sector"):
        inst.sector = 3
    out = inst.from_dict(inst.to_dict())
    assert out.ticker == "TEST"
    assert out.about == "Test asset"
    if hasattr(inst, "sector"):
        assert out.sector == 3


def test_from_dict_restores_frame_schemas():
    s = StockData.default_instance()
    s.ticker = "AAPL"
    s2 = StockData.from_dict(s.to_dict())
    assert dict(s2.shareprice_daily.schema) == SCHEMAS["shareprice_daily"]
    assert dict(s2.financials_quarterly.schema) == SCHEMAS["financials_quarterly"]


# ── copy ──────────────────────────────────────────────────────────────────────

def test_copy_returns_independent_instance():
    s = StockData.default_instance()
    s.ticker = "AAPL"
    s.sector = 9
    s_copy = s.copy()
    s_copy.ticker = "MSFT"
    s_copy.sector = 0
    assert s.ticker == "AAPL"
    assert s.sector == 9


def test_copy_clones_dataframes():
    s = StockData.default_instance()
    s.shareprice_daily = pl.DataFrame(
        [{"Date": None, "Open": 1.0, "High": 1.0, "Low": 1.0, "Close": 1.0,
          "Volume": 1.0, "DividendAmount": 0.0, "SplitCoefficient": 1.0,
          "AdjFactor": 1.0}],
        schema=SCHEMAS["shareprice_daily"],
    )
    s_copy = s.copy()
    assert s_copy.shareprice_daily.equals(s.shareprice_daily)
    # Mutating the copy's frame must not affect the original
    s_copy.shareprice_daily = s_copy.shareprice_daily.with_columns(
        pl.col("Open") * 2
    )
    assert s.shareprice_daily["Open"][0] == 1.0


# ── save_to / load_from ───────────────────────────────────────────────────────

@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_save_to_writes_metadata_and_one_parquet_per_frame(cls, tmp_path):
    inst = cls.default_instance()
    out_dir = tmp_path / "asset"
    inst.save_to(out_dir)
    layout = ASSET_LAYOUT[cls.__name__]
    assert (out_dir / "metadata.json").exists()
    for frame_field in layout["frames"]:
        assert (out_dir / f"{frame_field}.parquet").exists()


@pytest.mark.parametrize("cls", ALL_CLASSES)
def test_save_to_load_from_roundtrip(cls, tmp_path):
    inst = cls.default_instance()
    inst.ticker = "TKR"
    inst.about = "Round-trip"
    if hasattr(inst, "sector"):
        inst.sector = 5
    out_dir = tmp_path / "asset"
    inst.save_to(out_dir)
    loaded = cls.load_from(out_dir)
    assert loaded.ticker == "TKR"
    assert loaded.about == "Round-trip"
    if hasattr(inst, "sector"):
        assert loaded.sector == 5


def test_load_from_uses_default_frame_when_parquet_missing(tmp_path):
    """If a frame parquet is missing, load_from should fall back to an empty-schema frame."""
    s = StockData.default_instance()
    s.ticker = "AAPL"
    out_dir = tmp_path / "aapl"
    s.save_to(out_dir)
    (out_dir / "shareprice_daily.parquet").unlink()
    loaded = StockData.load_from(out_dir)
    assert loaded.shareprice_daily.height == 0
    assert dict(loaded.shareprice_daily.schema) == SCHEMAS["shareprice_daily"]


def test_etf_holdings_list_struct_roundtrip(tmp_path):
    """Verify the List(Struct) holdings column survives parquet round-trip."""
    e = ETFData.default_instance()
    e.ticker = "SPY"
    e.etf_profile = pl.DataFrame(
        [{
            "Date": None,
            "information_technology": 0.30, "communication_services": None,
            "consumer_discretionary": None, "consumer_staples": None,
            "healthcare": None, "industrials": None, "utilities": None,
            "materials": None, "energy": None, "financials": None,
            "real_estate": None, "other": 0.0,
            "holdings": [
                {"symbol": "AAPL", "weight": 0.07},
                {"symbol": "MSFT", "weight": 0.06},
            ],
            "net_assets": None, "net_expense_ratio": None,
            "portfolio_turnover": None, "dividend_yield": None,
            "leveraged": "NO",
        }],
        schema=SCHEMAS["etf_profile"],
    )
    out_dir = tmp_path / "spy"
    e.save_to(out_dir)
    loaded = ETFData.load_from(out_dir)
    holdings = loaded.etf_profile["holdings"].to_list()[0]
    assert {h["symbol"] for h in holdings} == {"AAPL", "MSFT"}
    assert pytest.approx(0.07, rel=1e-3) == next(
        h["weight"] for h in holdings if h["symbol"] == "AAPL"
    )
