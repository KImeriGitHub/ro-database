"""End-to-end tests for ``data_transformation/transform.py``.

Each test invokes the CLI as a subprocess, mirroring how the user would
run it. Synthesizes a small catalog tree + per-asset-type source files,
then asserts the destination layout, schemas, filters, resume, and
default-path behaviour.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from config import settings


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TRANSFORM_PY = REPO_ROOT / "data_transformation" / "transform.py"


# ── Shared catalog/source-tree builder ───────────────────────────────────────

def _build_synth_universe(
    root: Path,
    *,
    include_intraday: bool = True,
    include_etf_profile: bool = True,
) -> tuple[Path, Path, Path]:
    """Create a minimal universe: 1 stock, 1 etf, 1 forex, 1 index, 1 crypto,
    1 commodity, 1 economic indicator. Returns (catalog_dir, historical_dir,
    daily_dir). dest_dir is the caller's responsibility.
    """
    cat = root / "catalog"
    cat.mkdir(parents=True, exist_ok=True)

    pl.DataFrame({
        "symbol": ["AAPL"], "name": ["Apple"], "sector": ["Technology"],
    }).write_parquet(cat / "stocks.parquet")
    pl.DataFrame({"symbol": ["SPY"], "name": ["SPDR"]}).write_parquet(cat / "etfs.parquet")
    pl.DataFrame({"symbol": ["EURUSD"], "name": ["Euro"]}).write_parquet(cat / "forex.parquet")
    pl.DataFrame({"symbol": ["SPX"], "name": ["S&P 500"]}).write_parquet(cat / "indices.parquet")
    pl.DataFrame({"symbol": ["BTC"], "name": ["Bitcoin"]}).write_parquet(cat / "cryptocurrencies.parquet")
    pl.DataFrame({"symbol": ["WTI"], "name": ["Crude"]}).write_parquet(cat / "commodities.parquet")
    pl.DataFrame({"symbol": ["CPI"], "name": ["CPI"]}).write_parquet(cat / "economic.parquet")

    historical = root / "historical"
    daily = root / "daily"

    # Stocks: prices_daily (+ optional intraday)
    daily_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        "DividendAmount": pl.Float32, "SplitCoefficient": pl.Float32,
    }
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 100.0, "High": 100.0, "Low": 100.0,
         "Close": 100.0, "Volume": 1000.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(p)

    if include_intraday:
        intra_schema = {
            "Date": pl.Datetime, "Open": pl.Float32, "High": pl.Float32,
            "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        }
        pi = historical / "stocks" / "prices" / "stocks_AAPL.parquet"
        pi.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([
            {"Date": datetime(2026, 4, 15, 9, 30), "Open": 100.0, "High": 101.0,
             "Low": 99.0, "Close": 100.0, "Volume": 500.0},
        ], schema=intra_schema).write_parquet(pi)

    # ETF: prices_daily (+ optional profile)
    pe = historical / "etfs" / "prices_daily" / "etfs_SPY.parquet"
    pe.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 300.0, "High": 300.0, "Low": 300.0,
         "Close": 300.0, "Volume": 100000.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(pe)

    if include_etf_profile:
        prof_schema = {
            "date": pl.Date,
            "information_technology": pl.Float32, "communication_services": pl.Float32,
            "consumer_discretionary": pl.Float32, "consumer_staples": pl.Float32,
            "healthcare": pl.Float32, "industrials": pl.Float32, "utilities": pl.Float32,
            "materials": pl.Float32, "energy": pl.Float32, "financials": pl.Float32,
            "real_estate": pl.Float32, "other": pl.Float32,
            "holdings": pl.List(pl.Struct({"symbol": pl.Utf8, "weight": pl.Float32})),
            "net_assets": pl.Float32, "net_expense_ratio": pl.Float32,
            "portfolio_turnover": pl.Float32, "dividend_yield": pl.Float32,
            "inception_date": pl.Utf8, "leveraged": pl.Utf8,
        }
        pp = historical / "etfs" / "etf_profile" / "etfs_SPY.parquet"
        pp.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([{
            "date": date(2026, 4, 15),
            "information_technology": 0.30, "communication_services": 0.10,
            "consumer_discretionary": 0.10, "consumer_staples": 0.05,
            "healthcare": 0.13, "industrials": 0.08, "utilities": 0.03,
            "materials": 0.02, "energy": 0.03, "financials": 0.13,
            "real_estate": 0.02, "other": 0.01,
            "holdings": [{"symbol": "AAPL", "weight": 0.07}],
            "net_assets": 4.2e11, "net_expense_ratio": 0.0009,
            "portfolio_turnover": None, "dividend_yield": 0.014,
            "inception_date": "1993-01-22", "leveraged": "NO",
        }], schema=prof_schema).write_parquet(pp)

    # Forex
    fx_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32,
    }
    pf = historical / "forex" / "forex_EURUSD.parquet"
    pf.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": date(2026, 4, 15), "Open": 1.10, "High": 1.11,
                   "Low": 1.09, "Close": 1.105}], schema=fx_schema).write_parquet(pf)

    # Indices (same shape as forex)
    pi = historical / "indices" / "indices_SPX.parquet"
    pi.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": date(2026, 4, 15), "Open": 5000.0, "High": 5050.0,
                   "Low": 4990.0, "Close": 5025.0}], schema=fx_schema).write_parquet(pi)

    # Crypto
    cr_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
    }
    pc = historical / "cryptocurrencies" / "cryptocurrencies_BTC.parquet"
    pc.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": date(2026, 4, 15), "Open": 65000.0, "High": 66000.0,
                   "Low": 64000.0, "Close": 65500.0, "Volume": 1000.0}],
                 schema=cr_schema).write_parquet(pc)

    # Commodity (Date, value, unit)
    co_schema = {"Date": pl.Date, "value": pl.Float32, "unit": pl.Utf8}
    pco = historical / "commodities" / "commodities_WTI.parquet"
    pco.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": date(2026, 4, 15), "value": 73.0,
                   "unit": "dollars per barrel"}],
                 schema=co_schema).write_parquet(pco)

    # Economic (Date, value)
    ec_schema = {"Date": pl.Date, "value": pl.Float32}
    pec = historical / "economic" / "economic_CPI.parquet"
    pec.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"Date": date(2026, 4, 15), "value": 320.0}],
                 schema=ec_schema).write_parquet(pec)

    return cat, historical, daily


def _run_cli(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(TRANSFORM_PY), *args]
    return subprocess.run(
        cmd, capture_output=True, text=True, cwd=cwd or REPO_ROOT,
    )


# ── 1. End-to-end happy path ──────────────────────────────────────────────────

def test_end_to_end_happy_path(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    dest = tmp_path / "transformed"

    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
    )
    assert r.returncode == 0, f"stderr: {r.stderr}"

    # Top-level artifacts.
    assert (dest / "assets_overview.parquet").exists()
    assert (dest / "transformation_report.parquet").exists()

    # data_<SYMBOL>/ folders for each asset with source files.
    expected = [
        ("stocks", "AAPL"),
        ("etfs", "SPY"),
        ("forex", "EURUSD"),
        ("indices", "SPX"),
        ("cryptocurrencies", "BTC"),
        ("commodities", "WTI"),
        ("economic", "CPI"),
    ]
    for at, sym in expected:
        d = dest / at / f"data_{sym}"
        assert (d / "metadata.json").exists(), f"missing metadata for {at}/{sym}"

    # Stocks dataclass round-trips with both shareprice_daily and intraday.
    md = json.loads((dest / "stocks" / "data_AAPL" / "metadata.json").read_text())
    assert md["_asset_type"] == "StockData"
    assert md["ticker"] == "AAPL"
    assert (dest / "stocks" / "data_AAPL" / "shareprice_daily.parquet").exists()
    assert (dest / "stocks" / "data_AAPL" / "shareprice_intraday.parquet").exists()

    # ETFs carry etf_profile.
    assert (dest / "etfs" / "data_SPY" / "etf_profile.parquet").exists()


# ── 2. --asset-types filter ───────────────────────────────────────────────────

def test_asset_types_filter_only_creates_requested_trees(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    dest = tmp_path / "transformed"
    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
    )
    assert r.returncode == 0, r.stderr
    assert (dest / "stocks" / "data_AAPL").is_dir()
    # Other asset types must not have any per-symbol folder.
    for at in ("etfs", "forex", "indices", "cryptocurrencies",
               "commodities", "economic"):
        at_dir = dest / at
        if at_dir.exists():
            assert list(at_dir.iterdir()) == [], f"{at}/ should be empty"


# ── 3. --symbols filter ───────────────────────────────────────────────────────

def test_symbols_filter_restricts_to_named_symbols(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    # Add a second stock so the filter has something to exclude.
    pl.DataFrame({
        "symbol": ["AAPL", "MSFT"], "name": ["Apple", "Microsoft"],
        "sector": ["Technology", "Technology"],
    }).write_parquet(cat / "stocks.parquet")
    daily_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        "DividendAmount": pl.Float32, "SplitCoefficient": pl.Float32,
    }
    p = historical / "stocks" / "prices_daily" / "stocks_MSFT.parquet"
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 200.0, "High": 200.0, "Low": 200.0,
         "Close": 200.0, "Volume": 1000.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(p)

    dest = tmp_path / "transformed"
    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--symbols", "AAPL",
    )
    assert r.returncode == 0, r.stderr
    assert (dest / "stocks" / "data_AAPL").is_dir()
    assert not (dest / "stocks" / "data_MSFT").exists()


# ── 4. Resume across runs ─────────────────────────────────────────────────────

def test_resume_does_not_overwrite_existing_symbol(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    dest = tmp_path / "transformed"

    r1 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
    )
    assert r1.returncode == 0, r1.stderr
    sd_path = dest / "stocks" / "data_AAPL" / "shareprice_daily.parquet"
    first_close = pl.read_parquet(sd_path)["Close"][0]

    # Mutate the source close.
    daily_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        "DividendAmount": pl.Float32, "SplitCoefficient": pl.Float32,
    }
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 999.0, "High": 999.0, "Low": 999.0,
         "Close": 999.0, "Volume": 1.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(p)

    r2 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
    )
    assert r2.returncode == 0, r2.stderr
    second_close = pl.read_parquet(sd_path)["Close"][0]
    assert first_close == second_close == 100.0  # resume preserved original


# ── 5. Default --dest-dir resolves under PROJECT_ROOT ─────────────────────────

def test_default_dest_dir_resolves_to_project_root_transformed():
    """Sanity-check that the CLI's default ``--dest-dir`` is
    ``<PROJECT_ROOT>/transformed/``. We only inspect ``settings.TRANSFORMED_DIR``
    here; we do not actually launch the CLI without arguments because it
    would write into the live repo."""
    assert settings.TRANSFORMED_DIR == settings.PROJECT_ROOT / "transformed"


# ── 6. Phase 6 orchestrator wiring ────────────────────────────────────────────

_INSIDER_SOURCE_SCHEMA = {
    "transactionDate": pl.Date,
    "executive": pl.Utf8,
    "executive_title": pl.Utf8,
    "security_type": pl.Utf8,
    "acquisition_or_disposal": pl.Utf8,
    "shares": pl.Float32,
    "share_price": pl.Float32,
}

_SENT_TOPIC_COLS = (
    "blockchain", "earnings", "ipo", "mergers_and_acquisitions",
    "financial_markets", "economy_fiscal", "economy_monetary",
    "economy_macro", "energy_transportation", "finance",
    "life_sciences", "manufacturing", "real_estate",
    "retail_wholesale", "technology",
)
_SENT_SOURCE_SCHEMA: dict = {
    "time_published": pl.Datetime("us"),
    "ticker": pl.Utf8,
    "ticker_relevance_score": pl.Float32,
    "ticker_sentiment_score": pl.Float32,
    "ticker_sentiment_label": pl.Utf8,
    "title": pl.Utf8, "url": pl.Utf8, "authors": pl.Utf8,
    "summary": pl.Utf8, "banner_image": pl.Utf8,
    "source": pl.Utf8, "category_within_source": pl.Utf8,
    "source_domain": pl.Utf8,
    "overall_sentiment_score": pl.Float32,
    "overall_sentiment_label": pl.Utf8,
    **{t: pl.Float32 for t in _SENT_TOPIC_COLS},
}


def _write_insider_for(historical: Path, symbol: str = "AAPL") -> None:
    p = historical / "stocks" / "insider" / f"stocks_{symbol}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{
        "transactionDate": date(2026, 4, 10),
        "executive": "Jane Doe",
        "executive_title": "CFO",
        "security_type": "Common Stock",
        "acquisition_or_disposal": "A",
        "shares": 100.0,
        "share_price": 50.0,
    }], schema=_INSIDER_SOURCE_SCHEMA).write_parquet(p)


def _write_sentiment_for(historical: Path, symbol: str = "AAPL") -> None:
    p = historical / "stocks" / "sentiment" / f"stocks_{symbol}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "time_published": datetime(2026, 4, 12, 9, 30),
        "ticker": symbol,
        "ticker_relevance_score": 0.5,
        "ticker_sentiment_score": 0.1,
        "ticker_sentiment_label": "Neutral",
        "title": "An article",
        "url": "https://example.com/a",
        "authors": "Some Author",
        "summary": "A summary.",
        "banner_image": None,
        "source": "Reuters",
        "category_within_source": "n/a",
        "source_domain": "reuters.com",
        "overall_sentiment_score": 0.05,
        "overall_sentiment_label": "Neutral",
    }
    for t in _SENT_TOPIC_COLS:
        row[t] = 0.0
    pl.DataFrame([row], schema=_SENT_SOURCE_SCHEMA).write_parquet(p)


def test_orchestrator_builds_insider_and_sentiment_before_save(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    _write_insider_for(historical)
    _write_sentiment_for(historical)
    dest = tmp_path / "transformed"

    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
    )
    assert r.returncode == 0, r.stderr

    sym_dir = dest / "stocks" / "data_AAPL"
    assert (sym_dir / "insider_df.parquet").exists()
    assert (sym_dir / "sentiment_df.parquet").exists()
    insider = pl.read_parquet(sym_dir / "insider_df.parquet")
    sentiment = pl.read_parquet(sym_dir / "sentiment_df.parquet")
    assert insider.height == 1
    assert sentiment.height == 1


def test_orchestrator_includes_symbol_with_only_insider(tmp_path):
    """A stock with no prices but with insider data still gets a
    data_<SYM>/ folder, with empty shareprice frames and a populated
    insider_df."""
    cat = tmp_path / "catalog"
    historical = tmp_path / "historical"
    daily = tmp_path / "daily"
    cat.mkdir(parents=True, exist_ok=True)

    pl.DataFrame({
        "symbol": ["AAPL"], "name": ["Apple"], "sector": ["Technology"],
    }).write_parquet(cat / "stocks.parquet")
    for at in ("etfs", "forex", "indices", "cryptocurrencies",
               "commodities", "economic"):
        pl.DataFrame({"symbol": [], "name": []},
                     schema={"symbol": pl.Utf8, "name": pl.Utf8}
                     ).write_parquet(cat / f"{at}.parquet")

    _write_insider_for(historical, "AAPL")

    dest = tmp_path / "transformed"
    r = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--skip-financials",
    )
    assert r.returncode == 0, r.stderr

    sym_dir = dest / "stocks" / "data_AAPL"
    assert (sym_dir / "metadata.json").exists()
    assert pl.read_parquet(sym_dir / "shareprice_daily.parquet").height == 0
    assert pl.read_parquet(sym_dir / "shareprice_intraday.parquet").height == 0
    assert pl.read_parquet(sym_dir / "insider_df.parquet").height == 1


def test_rebuild_stocks_wipes_stocks_and_rebuilds(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    dest = tmp_path / "transformed"

    # First run.
    r1 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--skip-financials",
    )
    assert r1.returncode == 0, r1.stderr
    sd_path = dest / "stocks" / "data_AAPL" / "shareprice_daily.parquet"
    assert pl.read_parquet(sd_path)["Close"][0] == 100.0

    # Mutate the source.
    daily_schema = {
        "Date": pl.Date, "Open": pl.Float32, "High": pl.Float32,
        "Low": pl.Float32, "Close": pl.Float32, "Volume": pl.Float32,
        "DividendAmount": pl.Float32, "SplitCoefficient": pl.Float32,
    }
    p = historical / "stocks" / "prices_daily" / "stocks_AAPL.parquet"
    pl.DataFrame([
        {"Date": date(2026, 4, 15), "Open": 250.0, "High": 250.0, "Low": 250.0,
         "Close": 250.0, "Volume": 1000.0, "DividendAmount": 0.0,
         "SplitCoefficient": 1.0},
    ], schema=daily_schema).write_parquet(p)

    r2 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--asset-types", "stocks",
        "--rebuild-stocks",
        "--skip-financials",
    )
    assert r2.returncode == 0, r2.stderr
    assert pl.read_parquet(sd_path)["Close"][0] == 250.0


def test_rebuild_stocks_does_not_touch_other_asset_trees(tmp_path):
    cat, historical, daily = _build_synth_universe(tmp_path)
    dest = tmp_path / "transformed"
    r1 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--skip-financials",
    )
    assert r1.returncode == 0, r1.stderr
    # Capture mtimes of non-stocks per-symbol metadata.json before rebuild.
    targets = [
        dest / "etfs" / "data_SPY" / "metadata.json",
        dest / "forex" / "data_EURUSD" / "metadata.json",
        dest / "indices" / "data_SPX" / "metadata.json",
        dest / "cryptocurrencies" / "data_BTC" / "metadata.json",
        dest / "commodities" / "data_WTI" / "metadata.json",
        dest / "economic" / "data_CPI" / "metadata.json",
    ]
    pre = {t: t.stat().st_mtime_ns for t in targets if t.exists()}

    r2 = _run_cli(
        "--catalog-dir", str(cat),
        "--historical-dir", str(historical),
        "--daily-dir", str(daily),
        "--dest-dir", str(dest),
        "--rebuild-stocks",
        "--skip-financials",
    )
    assert r2.returncode == 0, r2.stderr
    post = {t: t.stat().st_mtime_ns for t in targets if t.exists()}
    # Non-stocks artefacts unchanged.
    assert pre == post
    # Stocks artefacts re-emitted and intact.
    assert (dest / "stocks" / "data_AAPL" / "metadata.json").exists()
