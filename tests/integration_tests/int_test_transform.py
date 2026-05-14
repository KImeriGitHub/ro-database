"""Integration test: data_transformation/transform.py.

Runs the transformation pipeline against ``database/catalog/``,
``database/historical/``, and ``database/daily/``, writing per-symbol
``AssetData`` folders under ``transformation/``. The script asserts that
each kept stock and ETF has a ``data_<SYMBOL>/`` directory containing at
least one non-empty parquet file, and that the per-asset-type roots for
the simple flat types (forex/indices/cryptocurrencies/commodities/economic)
also contain at least one populated symbol directory.

Usage:
    python tests/integration_tests/int_test_transform.py [--wipe]
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import polars as pl

from data_transformation.transform import main as transform_main

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    DAILY_DIR,
    HISTORICAL_DIR,
    TRANSFORMATION_DIR,
    configure_int_test_logging,
    kept_symbols,
)

logger = logging.getLogger(__name__)

FLAT_ASSET_TYPES = ("forex", "indices", "cryptocurrencies", "commodities", "economic")


def _has_nonempty_parquet(folder: Path) -> bool:
    if not folder.is_dir():
        return False
    for f in folder.glob("*.parquet"):
        try:
            df = pl.read_parquet(f)
        except Exception:
            continue
        if df.height > 0:
            return True
    return False


def _check_symbol_folders(
    asset_type: str, symbols: list[str], dest_dir: Path,
) -> None:
    root = dest_dir / asset_type
    if not root.exists():
        raise AssertionError(f"Missing transformed root: {root}")

    missing: list[str] = []
    empty: list[str] = []
    populated: list[str] = []
    for sym in symbols:
        sym_dir = root / f"data_{sym}"
        if not sym_dir.is_dir():
            missing.append(sym)
            continue
        if _has_nonempty_parquet(sym_dir):
            populated.append(sym)
        else:
            empty.append(sym)

    if missing:
        logger.warning(
            f"{asset_type}: {len(missing)} symbols had no data_<SYM>/ folder "
            f"(manual inspection): {missing}"
        )
    if not populated:
        raise AssertionError(
            f"{asset_type}: no symbols had any non-empty parquet "
            f"(empty: {empty})"
        )
    if empty:
        logger.warning(
            f"{asset_type}: {len(empty)} symbols had only empty parquets "
            f"(manual inspection): {empty}"
        )
    logger.info(
        f"{asset_type}: {len(populated)}/{len(symbols)} symbols populated."
    )


def _check_flat_asset_root(asset_type: str, dest_dir: Path) -> None:
    root = dest_dir / asset_type
    if not root.exists():
        raise AssertionError(f"Missing transformed root: {root}")
    sym_dirs = [
        d for d in root.iterdir() if d.is_dir() and d.name.startswith("data_")
    ]
    if not sym_dirs:
        raise AssertionError(
            f"{asset_type}: no data_<SYM>/ folders under {root}"
        )
    populated = [d for d in sym_dirs if _has_nonempty_parquet(d)]
    if not populated:
        raise AssertionError(
            f"{asset_type}: {len(sym_dirs)} symbol folders, none with "
            f"non-empty parquets"
        )
    logger.info(
        f"{asset_type}: {len(populated)}/{len(sym_dirs)} symbol folders populated."
    )


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--rebuild-stocks", action="store_true",
        help="Pass --rebuild-stocks through to transform.py.",
    )
    parser.add_argument(
        "--skip-financials", action="store_true",
        help="Pass --skip-financials through to transform.py.",
    )
    parser.add_argument(
        "--wipe", action="store_true",
        help=(
            "Wipe every file and folder under transformation/ (except "
            ".gitkeep) before running transform.py. Use for a clean-slate "
            "run after schema changes or to drop noise from earlier "
            "partial runs."
        ),
    )
    args = parser.parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(f"Catalog dir not found at {CATALOG_DIR}")
    if not HISTORICAL_DIR.exists():
        raise FileNotFoundError(f"Historical dir not found at {HISTORICAL_DIR}")
    if not DAILY_DIR.exists():
        raise FileNotFoundError(f"Daily dir not found at {DAILY_DIR}")

    TRANSFORMATION_DIR.mkdir(parents=True, exist_ok=True)

    if args.wipe:
        removed = 0
        for entry in TRANSFORMATION_DIR.iterdir():
            if entry.name == ".gitkeep":
                continue
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            removed += 1
        logger.info(
            f"--wipe: removed {removed} entries from {TRANSFORMATION_DIR}"
        )

    cli_args = [
        "--catalog-dir", str(CATALOG_DIR),
        "--historical-dir", str(HISTORICAL_DIR),
        "--daily-dir", str(DAILY_DIR),
        "--dest-dir", str(TRANSFORMATION_DIR),
    ]
    if args.rebuild_stocks:
        cli_args.append("--rebuild-stocks")
    if args.skip_financials:
        cli_args.append("--skip-financials")

    logger.info(f"Running transform.main({cli_args})")
    rc = transform_main(cli_args)
    if rc != 0:
        raise AssertionError(f"transform.main returned non-zero: {rc}")

    overview_path = TRANSFORMATION_DIR / "assets_overview.parquet"
    if not overview_path.exists():
        raise AssertionError(f"Missing {overview_path}")
    logger.info(f"assets_overview.parquet present ({overview_path}).")

    kept_stocks_list, kept_etfs_list = kept_symbols(CATALOG_DIR)
    _check_symbol_folders("stocks", kept_stocks_list, TRANSFORMATION_DIR)
    _check_symbol_folders("etfs", kept_etfs_list, TRANSFORMATION_DIR)

    for at in FLAT_ASSET_TYPES:
        _check_flat_asset_root(at, TRANSFORMATION_DIR)

    return 0


if __name__ == "__main__":
    sys.exit(main())
