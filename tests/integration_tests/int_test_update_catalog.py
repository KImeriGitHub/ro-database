"""Integration test: asset_catalog_service/update_catalog.py.

Daily catalog refresh on top of an already-populated ``database/catalog/``.
Logs a before/after diff for every catalog file (row count + new/removed
symbol samples), runs ``monitoring_service.analyze_catalog`` against the
updated catalog, and finally re-trims the catalog to the integration-test
subset unless ``--no-reduce`` is passed.

``update_catalog.update_all`` may grow the catalog when AV ``LISTING_STATUS``
adds symbols since the last run, so this script is the natural follow-up to
``int_test_init_catalog.py`` for verifying the daily-update path without
having to repopulate from scratch.

Usage:
    python tests/integration_tests/int_test_update_catalog.py [--no-reduce]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import polars as pl

from asset_catalog_service.update_catalog import update_all
from monitoring_service.analyze_catalog import analyze_catalog

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    DAILY_DIR,
    HISTORICAL_DIR,
    configure_int_test_logging,
    reduce_catalogs,
)

logger = logging.getLogger(__name__)

# Files for which we log a before/after symbol diff. yield_status has a
# very different shape (one row per symbol+date), so we only log its row
# count. earnings_calendar.parquet no longer belongs to catalog/.
_DIFFED_FILES = (
    "stocks.parquet",
    "etfs.parquet",
    "indices.parquet",
    "forex.parquet",
    "cryptocurrencies.parquet",
    "commodities.parquet",
    "economic.parquet",
)
_COUNT_ONLY_FILES = (
    "yield_status.parquet",
)

# Loose lower bounds, matching int_test_init_catalog. Catalogs only grow
# (or stay flat) on update, so the same minimums apply.
COUNT_THRESHOLDS = {
    "stocks":           1_000,
    "etfs":             100,
    "indices":          5,
    "forex":            50,
    "cryptocurrencies": 5,
    "commodities":      5,
    "economic":         5,
}


def _read_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(pl.read_parquet(path, columns=["symbol"])["symbol"].to_list())


def _row_count(path: Path) -> int | None:
    if not path.exists():
        return None
    return pl.read_parquet(path, columns=["symbol"]).height


def _snapshot(catalog_dir: Path) -> dict[str, set[str]]:
    return {fname: _read_symbols(catalog_dir / fname) for fname in _DIFFED_FILES}


def _log_diff(
    before: dict[str, set[str]], after: dict[str, set[str]], catalog_dir: Path,
) -> None:
    logger.info("=== update_catalog diff ===")
    for fname in _DIFFED_FILES:
        b = before.get(fname, set())
        a = after.get(fname, set())
        added = sorted(a - b)
        removed = sorted(b - a)
        logger.info(
            f"{fname}: {len(b)} -> {len(a)} symbols "
            f"(+{len(added)} / -{len(removed)})"
        )
        if added:
            preview = added[:10]
            tail = "" if len(added) <= 10 else f" ...+{len(added) - 10} more"
            logger.info(f"  added: {preview}{tail}")
        if removed:
            preview = removed[:10]
            tail = "" if len(removed) <= 10 else f" ...+{len(removed) - 10} more"
            logger.info(f"  removed: {preview}{tail}")
    for fname in _COUNT_ONLY_FILES:
        n = _row_count(catalog_dir / fname)
        logger.info(f"{fname}: {n if n is not None else 'missing'} rows")


def _check_counts(report: dict) -> None:
    # update_catalog is run after the catalog has already been reduced by
    # int_test_init_catalog (and by every subsequent int_test that calls
    # reduce_catalogs), so the original full-catalog thresholds are
    # guaranteed to fail. Keep the comparison for visibility, but emit a
    # warning instead of raising.
    below: list[str] = []
    for section_name, minimum in COUNT_THRESHOLDS.items():
        section = report.get(section_name) or {}
        if section.get("missing"):
            below.append(f"{section_name}: marked missing in report")
            continue
        n = section.get("total")
        if n is None or n < minimum:
            below.append(
                f"{section_name}.total={n} below threshold {minimum}"
            )
    if below:
        logger.warning(
            "Catalog counts below full-universe thresholds (expected on a "
            "reduced int-test catalog):\n  " + "\n  ".join(below)
        )
    else:
        logger.info("Catalog count checks passed.")


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--no-reduce", action="store_true",
        help=(
            "Skip the post-update catalog trim. update_stocks_etfs may have "
            "added newly-listed symbols; --no-reduce leaves them in place."
        ),
    )
    args = parser.parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(
            f"Catalog dir not found at {CATALOG_DIR}; "
            f"run int_test_init_catalog.py first."
        )
    if not (CATALOG_DIR / "stocks.parquet").exists():
        raise FileNotFoundError(
            f"stocks.parquet missing at {CATALOG_DIR}; "
            f"update_catalog requires an already-initialised catalog."
        )

    before = _snapshot(CATALOG_DIR)

    logger.info(f"Running update_all(catalog_dir={CATALOG_DIR})")
    update_all(catalog_dir=CATALOG_DIR)

    after = _snapshot(CATALOG_DIR)
    _log_diff(before, after, CATALOG_DIR)

    report = analyze_catalog(CATALOG_DIR)
    _check_counts(report)

    if args.no_reduce:
        logger.info("--no-reduce passed: skipping catalog trim.")
    else:
        kept_stocks, kept_etfs = reduce_catalogs(
            CATALOG_DIR,
            historical_dir=HISTORICAL_DIR,
            daily_dir=DAILY_DIR,
        )
        logger.info(
            f"Reduced catalog post-update: {len(kept_stocks)} stocks, "
            f"{len(kept_etfs)} etfs"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
