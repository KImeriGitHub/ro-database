"""Integration test: asset_catalog_service/init_catalog.py.

Initialises ``database/catalog/`` from the local FRD CSVs, runs
``monitoring_service.analyze_catalog`` against the result, asserts the
expected files are present with non-trivial counts, then trims the catalog
to the integration-test subset so the downstream int_test scripts (historical,
daily, weekly, transform) only operate on a small number of symbols.

Real Alpha Vantage calls are made (LISTING_STATUS, OVERVIEW for new stock
sectors, indices/forex/crypto/commodities/economic catalogs, earnings
calendar). The catalog is left in place after the run for manual inspection
and for the next int_test in the chain.

Usage:
    python tests/integration_tests/int_test_init_catalog.py [--wipe] [--no-reduce]
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from asset_catalog_service.init_catalog import init_all
from monitoring_service.analyze_catalog import analyze_catalog

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    FRD_DIR,
    configure_int_test_logging,
    reduce_catalogs,
)

logger = logging.getLogger(__name__)

# Loose lower bounds for "not too low". These are *catalog* row counts, not
# data quality checks; thresholds are well below realistic values so they
# only trip when something is actually broken.
EXPECTED_FILES = {
    "stocks.parquet":            ("total", 1_000),
    "etfs.parquet":              ("total", 100),
    "indices.parquet":           ("total", 5),
    "forex.parquet":             ("total", 50),
    "cryptocurrencies.parquet":  ("total", 5),
    "commodities.parquet":       ("total", 5),
    "economic.parquet":          ("total", 5),
    # yield_status & earnings_calendar are required to exist but their row
    # counts are derived from the catalogs above; checking presence is enough.
    "yield_status.parquet":      None,
    "earnings_calendar.parquet": None,
}


def _check_files(catalog_dir: Path) -> None:
    missing = [
        fname for fname in EXPECTED_FILES
        if not (catalog_dir / fname).exists()
    ]
    if missing:
        raise AssertionError(
            f"Missing catalog files after init: {missing}"
        )
    logger.info(f"All {len(EXPECTED_FILES)} expected catalog files present.")


def _check_counts(report: dict) -> None:
    failures: list[str] = []
    for fname, threshold in EXPECTED_FILES.items():
        if threshold is None:
            continue
        key, minimum = threshold
        section_name = fname.removesuffix(".parquet")
        section = report.get(section_name) or {}
        if section.get("missing"):
            failures.append(f"{section_name}: marked missing in report")
            continue
        n = section.get(key)
        if n is None or n < minimum:
            failures.append(
                f"{section_name}.{key}={n} below threshold {minimum}"
            )
    if failures:
        raise AssertionError(
            "Catalog count checks failed:\n  " + "\n  ".join(failures)
        )
    logger.info("Catalog count checks passed.")


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--wipe", action="store_true",
        help="Remove database/catalog/ before initialisation.",
    )
    parser.add_argument(
        "--no-reduce", action="store_true",
        help=(
            "Skip the post-init catalog trim. Use this if you want to inspect "
            "the full catalog or run the next step against the entire universe."
        ),
    )
    args = parser.parse_args(argv)

    catalog_dir = CATALOG_DIR
    frd_dir = FRD_DIR

    if not (frd_dir / "catalog_stocks.csv").exists():
        raise FileNotFoundError(
            f"FRD stocks catalog missing at {frd_dir / 'catalog_stocks.csv'}"
        )
    if not (frd_dir / "catalog_etfs.csv").exists():
        raise FileNotFoundError(
            f"FRD ETFs catalog missing at {frd_dir / 'catalog_etfs.csv'}"
        )

    if args.wipe and catalog_dir.exists():
        logger.info(f"Wiping existing catalog at {catalog_dir}")
        shutil.rmtree(catalog_dir)

    catalog_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Running init_all(catalog_dir={catalog_dir}, frd_dir={frd_dir})")
    init_all(
        catalog_dir=catalog_dir,
        stocks_dir=frd_dir,
        etfs_dir=frd_dir,
    )

    _check_files(catalog_dir)

    report = analyze_catalog(catalog_dir)
    _check_counts(report)

    if args.no_reduce:
        logger.info("--no-reduce passed: skipping catalog trim.")
    else:
        kept_stocks, kept_etfs = reduce_catalogs(catalog_dir)
        logger.info(
            f"Reduced catalog: {len(kept_stocks)} stocks, {len(kept_etfs)} etfs"
        )
        logger.info(f"Stocks kept: {kept_stocks}")
        logger.info(f"ETFs kept:   {kept_etfs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
