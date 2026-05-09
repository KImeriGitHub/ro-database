"""Standalone helper: reduce ``database/catalog/`` to the int-test subset.

Useful for re-trimming a catalog that was rebuilt to its full universe
(e.g. by re-running ``int_test_init_catalog.py --no-reduce``, or by a
``setup_daily`` / ``adjust_weekly`` finalize that appended new symbols),
without re-running the upstream pipeline.

Calls ``_helpers.reduce_catalogs`` against ``database/catalog/`` and logs
the final kept stocks and ETFs.

Usage:
    python tests/integration_tests/int_helper_reduce_catalog.py
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tests.integration_tests._helpers import (
    CATALOG_DIR,
    DAILY_DIR,
    HISTORICAL_DIR,
    configure_int_test_logging,
    reduce_catalogs,
)

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    configure_int_test_logging(__file__)
    argparse.ArgumentParser(description=__doc__.split("\n", 1)[0]).parse_args(argv)

    if not CATALOG_DIR.exists():
        raise FileNotFoundError(
            f"Catalog dir not found at {CATALOG_DIR}; "
            f"run int_test_init_catalog.py first."
        )

    kept_stocks, kept_etfs = reduce_catalogs(
        CATALOG_DIR,
        historical_dir=HISTORICAL_DIR,
        daily_dir=DAILY_DIR,
    )
    logger.info(
        f"Reduced catalog: {len(kept_stocks)} stocks, {len(kept_etfs)} etfs"
    )
    logger.info(f"Stocks kept: {kept_stocks}")
    logger.info(f"ETFs kept:   {kept_etfs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
