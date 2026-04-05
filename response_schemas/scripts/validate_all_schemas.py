"""
Validate saved schemas by calling each endpoint with a different symbol/params.

Requires that infer_all_schemas.py has been run first so that
response_schemas/schemas/*.json files exist.

Usage:
    python -m response_schemas.scripts.validate_all_schemas --tier standard
    python -m response_schemas.scripts.validate_all_schemas --tier premium --include-premium
    python -m response_schemas.scripts.validate_all_schemas --category fundamental
"""

import sys
import time
import argparse
import logging
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts.get_api_key import get_alpha_vantage_key
from response_schemas.schema_inferrer import load_schema
from response_schemas.schema_validator import validate_response
from response_schemas.scripts.endpoint_definitions import (
    ENDPOINTS,
    BASE_URL,
    ALL_CATEGORIES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"


def _is_api_error(data) -> str | None:
    if not isinstance(data, dict):
        return None
    if "Error Message" in data:
        return data["Error Message"]
    if "Note" in data:
        return data["Note"]
    if "Information" in data and len(data) == 1:
        return data["Information"]
    return None


def run(
    api_key: str,
    include_premium: bool,
    categories: list[str] | None,
    delay: float,
):
    selected = ENDPOINTS
    if categories:
        selected = [e for e in selected if e["category"] in categories]

    total = len(selected)
    ok, skipped, failed, violations_total = 0, 0, 0, 0

    for i, ep in enumerate(selected, 1):
        name = ep["function"]
        tag = f"[{i}/{total}]"

        if ep.get("csv_only"):
            logger.info(f"{tag} SKIP (CSV-only): {name}")
            skipped += 1
            continue
        if ep["premium"] and not include_premium:
            logger.info(f"{tag} SKIP (premium):  {name}")
            skipped += 1
            continue

        # Check that the schema exists
        schema_path = SCHEMAS_DIR / f"{name}.json"
        if not schema_path.exists():
            logger.warning(f"{tag} SKIP (no schema): {name}")
            skipped += 1
            continue

        schema = load_schema(name)
        params = {"function": name, "apikey": api_key, **ep["alt_params"]}
        logger.info(f"{tag} Calling {name} with alt params ...")

        try:
            resp = requests.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.error(f"{tag} FAIL {name}: {exc}")
            failed += 1
            time.sleep(delay)
            continue

        err = _is_api_error(data)
        if err:
            logger.error(f"{tag} FAIL {name}: {err}")
            failed += 1
            time.sleep(delay)
            continue

        violations = validate_response(data, schema)
        if violations:
            violations_total += len(violations)
            logger.warning(f"{tag} VIOLATIONS {name}: {len(violations)}")
            for v in violations:
                logger.warning(f"       {v}")
        else:
            logger.info(f"{tag} OK   {name}")
        ok += 1

        if i < total:
            time.sleep(delay)

    logger.info(
        f"Done — {ok} validated, {skipped} skipped, {failed} failed, "
        f"{violations_total} total violations"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Validate saved schemas against live API responses with alternate symbols.",
    )
    parser.add_argument(
        "--category",
        action="append",
        choices=ALL_CATEGORIES,
        dest="categories",
        help="Only process these categories (repeat for multiple)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds between API calls (default: 2)",
    )
    args = parser.parse_args()

    api_key = get_alpha_vantage_key("premium")
    run(api_key, True, args.categories, args.delay)


if __name__ == "__main__":
    main()
