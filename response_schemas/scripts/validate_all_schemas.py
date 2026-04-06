"""
Validate saved schemas by calling each endpoint and checking against saved schemas.

Requires that infer_all_schemas.py has been run first so that
response_schemas/schemas/*.json files exist.

Usage:
    python -m response_schemas.scripts.validate_all_schemas
    python -m response_schemas.scripts.validate_all_schemas --params symbol=AAPL
    python -m response_schemas.scripts.validate_all_schemas --category forex --params from_currency=EUR to_currency=GBP
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


def _resolve_params(ep_params: dict, cli_overrides: dict | None) -> dict:
    """Build validation params by merging CLI overrides into endpoint params.

    If cli_overrides is provided, any keys that overlap with ep_params are
    replaced.  Keys in cli_overrides that don't exist in ep_params are ignored
    so that unrelated endpoints fall back to their inference params unchanged.
    """
    if not cli_overrides:
        return dict(ep_params)
    merged = dict(ep_params)
    for key, value in cli_overrides.items():
        if key in merged:
            merged[key] = value
    return merged


def run(
    api_key: str,
    include_premium: bool,
    categories: list[str] | None,
    delay: float,
    param_overrides: dict | None = None,
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
        resolved = _resolve_params(ep["params"], param_overrides)
        params = {"function": name, "apikey": api_key, **resolved}
        logger.info(f"{tag} Calling {name} with params {resolved} ...")

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


def _parse_key_value(arg: str) -> tuple[str, str]:
    """Parse a 'key=value' string."""
    if "=" not in arg:
        raise argparse.ArgumentTypeError(
            f"Invalid format '{arg}', expected key=value"
        )
    key, value = arg.split("=", 1)
    return key, value


def main():
    parser = argparse.ArgumentParser(
        description="Validate saved schemas against live API responses.",
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
    parser.add_argument(
        "--params",
        nargs="+",
        metavar="KEY=VALUE",
        help="Override validation params (e.g. --params symbol=AAPL interval=1min). "
        "Only matching keys in each endpoint's params are overridden.",
    )
    args = parser.parse_args()

    param_overrides = None
    if args.params:
        param_overrides = dict(_parse_key_value(p) for p in args.params)

    api_key = get_alpha_vantage_key("premium")
    run(api_key, True, args.categories, args.delay, param_overrides)


if __name__ == "__main__":
    main()
