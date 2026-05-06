"""Diff a freshly built monitoring report against the previous one.

The previous report is whatever JSON file the orchestrator hands us. We don't
go and fetch it here -- callers are expected to have already located it
(e.g. by downloading ``daily/<previous-date>/monitoring_report.json`` from
GCS) before invoking the monitor.

The diff focuses on the load-bearing fields: catalog status counts, yield
True/False counts, ingestion issue totals, and coverage ok/total. Anything
that wasn't tracked previously is reported as ``previous=null``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_STATUSED = ("stocks", "etfs")
_STATUS_FIELDS = ("total", "active", "delisted", "corrupted")
_COUNT_ONLY = ("indices", "forex", "cryptocurrencies", "commodities", "economic")


def load_previous_report(path: Path | None) -> dict | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        logger.warning(f"Previous monitoring report at {path} is invalid JSON: {e}")
        return None


def _delta(curr: Any, prev: Any) -> Any:
    if isinstance(curr, (int, float)) and isinstance(prev, (int, float)):
        return curr - prev
    return None


def _diff_catalog(curr: dict, prev: dict) -> dict:
    out: dict = {}
    for name in _STATUSED:
        c = curr.get(name, {}) or {}
        p = prev.get(name, {}) or {}
        out[name] = {f: _delta(c.get(f), p.get(f)) for f in _STATUS_FIELDS}
    for name in _COUNT_ONLY:
        c = curr.get(name, {}) or {}
        p = prev.get(name, {}) or {}
        out[name] = {"total": _delta(c.get("total"), p.get("total"))}

    curr_yield = (curr.get("yield_status") or {}).get("endpoints") or {}
    prev_yield = (prev.get("yield_status") or {}).get("endpoints") or {}
    yield_diff: dict = {}
    for ep in sorted(set(curr_yield) | set(prev_yield)):
        c = curr_yield.get(ep, {}) or {}
        p = prev_yield.get(ep, {}) or {}
        yield_diff[ep] = {
            "true": _delta(c.get("true"), p.get("true")),
            "false": _delta(c.get("false"), p.get("false")),
        }
    out["yield_status"] = yield_diff
    return out


def _diff_ingestion(curr: dict, prev: dict) -> dict:
    fields = ("timezone_mismatch", "av_throttle", "total_issues")
    out: dict = {f: _delta(curr.get(f), prev.get(f)) for f in fields}
    for issue in ("structure_error", "empty_content", "cast_failure"):
        c_total = (curr.get(issue) or {}).get("total")
        p_total = (prev.get(issue) or {}).get("total")
        out[issue] = {"total": _delta(c_total, p_total)}
    return out


def _diff_coverage(curr: dict, prev: dict) -> dict:
    c = (curr.get("summary") or {})
    p = (prev.get("summary") or {})
    return {
        "total_checked": _delta(c.get("total_checked"), p.get("total_checked")),
        "intraday_ok": _delta(c.get("intraday_ok"), p.get("intraday_ok")),
        "daily_ok": _delta(c.get("daily_ok"), p.get("daily_ok")),
    }


def diff_reports(current: dict, previous: dict | None) -> dict:
    if previous is None:
        return {"previous_available": False}

    return {
        "previous_available": True,
        "previous_folder_date": previous.get("folder_date"),
        "previous_mode": previous.get("mode"),
        "catalog": _diff_catalog(
            current.get("catalog") or {}, previous.get("catalog") or {}
        ),
        "ingestion": _diff_ingestion(
            current.get("ingestion") or {}, previous.get("ingestion") or {}
        ),
        "coverage": _diff_coverage(
            current.get("coverage") or {}, previous.get("coverage") or {}
        ),
    }
