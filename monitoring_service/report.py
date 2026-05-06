"""Assemble the four analyzers into a report, log a summary, write to disk."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

from monitoring_service.analyze_catalog import analyze_catalog
from monitoring_service.analyze_coverage import analyze_coverage
from monitoring_service.analyze_files import analyze_files, analyze_storage
from monitoring_service.analyze_ingestion import analyze_ingestion
from monitoring_service.diff import diff_reports, load_previous_report

logger = logging.getLogger(__name__)

REPORT_FILENAME_JSON = "monitoring_report.json"
REPORT_FILENAME_MD = "monitoring_report.md"


def build_report(
    *,
    mode: str,
    folder_date: date,
    catalog_dir: Path,
    folder_dir: Path,
    previous_report: dict | None = None,
    api_call_count: int | None = None,
) -> dict:
    """Build the full monitoring-report dict.

    *folder_dir* is the daily/historical folder that contains
    ``ingestion_report.parquet`` and the per-asset_type subtrees.
    """
    catalog = analyze_catalog(catalog_dir, today=folder_date)
    ingestion = analyze_ingestion(folder_dir / "ingestion_report.parquet")
    coverage = analyze_coverage(folder_dir)
    file_counts = analyze_files(folder_dir, catalog_dir)
    storage = analyze_storage(folder_dir)

    report: dict = {
        "mode": mode,
        "folder_date": folder_date.isoformat(),
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "catalog": catalog,
        "ingestion": ingestion,
        "coverage": coverage,
        "file_counts": file_counts,
        "storage": storage,
        "api_calls": {
            "total_calls_made": api_call_count if api_call_count is not None else None,
        },
    }
    report["delta"] = diff_reports(report, previous_report)
    return report


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_int(n: int | None) -> str:
    return "n/a" if n is None else f"{n:,}"


def _fmt_signed(n: int | None) -> str:
    if n is None:
        return "n/a"
    return f"{n:+,}"


def render_markdown(report: dict) -> str:
    lines: list[str] = []
    lines.append(f"# Monitoring report ({report['mode']}, {report['folder_date']})")
    lines.append("")
    lines.append(f"Generated at: {report['generated_at']}")
    api = report.get("api_calls", {}).get("total_calls_made")
    if api is not None:
        lines.append(f"Alpha Vantage calls this run: {api:,}")
    storage = report.get("storage", {})
    if storage and not storage.get("missing"):
        lines.append(
            f"Folder size: {storage['bytes']:,} bytes "
            f"across {storage['file_count']:,} files"
        )
    lines.append("")

    # Catalog
    catalog = report["catalog"]
    lines.append("## Catalog")
    lines.append("")
    lines.append("| Catalog | Total | Active | Delisted | Corrupted |")
    lines.append("|---|---|---|---|---|")
    for name in ("stocks", "etfs"):
        c = catalog.get(name, {})
        if c.get("missing"):
            lines.append(f"| {name} | missing | -- | -- | -- |")
            continue
        lines.append(
            f"| {name} | {c.get('total', 0):,} | {c.get('active', 0):,} "
            f"| {c.get('delisted', 0):,} | {c.get('corrupted', 0):,} |"
        )
    for name in ("indices", "forex", "cryptocurrencies", "commodities", "economic"):
        c = catalog.get(name, {})
        total = "missing" if c.get("missing") else f"{c.get('total', 0):,}"
        lines.append(f"| {name} | {total} | -- | -- | -- |")
    lines.append("")

    yld = catalog.get("yield_status", {})
    if not yld.get("missing"):
        lines.append("### yield_status")
        lines.append("")
        lines.append("| Endpoint | True | False | Null | True ratio | False ratio |")
        lines.append("|---|---|---|---|---|---|")
        for ep, vals in yld.get("endpoints", {}).items():
            lines.append(
                f"| {ep} | {vals['true']:,} | {vals['false']:,} | "
                f"{vals['null']:,} | {vals['true_ratio']:.4f} | "
                f"{vals['false_ratio']:.4f} |"
            )
        lines.append("")

    ec = catalog.get("earnings_calendar", {})
    if not ec.get("missing"):
        lines.append(
            f"earnings_calendar: {ec['total']:,} rows, "
            f"{ec['cast_issues']:,} cast issues, "
            f"avg days to next reportedDate: {ec['avg_days_to_next_reportedDate']}"
        )
        lines.append("")

    # Ingestion
    ing = report["ingestion"]
    lines.append("## Ingestion report")
    lines.append("")
    if ing.get("missing"):
        lines.append("No ingestion_report.parquet found for this folder.")
    else:
        lines.append(f"Total issues recorded: {ing['total_issues']:,}")
        lines.append(f"timezone_mismatch: {ing['timezone_mismatch']:,}")
        lines.append(f"av_throttle: {ing['av_throttle']:,}")
        for issue in ("structure_error", "empty_content", "cast_failure"):
            sub = ing.get(issue, {})
            lines.append(f"{issue}: {sub.get('total', 0):,}")
            for row in sub.get("by_asset_endpoint", []):
                lines.append(
                    f"  {row['asset_type']}/{row['endpoint']}: {row['count']:,}"
                )
    lines.append("")

    # Coverage
    cov = report["coverage"]
    lines.append("## Coverage probes")
    lines.append("")
    lines.append(f"QQQ profile status: {cov['qqq_profile_status']}")
    lines.append(f"QQQ holdings checked: {cov['qqq_holdings_count']:,}")
    s = cov["summary"]
    lines.append(
        f"Probes: {s['total_checked']:,} total, "
        f"{s['intraday_ok']:,} intraday OK, {s['daily_ok']:,} daily OK"
    )
    if s["failures"]:
        lines.append("")
        lines.append("Failures:")
        for f in s["failures"][:50]:
            lines.append(f"  - {f}")
        if len(s["failures"]) > 50:
            lines.append(f"  ... and {len(s['failures']) - 50:,} more")
    lines.append("")

    # File counts
    lines.append("## File counts vs expected")
    lines.append("")
    lines.append("| Asset | Endpoint | Written | Expected | Ratio |")
    lines.append("|---|---|---|---|---|")
    for asset, eps in report["file_counts"].items():
        for ep, info in eps.items():
            ratio = info.get("ratio")
            ratio_s = f"{ratio:.3f}" if ratio is not None else "n/a"
            lines.append(
                f"| {asset} | {ep} | {info['files_written']:,} "
                f"| {_fmt_int(info.get('expected'))} | {ratio_s} |"
            )
    lines.append("")

    # Delta
    delta = report.get("delta", {})
    if delta.get("previous_available"):
        lines.append("## Delta vs previous report")
        lines.append("")
        lines.append(
            f"Previous report: {delta.get('previous_mode')} / "
            f"{delta.get('previous_folder_date')}"
        )
        cat_d = delta.get("catalog", {})
        for name in ("stocks", "etfs"):
            d = cat_d.get(name, {})
            lines.append(
                f"  {name}: total {_fmt_signed(d.get('total'))}, "
                f"active {_fmt_signed(d.get('active'))}, "
                f"delisted {_fmt_signed(d.get('delisted'))}, "
                f"corrupted {_fmt_signed(d.get('corrupted'))}"
            )
        for name in ("indices", "forex", "cryptocurrencies", "commodities", "economic"):
            d = cat_d.get(name, {})
            lines.append(f"  {name}: total {_fmt_signed(d.get('total'))}")
        ing_d = delta.get("ingestion", {})
        lines.append(
            f"  ingestion: total {_fmt_signed(ing_d.get('total_issues'))}, "
            f"av_throttle {_fmt_signed(ing_d.get('av_throttle'))}, "
            f"timezone_mismatch {_fmt_signed(ing_d.get('timezone_mismatch'))}"
        )
    else:
        lines.append("## Delta vs previous report")
        lines.append("")
        lines.append("No previous monitoring report available; delta skipped.")
    lines.append("")

    return "\n".join(lines)


def log_summary(report: dict) -> None:
    """One-screen INFO-level summary of the report.

    Headline numbers also go on ``extra={...}`` so Cloud Logging picks them
    up as structured fields (queryable as ``jsonPayload.<field>``).
    """
    catalog = report["catalog"]
    ing = report["ingestion"]
    cov = report["coverage"]
    extra: dict = {
        "monitor.mode": report["mode"],
        "monitor.folder_date": report["folder_date"],
        "monitor.ingestion.total_issues": ing.get("total_issues", 0),
        "monitor.ingestion.av_throttle": ing.get("av_throttle", 0),
        "monitor.ingestion.timezone_mismatch": ing.get("timezone_mismatch", 0),
        "monitor.coverage.intraday_ok": cov["summary"]["intraday_ok"],
        "monitor.coverage.daily_ok": cov["summary"]["daily_ok"],
        "monitor.coverage.total_checked": cov["summary"]["total_checked"],
    }
    for name in ("stocks", "etfs"):
        c = catalog.get(name, {})
        if not c.get("missing"):
            extra[f"monitor.catalog.{name}.active"] = c.get("active", 0)
            extra[f"monitor.catalog.{name}.delisted"] = c.get("delisted", 0)
            extra[f"monitor.catalog.{name}.corrupted"] = c.get("corrupted", 0)
    api = report.get("api_calls", {}).get("total_calls_made")
    if api is not None:
        extra["monitor.api_calls.total"] = api

    logger.info(
        "Monitoring report ready for %s/%s: %d issues, %d/%d intraday OK, %d/%d daily OK",
        report["mode"], report["folder_date"],
        ing.get("total_issues", 0),
        cov["summary"]["intraday_ok"], cov["summary"]["total_checked"],
        cov["summary"]["daily_ok"], cov["summary"]["total_checked"],
        extra=extra,
    )

    if ing.get("av_throttle", 0) or ing.get("timezone_mismatch", 0):
        logger.warning(
            "Monitoring: %d av_throttle and %d timezone_mismatch issues",
            ing.get("av_throttle", 0), ing.get("timezone_mismatch", 0),
        )
    failures = cov["summary"]["failures"]
    if failures:
        logger.warning(
            "Monitoring: %d coverage failures (first: %s)",
            len(failures), failures[0],
        )


def write_report(report: dict, folder_dir: Path) -> tuple[Path, Path]:
    folder_dir.mkdir(parents=True, exist_ok=True)
    json_path = folder_dir / REPORT_FILENAME_JSON
    md_path = folder_dir / REPORT_FILENAME_MD
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    md_path.write_text(render_markdown(report), encoding="utf-8")
    logger.info(f"Wrote monitoring report to {json_path} and {md_path}")
    return json_path, md_path


def run_and_persist(
    *,
    mode: str,
    folder_date: date,
    catalog_dir: Path,
    folder_dir: Path,
    previous_report_path: Path | None = None,
    api_call_count: int | None = None,
) -> tuple[dict, Path, Path]:
    """Build report, log summary, write JSON and Markdown.

    Returns ``(report, json_path, md_path)``. Never raises on missing inputs;
    analyzers fall back to ``missing=True`` style entries.
    """
    previous = load_previous_report(previous_report_path)
    report = build_report(
        mode=mode,
        folder_date=folder_date,
        catalog_dir=catalog_dir,
        folder_dir=folder_dir,
        previous_report=previous,
        api_call_count=api_call_count,
    )
    log_summary(report)
    json_path, md_path = write_report(report, folder_dir)
    return report, json_path, md_path
