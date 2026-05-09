"""End-of-run monitoring for daily, weekend, and historical pulls.

Public entry points:

- :func:`monitoring_service.report.build_report` - assemble all analyzers into
  a single report dict.
- :func:`monitoring_service.report.run_report_and_persist` - build the report,
  log the summary, write JSON + Markdown to disk, return the path.
- :mod:`monitoring_service.run_monitor` - CLI wrapper.
"""

from monitoring_service.report import build_report, run_report_and_persist

__all__ = ["build_report", "run_report_and_persist"]
