"""Project-wide logging configuration.

Entrypoints call ``configure_logging()`` once inside ``__main__`` so
formatting stays consistent across the codebase.

On Cloud Run (``K_SERVICE`` is set) the handler swaps to a JSON formatter
whose field names match Cloud Logging's structured-log spec, so severity,
source location, and any ``extra={...}`` payload become queryable fields
in Logs Explorer (e.g. ``jsonPayload.ticker = "AAPL"``).
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

DEFAULT_FORMAT = "%(asctime)s  %(levelname)s %(message)s"
DEFAULT_DATEFMT = "%H:%M:%S"


class CloudLoggingJsonFormatter(logging.Formatter):
    """Emit one JSON object per record using Cloud Logging field names.

    Reference: https://cloud.google.com/logging/docs/structured-logging
    """

    # LogRecord attributes set by the logging machinery itself; anything
    # else on record.__dict__ came from the caller's ``extra={...}`` and
    # should bubble up to the top-level JSON payload.
    _RESERVED = frozenset({
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "message", "module",
        "msecs", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "taskName", "thread", "threadName",
    })

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "severity": record.levelname,
            "message": record.getMessage(),
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "logger": record.name,
            "logging.googleapis.com/sourceLocation": {
                "file": record.pathname,
                "line": str(record.lineno),
                "function": record.funcName,
            },
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)

        for key, value in record.__dict__.items():
            if key in self._RESERVED or key.startswith("_"):
                continue
            payload[key] = value

        return json.dumps(payload, default=str)


def configure_logging(
    level: int | str = logging.INFO,
    fmt: str = DEFAULT_FORMAT,
    datefmt: str = DEFAULT_DATEFMT,
    stream: TextIO = sys.stdout,
    structured: bool | None = None,
    log_to_file: bool | None = None,
    log_dir: Path | None = None,
) -> None:
    """Install a stream handler (and optionally a file handler) on the root logger.

    ``structured`` defaults to :func:`detect_cloud_run`; pass ``True``/
    ``False`` to force JSON or text output (useful for local smoke tests
    of the Cloud Logging payload).

    ``log_to_file`` defaults to ``not detect_cloud_run()``: locally we mirror
    each run to ``logs/<UTC-timestamp>_<script>.log`` so failed jobs leave a
    durable trail; on Cloud Run the stream is captured by Cloud Logging and a
    file would just bloat the ephemeral container disk. ``log_dir`` defaults
    to ``<PROJECT_ROOT>/logs`` (the folder is gitignored).

    Idempotent: repeat calls remove and close the previous handlers so tests
    and re-entries do not accumulate duplicates or leak file descriptors.
    """
    if structured is None:
        structured = detect_cloud_run()
    if log_to_file is None:
        log_to_file = not detect_cloud_run()

    root = logging.getLogger()
    for h in list(root.handlers):
        # Handlers tagged with `_keep_through_reconfigure` (e.g. the
        # integration-test file handler) must survive nested configure_logging()
        # calls made by sub-pipelines, otherwise the per-run log file only
        # captures messages emitted before the first sub-pipeline starts.
        if getattr(h, "_keep_through_reconfigure", False):
            continue
        root.removeHandler(h)
        if isinstance(h, logging.FileHandler):
            h.close()

    if structured:
        formatter: logging.Formatter = CloudLoggingJsonFormatter()
    else:
        formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)

    handler = logging.StreamHandler(stream)
    handler.setFormatter(formatter)
    root.addHandler(handler)

    if log_to_file:
        if log_dir is None:
            from config.settings import PROJECT_ROOT
            log_dir = PROJECT_ROOT / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stem = Path(sys.argv[0]).stem if sys.argv and sys.argv[0] else "session"
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        file_handler = logging.FileHandler(log_dir / f"{timestamp}_{stem}.log", encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    root.setLevel(level)


def detect_cloud_run() -> bool:
    """Return True when running inside Cloud Run (env var ``K_SERVICE``)."""
    return "K_SERVICE" in os.environ
