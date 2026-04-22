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
) -> None:
    """Install a single stream handler on the root logger.

    ``structured`` defaults to :func:`detect_cloud_run`; pass ``True``/
    ``False`` to force JSON or text output (useful for local smoke tests
    of the Cloud Logging payload).

    Idempotent: repeat calls replace the existing handler so tests and
    re-entries do not accumulate duplicates.
    """
    if structured is None:
        structured = detect_cloud_run()

    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)

    handler = logging.StreamHandler(stream)
    if structured:
        handler.setFormatter(CloudLoggingJsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    root.addHandler(handler)
    root.setLevel(level)


def detect_cloud_run() -> bool:
    """Return True when running inside Cloud Run (env var ``K_SERVICE``)."""
    return "K_SERVICE" in os.environ
