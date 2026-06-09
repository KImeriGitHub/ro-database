"""Tests for ``maintainance_scripts.logging_setup``.

The module is small but load-bearing: the Cloud Run container relies on the
JSON formatter to emit Cloud Logging-compatible structured log fields, while
local runs rely on the text formatter being installed exactly once even
across re-entries. Both paths are exercised here without touching real GCP.
"""

from __future__ import annotations

import io
import json
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import logging_setup
from maintainance_scripts.logging_setup import (
    CloudLoggingJsonFormatter,
    configure_logging,
    detect_cloud_run,
)


# ---------------------------------------------------------------------------
# Test fixtures: restore root logger between tests
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _restore_root_logger():
    """Capture and restore root logger handlers + level so a misbehaving test
    doesn't pollute later tests' logging."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)


# ---------------------------------------------------------------------------
# detect_cloud_run
# ---------------------------------------------------------------------------


def test_detect_cloud_run_true_when_k_service_set(monkeypatch):
    monkeypatch.setenv("K_SERVICE", "ro-daily-ingest")
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    assert detect_cloud_run() is True


def test_detect_cloud_run_true_when_cloud_run_job_set(monkeypatch):
    """Cloud Run Jobs do not inject ``K_SERVICE`` (only Services do); they
    inject ``CLOUD_RUN_JOB`` instead. Detection must accept either."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.setenv("CLOUD_RUN_JOB", "ro-daily-run")
    assert detect_cloud_run() is True


def test_detect_cloud_run_false_when_neither_set(monkeypatch):
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    assert detect_cloud_run() is False


def test_detect_cloud_run_truthy_value_only_checks_presence(monkeypatch):
    """The check is presence-based: any value, even empty string, qualifies.
    This matches Cloud Run's behaviour, which always sets the variable to a
    non-empty name in practice but the contract is presence-based, not
    value-based."""
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.setenv("K_SERVICE", "")
    assert detect_cloud_run() is True
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.setenv("CLOUD_RUN_JOB", "")
    assert detect_cloud_run() is True


# ---------------------------------------------------------------------------
# CloudLoggingJsonFormatter
# ---------------------------------------------------------------------------


def _make_record(
    name: str = "test",
    level: int = logging.INFO,
    msg: str = "hello",
    extra: dict | None = None,
    exc_info=None,
) -> logging.LogRecord:
    record = logging.LogRecord(
        name=name, level=level, pathname=__file__, lineno=42,
        msg=msg, args=(), exc_info=exc_info, func="test_func",
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


def test_json_formatter_emits_required_cloud_logging_fields():
    """``severity``, ``message``, ``time`` and ``logger`` are the load-bearing
    fields that Cloud Logging indexes by; they must be present on every record."""
    rec = _make_record(level=logging.WARNING, msg="rate limited")
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    assert out["severity"] == "WARNING"
    assert out["message"] == "rate limited"
    assert out["logger"] == "test"
    # Time is RFC3339-ish ISO with timezone (parseable by Cloud Logging).
    assert "T" in out["time"]
    assert out["time"].endswith("+00:00") or out["time"].endswith("Z")


def test_json_formatter_includes_source_location():
    """Cloud Logging displays this as the file/line of the log call --
    critical for debugging container runs without a debugger attached."""
    rec = _make_record()
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    src = out["logging.googleapis.com/sourceLocation"]
    assert src["file"].endswith("test_logging_setup.py")
    assert src["line"] == "42"
    assert src["function"] == "test_func"


def test_json_formatter_promotes_extra_fields_to_top_level():
    """``logger.info("...", extra={"ticker": "AAPL"})`` must produce a top-
    level ``ticker`` field (queryable in Logs Explorer as ``jsonPayload.ticker``)."""
    rec = _make_record(extra={"ticker": "AAPL", "endpoint": "prices_daily"})
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    assert out["ticker"] == "AAPL"
    assert out["endpoint"] == "prices_daily"


def test_json_formatter_drops_underscore_prefixed_extras():
    """Internal/private fields starting with ``_`` must not leak into the
    payload -- these are typically helpers stashed on the record by callers
    and aren't intended for the log shipper."""
    rec = _make_record(extra={"_internal": "secret", "public": "ok"})
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    assert "_internal" not in out
    assert out["public"] == "ok"


def test_json_formatter_skips_reserved_logrecord_attributes():
    """Built-in LogRecord attributes (e.g. ``args``, ``msg``, ``levelno``)
    must not be re-emitted -- otherwise the payload would balloon with
    redundant data."""
    rec = _make_record()
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    for noisy in ("args", "msg", "levelno", "pathname", "filename",
                  "module", "process", "thread"):
        assert noisy not in out


def test_json_formatter_serialises_exceptions():
    """Exception info must produce a string ``exception`` field with the
    traceback so it appears under jsonPayload.exception in Cloud Logging."""
    try:
        raise ValueError("boom")
    except ValueError:
        exc_info = sys.exc_info()
        rec = _make_record(exc_info=exc_info)
    out = json.loads(CloudLoggingJsonFormatter().format(rec))
    assert "exception" in out
    assert "ValueError" in out["exception"]
    assert "boom" in out["exception"]


def test_json_formatter_handles_non_serializable_extras():
    """``json.dumps(default=str)`` lets the formatter emit even Path or
    datetime objects without crashing -- the alternative would be losing the
    log line entirely on a non-JSON-safe ``extra``."""
    rec = _make_record(extra={"path": Path("/tmp/x"), "n": 3})
    payload = CloudLoggingJsonFormatter().format(rec)
    out = json.loads(payload)
    # str(Path('/tmp/x')) is platform-dependent; accept any string repr.
    assert isinstance(out["path"], str)
    assert "x" in out["path"]
    assert out["n"] == 3


# ---------------------------------------------------------------------------
# configure_logging
# ---------------------------------------------------------------------------


def test_configure_logging_idempotent_replaces_handler(monkeypatch):
    """Re-entering ``configure_logging`` (e.g. across CLI subcommands or
    pytest collections) must replace the handler, not stack a second one,
    or every log line would be duplicated."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    configure_logging(log_to_file=False)
    configure_logging(log_to_file=False)
    configure_logging(log_to_file=False)
    assert len(logging.getLogger().handlers) == 1


def test_configure_logging_text_mode_writes_human_format(monkeypatch):
    """When ``structured=False`` the human formatter is installed -- must
    contain the level name and message verbatim (i.e. not JSON)."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    buf = io.StringIO()
    configure_logging(stream=buf, structured=False, log_to_file=False)

    logging.getLogger("trial").info("plain text message")
    out = buf.getvalue()
    assert "INFO" in out
    assert "plain text message" in out
    # Not JSON: braces should be absent in text mode.
    assert not out.lstrip().startswith("{")


def test_configure_logging_json_mode_writes_structured_payload(monkeypatch):
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    buf = io.StringIO()
    configure_logging(stream=buf, structured=True, log_to_file=False)

    logging.getLogger("trial").info("structured", extra={"ticker": "AAPL"})
    out = buf.getvalue().strip()
    payload = json.loads(out)
    assert payload["message"] == "structured"
    assert payload["ticker"] == "AAPL"
    assert payload["severity"] == "INFO"


def test_configure_logging_default_structured_follows_detect_cloud_run(monkeypatch):
    """Without an explicit ``structured`` arg the function must follow
    ``detect_cloud_run``; setting K_SERVICE flips the formatter to JSON."""
    monkeypatch.setenv("K_SERVICE", "ro-daily-ingest")
    buf = io.StringIO()
    configure_logging(stream=buf)  # structured=None -> auto-detect

    logging.getLogger("trial").info("hi")
    out = buf.getvalue().strip()
    # Cloud Run path => JSON formatter => first non-space char must be '{'.
    assert out.startswith("{")
    assert json.loads(out)["message"] == "hi"


def test_configure_logging_respects_level(monkeypatch):
    """A WARNING-level configure must drop INFO messages."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    buf = io.StringIO()
    configure_logging(level=logging.WARNING, stream=buf, structured=False, log_to_file=False)

    logger = logging.getLogger("trial-level")
    logger.info("dropped")
    logger.warning("kept")

    out = buf.getvalue()
    assert "dropped" not in out
    assert "kept" in out


def test_configure_logging_writes_file_when_log_to_file_true(monkeypatch, tmp_path):
    """A file handler must be installed alongside the stream handler and
    receive the same formatted output, so failed local runs leave a trail."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    buf = io.StringIO()
    configure_logging(stream=buf, structured=False, log_to_file=True, log_dir=tmp_path)

    handlers = logging.getLogger().handlers
    assert len(handlers) == 2
    assert any(isinstance(h, logging.FileHandler) for h in handlers)

    logging.getLogger("trial-file").info("recorded to disk")

    # Close handler so Windows releases the file before we read it.
    for h in list(logging.getLogger().handlers):
        if isinstance(h, logging.FileHandler):
            h.close()

    log_files = list(tmp_path.glob("*.log"))
    assert len(log_files) == 1
    contents = log_files[0].read_text(encoding="utf-8")
    assert "recorded to disk" in contents


def test_configure_logging_file_handler_off_on_cloud_run(monkeypatch, tmp_path):
    """``log_to_file`` defaults to off when ``K_SERVICE`` is set, so the
    Cloud Run container does not write to its ephemeral disk."""
    monkeypatch.setenv("K_SERVICE", "ro-daily-ingest")
    buf = io.StringIO()
    configure_logging(stream=buf, log_dir=tmp_path)

    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert not any(isinstance(h, logging.FileHandler) for h in handlers)
    assert list(tmp_path.glob("*.log")) == []


def test_configure_logging_file_handler_off_when_env_opts_out(monkeypatch, tmp_path):
    """``RO_DB_NO_LOG_FILE`` forces the file handler off even off Cloud Run, so
    the test suite stops dropping a ``logs/*.log`` file per ``main()`` call."""
    monkeypatch.delenv("K_SERVICE", raising=False)
    monkeypatch.delenv("CLOUD_RUN_JOB", raising=False)
    monkeypatch.setenv("RO_DB_NO_LOG_FILE", "1")
    buf = io.StringIO()
    configure_logging(stream=buf, log_dir=tmp_path)  # log_to_file=None -> default

    handlers = logging.getLogger().handlers
    assert len(handlers) == 1
    assert not any(isinstance(h, logging.FileHandler) for h in handlers)
    assert list(tmp_path.glob("*.log")) == []
