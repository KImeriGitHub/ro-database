"""Shared pytest fixtures for the whole test suite."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _suppress_log_files(monkeypatch):
    """Stop entrypoint ``main()`` calls from littering ``logs/`` during tests.

    ``configure_logging`` defaults to writing ``logs/<UTC-timestamp>_<script>.log``
    on every call when not on Cloud Run. Tests that exercise ``main()`` (often
    many times, sometimes as subprocesses) would otherwise drop a durable log
    file per invocation into the real project ``logs/`` folder. Setting the env
    var here flips the file handler off; because subprocesses inherit
    ``os.environ``, it also reaches the CLI tests that shell out to a script.
    """
    monkeypatch.setenv("RO_DB_NO_LOG_FILE", "1")
