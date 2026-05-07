"""Tests for ``asset_catalog_service.updates._common`` HTTP helpers.

The motivating regression: ``requests.HTTPError`` and connection errors
stringify with the full request URL inlined, and catalog URLs carry the API
key as ``apikey=...`` query param. Both ``fetch_text`` and ``fetch_json`` must
translate every ``requests`` failure into a ``CatalogFetchError`` whose
message contains only the HTTP status code or exception type name -- never
the URL.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from asset_catalog_service.updates._common import (
    CatalogFetchError,
    fetch_json,
    fetch_text,
)


_LEAKY_URL = (
    "https://www.alphavantage.co/query?function=LISTING_STATUS"
    "&state=active&apikey=SECRET_KEY_42"
)


def _http_error(status: int) -> requests.HTTPError:
    """Build a real ``HTTPError`` whose ``str()`` embeds the URL like requests
    does in the wild (``"503 Server Error: Service Unavailable for url: ..."``).
    The sanitizer must extract the status code and discard the rest."""
    response = MagicMock(spec=requests.Response)
    response.status_code = status
    return requests.HTTPError(
        f"{status} Server Error: Service Unavailable for url: {_LEAKY_URL}",
        response=response,
    )


def test_fetch_text_http_error_message_does_not_leak_api_key():
    """``raise_for_status`` failure must surface as a sanitized
    ``CatalogFetchError`` with the status code only."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        resp = MagicMock()
        resp.raise_for_status.side_effect = _http_error(503)
        mock_get.return_value = resp

        with pytest.raises(CatalogFetchError) as exc_info:
            fetch_text(_LEAKY_URL)

    msg = str(exc_info.value)
    assert "SECRET_KEY_42" not in msg
    assert "apikey" not in msg
    assert "alphavantage.co" not in msg
    assert "503" in msg


def test_fetch_text_suppresses_chained_cause_so_traceback_stays_clean():
    """``logger.exception`` walks the ``__cause__`` chain and prints the
    original exception's message. The helper must use ``raise ... from None``
    so the URL-bearing requests exception doesn't reappear in tracebacks."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        resp = MagicMock()
        resp.raise_for_status.side_effect = _http_error(503)
        mock_get.return_value = resp

        with pytest.raises(CatalogFetchError) as exc_info:
            fetch_text(_LEAKY_URL)

    assert exc_info.value.__cause__ is None
    assert exc_info.value.__suppress_context__ is True


def test_fetch_text_connection_error_keeps_only_type_name():
    """``ConnectionError``'s ``str()`` typically includes the host/URL via
    the underlying urllib3 error. The sanitizer falls back to the type name."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        mock_get.side_effect = requests.ConnectionError(
            f"HTTPSConnectionPool(host='www.alphavantage.co'): "
            f"Max retries exceeded with url: {_LEAKY_URL}"
        )

        with pytest.raises(CatalogFetchError) as exc_info:
            fetch_text(_LEAKY_URL)

    msg = str(exc_info.value)
    assert "SECRET_KEY_42" not in msg
    assert "alphavantage.co" not in msg
    assert "ConnectionError" in msg


def test_fetch_json_http_error_message_does_not_leak_api_key():
    """Same contract as ``fetch_text`` -- a 4xx/5xx becomes a sanitized
    ``CatalogFetchError`` with no URL in the message."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        resp = MagicMock()
        resp.raise_for_status.side_effect = _http_error(401)
        mock_get.return_value = resp

        with pytest.raises(CatalogFetchError) as exc_info:
            fetch_json(_LEAKY_URL)

    msg = str(exc_info.value)
    assert "SECRET_KEY_42" not in msg
    assert "401" in msg


def test_fetch_text_returns_text_on_success():
    """Sanity: success path is unchanged."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.text = "symbol,name\nAAPL,Apple Inc.\n"
        mock_get.return_value = resp

        out = fetch_text(_LEAKY_URL)

    assert out.startswith("symbol,name")


def test_fetch_text_rejects_json_response_as_catalog_fetch_error():
    """AV returns a JSON error blob in place of CSV when throttled. The body
    itself doesn't echo the URL, so logging it is fine -- but it must surface
    as a ``CatalogFetchError`` so callers' ``except`` clauses match."""
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get:
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.text = '{"Information": "API rate limit hit"}'
        mock_get.return_value = resp

        with pytest.raises(CatalogFetchError, match="JSON"):
            fetch_text(_LEAKY_URL)
