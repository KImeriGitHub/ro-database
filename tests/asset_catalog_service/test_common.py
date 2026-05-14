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
    with_network_retry,
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
    as a ``CatalogFetchError`` so callers' ``except`` clauses match.

    ``fetch_text`` retries the throttle 6 times with linear backoff
    (15+30+45+60+75 = 225s of real wall time). Patch ``time.sleep`` so the
    retry logic is exercised without paying the production backoff.
    """
    with patch(
        "asset_catalog_service.updates._common.requests.get"
    ) as mock_get, patch(
        "asset_catalog_service.updates._common.time.sleep"
    ):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.text = '{"Information": "API rate limit hit"}'
        mock_get.return_value = resp

        with pytest.raises(CatalogFetchError, match="JSON"):
            fetch_text(_LEAKY_URL)


# ── with_network_retry ──────────────────────────────────────────────


def test_with_network_retry_returns_first_call_result_on_success():
    """Success path: ``fn`` is called once, no sleep, value is returned."""
    fn = MagicMock(return_value="ok")
    with patch("asset_catalog_service.updates._common.time.sleep") as sleep:
        out = with_network_retry(fn, "a", kw="b", max_attempts=3, backoff=5.0)

    assert out == "ok"
    fn.assert_called_once_with("a", kw="b")
    sleep.assert_not_called()


def test_with_network_retry_retries_until_success():
    """A transient ``CatalogFetchError`` is swallowed; the next attempt wins.

    Sleep is asserted to use the configured backoff so callers can rely on
    pacing being honoured (the per-symbol sector fetch uses a tighter backoff
    than the default and we don't want the wrapper to silently override it).
    """
    fn = MagicMock(side_effect=[
        CatalogFetchError("ConnectionError"),
        CatalogFetchError("ConnectionError"),
        "ok",
    ])
    with patch("asset_catalog_service.updates._common.time.sleep") as sleep:
        out = with_network_retry(fn, max_attempts=3, backoff=7.0)

    assert out == "ok"
    assert fn.call_count == 3
    # Linear backoff: 7.0 * 1, 7.0 * 2. No sleep before the first attempt or
    # after the successful one.
    assert [c.args[0] for c in sleep.call_args_list] == [7.0, 14.0]


def test_with_network_retry_reraises_last_error_after_exhausting_attempts():
    """Final-attempt failure: the most recent error propagates, no extra sleep."""
    fn = MagicMock(side_effect=[
        CatalogFetchError("HTTP 503"),
        CatalogFetchError("HTTP 503"),
        CatalogFetchError("ConnectionError"),
    ])
    with patch("asset_catalog_service.updates._common.time.sleep") as sleep:
        with pytest.raises(CatalogFetchError, match="ConnectionError"):
            with_network_retry(fn, max_attempts=3, backoff=1.0)

    assert fn.call_count == 3
    # No sleep after the final (failing) attempt.
    assert sleep.call_count == 2


def test_with_network_retry_does_not_swallow_non_catalog_errors():
    """``ValueError`` (or any non-``CatalogFetchError``) must propagate
    immediately -- the retry policy is scoped to transient HTTP failures."""
    fn = MagicMock(side_effect=ValueError("boom"))
    with patch("asset_catalog_service.updates._common.time.sleep") as sleep:
        with pytest.raises(ValueError, match="boom"):
            with_network_retry(fn, max_attempts=5, backoff=1.0)

    fn.assert_called_once()
    sleep.assert_not_called()
