"""Tests for ``maintainance_scripts.secret_manager_client``.

Covers the cached-client contract, the resource-path construction (which
the ping script's friendlier error messages rely on), and the
whitespace-strip behaviour on the returned payload. The real
``SecretManagerServiceClient`` is replaced with a stub so the tests need
neither network nor google-cloud-secret-manager wiring.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from maintainance_scripts import secret_manager_client


class _FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = type("Payload", (), {"data": payload})


class _FakeClient:
    def __init__(self):
        self.calls: list[dict] = []
        self.payloads: dict[str, bytes] = {}

    def access_secret_version(self, request):
        self.calls.append(request)
        name = request["name"]
        if name not in self.payloads:
            raise KeyError(f"Unexpected secret access: {name}")
        return _FakeResponse(self.payloads[name])


@pytest.fixture(autouse=True)
def _reset_cached_client():
    """Module-level ``_client`` cache must not leak between tests, otherwise
    the second test would reuse the first test's stub."""
    secret_manager_client._client = None
    yield
    secret_manager_client._client = None


@pytest.fixture
def fake_client(monkeypatch):
    """Install a fake SecretManagerServiceClient and a no-op credential
    resolver. Returns the fake so tests can populate payloads/inspect calls."""
    fake = _FakeClient()

    def factory(credentials=None):
        return fake

    monkeypatch.setattr(
        secret_manager_client.secretmanager,
        "SecretManagerServiceClient",
        factory,
    )
    monkeypatch.setattr(
        secret_manager_client, "get_gcp_credentials", lambda: None
    )
    return fake


def test_get_client_caches(fake_client, monkeypatch):
    """The first call instantiates a client; subsequent calls return the
    cached instance so the underlying HTTPS session is pooled."""
    call_count = {"n": 0}
    original = fake_client

    def factory(credentials=None):
        call_count["n"] += 1
        return original

    monkeypatch.setattr(
        secret_manager_client.secretmanager,
        "SecretManagerServiceClient",
        factory,
    )

    c1 = secret_manager_client.get_client()
    c2 = secret_manager_client.get_client()
    assert c1 is c2
    assert call_count["n"] == 1


def test_get_secret_builds_correct_resource_path(fake_client):
    """The ping script's NotFound/FailedPrecondition diagnostics rely on the
    resource path being exactly
    ``projects/{project}/secrets/{name}/versions/{version}``."""
    fake_client.payloads["projects/my-project/secrets/av-standard/versions/latest"] = b"key123"

    result = secret_manager_client.get_secret(
        "av-standard", project_id="my-project"
    )

    assert result == "key123"
    assert fake_client.calls == [
        {"name": "projects/my-project/secrets/av-standard/versions/latest"}
    ]


def test_get_secret_honours_explicit_version(fake_client):
    """Callers can pin a specific version (useful for rollback drills)."""
    fake_client.payloads["projects/p/secrets/s/versions/3"] = b"older"

    assert secret_manager_client.get_secret(
        "s", version="3", project_id="p"
    ) == "older"


def test_get_secret_strips_trailing_whitespace(fake_client):
    """``gcloud secrets create --data-file=-`` appends a trailing newline on
    most shells; the helper strips it so callers do not need to remember to."""
    fake_client.payloads[
        "projects/p/secrets/s/versions/latest"
    ] = b"  payload-with-newline\n"

    assert secret_manager_client.get_secret("s", project_id="p") == \
        "payload-with-newline"


def test_get_secret_decodes_utf8(fake_client):
    """Secret payloads are arbitrary bytes; the helper assumes UTF-8 because
    every credential we store is ASCII-or-UTF-8 text. Non-UTF-8 bytes would
    raise loudly, which is the correct failure mode."""
    fake_client.payloads["projects/p/secrets/s/versions/latest"] = "köy".encode("utf-8")

    assert secret_manager_client.get_secret("s", project_id="p") == "köy"
