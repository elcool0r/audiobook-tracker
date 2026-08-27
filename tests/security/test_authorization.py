"""Authorization and secret-exposure invariants.

These tests use a real mongomock-backed database and real JWT cookies rather than
dependency overrides, so that role checks are actually exercised end to end. Tests
that override `get_current_user` cannot catch a missing admin check.
"""
import os

import mongomock
import pytest
from unittest.mock import patch


SECRET = "test_secret_key_for_authz"


@pytest.fixture
def db():
    """A fresh in-memory database wired into every collection accessor."""
    client = mongomock.MongoClient()
    database = client["test_authz"]
    with patch("tracker.db.get_db", return_value=database):
        yield database


@pytest.fixture(autouse=True)
def no_rate_limiting():
    """The module-level Limiter keeps counters across the whole test session.

    These tests exercise auth-log handling rather than throttling, so disable it
    and restore the previous state afterwards.
    """
    from tracker.app import limiter
    previous = limiter.enabled
    limiter.enabled = False
    limiter.reset()
    yield
    limiter.reset()
    limiter.enabled = previous


@pytest.fixture
def client(db):
    os.environ["SECRET_KEY"] = SECRET
    from tracker.security import get_password_hash

    db["users"].insert_many([
        {"username": "boss", "password_hash": get_password_hash("pw"), "role": "admin",
         "date_format": "iso"},
        {"username": "peon", "password_hash": get_password_hash("pw"), "role": "user",
         "date_format": "iso"},
    ])
    db["settings"].insert_one({
        "_id": "global",
        "secret_key": SECRET,
        "proxy_url": "http://proxy.internal:8080",
        "proxy_username": "proxyuser",
        "proxy_password": "sup3rs3cr3t",
        "proxy_enabled": True,
        "audiobookshelf_api_token": "abs-token-value",
        "developer_mode": False,
    })

    from tracker.app import create_app
    from fastapi.testclient import TestClient

    # The background worker is not under test and would make live network calls.
    with patch("tracker.tasks.worker.start"), patch("tracker.tasks.worker.stop"):
        app = create_app()
        app.user_middleware = [m for m in app.user_middleware if "SlowAPI" not in str(m.cls)]
        app.middleware_stack = app.build_middleware_stack()
        with TestClient(app) as test_client:
            yield test_client


def _cookies(username):
    from tracker.auth import create_access_token, TOKEN_NAME
    return {TOKEN_NAME: create_access_token({"sub": username})}


@pytest.fixture
def admin_cookies():
    return _cookies("boss")


@pytest.fixture
def user_cookies():
    return _cookies("peon")


class TestSettingsSecrets:
    def test_settings_api_never_returns_secrets(self, client, admin_cookies):
        """The signing key and proxy password must never cross the wire."""
        resp = client.get("/config/api/settings", cookies=admin_cookies)
        assert resp.status_code == 200
        body = resp.json()
        for field in ("secret_key", "proxy_password", "audiobookshelf_api_token"):
            assert field not in body, f"{field} leaked from GET /api/settings"
        raw = resp.text
        assert SECRET not in raw
        assert "sup3rs3cr3t" not in raw
        assert "abs-token-value" not in raw
        # Presence is still reported so the UI can show configured state.
        assert body["proxy_password_configured"] is True
        assert body["audiobookshelf_api_token_configured"] is True

    def test_settings_api_requires_admin(self, client, user_cookies):
        assert client.get("/config/api/settings", cookies=user_cookies).status_code == 403

    def test_settings_page_requires_admin(self, client, user_cookies, admin_cookies):
        assert client.get("/config/settings", cookies=user_cookies).status_code == 403
        assert client.get("/config/settings", cookies=admin_cookies).status_code == 200

    def test_settings_page_does_not_render_proxy_password(self, client, admin_cookies):
        resp = client.get("/config/settings", cookies=admin_cookies)
        assert resp.status_code == 200
        assert "sup3rs3cr3t" not in resp.text

    def test_client_cannot_overwrite_the_signing_key(self, client, admin_cookies, db):
        """A forged secret_key in the payload must be ignored, not persisted."""
        resp = client.post("/config/api/settings", cookies=admin_cookies,
                           json={"secret_key": "attacker-chosen-key", "debug_logging": True})
        assert resp.status_code == 200
        assert db["settings"].find_one({"_id": "global"})["secret_key"] == SECRET

    def test_blank_proxy_password_is_preserved(self, client, admin_cookies, db):
        """Saving unrelated settings must not wipe the stored proxy password."""
        resp = client.post("/config/api/settings", cookies=admin_cookies,
                           json={"debug_logging": True})
        assert resp.status_code == 200
        assert db["settings"].find_one({"_id": "global"})["proxy_password"] == "sup3rs3cr3t"

    def test_proxy_password_can_still_be_cleared_explicitly(self, client, admin_cookies, db):
        resp = client.post("/config/api/settings", cookies=admin_cookies,
                           json={"proxy_password": None})
        assert resp.status_code == 200
        assert db["settings"].find_one({"_id": "global"})["proxy_password"] is None


class TestLogViewerEscaping:
    """The auth log records unauthenticated input; the admin view must not execute it."""

    PAYLOAD = "<img src=x onerror=alert(1)>"

    def test_hostile_user_agent_is_not_reflected_as_markup(self, client, admin_cookies, db):
        # An unauthenticated failed login is enough to plant the payload.
        client.post("/config/login",
                    data={"username": self.PAYLOAD, "password": "nope"},
                    headers={"User-Agent": self.PAYLOAD})
        entry = db["logs"].find_one({"event": "login_failed"})
        assert entry is not None
        assert entry["user_agent"].startswith("<img")

        resp = client.get("/config/logs", cookies=admin_cookies)
        assert resp.status_code == 200
        # tojson escapes < and > for safe embedding in a <script> block, and the
        # renderer uses textContent, so the raw tag must never appear.
        assert self.PAYLOAD not in resp.text
        assert "<img src=x" not in resp.text

    def test_log_fields_are_length_bounded(self, client, db):
        from tracker.auth import MAX_LOG_FIELD_LEN

        client.post("/config/login",
                    data={"username": "someone", "password": "nope"},
                    headers={"User-Agent": "A" * 10000})
        entry = db["logs"].find_one({"username": "someone"})
        assert entry is not None
        assert len(entry["user_agent"]) <= MAX_LOG_FIELD_LEN + 1


class TestLoginRobustness:
    def test_failed_login_without_client_address_does_not_500(self, client):
        """Some ASGI transports leave request.client unset."""
        from tracker.auth import client_ip

        class _NoClient:
            client = None

        assert client_ip(_NoClient()) == "unknown"


class TestUnauthenticatedSurface:
    """Anonymous callers must not drive outbound Audible traffic or create records."""

    def test_search_and_product_require_authentication(self, client):
        assert client.post("/config/api/search", json={"title": "dune"}).status_code == 401
        assert client.get("/config/api/product/B00ABCDEFG").status_code == 401

    def test_public_series_does_not_fetch_or_create(self, client, db):
        from unittest.mock import patch

        with patch("tracker.library._fetch_series_books_internal") as fetch:
            resp = client.get("/config/api/public/series/NOTREAL123")
            books = client.get("/config/api/public/series/NOTREAL123/books")

        assert resp.status_code == 404
        assert books.status_code == 404
        fetch.assert_not_called()
        # and nothing was inserted for the unknown ASIN
        assert db["series"].find_one({"_id": "NOTREAL123"}) is None

    def test_public_series_serves_cached_data(self, client, db):
        db["series"].insert_one({
            "_id": "S1", "title": "Cached", "books": [{"asin": "B1", "title": "Book 1"}],
        })
        resp = client.get("/config/api/public/series/S1")
        assert resp.status_code == 200
        assert resp.json()["title"] == "Cached"
        assert client.get("/config/api/public/series/S1/books").json()[0]["asin"] == "B1"


class TestSeriesMetadataWritesDoNotCreate:
    """Metadata helpers must not conjure titleless series the scheduler then polls."""

    def test_metadata_helpers_do_not_upsert(self, db):
        from tracker.library import (
            set_series_raw, touch_series_fetched, set_series_next_refresh,
        )
        set_series_raw("GHOST", {"title": "nope"})
        touch_series_fetched("GHOST")
        set_series_next_refresh("GHOST", "2030-01-01T00:00:00Z")
        assert db["series"].find_one({"_id": "GHOST"}) is None

    def test_metadata_helpers_update_an_existing_series(self, db):
        db["series"].insert_one({"_id": "REAL", "title": "Real", "books": []})
        from tracker.library import set_series_next_refresh
        set_series_next_refresh("REAL", "2030-01-01T00:00:00Z")
        assert db["series"].find_one({"_id": "REAL"})["next_refresh_at"] == "2030-01-01T00:00:00Z"
