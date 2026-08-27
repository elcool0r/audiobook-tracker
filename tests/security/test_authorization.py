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
