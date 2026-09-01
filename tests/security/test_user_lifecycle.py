"""User rename and delete must carry the account's data with them.

user_library, api_keys and both notification sweepers key on `username`, so
touching only the users collection orphaned the library, left series.user_count
inflated, and silently broke the user's session.
"""
import os

import mongomock
import pytest
from unittest.mock import patch


SECRET = "test_secret_key_for_lifecycle"


@pytest.fixture
def db():
    database = mongomock.MongoClient()["test_lifecycle"]
    with patch("tracker.db.get_db", return_value=database):
        yield database


@pytest.fixture
def client(db):
    os.environ["SECRET_KEY"] = SECRET
    from tracker.security import get_password_hash
    from tracker.app import create_app
    from fastapi.testclient import TestClient

    db["users"].insert_many([
        {"username": "boss", "password_hash": get_password_hash("pw"), "role": "admin"},
        {"username": "reader", "password_hash": get_password_hash("pw"), "role": "user"},
    ])
    db["series"].insert_one({"_id": "S1", "title": "Series One", "books": [], "user_count": 1})
    db["user_library"].insert_one({"username": "reader", "series_asin": "S1", "title": "Series One"})
    db["api_keys"].insert_one({"username": "reader", "key": "abat_x", "description": "k"})

    with patch("tracker.tasks.worker.start"), patch("tracker.tasks.worker.stop"):
        app = create_app()
        with TestClient(app) as test_client:
            yield test_client


@pytest.fixture
def admin_cookies():
    from tracker.auth import create_access_token, TOKEN_NAME
    return {TOKEN_NAME: create_access_token({"sub": "boss"})}


def test_rename_carries_library_and_api_keys(client, admin_cookies, db):
    resp = client.put("/config/api/users/reader", cookies=admin_cookies,
                      json={"username": "newname"})
    assert resp.status_code == 200

    assert db["user_library"].count_documents({"username": "reader"}) == 0
    assert db["user_library"].count_documents({"username": "newname"}) == 1
    assert db["api_keys"].count_documents({"username": "newname"}) == 1


def test_delete_removes_library_and_corrects_user_count(client, admin_cookies, db):
    resp = client.delete("/config/api/users/reader", cookies=admin_cookies)
    assert resp.status_code == 200
    assert resp.json()["library_entries_removed"] == 1

    assert db["user_library"].count_documents({"username": "reader"}) == 0
    assert db["api_keys"].count_documents({"username": "reader"}) == 0
    assert db["series"].find_one({"_id": "S1"})["user_count"] == 0


def test_cannot_delete_the_last_admin(client, admin_cookies, db):
    """The old guard hard-coded the name "admin", so a deployment using a custom
    ADMIN_USERNAME could delete its only real admin."""
    assert db["users"].find_one({"username": "boss"})["role"] == "admin"
    resp = client.delete("/config/api/users/boss", cookies=admin_cookies)
    assert resp.status_code == 400
    assert db["users"].find_one({"username": "boss"}) is not None


def test_a_non_last_admin_can_still_be_deleted(client, admin_cookies, db):
    from tracker.security import get_password_hash
    db["users"].insert_one({"username": "boss2",
                            "password_hash": get_password_hash("pw"), "role": "admin"})
    assert client.delete("/config/api/users/boss2", cookies=admin_cookies).status_code == 200
    assert db["users"].find_one({"username": "boss2"}) is None


class TestFirstAdminProvisioning:
    """There must be no built-in admin/admin account."""

    def test_refuses_to_create_an_admin_without_credentials(self, db, monkeypatch):
        from tracker.settings import ensure_default_admin, MissingAdminCredentials

        db["users"].delete_many({})
        monkeypatch.delenv("ADMIN_USERNAME", raising=False)
        monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
        with pytest.raises(MissingAdminCredentials):
            ensure_default_admin()
        assert db["users"].count_documents({}) == 0

    def test_creates_the_admin_from_the_configured_credentials(self, db, monkeypatch):
        from tracker.settings import ensure_default_admin
        from tracker.security import verify_password

        db["users"].delete_many({})
        monkeypatch.setenv("ADMIN_USERNAME", "owner")
        monkeypatch.setenv("ADMIN_PASSWORD", "a-strong-password")
        ensure_default_admin()

        created = db["users"].find_one({"username": "owner"})
        assert created["role"] == "admin"
        assert verify_password("a-strong-password", created["password_hash"])
        assert db["users"].find_one({"username": "admin"}) is None

    def test_load_settings_has_no_admin_side_effect(self, db, monkeypatch):
        """load_settings runs on nearly every request; provisioning from there
        turned a configuration problem into a 500 on every page."""
        from tracker.settings import load_settings

        db["users"].delete_many({})
        monkeypatch.delenv("ADMIN_USERNAME", raising=False)
        monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
        load_settings()  # must not raise
        assert db["users"].count_documents({}) == 0
