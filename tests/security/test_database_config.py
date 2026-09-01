"""The database backend must never silently degrade to a throwaway store.

Falling back to an in-memory database on a connection error let the application
start, create a default admin, and accept writes that vanished on restart — with
no log line to say so.
"""
import pytest

from tracker.db import get_db


@pytest.fixture(autouse=True)
def clear_db_cache(monkeypatch):
    """Isolate the lru_cache, and leave a usable handle for later test modules."""
    get_db.cache_clear()
    yield
    get_db.cache_clear()
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.setenv("ALLOW_IN_MEMORY_DB", "1")


def test_unreachable_mongo_uri_raises_instead_of_falling_back(monkeypatch):
    # Port 1 is reserved and will refuse the connection.
    monkeypatch.setenv("MONGO_URI", "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=200")
    monkeypatch.setenv("ALLOW_IN_MEMORY_DB", "1")  # must not rescue a real URI
    with pytest.raises(Exception) as excinfo:
        get_db()
    assert "mongomock" not in type(excinfo.value).__module__


def test_missing_uri_without_opt_in_raises(monkeypatch):
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.delenv("ALLOW_IN_MEMORY_DB", raising=False)
    with pytest.raises(RuntimeError, match="MONGO_URI"):
        get_db()


def test_missing_uri_with_opt_in_uses_in_memory_backend(monkeypatch, caplog):
    monkeypatch.delenv("MONGO_URI", raising=False)
    monkeypatch.setenv("ALLOW_IN_MEMORY_DB", "1")
    with caplog.at_level("WARNING"):
        db = get_db()
    assert "mongomock" in type(db).__module__
    assert any("will NOT persist" in record.message for record in caplog.records)
